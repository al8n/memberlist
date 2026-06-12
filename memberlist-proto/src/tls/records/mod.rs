//! Severable rustls Sans-I/O record unit.
//!
//! Wraps a rustls `ClientConnection` / `ServerConnection` behind a byte-only
//! interface (`&[u8]` in / `&mut Vec<u8>` out): ciphertext from the transport
//! goes through [`TlsRecords::handle_transport_data`] +
//! [`TlsRecords::poll_transport_transmit`]; application plaintext goes through
//! [`TlsRecords::read_plaintext`] / [`TlsRecords::write_plaintext`]. rustls is
//! itself Sans-I/O — `read_tls` / `process_new_packets` / `write_tls` operate
//! on byte buffers — so the whole TLS record layer runs in the deterministic
//! core with no sockets, runtime, or wall-clock.
//!
//! `is_handshaking()` is the handshake-pump completion check; once it clears
//! the bridge mints the `Stream`. `send_close_notify` + `peer_has_closed`
//! anchor the graceful half-close.

use std::{
  io::{Read, Write},
  sync::Arc,
};

use rustls::{
  ClientConfig, ClientConnection, ServerConfig, ServerConnection, pki_types::ServerName,
};

/// Floor for the rustls send-buffer limit, so a small configured
/// `max_stream_frame_size` cannot starve the handshake flights of buffer space
/// (a TLS 1.3 handshake flight with a cert chain is a few KiB).
const MIN_TLS_SEND_BUFFER: usize = 64 * 1024;

/// The rustls send-buffer limit for a `max_frame`-sized reliable unit: the unit
/// plus headroom for TLS record framing/overhead, floored so the handshake
/// always fits. Saturating so a huge configured frame size cannot wrap.
fn sized_send_limit(max_frame: usize) -> usize {
  max_frame
    .saturating_add(max_frame / 16)
    .saturating_add(16 * 1024)
    .max(MIN_TLS_SEND_BUFFER)
}

/// One side of a TLS connection, behind a byte-only Sans-I/O interface.
/// Accessor-only; no `pub` fields. Built via the crate-internal `client` /
/// `server` constructors.
pub struct TlsRecords {
  conn: Conn,
  /// `peer_has_closed` latched once observed (`process_new_packets` reports it
  /// per-call; rustls only surfaces the close once, so we sticky it).
  peer_closed: bool,
  /// Cap on `pending` — the bound a slow-reading peer cannot grow past. Sized to
  /// one reliable unit (plus framing/TLS-record headroom) by
  /// [`Self::set_send_capacity`], so a single legitimate unit is always admitted
  /// while accumulation beyond it is refused.
  send_limit: usize,
  /// Outbound application plaintext staged for the next transmit, bounded by
  /// `send_limit`. This — not rustls's internal send buffers — is the bound
  /// authority: [`Self::write_plaintext`] stages here, and
  /// [`Self::poll_transport_transmit`] feeds it to rustls (only post-handshake,
  /// only what rustls accepts; the rest stays staged). Tracking our OWN
  /// plaintext, rather than a counter against rustls's opaque ciphertext queue,
  /// is what keeps the accounting honest across handshake flights and record
  /// overhead.
  pending: Vec<u8>,
  /// A graceful close was requested but `pending` was not yet fully fed to
  /// rustls. The `close_notify` alert is deferred until `pending` drains, so it
  /// can never overtake a still-staged frame (a short rustls accept would
  /// otherwise leave a suffix the alert would jump ahead of). Fired by
  /// [`Self::flush_close`] the moment `pending` empties.
  close_requested: bool,
}

/// Outcome of one [`TlsRecords::handle_transport_data`] intake step.
///
/// rustls bounds its decrypted-plaintext buffer at `DEFAULT_RECEIVED_PLAINTEXT_LIMIT`
/// (16 KiB; `set_buffer_limit` only raises the SEND side), so a single coalesced
/// transport read of a valid frame larger than that cannot be fed in one pass:
/// `read_tls` returns a "received plaintext buffer full" `Err` once the buffer
/// fills. The intake therefore reports progress so the caller can drain the
/// decrypted plaintext and re-feed the unconsumed tail.
pub(crate) enum Intake {
  /// All supplied ciphertext was consumed.
  Done,
  /// rustls's received-plaintext buffer filled mid-feed; the carried count is
  /// the number of ciphertext bytes consumed so far. The caller must drain
  /// plaintext (which frees the buffer) and re-feed the remaining tail.
  Pending(usize),
}

/// The two rustls connection roles. Newtype variants over the concrete rustls
/// types (the no-multi-field-tuple-variant rule: each carries exactly one).
enum Conn {
  Client(ClientConnection),
  Server(ServerConnection),
}

impl Conn {
  /// Mutable borrow of the shared `read_tls`/`write_tls`/`process_new_packets`/
  /// `reader`/`writer` surface (the `Deref` target `ConnectionCommon` of both
  /// roles exposes these; rustls 0.23.38 conn.rs:760/788/437/558/570).
  fn read_tls(&mut self, rd: &mut dyn Read) -> std::io::Result<usize> {
    match self {
      Conn::Client(c) => c.read_tls(rd),
      Conn::Server(c) => c.read_tls(rd),
    }
  }
  fn write_tls(&mut self, wr: &mut dyn Write) -> std::io::Result<usize> {
    match self {
      Conn::Client(c) => c.write_tls(wr),
      Conn::Server(c) => c.write_tls(wr),
    }
  }
  fn process_new_packets(&mut self) -> Result<rustls::IoState, rustls::Error> {
    match self {
      Conn::Client(c) => c.process_new_packets(),
      Conn::Server(c) => c.process_new_packets(),
    }
  }
  fn is_handshaking(&self) -> bool {
    match self {
      Conn::Client(c) => c.is_handshaking(),
      Conn::Server(c) => c.is_handshaking(),
    }
  }
  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    // Drain rustls's received-plaintext buffer fully. `Reader::read` returns
    // `Ok(0)` after a clean peer close_notify, `Err(WouldBlock)` when empty;
    // both terminate the drain. An `UnexpectedEof` (TCP EOF without
    // close_notify) is treated as "no more plaintext" here — truncation is
    // surfaced via the bridge's `read == 0` anchor, not as a TlsRecords error.
    let mut total = 0;
    let mut scratch = [0u8; 4096];
    loop {
      let reader = match &mut *self {
        Conn::Client(c) => c.reader(),
        Conn::Server(c) => c.reader(),
      };
      match read_once(reader, &mut scratch) {
        Some(n) if n > 0 => {
          out.extend_from_slice(&scratch[..n]);
          total += n;
        }
        _ => break,
      }
    }
    total
  }
  /// Hand staged plaintext to rustls, returning how many bytes it accepted. Uses
  /// `Writer::write` (short-write-tolerant), NOT `write_all`: rustls clamps the
  /// accepted count against its send-buffer limit (which also bounds the
  /// ciphertext queue still holding the handshake flight), so a short write is
  /// normal back-pressure — the caller leaves the unaccepted suffix staged. An
  /// in-memory `Writer::write` never errors; map the impossible `Err` to 0.
  fn write(&mut self, plaintext: &[u8]) -> usize {
    match self {
      Conn::Client(c) => c.writer().write(plaintext).unwrap_or(0),
      Conn::Server(c) => c.writer().write(plaintext).unwrap_or(0),
    }
  }
  fn set_buffer_limit(&mut self, limit: usize) {
    match self {
      Conn::Client(c) => c.set_buffer_limit(Some(limit)),
      Conn::Server(c) => c.set_buffer_limit(Some(limit)),
    }
  }
  fn send_close_notify(&mut self) {
    match self {
      Conn::Client(c) => c.send_close_notify(),
      Conn::Server(c) => c.send_close_notify(),
    }
  }
}

/// Drain one `Reader::read`; `Some(n)` on bytes, `None` on WouldBlock / Ok(0) /
/// any error (caller stops). Split out so the borrow on `conn` is released
/// between iterations.
///
/// Collapsing `Ok(0)` (clean peer `close_notify`), `WouldBlock` (buffer
/// empty), and `Err(_)` (`UnexpectedEof` — TCP half-close without
/// `close_notify`) into `None` is safe: the bridge's `read == 0` / EOF anchor
/// is what surfaces closure to the application; `TlsRecords` does not need to
/// distinguish the reason, only to stop the drain.
fn read_once(mut reader: impl Read, scratch: &mut [u8]) -> Option<usize> {
  match reader.read(scratch) {
    Ok(0) | Err(_) => None,
    Ok(n) => Some(n),
  }
}

impl TlsRecords {
  /// Client role: drives the handshake against `name` using `client`.
  pub(crate) fn client(
    client: Arc<ClientConfig>,
    name: ServerName<'static>,
  ) -> Result<Self, rustls::Error> {
    let mut c = ClientConnection::new(client, name)?;
    // rustls's own send-buffer limit is only a LOOSE BACKSTOP at 2x our staging
    // cap: `pending <= send_limit` plaintext encrypts to well under 2x, so it
    // never binds on the normal path. The real bound is `pending`/`send_limit`.
    // `StreamBridge::new` re-sizes both via `set_send_capacity` right after
    // construction; this default keeps a direct caller bounded too.
    let limit = sized_send_limit(crate::config::DEFAULT_MAX_STREAM_FRAME_SIZE);
    c.set_buffer_limit(Some(limit.saturating_mul(2)));
    Ok(Self {
      conn: Conn::Client(c),
      peer_closed: false,
      send_limit: limit,
      pending: Vec::new(),
      close_requested: false,
    })
  }

  /// Server role: verifies the client cert via the verifier the operator
  /// installed on `server`.
  pub(crate) fn server(server: Arc<ServerConfig>) -> Result<Self, rustls::Error> {
    let mut s = ServerConnection::new(server)?;
    // Loose 2x backstop; same rationale as the client constructor.
    let limit = sized_send_limit(crate::config::DEFAULT_MAX_STREAM_FRAME_SIZE);
    s.set_buffer_limit(Some(limit.saturating_mul(2)));
    Ok(Self {
      conn: Conn::Server(s),
      peer_closed: false,
      send_limit: limit,
      pending: Vec::new(),
      close_requested: false,
    })
  }

  /// Feed ciphertext the driver read from the transport, in bounded steps.
  ///
  /// `read_tls` consumes from the `&mut dyn Read` and decrypts into rustls's
  /// received-plaintext buffer; `process_new_packets` then advances the
  /// handshake / surfaces records. Two distinct failure modes are kept apart:
  ///
  /// * A `read_tls` `Err` here is the "received plaintext buffer full"
  ///   backpressure signal: rustls runs that buffer-full check at the top of
  ///   `read_tls` BEFORE touching `rd` (so zero ciphertext is consumed on that
  ///   call). rustls's other `read_tls` error — the deframer "message buffer
  ///   full" (`InvalidData`) for an oversized/incomplete record — cannot be
  ///   reached through this method: `process_new_packets` runs after EVERY
  ///   `read_tls` (which reads at most one ~4 KiB `READ_SIZE` chunk), so it
  ///   consumes a complete record or rejects a malformed/oversized one
  ///   (`DecryptError` / `MessageTooLarge` / `HandshakePayloadTooLarge`) before
  ///   the deframer buffer can fill incomplete to its limit. So a `read_tls`
  ///   `Err` is treated as backpressure: stop and return [`Intake::Pending`]
  ///   with the bytes consumed so far; the caller drains plaintext and re-feeds
  ///   the tail. (This rests on the per-chunk `process_new_packets` interleave
  ///   below; a future change that fed `read_tls` without it would make the
  ///   deframer error reachable and have to split it out as a terminal failure
  ///   — though even then an unexpected non-backpressure stall is bounded by
  ///   the exchange/handshake deadline.)
  /// * A `process_new_packets` `Err` is a REAL protocol failure (mTLS reject,
  ///   decrypt error, malformed record). It is returned as `Err` so the bridge
  ///   tears the exchange down — never confused with backpressure.
  ///
  /// Returns [`Intake::Done`] once all supplied ciphertext is consumed.
  pub(crate) fn handle_transport_data(
    &mut self,
    ciphertext: &[u8],
  ) -> Result<Intake, rustls::Error> {
    // `<&[u8] as Read>::read` advances the slice it borrows; track consumption
    // by re-slicing from a running offset so a backpressure stop can report the
    // exact number of ciphertext bytes taken.
    let mut consumed = 0usize;
    let mut fed = false;
    while consumed < ciphertext.len() {
      let mut rest = &ciphertext[consumed..];
      let n = match self.conn.read_tls(&mut rest) {
        Ok(n) => n,
        // The received-plaintext buffer is full (rustls checks it at the top
        // of `read_tls`, before reading `rd`). The deframer-level `read_tls`
        // error is pre-empted by the per-chunk `process_new_packets` below, so
        // this is the only reachable `read_tls` `Err` (see the method doc).
        // Stop with the progress so far; the caller drains and re-feeds.
        Err(_) => return Ok(Intake::Pending(consumed)),
      };
      if n == 0 {
        break;
      }
      consumed += n;
      let io = self.conn.process_new_packets()?;
      if io.peer_has_closed() {
        self.peer_closed = true;
      }
      fed = true;
    }
    // When the input was empty (zero-length transport read / EOF poll) the loop
    // above did not call `process_new_packets`; call it now so any already-
    // buffered records are advanced. Skip it when the loop ran — it was already
    // called after every fed chunk.
    if !fed {
      let io = self.conn.process_new_packets()?;
      if io.peer_has_closed() {
        self.peer_closed = true;
      }
    }
    Ok(Intake::Done)
  }

  /// Hand staged plaintext to rustls, ready to be encrypted by `write_tls`.
  /// Only ONCE the handshake has completed: mid-handshake rustls cannot encrypt
  /// app data (no traffic keys yet), so the eagerly-staged cluster label must
  /// stay in `pending` — where `pending.len()` still accounts for it — until the
  /// handshake clears. Feed only what rustls accepts and keep the unaccepted
  /// suffix staged; a short accept is normal back-pressure (rustls clamps against
  /// its own send-buffer limit), never a dropped frame.
  fn flush_pending(&mut self) {
    if !self.conn.is_handshaking() && !self.pending.is_empty() {
      let accepted = self.conn.write(&self.pending);
      self.pending.drain(..accepted);
    }
  }

  /// Flush staged plaintext, then fire a deferred `close_notify` ONLY once
  /// `pending` is fully drained — so the close alert can never overtake a
  /// staged frame even when [`Self::flush_pending`] short-accepts. While
  /// `pending` is non-empty (mid-handshake, or rustls back-pressured) the close
  /// stays deferred and re-fires on the next transmit.
  fn flush_close(&mut self) {
    self.flush_pending();
    if self.close_requested && self.pending.is_empty() {
      self.conn.send_close_notify();
      self.close_requested = false;
    }
  }

  /// Drain ciphertext rustls wants to write (handshake flights, app records,
  /// close_notify) into `out`. Returns the number of bytes appended.
  ///
  /// Drains `pending` to EMPTY before returning (post-handshake): each
  /// `write_tls` pass frees rustls's send-buffer room, so a `flush_close` that
  /// short-accepted re-feeds the suffix on the next pass. This leaves NO hidden
  /// outbound — staged plaintext or a deferred `close_notify` — after the call,
  /// so the coordinator's teardown gating (which sees only `out`, not `pending`)
  /// can never let a `Shutdown` overtake a staged suffix. While handshaking,
  /// `flush_close` makes no progress on `pending` (no traffic keys yet), so the
  /// loop drains the handshake flight once and stops, leaving `pending` staged.
  pub(crate) fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    let before = out.len();
    // `write_tls` writes one record-layer message per call into the
    // `&mut dyn Write`; the inner loop drains rustls fully. Writing into a
    // `Vec<u8>` is infallible (the only `Err` path is a rustls-internal
    // consistency guard signalling a bug, never a normal runtime condition).
    loop {
      let pending_before = self.pending.len();
      self.flush_close();
      let fed = self.pending.len() < pending_before;
      let mut drained = false;
      while self
        .conn
        .write_tls(out)
        .expect("write_tls to an in-memory Vec<u8> is infallible")
        > 0
      {
        drained = true;
      }
      // Re-feed while `pending` remains AND this pass made progress — either
      // `flush_close` accepted bytes, OR `write_tls` freed rustls room so the
      // NEXT feed can accept (the case where rustls was full at entry: the feed
      // takes 0 but the drain unblocks it). Break when `pending` is empty, or no
      // progress at all (mid-handshake gate, or genuinely stuck): never spin.
      if self.pending.is_empty() || (!fed && !drained) {
        break;
      }
    }
    out.len() - before
  }

  /// `true` while the TLS handshake has not completed. The handshake pump
  /// shuttles ciphertext until this clears, then the bridge mints the `Stream`.
  pub(crate) fn is_handshaking(&self) -> bool {
    self.conn.is_handshaking()
  }

  /// Drain decrypted application plaintext into `out`. Returns the byte count.
  pub(crate) fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    self.conn.read_plaintext(out)
  }

  /// Stage application plaintext for the next transmit (fed to rustls + drained
  /// as ciphertext by [`Self::poll_transport_transmit`]). Returns `false` if the
  /// unit would overflow `send_limit`.
  ///
  /// Staged WHOLE into `pending`: a unit that would push `pending` past the bound
  /// (a second unit offered before the first drained) is rejected entirely —
  /// never staging a partial frame — so the caller fails the exchange
  /// synchronously. For memberlist's one-unit-per-exchange the bound always
  /// admits the unit, so this never rejects; it is the by-construction bound the
  /// FSM cadence otherwise relied on implicitly.
  pub(crate) fn write_plaintext(&mut self, plaintext: &[u8]) -> bool {
    if self.pending.len().saturating_add(plaintext.len()) > self.send_limit {
      return false;
    }
    self.pending.extend_from_slice(plaintext);
    true
  }

  /// Drop staged outbound plaintext on a failure transition, so a failed
  /// exchange cannot leak a partial reply onto the wire, and cancel any deferred
  /// graceful close (a failed exchange resets rather than half-closes).
  pub(crate) fn clear_outbound(&mut self) {
    self.pending.clear();
    self.close_requested = false;
  }

  /// Bound the staging buffer to one `max_frame`-sized reliable unit. Called once
  /// by `StreamBridge::new` before any plaintext is staged, replacing the
  /// generous construction-time default with the configured frame size; the
  /// rustls backstop tracks at 2x.
  pub(crate) fn set_send_capacity(&mut self, max_frame: usize) {
    let limit = sized_send_limit(max_frame);
    self.send_limit = limit;
    self.conn.set_buffer_limit(limit.saturating_mul(2));
  }

  /// Begin a graceful close: queue a `close_notify` alert (drained via
  /// [`Self::poll_transport_transmit`]).
  pub(crate) fn send_close_notify(&mut self) {
    // Defer the close alert until all staged plaintext has reached rustls, so
    // close_notify never overtakes a frame. The bridge stages a unit and
    // requests the close in the same tick (before the transmit drain), and a
    // short rustls accept can leave a suffix staged — so `flush_close` only
    // queues the alert once `pending` is empty, here or on a later transmit.
    self.close_requested = true;
    self.flush_close();
  }

  /// `true` once the peer has sent us a `close_notify` (latched).
  pub(crate) fn peer_has_closed(&self) -> bool {
    self.peer_closed
  }
}

#[cfg(test)]
mod tests;
