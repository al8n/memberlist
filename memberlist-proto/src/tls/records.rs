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
mod tests {
  use super::*;
  use rustls::pki_types::{CertificateDer, PrivateKeyDer};

  fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
  }

  /// Self-signed cert + accept-any verifier — sim/test only. Mirrors the
  /// quic crypto tests; behavioural determinism is the virtual clock, not any
  /// test-only crypto hook.
  #[derive(Debug)]
  struct AnyServer(Arc<rustls::crypto::CryptoProvider>);
  impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
      &self,
      _e: &CertificateDer<'_>,
      _i: &[CertificateDer<'_>],
      _n: &ServerName<'_>,
      _o: &[u8],
      _t: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
    (chain, key)
  }

  fn pair() -> (TlsRecords, TlsRecords) {
    let p = provider();
    let (chain, key) = self_signed();
    let server = rustls::ServerConfig::builder_with_provider(p.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .with_no_client_auth()
      .with_single_cert(chain, key)
      .unwrap();
    let client = rustls::ClientConfig::builder_with_provider(p.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AnyServer(p)))
      .with_no_client_auth();
    let c =
      TlsRecords::client(Arc::new(client), ServerName::try_from("localhost").unwrap()).unwrap();
    let s = TlsRecords::server(Arc::new(server)).unwrap();
    (c, s)
  }

  /// Shuttle ciphertext between two `TlsRecords` until neither has anything
  /// buffered to write — the in-memory equivalent of the bridge's handshake
  /// pump. Bounded so a stuck handshake fails the test rather than hanging.
  fn pump(a: &mut TlsRecords, b: &mut TlsRecords) {
    for _ in 0..64 {
      let mut a_out = Vec::new();
      a.poll_transport_transmit(&mut a_out);
      if !a_out.is_empty() {
        b.handle_transport_data(&a_out).unwrap();
      }
      let mut b_out = Vec::new();
      b.poll_transport_transmit(&mut b_out);
      if !b_out.is_empty() {
        a.handle_transport_data(&b_out).unwrap();
      }
      if a_out.is_empty() && b_out.is_empty() {
        break;
      }
    }
  }

  /// A single coalesced ciphertext for a plaintext payload LARGER than rustls's
  /// 16 KiB received-plaintext limit, delivered in ONE `handle_transport_data`
  /// call, must NOT panic: the intake feeds bounded steps and reports
  /// backpressure (`Intake::Pending`) instead of treating `read_tls` as
  /// infallible. The caller drains plaintext between steps and re-feeds the
  /// unconsumed tail until `Intake::Done`; the full payload decrypts intact.
  #[test]
  fn coalesced_ciphertext_over_received_limit_does_not_panic() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking() && !server.is_handshaking());

    // 48 KiB > the 16 KiB DEFAULT_RECEIVED_PLAINTEXT_LIMIT, < the 64 MiB
    // max_stream_frame_size: a VALID large frame that overflows the received
    // buffer if fed without interleaved draining.
    let payload: Vec<u8> = (0..48 * 1024).map(|i| (i % 251) as u8).collect();
    client.write_plaintext(&payload);

    // Drain ALL of the client's ciphertext into ONE buffer (a single coalesced
    // TCP read on the peer).
    let mut ciphertext = Vec::new();
    client.poll_transport_transmit(&mut ciphertext);
    assert!(
      ciphertext.len() > 16 * 1024,
      "the coalesced ciphertext exceeds the received-plaintext limit"
    );

    // Feed the whole buffer in bounded steps, draining plaintext between steps.
    // This is exactly what the bridge does; here it stands in for the bridge so
    // the records-layer contract is exercised directly.
    let mut got = Vec::new();
    let mut rest = ciphertext.as_slice();
    loop {
      match server
        .handle_transport_data(rest)
        .expect("a coalesced in-memory feed has no real-I/O failure")
      {
        Intake::Done => {
          server.read_plaintext(&mut got);
          break;
        }
        Intake::Pending(consumed) => {
          assert!(consumed > 0, "each backpressure step consumes ciphertext");
          rest = &rest[consumed..];
          server.read_plaintext(&mut got);
        }
      }
    }
    assert_eq!(got, payload, "the full >16 KiB payload decrypted intact");
  }

  #[test]
  fn handshake_completes_then_plaintext_round_trips_then_close_notify() {
    let (mut client, mut server) = pair();
    assert!(client.is_handshaking() && server.is_handshaking());

    pump(&mut client, &mut server);
    assert!(!client.is_handshaking(), "client handshake completed");
    assert!(!server.is_handshaking(), "server handshake completed");

    // App-data round trip: client → server.
    client.write_plaintext(b"ping-frame");
    pump(&mut client, &mut server);
    let mut got = Vec::new();
    server.read_plaintext(&mut got);
    assert_eq!(&got, b"ping-frame", "server decrypted the client plaintext");

    // server → client.
    server.write_plaintext(b"ack-frame");
    pump(&mut client, &mut server);
    let mut got = Vec::new();
    client.read_plaintext(&mut got);
    assert_eq!(&got, b"ack-frame");

    // Graceful close: client close_notify → server observes peer_has_closed.
    assert!(!server.peer_has_closed());
    client.send_close_notify();
    pump(&mut client, &mut server);
    assert!(
      server.peer_has_closed(),
      "server saw the client's close_notify"
    );
  }

  /// An empty-ciphertext `handle_transport_data(&[])` is the EOF-poll case the
  /// bridge issues on a transport `read == 0`: the feed loop never runs (no
  /// bytes to consume), so the `!fed` branch calls `process_new_packets` once to
  /// advance any already-buffered records and refresh the close latch. Post
  /// handshake-and-close, an empty feed reports `Done` and latches the peer
  /// close exactly once.
  #[test]
  fn empty_feed_processes_buffered_records_and_refreshes_close_latch() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking() && !server.is_handshaking());

    // An empty feed on a live (un-closed) connection is a clean no-op `Done`
    // with no close observed.
    assert!(matches!(
      server
        .handle_transport_data(&[])
        .expect("empty feed is Done"),
      Intake::Done
    ));
    assert!(!server.peer_has_closed(), "no close yet");

    // Deliver the client's close_notify ciphertext, THEN an empty feed: the
    // empty-feed `!fed` branch re-runs `process_new_packets`, which keeps the
    // latched close sticky.
    client.send_close_notify();
    let mut alert = Vec::new();
    client.poll_transport_transmit(&mut alert);
    assert!(!alert.is_empty(), "close_notify produced ciphertext");
    server
      .handle_transport_data(&alert)
      .expect("server consumes the close_notify");
    assert!(server.peer_has_closed(), "close latched");
    // A subsequent empty feed (the bridge's repeated EOF poll) is still `Done`
    // and the latch stays set.
    assert!(matches!(
      server
        .handle_transport_data(&[])
        .expect("empty feed is Done"),
      Intake::Done
    ));
    assert!(server.peer_has_closed(), "close latch stays sticky");
  }

  /// A unit sized to the configured `max_stream_frame_size` is always admitted
  /// (no false-close), and draining the ciphertext resets the fit-check
  /// accounting so the next unit is measured from empty.
  #[test]
  fn full_unit_admitted_and_drain_resets_accounting() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking());

    let frame = 200 * 1024;
    client.set_send_capacity(frame);

    // A full-frame-sized unit fits the bound (sized to the frame + headroom).
    assert!(
      client.write_plaintext(&vec![7u8; frame]),
      "a full-frame unit is admitted, never false-closed"
    );
    assert_eq!(client.pending.len(), frame);

    // Draining the ciphertext empties the send buffer and resets the accounting.
    let mut out = Vec::new();
    client.poll_transport_transmit(&mut out);
    assert!(!out.is_empty(), "the unit's ciphertext was produced");
    assert_eq!(
      client.pending.len(),
      0,
      "a post-handshake full drain resets the fit-check accounting"
    );

    // After the drain, the next full unit is admitted again.
    assert!(client.write_plaintext(&vec![8u8; frame]));
    assert_eq!(client.pending.len(), frame);
  }

  /// A second unit offered before the first drains, overflowing the bound, is
  /// rejected WHOLE (`write_plaintext` returns `false`, never buffering a
  /// partial frame) so the caller fails the exchange synchronously — the
  /// by-construction backstop for the one-unit-per-exchange cadence.
  #[test]
  fn send_capacity_overflow_returns_false_not_partial() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking());

    // Floor-sized bound (a tiny frame size floors at MIN_TLS_SEND_BUFFER).
    client.set_send_capacity(8 * 1024);
    let limit = client.send_limit;
    assert_eq!(
      limit, MIN_TLS_SEND_BUFFER,
      "a sub-floor frame size uses the floor"
    );

    // One unit well under the bound is admitted.
    let unit1 = vec![0u8; 30 * 1024];
    assert!(client.write_plaintext(&unit1));
    assert_eq!(client.pending.len(), unit1.len());

    // A second unit that would exceed the bound, before any drain, is rejected.
    let unit2 = vec![1u8; 40 * 1024];
    assert!(
      unit1.len() + unit2.len() > limit,
      "the two units together exceed the bound"
    );
    assert!(
      !client.write_plaintext(&unit2),
      "the overflowing unit is rejected so the caller can fail the exchange"
    );
    assert_eq!(
      client.pending.len(),
      unit1.len(),
      "the rejected unit was NOT buffered — no partial frame reaches the wire"
    );
  }

  /// The eagerly-queued cluster label is written via `write_plaintext` BEFORE
  /// the handshake completes. A `poll_transport_transmit` DURING the handshake
  /// (which drains handshake flights but cannot yet send app plaintext) must NOT
  /// clear `buffered`, or a later near-limit write would over-fill the real
  /// rustls buffer. The accounting stays until a post-handshake drain.
  #[test]
  fn pre_handshake_plaintext_stays_accounted_during_handshake() {
    let (mut client, mut server) = pair();
    assert!(client.is_handshaking());
    client.set_send_capacity(64 * 1024);

    // Queue app plaintext (stand-in for the eagerly-written label) before the
    // handshake clears.
    let label = vec![9u8; 4 * 1024];
    assert!(client.write_plaintext(&label));
    assert_eq!(client.pending.len(), label.len());

    // A mid-handshake drain shuttles the ClientHello flight but CANNOT send the
    // app plaintext yet (it stays in rustls's send buffer), so the accounting
    // must survive — clearing it here would under-count the still-buffered label.
    let mut out = Vec::new();
    client.poll_transport_transmit(&mut out);
    assert!(
      client.is_handshaking(),
      "still handshaking after ClientHello"
    );
    assert!(!out.is_empty(), "the ClientHello flight was produced");
    assert_eq!(
      client.pending.len(),
      label.len(),
      "pre-handshake plaintext stays counted while handshaking"
    );

    // Deliver the just-drained ClientHello so the handshake can proceed, then
    // complete it; the label is sent and a post-handshake drain clears the count.
    server
      .handle_transport_data(&out)
      .expect("server consumes the ClientHello");
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking(), "handshake completed");
    assert_eq!(
      client.pending.len(),
      0,
      "the post-handshake drain clears the accounting"
    );
  }

  /// `write_plaintext` only STAGES into `pending`; it never feeds rustls. So it
  /// cannot short-write or panic regardless of what rustls holds in its send
  /// buffers — the failure mode of feeding `write_all` straight into rustls
  /// while its ciphertext queue still held an undrained handshake flight. The
  /// staged unit is fed to rustls and drained by `poll_transport_transmit`,
  /// round-tripping intact.
  #[test]
  fn write_plaintext_only_stages_and_round_trips_via_transmit() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking());

    assert!(client.write_plaintext(b"a reliable unit"));
    assert_eq!(
      client.pending.len(),
      15,
      "the unit is staged, not yet in rustls"
    );

    let mut out = Vec::new();
    client.poll_transport_transmit(&mut out);
    assert!(
      client.pending.is_empty(),
      "transmit feeds the staged unit to rustls"
    );
    assert!(!out.is_empty(), "the unit's ciphertext was produced");
    server
      .handle_transport_data(&out)
      .expect("server consumes the frame");
    let mut got = Vec::new();
    server.read_plaintext(&mut got);
    assert_eq!(
      &got, b"a reliable unit",
      "the staged unit round-trips intact"
    );
  }

  /// A close requested while staged plaintext cannot yet reach rustls (here the
  /// handshake gate keeps it in `pending`) DEFERS the `close_notify` rather than
  /// queueing the alert ahead of the still-staged frame.
  #[test]
  fn close_notify_defers_while_plaintext_is_still_staged() {
    let (mut client, _server) = pair();
    assert!(client.is_handshaking());
    client.set_send_capacity(64 * 1024);

    assert!(client.write_plaintext(b"staged"));
    client.send_close_notify();
    assert!(
      client.close_requested,
      "the close defers while the staged frame cannot yet reach rustls"
    );
    assert_eq!(
      client.pending.len(),
      6,
      "the frame stays staged ahead of the close, never dropped"
    );
  }

  /// One transmit drains the staged frame AND the deferred close — leaving NO
  /// hidden outbound (`pending` empty, no deferred close) — so the coordinator's
  /// teardown gating (which sees only the drained bytes) can never let a
  /// shutdown overtake a staged suffix.
  #[test]
  fn frame_and_deferred_close_both_drain_in_one_transmit() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking());
    client.set_send_capacity(64 * 1024);

    assert!(client.write_plaintext(b"final frame"));
    client.send_close_notify();

    let mut out = Vec::new();
    client.poll_transport_transmit(&mut out);
    assert!(
      client.pending.is_empty(),
      "no staged plaintext is hidden after the transmit"
    );
    assert!(
      !client.close_requested,
      "no deferred close is hidden after the transmit"
    );

    server
      .handle_transport_data(&out)
      .expect("server consumes the frame + close");
    let mut got = Vec::new();
    server.read_plaintext(&mut got);
    assert_eq!(
      &got, b"final frame",
      "the frame arrived intact, before the close"
    );
    assert!(
      server.peer_has_closed(),
      "and the close_notify followed it in the same transmit"
    );
  }

  /// A pathologically large `max_stream_frame_size` must not overflow the 2x
  /// rustls backstop multiply (saturating), and a normal unit still stages.
  #[test]
  fn send_capacity_saturates_for_a_huge_frame_size() {
    let (mut client, _server) = pair();
    client.set_send_capacity(usize::MAX);
    assert!(client.send_limit > 0);
    assert!(
      client.write_plaintext(b"unit"),
      "a normal unit stages without panic under a saturated backstop"
    );
  }

  /// The drain loop must fully empty `pending` in ONE poll even when rustls is
  /// already at its send-buffer limit at entry (the first feed takes 0, but the
  /// `write_tls` drain frees room and the loop must re-feed) — otherwise a suffix
  /// stays hidden from the coordinator. A small rustls limit + a larger staging
  /// cap + a pre-flush that fills rustls reproduce that entry state.
  #[test]
  fn drain_loop_re_feeds_after_freeing_a_full_rustls_buffer() {
    let (mut client, mut server) = pair();
    pump(&mut client, &mut server);
    assert!(!client.is_handshaking());

    // Decouple the staging cap from a deliberately tiny rustls limit so a unit
    // can only reach rustls a little per drain cycle.
    client.send_limit = 8 * 1024;
    client.conn.set_buffer_limit(256);

    let unit = vec![5u8; 2000];
    assert!(client.write_plaintext(&unit));
    // Pre-fill rustls to its limit WITHOUT draining: now a poll enters with
    // rustls full (the first feed accepts 0) and `pending` still non-empty.
    client.flush_pending();
    assert!(!client.pending.is_empty(), "rustls accepted only a prefix");

    let mut out = Vec::new();
    client.poll_transport_transmit(&mut out);
    assert!(
      client.pending.is_empty(),
      "the loop re-fed after each drain freed rustls room, fully emptying pending"
    );

    server
      .handle_transport_data(&out)
      .expect("server consumes the streamed unit");
    let mut got = Vec::new();
    server.read_plaintext(&mut got);
    assert_eq!(
      got, unit,
      "the unit streamed across constrained writes, reassembled intact"
    );
  }
}
