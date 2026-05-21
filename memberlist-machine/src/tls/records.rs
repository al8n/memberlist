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

/// One side of a TLS connection, behind a byte-only Sans-I/O interface.
/// Accessor-only; no `pub` fields. Built via [`TlsRecords::client`] /
/// [`TlsRecords::server`].
pub(crate) struct TlsRecords {
  conn: Conn,
  /// `peer_has_closed` latched once observed (`process_new_packets` reports it
  /// per-call; rustls only surfaces the close once, so we sticky it).
  peer_closed: bool,
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
  fn write_plaintext(&mut self, plaintext: &[u8]) {
    // `Writer::write_all` buffers into rustls's send queue. With the buffer
    // limit set to `None` (unbounded) in the constructors, there is no
    // backpressure: the bridge writes at most one `max_stream_frame_size`
    // frame per exchange, so memory stays bounded by the FSM cap, not by
    // rustls's own DEFAULT_BUFFER_LIMIT. `write_all` loops internally and
    // returns `Err` only if the underlying `Write::write` returns 0, which
    // cannot happen against an unbounded buffer.
    match self {
      Conn::Client(c) => c
        .writer()
        .write_all(plaintext)
        .expect("rustls Writer::write_all to an unbounded buffer is infallible"),
      Conn::Server(c) => c
        .writer()
        .write_all(plaintext)
        .expect("rustls Writer::write_all to an unbounded buffer is infallible"),
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
    // Unbounded plaintext buffer: the bridge writes at most one
    // `max_stream_frame_size` frame per exchange, so memory is bounded by the
    // FSM cap — rustls's DEFAULT_BUFFER_LIMIT would cause silent short-writes.
    c.set_buffer_limit(None);
    Ok(Self {
      conn: Conn::Client(c),
      peer_closed: false,
    })
  }

  /// Server role: verifies the client cert via the verifier the operator
  /// installed on `server`.
  pub(crate) fn server(server: Arc<ServerConfig>) -> Result<Self, rustls::Error> {
    let mut s = ServerConnection::new(server)?;
    // Unbounded plaintext buffer: same rationale as the client constructor.
    s.set_buffer_limit(None);
    Ok(Self {
      conn: Conn::Server(s),
      peer_closed: false,
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

  /// Drain ciphertext rustls wants to write (handshake flights, app records,
  /// close_notify) into `out`. Returns the number of bytes appended.
  pub(crate) fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    let before = out.len();
    // `write_tls` writes one record-layer message per call into the
    // `&mut dyn Write`; loop until rustls has nothing more buffered.
    // Writing into a `Vec<u8>` is infallible (Vec writes do not fail; the
    // only `Err` path is a rustls-internal consistency guard that signals a
    // bug, never a normal runtime condition).
    while self
      .conn
      .write_tls(out)
      .expect("write_tls to an in-memory Vec<u8> is infallible")
      > 0
    {}
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

  /// Queue application plaintext to be encrypted; drained as ciphertext via
  /// [`Self::poll_transport_transmit`].
  pub(crate) fn write_plaintext(&mut self, plaintext: &[u8]) {
    self.conn.write_plaintext(plaintext);
  }

  /// Begin a graceful close: queue a `close_notify` alert (drained via
  /// [`Self::poll_transport_transmit`]).
  pub(crate) fn send_close_notify(&mut self) {
    self.conn.send_close_notify();
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
}
