use super::*;
use rustls::{
  client::danger::{HandshakeSignatureValid, ServerCertVerified},
  crypto::CryptoProvider,
  pki_types::{CertificateDer, PrivateKeyDer},
  version::TLS13,
};

fn provider() -> Arc<CryptoProvider> {
  Arc::new(rustls::crypto::ring::default_provider())
}

/// Self-signed cert + accept-any verifier — sim/test only. Mirrors the
/// quic crypto tests; behavioural determinism is the virtual clock, not any
/// test-only crypto hook.
#[derive(Debug)]
struct AnyServer(Arc<CryptoProvider>);
impl rustls::client::danger::ServerCertVerifier for AnyServer {
  fn verify_server_cert(
    &self,
    _e: &CertificateDer<'_>,
    _i: &[CertificateDer<'_>],
    _n: &ServerName<'_>,
    _o: &[u8],
    _t: rustls::pki_types::UnixTime,
  ) -> Result<ServerCertVerified, rustls::Error> {
    Ok(ServerCertVerified::assertion())
  }
  fn verify_tls12_signature(
    &self,
    _m: &[u8],
    _c: &CertificateDer<'_>,
    _d: &rustls::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    Ok(HandshakeSignatureValid::assertion())
  }
  fn verify_tls13_signature(
    &self,
    _m: &[u8],
    _c: &CertificateDer<'_>,
    _d: &rustls::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    Ok(HandshakeSignatureValid::assertion())
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
    .with_protocol_versions(&[&TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
  let client = rustls::ClientConfig::builder_with_provider(p.clone())
    .with_protocol_versions(&[&TLS13])
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AnyServer(p)))
    .with_no_client_auth();
  let c = TlsRecords::client(Arc::new(client), ServerName::try_from("localhost").unwrap()).unwrap();
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
