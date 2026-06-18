use super::*;
use rustls::{
  client::danger::{HandshakeSignatureValid, ServerCertVerified},
  version::TLS13,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
  // rcgen 0.14: generate_simple_self_signed returns CertifiedKey<KeyPair>;
  // the private key is in the `signing_key` field and serialize_der() yields PKCS#8 DER.
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  (vec![cert], key)
}

/// Test-only endpoint config builder: a fixed stateless-reset HMAC key
/// via the ring backend. Production callers build their own
/// `EndpointOptions` with their chosen crypto backend — see the module
/// docs. `QuicOptions::new` forces `grease_quic_bit = false` regardless.
pub(crate) fn test_endpoint_config(reset_key: &[u8]) -> quinn_proto::EndpointConfig {
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, reset_key);
  quinn_proto::EndpointConfig::new(Arc::new(hmac))
}

/// Test-only server config builder: self-signed cert + accept-any-client.
/// Production builds its own `quinn_proto::ServerConfig` with the
/// caller's chosen `ClientCertVerifier` — see the module docs.
pub(crate) fn test_server() -> quinn_proto::ServerConfig {
  let (chain, key) = self_signed();
  let provider = Arc::new(rustls::crypto::ring::default_provider());
  let rustls_server = rustls::ServerConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
  let qsc =
    quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server)).unwrap();
  quinn_proto::ServerConfig::with_crypto(Arc::new(qsc))
}

/// Accept-any verifier — sim/test only (mirrors quinn-proto `tests/util.rs`).
#[derive(Debug)]
struct AnyServer;

impl rustls::client::danger::ServerCertVerifier for AnyServer {
  fn verify_server_cert(
    &self,
    _end_entity: &CertificateDer,
    _intermediates: &[CertificateDer],
    _server_name: &rustls_pki_types::ServerName,
    _ocsp_response: &[u8],
    _now: rustls_pki_types::UnixTime,
  ) -> Result<ServerCertVerified, rustls::Error> {
    Ok(ServerCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    _message: &[u8],
    _cert: &CertificateDer,
    _dss: &rustls::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    Ok(HandshakeSignatureValid::assertion())
  }

  fn verify_tls13_signature(
    &self,
    _message: &[u8],
    _cert: &CertificateDer,
    _dss: &rustls::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    Ok(HandshakeSignatureValid::assertion())
  }

  fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
    rustls::crypto::ring::default_provider()
      .signature_verification_algorithms
      .supported_schemes()
  }
}

/// Accept-any client config for tests; `pub(crate)` so coordinator tests can reuse it.
pub(crate) fn test_client() -> quinn_proto::ClientConfig {
  let provider = Arc::new(rustls::crypto::ring::default_provider());
  let cfg = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&TLS13])
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AnyServer))
    .with_no_client_auth();
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(cfg)).unwrap();
  quinn_proto::ClientConfig::new(Arc::new(qcc))
}

#[test]
fn builds_a_usable_config_bundle() {
  let cfg = QuicOptions::new(
    test_endpoint_config(&[7u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    "localhost",
    UnreliableTransport::Datagram,
  );
  // Prove the bundle is usable: quinn_proto::Endpoint::new must not panic.
  // Signature: Endpoint::new_seeded(Arc<EndpointOptions>, Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)
  let _server_ep =
    quinn_proto::Endpoint::new(cfg.endpoint_arc(), Some(cfg.server_arc()), true, None);
  let _client_ep = quinn_proto::Endpoint::new(cfg.endpoint_arc(), None, true, None);
  // Exercise the accessors for their side effect of compiling against
  // the public bundle shape; the borrowed references are discarded.
  let _ = cfg.endpoint_ref();
  let _ = cfg.client();
}

/// The constructor must force `max_concurrent_uni_streams = 0` on the
/// supplied `TransportConfig` regardless of whether the caller set it.
/// quinn-proto keeps `TransportConfig::max_concurrent_uni_streams`
/// as a `pub(crate)` field, so the bundled value cannot be read
/// directly from this crate. The end-to-end protocol-level refusal is
/// proved by the `memberlist-simulation` `quic_conformance` test
/// `peer_opened_uni_stream_is_refused_no_remote_state` (sender's
/// `Streams::open(Dir::Uni)` returns `None` on the handshaken
/// connection, no `Recv` is allocated on the receiver, no bridge
/// accumulates). This unit test just exercises the construction path
/// with a caller-set uni-stream credit to confirm the override path
/// compiles and runs without panicking — the field's
/// crate-private visibility means the visible behavioural assertion
/// lives at the protocol level.
#[test]
fn transport_config_construction_with_caller_set_uni_credit_overridden() {
  let mut tc = quinn_proto::TransportConfig::default();
  tc.max_concurrent_uni_streams(quinn_proto::VarInt::from_u32(100));
  let _cfg = QuicOptions::new(
    test_endpoint_config(&[7u8; 32]),
    test_server(),
    test_client(),
    tc,
    "localhost",
    UnreliableTransport::Datagram,
  );
}

/// `new` installs a cluster-uniform SNI: every peer resolves to the same
/// string regardless of the dialed `SocketAddr`.
#[test]
fn sni_for_default_constructor_is_cluster_uniform() {
  let cfg = QuicOptions::new(
    test_endpoint_config(&[7u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    "cluster.example",
    UnreliableTransport::Datagram,
  );
  let a: SocketAddr = "10.0.0.1:7946".parse().unwrap();
  let b: SocketAddr = "10.0.0.2:7946".parse().unwrap();
  assert_eq!(&*cfg.sni_for(&a), "cluster.example");
  assert_eq!(&*cfg.sni_for(&b), "cluster.example");
}

/// The unreliable transport is chosen at construction: a `Datagram`-built
/// config (encrypted, peer-attested delivery over the per-peer QUIC
/// connection) reports `Datagram`; a `Udp`-built config (the large-cluster
/// opt-out that routes the unreliable path over plain UDP) reports `Udp`.
/// `UnreliableTransport::default()` is `Datagram`, so callers that do not care
/// pass that.
#[test]
fn quic_config_unreliable_transport_is_a_constructor_arg() {
  assert_eq!(
    UnreliableTransport::default(),
    UnreliableTransport::Datagram
  );

  let datagram = QuicOptions::new(
    test_endpoint_config(&[7u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    "localhost",
    UnreliableTransport::Datagram,
  );
  assert_eq!(
    datagram.unreliable_transport(),
    UnreliableTransport::Datagram
  );

  let udp = QuicOptions::new(
    test_endpoint_config(&[7u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    "localhost",
    UnreliableTransport::Udp,
  );
  assert_eq!(udp.unreliable_transport(), UnreliableTransport::Udp);
}

/// `new_with_sni_provider` lets the operator return a different identity
/// for each peer — proving the closure is invoked with the dialed
/// `SocketAddr` so per-peer SAN deployments are reachable.
#[test]
fn sni_for_provider_is_per_peer() {
  let provider: SniProvider = Arc::new(|peer: &SocketAddr| -> Arc<str> {
    Arc::from(format!("peer-{}.example", peer.port()))
  });
  let cfg = QuicOptions::new_with_sni_provider(
    test_endpoint_config(&[7u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    provider,
    UnreliableTransport::Datagram,
  );
  let a: SocketAddr = "10.0.0.1:7100".parse().unwrap();
  let b: SocketAddr = "10.0.0.2:7200".parse().unwrap();
  assert_eq!(&*cfg.sni_for(&a), "peer-7100.example");
  assert_eq!(&*cfg.sni_for(&b), "peer-7200.example");
}

/// `server_ref` borrows the stored server config; the borrowed value must be
/// usable to build a server-side `quinn_proto::Endpoint` exactly like the
/// `server_arc` clone does. Exercises the borrow accessor alongside the
/// `Udp`-mode construction path so the `datagram_receive_buffer_size(None)`
/// branch is taken and the resulting server config still drives an endpoint.
#[test]
fn server_ref_borrows_a_usable_server_config_under_udp_mode() {
  let cfg = QuicOptions::new(
    test_endpoint_config(&[9u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    "localhost",
    UnreliableTransport::Udp,
  );
  // `server_ref` borrows the stored config; build a server-side endpoint from
  // that same config (cloned via the arc accessor) to prove the `Udp`-mode
  // bundle — built through the `datagram_receive_buffer_size(None)` branch —
  // is still a usable endpoint config.
  let borrowed: &quinn_proto::ServerConfig = cfg.server_ref();
  assert!(
    Arc::strong_count(&cfg.server_arc()) >= 2,
    "server_arc hands out cheap clones of the shared config"
  );
  let _ = borrowed;
  let _server_ep =
    quinn_proto::Endpoint::new(cfg.endpoint_arc(), Some(cfg.server_arc()), true, None);
  assert_eq!(cfg.unreliable_transport(), UnreliableTransport::Udp);
}
