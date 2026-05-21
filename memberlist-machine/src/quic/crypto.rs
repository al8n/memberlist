//! QUIC crypto/config construction for the coordinator.
//!
//! # Cluster authentication on the QUIC reliable path
//!
//! The composed super-state-machine treats **QUIC's TLS 1.3 handshake as the
//! security layer** for the reliable path (push/pull, reliable-ping fallback,
//! reliable user messages). Memberlist's wire-level AES-GCM encryption is
//! skipped on QUIC — layering it atop QUIC's TLS is redundant double-
//! encryption. Memberlist's wire-label is similarly not applied
//! inside QUIC streams: cluster isolation comes from the operator's choice
//! of TLS authentication policy on the supplied `ServerConfig`.
//!
//! Two deployment models are supported by giving the caller full control
//! over the `quinn_proto::ServerConfig`:
//!
//! - **Trusted-network deployments**: build the server with
//!   `rustls::ServerConfig::builder_with_provider(...)
//!       .with_protocol_versions(&[&rustls::version::TLS13])?
//!       .with_no_client_auth()
//!       .with_single_cert(chain, key)?`.
//!   Any client that completes the server-authenticated TLS handshake can
//!   open streams. Use only when the network itself enforces cluster
//!   membership (private VPC, mTLS-terminating ingress proxy, …).
//!
//! - **Cluster-CA mTLS** (recommended for open networks): build the server
//!   with a `ClientCertVerifier` (typically
//!   `WebPkiClientVerifier::builder(root_store).build()?`) whose root store
//!   is the cluster CA. Peers without a cert signed by that CA fail the
//!   handshake — and a failed handshake produces no `EndpointEvent` side
//!   effects (no merge, no `UserDataReceived`, no reliable-ping ack).
//!
//! `QuicConfig::new` does NOT make this choice for the operator. It
//! accepts a fully caller-built `quinn_proto::EndpointConfig` /
//! `ServerConfig` / `ClientConfig` and only enforces the two
//! composition prerequisites the demux invariant demands:
//! `grease_quic_bit = false` (forced on the supplied `EndpointConfig`)
//! and `max_concurrent_uni_streams = 0` (forced on the supplied
//! `TransportConfig`).
//!
//! # Crypto backend
//!
//! The coordinator is crypto-backend-agnostic: it names no crypto
//! crate. The caller builds the `EndpointConfig` (whose stateless-reset
//! HMAC key comes from their chosen backend's `HmacKey`), the
//! `ServerConfig`, and the `ClientConfig` with whatever provider they
//! want (ring, aws-lc-rs, FIPS, …). Enable the matching `quic-*`
//! crypto-backend feature so that backend is present in the dependency
//! graph; the `EndpointConfig` is then typically built as
//! `EndpointConfig::new(Arc::new(<backend>::hmac::Key::new(HMAC_SHA256,
//! reset_key)))`.
//!
//! The sim path uses a self-signed cert + accept-any verifier with the
//! ring backend. Behavioral determinism comes from the injected virtual
//! clock, not from any test-only crypto hook.

use std::sync::Arc;

/// Immutable QUIC config bundle handed to the coordinator. Accessor-only.
pub struct QuicConfig {
  endpoint: Arc<quinn_proto::EndpointConfig>,
  server: Arc<quinn_proto::ServerConfig>,
  client: quinn_proto::ClientConfig,
}

impl QuicConfig {
  /// Build from a caller-built endpoint config, server config, client
  /// config, and the single `transport` config the coordinator owns.
  ///
  /// The CALLER builds `endpoint`, `server`, and `client` — including
  /// the stateless-reset HMAC key (on the `EndpointConfig`), the TLS
  /// versions, cert chains, and (critically) the **client-cert verifier
  /// on the server side**. The crypto backend is the caller's choice;
  /// the coordinator names no crypto crate. The constructor's job is
  /// the two composition prerequisites the inbound first-byte demux
  /// invariant depends on, NOT the operator's security policy. See the
  /// module docs for the two supported cluster-auth deployment models
  /// (trusted-network with `with_no_client_auth` vs cluster-CA mTLS with
  /// `with_client_cert_verifier`).
  ///
  /// `endpoint` carries the stateless-reset HMAC key the caller seeded.
  /// The constructor forces `grease_quic_bit = false` on it (see below).
  ///
  /// `transport` is the single `quinn_proto::TransportConfig` the
  /// constructor installs on both the `ServerConfig` and the supplied
  /// `ClientConfig` (any `transport_config` already set on `server` /
  /// `client` is overwritten — the composed unit owns the transport
  /// policy of its endpoint, and a divergence between server and client
  /// direction would be a footgun). The caller controls every other
  /// tunable (`max_idle_timeout`, `stream_receive_window`, MTU
  /// discovery, congestion controller, …) by populating that
  /// `TransportConfig` before handing it in. The constructor
  /// unconditionally forces `max_concurrent_uni_streams = 0` on the
  /// supplied value (mutating it before moving it into a shared `Arc`,
  /// so a caller's surviving `Arc` view cannot accidentally re-enable
  /// remote-initiated unidirectional streams): memberlist's reliable
  /// exchanges (push/pull, reliable-ping fallback, reliable
  /// user-message) are ALL bidirectional, so a peer that opens a
  /// remote-initiated uni stream has no legitimate destination.
  /// Advertising zero credit is the protocol-layer refusal —
  /// quinn-proto's `Streams::open(Dir::Uni)` returns `None` on the
  /// sender side once `state.next[Uni] >= state.max[Uni]`, and no
  /// `Recv` is ever allocated on the receiver for an unwelcome
  /// uni-stream frame, so no remote uni-stream state can accumulate.
  ///
  /// quinn-proto APIs used here:
  /// - `EndpointConfig::grease_quic_bit(bool) -> &mut Self`.
  /// - `TransportConfig::max_concurrent_uni_streams(VarInt) -> &mut Self`.
  /// - `ServerConfig::transport_config(Arc<TransportConfig>) -> &mut Self`.
  /// - `ClientConfig::transport_config(Arc<TransportConfig>) -> &mut Self`.
  pub fn new(
    mut endpoint: quinn_proto::EndpointConfig,
    mut server: quinn_proto::ServerConfig,
    mut client: quinn_proto::ClientConfig,
    mut transport: quinn_proto::TransportConfig,
  ) -> Self {
    // Disable QUIC-bit greasing (RFC 9287). `EndpointConfig::new`
    // defaults `grease_quic_bit` to `true`, which advertises the
    // `grease_quic_bit` transport parameter so the PEER may clear
    // `FIXED_BIT` (`0x40`) on its outgoing short-header packets — and
    // quinn-proto's encoder is allowed to do the same on packets it sends
    // to a peer that advertised greasing. The composed coordinator's
    // inbound first-byte demux (`demux::classify`) is collision-free only
    // when every QUIC short header carries `FIXED_BIT` set (so the first
    // byte is `>= 0x40`) and every long-header packet has
    // `LONG_HEADER_FORM` (`0x80`) set; a FIXED_BIT-cleared short header
    // would route as `Class::Memberlist` (first byte ∈ `1..=15`) or
    // `Class::Reject`, handing valid post-handshake QUIC ciphertext to
    // the memberlist codec and silently dropping ACKs / stream data /
    // close packets. Disabling greasing on the constructed
    // `EndpointConfig` stops us advertising the transport parameter — a
    // protocol-compliant peer will not clear FIXED_BIT in packets it
    // sends us — and stops our encoder from greasing outbound. A single
    // setter call covers both directions: the same `Arc<EndpointConfig>`
    // is installed on every `quinn_proto::Endpoint` the coordinator
    // builds (`mod.rs::new` calls `Endpoint::new(cfg.endpoint().clone(),
    // ...)`), so server and client share this policy. Tradeoff: a
    // non-compliant peer that greases anyway would be rejected at packet
    // decode (quinn-proto rejects FIXED_BIT-cleared packets when this
    // side has greasing off); acceptable — the demux invariant is a
    // hard composition prerequisite and we own the deployment.
    endpoint.grease_quic_bit(false);
    let endpoint = Arc::new(endpoint);

    // Force `max_concurrent_uni_streams = 0` on the caller's transport
    // config, then move it into a single shared `Arc` installed on
    // both the server and client. The mutation happens BEFORE the
    // `Arc::new`, so any reference the caller still holds to the
    // value (impossible if they `move`d it as the signature requires,
    // but defensive against API misuse) is consumed.
    transport.max_concurrent_uni_streams(quinn_proto::VarInt::from_u32(0));
    let transport = Arc::new(transport);
    server.transport_config(transport.clone());
    client.transport_config(transport);

    let server = Arc::new(server);

    Self {
      endpoint,
      server,
      client,
    }
  }

  /// Returns the endpoint config (carries the stateless-reset HMAC key).
  pub fn endpoint(&self) -> &Arc<quinn_proto::EndpointConfig> {
    &self.endpoint
  }

  /// Returns the server config (TLS cert + key, QUIC transport parameters).
  pub fn server(&self) -> &Arc<quinn_proto::ServerConfig> {
    &self.server
  }

  /// Returns the client config used for outbound dials.
  pub fn client(&self) -> &quinn_proto::ClientConfig {
    &self.client
  }
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;
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
  /// `EndpointConfig` with their chosen crypto backend — see the module
  /// docs. `QuicConfig::new` forces `grease_quic_bit = false` regardless.
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
      .with_protocol_versions(&[&rustls::version::TLS13])
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
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
      &self,
      _message: &[u8],
      _cert: &CertificateDer,
      _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
      &self,
      _message: &[u8],
      _cert: &CertificateDer,
      _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
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
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AnyServer))
      .with_no_client_auth();
    let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(cfg)).unwrap();
    quinn_proto::ClientConfig::new(Arc::new(qcc))
  }

  #[test]
  fn builds_a_usable_config_bundle() {
    let cfg = QuicConfig::new(
      test_endpoint_config(&[7u8; 32]),
      test_server(),
      test_client(),
      quinn_proto::TransportConfig::default(),
    );
    // Prove the bundle is usable: quinn_proto::Endpoint::new must not panic.
    // Signature: Endpoint::new(Arc<EndpointConfig>, Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)
    let _server_ep = quinn_proto::Endpoint::new(
      cfg.endpoint().clone(),
      Some(cfg.server().clone()),
      true,
      None,
    );
    let _client_ep = quinn_proto::Endpoint::new(cfg.endpoint().clone(), None, true, None);
    let _ = cfg.endpoint();
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
    let _cfg = QuicConfig::new(
      test_endpoint_config(&[7u8; 32]),
      test_server(),
      test_client(),
      tc,
    );
  }
}
