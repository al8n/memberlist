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
//! This is a deliberate asymmetry with the other two reliable transports.
//! Plain TCP (`Labeled<Passthrough>`) and TLS (`Labeled<TlsRecords>`) prepend
//! the cluster wire-label to each reliable stream, because neither layer
//! authenticates a peer's cluster membership on its own — there, the label is
//! the separation marker. QUIC authenticates the peer during the
//! per-connection TLS 1.3 handshake before any stream opens, so a second
//! in-stream label would add nothing. When several clusters share one CA, the
//! per-peer SNI server name (see [`SniProvider`]) is the discriminator: a peer
//! that presents the wrong server name does not complete the handshake the
//! local endpoint expects. QUIC's unreliable gossip path is plain UDP and
//! carries the cluster label like every other gossip plane.
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

use core::net::SocketAddr;
use std::sync::Arc;

use super::UnreliableTransport;

/// Per-peer SNI lookup. The coordinator calls this once per outbound dial,
/// passing the dialed `SocketAddr`; the returned string is forwarded to
/// `quinn_proto::Endpoint::connect(client, addr, server_name)` and reaches the
/// operator's `ServerCertVerifier::verify_server_cert(_, _, server_name, _, _)`
/// at handshake time.
pub type SniProvider = Arc<dyn Fn(&SocketAddr) -> Arc<str> + Send + Sync>;

/// Immutable QUIC config bundle handed to the coordinator. Accessor-only.
pub struct QuicConfig {
  endpoint: Arc<quinn_proto::EndpointConfig>,
  server: Arc<quinn_proto::ServerConfig>,
  client: quinn_proto::ClientConfig,
  /// Per-peer TLS verification identity. The coordinator invokes this closure
  /// once per outbound dial with the dialed `SocketAddr`; the returned string
  /// is forwarded to `quinn_proto::Endpoint::connect(client, addr,
  /// server_name)` and surfaces at the operator's
  /// `ServerCertVerifier::verify_server_cert(_, _, server_name, _, _)` call.
  ///
  /// Default mode (built by [`Self::new`]) is cluster-uniform: a closure that
  /// returns the same string for every peer. Per-peer SAN deployments — where
  /// each peer's cert names its own hostname/IP and the verifier matches the
  /// supplied `server_name` against the SAN — supply a closure via
  /// [`Self::new_with_sni_provider`] that maps each peer `SocketAddr` to the
  /// expected identity (rustls's `ServerCertVerifier` receives only the
  /// supplied `server_name`, not the dialed `SocketAddr`, so a custom verifier
  /// cannot re-derive per-peer SAN identity from the address alone).
  sni_provider: SniProvider,
  /// Which wire the unreliable path (gossip + probes) routes over, chosen at
  /// construction. In [`UnreliableTransport::Datagram`] mode the constructor
  /// enables quinn's datagram extension (the buffers are sized); in
  /// [`UnreliableTransport::Udp`] mode it forces `datagram_receive_buffer_size`
  /// to `None` (quinn enables datagrams by default, so this must be explicit) so
  /// the endpoint does NOT advertise datagram support and a cross-mode peer
  /// falls back to plain UDP. The driver reads this via
  /// [`Self::unreliable_transport`] to route unreliable sends.
  unreliable_transport: UnreliableTransport,
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
  ///
  /// `server_name` installs the cluster-uniform TLS verification identity:
  /// every outbound dial reaches the operator's `ServerCertVerifier` with the
  /// same string. Per-peer SAN deployments must use
  /// [`Self::new_with_sni_provider`] instead — rustls's
  /// `ServerCertVerifier::verify_server_cert(_, _, server_name, _, _)` does not
  /// receive the dialed `SocketAddr`, so a verifier cannot re-derive per-peer
  /// SAN identity from the supplied address.
  ///
  /// `unreliable_transport` picks the wire for the unreliable path (gossip +
  /// probes) and is the single source of truth for whether quinn's datagram
  /// extension is enabled: [`UnreliableTransport::Datagram`] sizes the datagram
  /// buffers (the extension is advertised), [`UnreliableTransport::Udp`] forces
  /// `datagram_receive_buffer_size` to `None` so the endpoint does NOT advertise
  /// datagram support and cross-mode peers fall back to plain UDP.
  pub fn new(
    endpoint: quinn_proto::EndpointConfig,
    server: quinn_proto::ServerConfig,
    client: quinn_proto::ClientConfig,
    transport: quinn_proto::TransportConfig,
    server_name: impl Into<Arc<str>>,
    unreliable_transport: UnreliableTransport,
  ) -> Self {
    let name: Arc<str> = server_name.into();
    let provider: SniProvider = Arc::new(move |_addr: &SocketAddr| name.clone());
    Self::new_with_sni_provider(
      endpoint,
      server,
      client,
      transport,
      provider,
      unreliable_transport,
    )
  }

  /// Like [`Self::new`] but takes a per-peer SNI provider. The closure is
  /// invoked once per outbound dial with the dialed `SocketAddr`; the
  /// returned string is forwarded to
  /// `quinn_proto::Endpoint::connect(client, addr, server_name)` and reaches
  /// the operator's `ServerCertVerifier` at handshake time. Use this for
  /// per-peer SAN deployments where the verifier must see a different
  /// `server_name` for each peer.
  ///
  /// `unreliable_transport` governs the datagram extension exactly as for
  /// [`Self::new`]: enabled in [`UnreliableTransport::Datagram`] mode, left at
  /// quinn defaults (disabled, unadvertised) in [`UnreliableTransport::Udp`].
  pub fn new_with_sni_provider(
    mut endpoint: quinn_proto::EndpointConfig,
    mut server: quinn_proto::ServerConfig,
    mut client: quinn_proto::ClientConfig,
    mut transport: quinn_proto::TransportConfig,
    sni_provider: SniProvider,
    unreliable_transport: UnreliableTransport,
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
    // builds (`mod.rs::new` calls `Endpoint::new(cfg.endpoint_arc(),
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

    // Govern quinn's datagram extension by the chosen mode. The advertised
    // `max_datagram_frame_size` transport parameter is derived SOLELY from
    // `datagram_receive_buffer_size`: quinn maps `Some(n) -> advertise`,
    // `None -> do not advertise` (transport_parameters.rs builds the param as
    // `config.datagram_receive_buffer_size.map(...)`). It also gates the local
    // SEND path on the receive buffer being set — `datagrams().send(...)`
    // returns `SendDatagramError::Disabled` when it is `None`. Crucially,
    // `TransportConfig::default()` sets `datagram_receive_buffer_size =
    // Some(STREAM_RWND)`, so datagrams are ON by default; the mode must drive
    // BOTH directions explicitly rather than relying on the default.
    //
    // - `Datagram` mode: size both buffers so the extension is advertised and
    //   the send path is enabled.
    // - `Udp` mode: force `datagram_receive_buffer_size(None)` so the endpoint
    //   does NOT advertise datagram support. A `Datagram`-mode peer then sees
    //   `datagrams().max_size() == None` for this connection, reports
    //   `NotReady`, and falls back to plain UDP. This is the true pure-UDP
    //   opt-out AND the mixed-mode interop guarantee — a `Udp` node never
    //   silently swallows a datagram a peer believed it could deliver, because
    //   the peer never negotiated one.
    //
    // Both setters run before `Arc::new(transport)` so the values propagate into
    // the shared `Arc` installed on the server and client configs.
    // (`grease_quic_bit` and `max_concurrent_uni_streams` above are not
    // mode-dependent and stay forced unconditionally.)
    match unreliable_transport {
      UnreliableTransport::Datagram => {
        transport.datagram_receive_buffer_size(Some(64 * 1024));
        transport.datagram_send_buffer_size(64 * 1024);
      }
      UnreliableTransport::Udp => {
        transport.datagram_receive_buffer_size(None);
      }
    }

    let transport = Arc::new(transport);
    server.transport_config(transport.clone());
    client.transport_config(transport);

    let server = Arc::new(server);

    Self {
      endpoint,
      server,
      client,
      sni_provider,
      unreliable_transport,
    }
  }

  /// Resolve the TLS verification identity for an outbound dial to `peer`.
  /// Invokes the stored per-peer SNI closure (see the field docs on [`Self`]
  /// for the verifier-side semantics).
  #[inline(always)]
  pub fn sni_for(&self, peer: &SocketAddr) -> Arc<str> {
    (self.sni_provider)(peer)
  }

  /// Returns the endpoint config (carries the stateless-reset HMAC key).
  #[inline(always)]
  pub fn endpoint_ref(&self) -> &quinn_proto::EndpointConfig {
    &self.endpoint
  }

  /// Returns a cheap clone of the endpoint config arc.
  #[inline(always)]
  pub fn endpoint_arc(&self) -> Arc<quinn_proto::EndpointConfig> {
    self.endpoint.clone()
  }

  /// Returns the server config (TLS cert + key, QUIC transport parameters).
  #[inline(always)]
  pub fn server_ref(&self) -> &quinn_proto::ServerConfig {
    &self.server
  }

  /// Returns a cheap clone of the server config arc.
  #[inline(always)]
  pub fn server_arc(&self) -> Arc<quinn_proto::ServerConfig> {
    self.server.clone()
  }

  /// Returns the client config used for outbound dials.
  pub fn client(&self) -> &quinn_proto::ClientConfig {
    &self.client
  }

  /// Which wire the unreliable path (gossip + probes) rides, as chosen at
  /// construction. [`UnreliableTransport::Datagram`] has quinn's datagram
  /// extension enabled; [`UnreliableTransport::Udp`] does not advertise it. The
  /// driver reads this to route unreliable sends.
  #[inline(always)]
  pub fn unreliable_transport(&self) -> UnreliableTransport {
    self.unreliable_transport
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
      "localhost",
      UnreliableTransport::Datagram,
    );
    // Prove the bundle is usable: quinn_proto::Endpoint::new must not panic.
    // Signature: Endpoint::new(Arc<EndpointConfig>, Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)
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
    let _cfg = QuicConfig::new(
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
    let cfg = QuicConfig::new(
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

    let datagram = QuicConfig::new(
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

    let udp = QuicConfig::new(
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
    let cfg = QuicConfig::new_with_sni_provider(
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
}
