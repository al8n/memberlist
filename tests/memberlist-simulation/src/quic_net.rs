//! [`QuicCluster`]: the `quic`-gated conformance harness.
//!
//! Mirrors [`Cluster`](crate::cluster::Cluster) / [`Network`](crate::Network)
//! exactly, substituting [`QuicEndpoint`] for the bare `Endpoint`: reliable
//! exchanges (join/anti-entropy push-pull, reliable-ping fallback) ride a real
//! `quinn-proto` bidi stream instead of the plain-harness stub streams, while
//! unreliable gossip rides the same conceptual UDP socket. One in-memory
//! datagram queue carries BOTH protocols: a first byte `>= 0x40` is a QUIC
//! datagram (handed to [`QuicEndpoint::handle_udp`], which demuxes it into
//! quinn); a first byte in `1..=15` is a memberlist plain frame.
//!
//! `memberlist-proto` has no umbrella `codec` dependency, so the byte
//! encode/decode of the unreliable path lives here: outbound is drained
//! through [`QuicEndpoint::poll_memberlist_transmit`] (NEVER
//! `endpoint_mut().poll_transmit()` — that would double-drive the
//! `Event::LeftCluster` accounting `Endpoint::poll_transmit` performs) and
//! encoded with the machine's own `memberlist-wire` plain-frame codec;
//! inbound memberlist datagrams enter through the SAME public first-byte
//! demux (`handle_udp`), are surfaced raw via
//! `QuicEndpoint::poll_memberlist_ingress` (the coordinator has no codec dep,
//! so it never decodes them in-crate and never silently drops them), then
//! decoded here and fed back through the coordinator's public
//! `QuicEndpoint::handle_packet` pass-through. Virtual
//! time (the shared [`Clock`]) is injected as `now` into BOTH machines, and
//! the existing [`FaultConfig`] drop/delay/partition model is reused
//! unchanged — so the SWIM timing observed here is directly comparable to
//! the plain harness (the conformance-parity capstone).

use std::{
  collections::{HashMap, VecDeque},
  net::SocketAddr,
  sync::Arc,
  time::Duration,
};

use memberlist_proto::{
  Endpoint, EndpointOptions, Event, Instant, PushPullKind, QuicEndpoint, QuicOptions, Transmit,
  UnreliableTransport, framing, message_from_any, message_to_any,
  typed::{Ack, Alive, Message, Node, Suspect},
};
use smol_str::SmolStr;

use crate::{clock::Clock, faults::FaultConfig};

/// The concrete coordinator the harness drives.
///
/// The QUIC coordinator pins `A = SocketAddr` internally (`quinn_proto`'s
/// endpoint is structurally `SocketAddr`-typed). The cluster-uniform TLS
/// verification identity is sourced from `QuicOptions::server_name`
/// (`"localhost"`, matching the sim's self-signed certs — see
/// [`sim_quic_config`]).
type Node1 = QuicEndpoint<SmolStr>;

/// One in-flight datagram on the single shared socket. Carries raw bytes
/// (QUIC `>= 0x40` OR an encoded memberlist plain frame `1..=15`) so the
/// first-byte demux inside [`QuicEndpoint::handle_udp`] is genuinely
/// exercised. Fault/latency is applied ONCE per datagram (one `send_to`).
struct PendingDatagram {
  deliver_at: Instant,
  from: SocketAddr,
  to: SocketAddr,
  bytes: bytes::Bytes,
}

/// The rustls crypto provider the harness uses, selected by the active
/// backend feature. The entire conformance suite runs under whichever is
/// chosen (`quic` → ring, `quic-rustls-aws-lc-rs` → aws-lc-rs).
#[cfg(not(feature = "quic-rustls-aws-lc-rs"))]
pub fn sim_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
  Arc::new(rustls::crypto::ring::default_provider())
}

/// See [`sim_crypto_provider`]. aws-lc-rs variant.
#[cfg(feature = "quic-rustls-aws-lc-rs")]
pub fn sim_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
  Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// A `quinn_proto::EndpointConfig` with a fixed stateless-reset HMAC key,
/// built with the active backend. `QuicOptions::new` forces
/// `grease_quic_bit = false`; the fixed key keeps runs reproducible.
#[cfg(not(feature = "quic-rustls-aws-lc-rs"))]
pub fn sim_endpoint_config(reset_key: &[u8]) -> quinn_proto::EndpointConfig {
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, reset_key);
  quinn_proto::EndpointConfig::new(Arc::new(hmac))
}

/// See [`sim_endpoint_config`]. aws-lc-rs variant.
#[cfg(feature = "quic-rustls-aws-lc-rs")]
pub fn sim_endpoint_config(reset_key: &[u8]) -> quinn_proto::EndpointConfig {
  let hmac = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, reset_key);
  quinn_proto::EndpointConfig::new(Arc::new(hmac))
}

/// Self-signed cert + accept-any verifier, matching the memberlist-proto
/// quic crypto tests (sim/test only; behavioural determinism is the virtual
/// clock, not crypto). Returns a fresh [`QuicOptions`] each call.
fn sim_quic_config(shrink_flow_window: bool) -> QuicOptions {
  use rustls_pki_types::{CertificateDer, PrivateKeyDer};

  #[derive(Debug)]
  struct AnyServer;
  impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
      &self,
      _e: &CertificateDer<'_>,
      _i: &[CertificateDer<'_>],
      _n: &rustls_pki_types::ServerName<'_>,
      _o: &[u8],
      _t: rustls_pki_types::UnixTime,
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
      sim_crypto_provider()
        .signature_verification_algorithms
        .supported_schemes()
    }
  }

  // rcgen 0.14: `generate_simple_self_signed` -> `CertifiedKey`; the private
  // key is the `signing_key` field, `serialize_der()` yields PKCS#8 DER (the
  // same construction the memberlist-proto quic crypto tests use).
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
  let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());

  let provider = sim_crypto_provider();

  // Sim-only server config: self-signed cert + `with_no_client_auth`.
  // Production caller-built server configs install an operator-chosen
  // `ClientCertVerifier` for cluster-CA mTLS — see the `quic::crypto`
  // module docs. The mTLS-rejects-unauthenticated-peer property is
  // covered by `mtls_rejects_unauthenticated_client_no_side_effects`
  // in `tests/quic_conformance.rs`.
  let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(server_tls)).unwrap();
  let server = quinn_proto::ServerConfig::with_crypto(Arc::new(qsc));

  let client_tls = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AnyServer))
    .with_no_client_auth();
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(client_tls)).unwrap();
  let client = quinn_proto::ClientConfig::new(Arc::new(qcc));

  // `QuicOptions::new` installs the supplied `TransportConfig` on BOTH the
  // server and client (it also forces `max_concurrent_uni_streams = 0` on
  // it — memberlist's reliable exchanges are all bidirectional). Build
  // the transport here so the sim's per-stream backpressure tuning and
  // the 20s idle timeout the conformance tests rely on travel through
  // unchanged.
  let mut transport = quinn_proto::TransportConfig::default();
  if shrink_flow_window {
    // Force `WriteError::Blocked` on a large push/pull: a tiny per-stream
    // receive window means the writer cannot drain its snapshot in one go,
    // so `Bridge::pump_out` must retain + retry the remainder (zero byte
    // loss). Also cap the idle timeout so `idle_for` reaps deterministically.
    transport
      .stream_receive_window(quinn_proto::VarInt::from_u32(1200))
      .receive_window(quinn_proto::VarInt::from_u32(4096))
      .send_window(4096)
      .max_idle_timeout(Some(
        quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
      ));
  } else {
    transport.max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
    ));
  }

  QuicOptions::new(
    sim_endpoint_config(&[0x5au8; 32]),
    server,
    client,
    transport,
    "localhost",
    UnreliableTransport::Datagram,
  )
}

/// `quic`-gated deterministic conformance harness. Accessor-only; no `pub`
/// fields. Mirrors [`Cluster`](crate::cluster::Cluster)'s step contract.
pub struct QuicCluster {
  nodes: HashMap<SocketAddr, Node1>,
  clock: Clock,
  faults: FaultConfig,
  queue: VecDeque<PendingDatagram>,
  /// Per-host record of whether the host has EVER observed the named peer in
  /// `Suspect` (scanned every tick — a transient Suspect that is later
  /// refuted would otherwise be invisible to an end-state assertion).
  suspected: HashMap<SocketAddr, std::collections::HashSet<SmolStr>>,
  /// Per-host record of whether the host has EVER observed the named peer
  /// `Alive` (so a D1 test can assert "the merge reached the Endpoint" even
  /// if normal SWIM later transitions the peer away).
  ever_alive: HashMap<SocketAddr, std::collections::HashSet<SmolStr>>,
  /// Per-host record of whether the host has EVER observed the named peer
  /// `Dead` or `Left` (so a leave test can assert the transition was
  /// observed even after the dead/left entry is later GC'd to absent).
  ever_gone: HashMap<SocketAddr, std::collections::HashSet<SmolStr>>,
  /// Per-host record of whether `Event::LeftCluster` has fired.
  left: std::collections::HashSet<SocketAddr>,
  /// Per-host SET of every distinct reliable `Event::UserPacket` payload
  /// observed (dedup is required because `scan_events` re-queues every event
  /// via [`Endpoint::requeue_event`], so a single emission is observed once
  /// per scan — a `Vec::push` would count duplicates and over-report). Kept
  /// so a test can assert the payload arrived AND the bridge carrying it was
  /// reaped (no leak), even though SWIM later does nothing with it.
  user_packets: HashMap<SocketAddr, std::collections::HashSet<bytes::Bytes>>,
  /// One-shot D1 hook: the tick `host` FIRST merges a peer (the join reply
  /// reached its Endpoint), tear its QUIC connection down the SAME tick. The
  /// `bool` is whether it has fired.
  d1_drop_quic: Option<(SocketAddr, bool)>,
  /// Like [`d1_drop_quic`] but force-LOSES the connection (drops every QUIC
  /// datagram to/from the host so quinn idle-times-out → `ConnectionLost`).
  d1_lose_conn: Option<(SocketAddr, bool)>,
  /// Pairs whose QUIC (`>= 0x40`) datagrams are dropped while their
  /// memberlist (`1..=15`) frames still flow — models "the QUIC connection
  /// died but unreliable SWIM continues".
  quic_block: Vec<(SocketAddr, SocketAddr)>,
  /// ORDERED `(from, to)` QUIC drops (directional — only this orientation
  /// is dropped; the reverse direction and all memberlist frames keep
  /// flowing). Populated by [`drop_quic_directional`] for credit-starvation
  /// fault injection: the receiver of `from -> to` never sees `from`'s ACK /
  /// MAX_STREAM_DATA, so any data the receiver is itself sending stalls on
  /// flow-control credit while the connection stays alive.
  quic_block_directional: Vec<(SocketAddr, SocketAddr)>,
  /// Hosts whose direct + indirect probe UDP datagrams are dropped (forces
  /// the reliable-ping fallback), as an unordered pair set.
  probe_block: Vec<(SocketAddr, SocketAddr)>,
  /// Optional `(probe_interval, probe_timeout)` override applied to every
  /// node built via [`add_node`](Self::add_node). `None` uses the standard
  /// LAN knobs that match the plain harness (the conformance-parity
  /// invariant). A wider window is used ONLY by the reliable-fallback
  /// scenario, whose subject is the step-(2)-before-step-(3) ack-absorption
  /// ordering — NOT SWIM timing parity — so the QUIC reliable-ping has time
  /// to complete a real round-trip within the probe's cumulative deadline.
  probe_window: Option<(Duration, Duration)>,
  /// When `true`, every node built via [`add_node`](Self::add_node) uses a
  /// tiny quinn per-stream receive window so a large push/pull payload
  /// cannot be written in one go (`Bridge::pump_out` must hit
  /// `WriteError::Blocked` and retain + retry the remainder — zero byte
  /// loss). Set BEFORE the nodes are built (so the small window is in effect
  /// from the first handshake), never by rebuilding live coordinators.
  shrink_window: bool,
  /// Optional `stream_timeout` override applied to every node built via
  /// [`add_node`](Self::add_node). `None` uses the standard 10s LAN default
  /// the conformance-parity tests share with the plain harness. A short
  /// stream-timeout is used ONLY to exercise the response-deadline refresh
  /// invariant: the bridge's original accept-time deadline
  /// (`accept_time + stream_timeout`) must be tight enough relative to the
  /// post-refresh response window (`request_arrival + 5s`) that a backpressured
  /// response delivery strictly crosses the original deadline while staying
  /// within the refreshed one.
  stream_timeout: Option<Duration>,
  /// Cross-transport compression applied to every coordinator built via
  /// [`add_node`](Self::add_node). [`new`](memberlist_proto::CompressionOptions::new)
  /// by default — the standard conformance suite drives gossip through
  /// `compress_gossip` on egress and the single canonical `decrypt_gossip`
  /// unwrap on ingress regardless (which strips both the Encrypted and the
  /// Compressed wrapper in one pass, each identity when absent), and a
  /// disabled configuration makes the egress compression an identity, so
  /// the suite stays byte-unchanged. The `*_compressed` constructors
  /// install an enabled configuration.
  compression: memberlist_proto::CompressionOptions,
  /// Cross-transport encryption applied to every coordinator built via
  /// [`add_node`](Self::add_node). [`new`](memberlist_proto::EncryptionOptions::new)
  /// by default — the standard conformance suite drives gossip through
  /// `encrypt_gossip` / `decrypt_gossip` regardless, and a disabled
  /// configuration makes both identity, so the suite stays byte-unchanged.
  /// The `*_encrypted` constructors install an enabled configuration. The
  /// reliable path always installs this through `with_encryption` — the QUIC
  /// bridge force-disables reliable-path encryption (quinn-encrypted streams
  /// already provide confidentiality), so this knob influences only the
  /// gossip codec on the harness side.
  encryption: memberlist_proto::EncryptionOptions,
  /// Reliable-wire observation tap. Every QUIC datagram routed through the
  /// virtual reliable pipe (quinn-encrypted UDP carrying the reliable streams)
  /// is appended here so the encrypted conformance suite can assert the wire
  /// never carries an `Encrypted` wrapper: a quinn datagram's first byte has
  /// `b & 0xC0 != 0` (>= `0x40`), never [`memberlist_proto::ENCRYPTED_TAG`]
  /// (which is in `1..=15`) — the QUIC bridge skips reliable-path encryption,
  /// so no inner `[Encrypted[..]]` ever appears on the reliable path. Only
  /// compiled under the encryption-conformance feature — the standard suite
  /// stays byte-unchanged.
  #[cfg(feature = "__sim-encryption-aes-gcm")]
  observed_reliable_wire_bytes: Vec<Vec<u8>>,
}

impl QuicCluster {
  /// Empty cluster.
  fn empty() -> Self {
    Self {
      nodes: HashMap::new(),
      clock: Clock::new(),
      faults: FaultConfig::none(),
      queue: VecDeque::new(),
      suspected: HashMap::new(),
      ever_alive: HashMap::new(),
      ever_gone: HashMap::new(),
      left: std::collections::HashSet::new(),
      user_packets: HashMap::new(),
      d1_drop_quic: None,
      d1_lose_conn: None,
      quic_block: Vec::new(),
      quic_block_directional: Vec::new(),
      probe_block: Vec::new(),
      probe_window: None,
      shrink_window: false,
      stream_timeout: None,
      compression: memberlist_proto::CompressionOptions::new(),
      encryption: memberlist_proto::EncryptionOptions::new(),
      #[cfg(feature = "__sim-encryption-aes-gcm")]
      observed_reliable_wire_bytes: Vec::new(),
    }
  }

  /// Build one coordinator with the SAME membership knobs the plain
  /// [`Cluster::add_node_with`](crate::cluster::Cluster) uses, so SWIM timing
  /// is directly comparable (the conformance-parity invariant).
  fn add_node(&mut self, id: &str, addr: SocketAddr) {
    let (probe_interval, probe_timeout) = self
      .probe_window
      .unwrap_or((Duration::from_millis(1000), Duration::from_millis(500)));
    let mut cfg = EndpointOptions::new(SmolStr::new(id), addr)
      .with_gossip_interval(Duration::from_millis(200))
      .with_push_pull_interval(Duration::from_secs(30))
      .with_probe_interval(probe_interval)
      .with_probe_timeout(probe_timeout)
      .with_suspicion_mult(4)
      .with_retransmit_mult(4)
      .with_rng_seed(addr.port() as u64);
    if let Some(t) = self.stream_timeout {
      cfg = cfg.with_stream_timeout(t);
    }
    let now = self.clock.now();
    let mut ep = Endpoint::new_at(cfg, now);
    ep.start_scheduling(now);
    let qc = sim_quic_config(self.shrink_window);
    // Deterministic conformance harness: seed quinn's connection-ID /
    // path-challenge RNG from the node's port (distinct per node, fixed
    // across runs) so the QUIC transport — and therefore the composed
    // membership behaviour/timing — is bit-for-bit reproducible. (Temporal
    // determinism is the virtual clock; this removes the only remaining OS
    // entropy. Production uses `QuicEndpoint::new` → `None`.)
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&addr.port().to_le_bytes());
    // `QuicEndpoint` exposes the deterministic `rng_seed` seam and the
    // compression seam on two separate constructors. The standard conformance
    // suite (compression disabled) takes the seeded constructor so the QUIC
    // transport stays bit-for-bit reproducible. A `*_compressed` constructor
    // takes `with_compression`, whose membership behaviour is identical — the
    // virtual clock is the temporal-determinism anchor; the connection-ID /
    // path-challenge RNG does not affect SWIM convergence. Encryption is then
    // threaded fluently regardless of the base constructor; the QUIC bridge
    // always force-disables reliable-path encryption inside, so the field is
    // wired here to match the `TcpCluster`/`TlsCluster` shape but only
    // influences the gossip codec.
    let node = if self.compression.algorithm().is_some() {
      QuicEndpoint::with_compression(ep, qc, self.compression)
    } else {
      QuicEndpoint::with_quinn_rng_seed(ep, qc, Some(seed))
    };
    self
      .nodes
      .insert(addr, node.with_encryption(self.encryption.clone()));
  }

  /// Two nodes; `a` joins `b` via a QUIC push/pull. `a` starts knowing
  /// NOTHING about `b` (no bootstrap alive — `start_push_pull` dials the
  /// address directly), so `a` learns `b` ONLY by merging the join reply
  /// and `b` learns `a` ONLY by merging `a`'s push: both sides converge
  /// Alive purely through the reliable QUIC exchange. (Mirrors the plain
  /// harness `join`, whose `inject_alive` only seeds the dial address — here
  /// the address is passed straight to `start_push_pull`.)
  pub fn two_node_join(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Like [`two_node_join`](Self::two_node_join) but every node is built with
  /// `algorithm` compression enabled. Used by the compression-enabled
  /// conformance tests, which assert membership behavior is identical to the
  /// uncompressed run.
  ///
  /// `with_threshold(0)` forces every gossip datagram through the compressor so
  /// the conformance test exercises the compressed path even for small
  /// datagrams; the don't-expand fallback still emits a `Plain` outcome for
  /// incompressible tiny packets, so correctness is unchanged.
  pub fn two_node_join_compressed(
    a: SocketAddr,
    b: SocketAddr,
    algorithm: memberlist_proto::CompressAlgorithm,
  ) -> Self {
    let mut c = Self::empty();
    c.compression = memberlist_proto::CompressionOptions::new()
      .with_algorithm(algorithm)
      .with_threshold(0);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Like [`two_node_join`](Self::two_node_join) but every node is built with
  /// `primary_key` as the encryption primary. Used by the encryption-enabled
  /// conformance tests, which assert membership behavior is identical to the
  /// unencrypted run (encryption is transparent to SWIM). The QUIC bridge
  /// force-disables reliable-path encryption inside (quinn-encrypted streams
  /// already provide confidentiality), so the reliable-wire conformance test
  /// pins that no `[Encrypted[..]]` wrapper ever appears on the reliable path.
  pub fn two_node_join_encrypted(
    a: SocketAddr,
    b: SocketAddr,
    primary_key: memberlist_proto::SecretKey,
  ) -> Self {
    let mut c = Self::empty();
    c.encryption = memberlist_proto::EncryptionOptions::new()
      .with_keyring(memberlist_proto::Keyring::new(primary_key));
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Like [`two_node_join_compressed`](Self::two_node_join_compressed) but
  /// every node is built with BOTH `algorithm` compression and `primary_key`
  /// encryption enabled. Used by the compound-stack conformance test, which
  /// asserts membership behavior is identical to the disabled-both run.
  pub fn two_node_join_compressed_and_encrypted(
    a: SocketAddr,
    b: SocketAddr,
    algorithm: memberlist_proto::CompressAlgorithm,
    primary_key: memberlist_proto::SecretKey,
  ) -> Self {
    let mut c = Self::empty();
    c.compression = memberlist_proto::CompressionOptions::new()
      .with_algorithm(algorithm)
      .with_threshold(0);
    c.encryption = memberlist_proto::EncryptionOptions::new()
      .with_keyring(memberlist_proto::Keyring::new(primary_key));
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Two nodes pre-seeded Alive about each other (no join handshake), ready
  /// for a probe scenario.
  pub fn two_node_alive(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.handle_packet(
        a,
        Message::Alive(Alive::new(1, Node::new(SmolStr::new("b"), b))),
        now,
      );
    }
    if let Some(n) = c.nodes.get_mut(&b) {
      n.handle_packet(
        b,
        Message::Alive(Alive::new(1, Node::new(SmolStr::new("a"), a))),
        now,
      );
    }
    c
  }

  /// Two bare nodes that know NOTHING about each other and exchange no
  /// gossip until told to. Used by the one-way `start_user_message` test:
  /// the user-message dial takes its peer address from the call argument
  /// (not membership), so no pre-seeded Alive is needed — keeping the
  /// scenario minimal so the ONLY in-flight bridge is the user-message one,
  /// making `live_bridge_count == 0` a precise no-leak assertion.
  pub fn two_node_bare(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.add_node("a", a);
    c.add_node("b", b);
    c
  }

  /// Like [`two_node_join`](Self::two_node_join) but with a SHORT direct
  /// `probe_timeout` (500 ms) and a LONG `probe_interval` (10 s). The
  /// probe's single cumulative deadline is `sent + probe_interval`, while
  /// the reliable-ping fallback is opened only AFTER the direct
  /// `probe_timeout` elapses — so the fallback has ≈ `probe_interval -
  /// probe_timeout` (≈ 9.5 s) to complete a real QUIC reliable-ping
  /// round-trip on the pooled connection. The SUBJECT of the
  /// reliable-fallback scenario is the fixed per-tick step order (a
  /// fallback-ping ack drained into the `Endpoint` in step (2) BEFORE the
  /// probe `handle_timeout` in step (3)), NOT SWIM timing parity — the
  /// conformance-parity tests deliberately keep the standard LAN knobs.
  pub fn two_node_join_slow_probe(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.probe_window = Some((Duration::from_secs(10), Duration::from_millis(500)));
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  // ── Fault / scenario hooks ────────────────────────────────────────────────

  /// Tear down `host`'s QUIC connection the SAME tick its join push/pull
  /// reply first reaches its `Endpoint` (the tick `host` first sees the peer
  /// `Alive`). Only the QUIC (`>= 0x40`) datagrams between the pair are
  /// dropped from then on — the unreliable memberlist (`1..=15`) path keeps
  /// flowing, so this isolates the D1 invariant: the reply must have been
  /// drained into the `Endpoint` (peer `Alive`) BEFORE the bridge/connection
  /// was reaped, while ongoing UDP SWIM still sustains the membership.
  pub fn drop_connection_after_first_full_reply(&mut self, host: SocketAddr) {
    self.d1_drop_quic = Some((host, false));
  }

  /// Like [`drop_connection_after_first_full_reply`] but force-LOSES the
  /// connection: every QUIC datagram to/from `host` is dropped so quinn
  /// idle-times-out and the coordinator's `Event::ConnectionLost` path marks
  /// every bridge on it fatal (the StreamErrored teardown variant of D1).
  pub fn lose_connection_after_first_full_reply(&mut self, host: SocketAddr) {
    self.d1_lose_conn = Some((host, false));
  }

  /// Drop every direct + indirect UDP probe datagram between `a` and `b`,
  /// forcing the reliable-ping fallback over QUIC.
  pub fn drop_all_udp_probes_between(&mut self, a: SocketAddr, b: SocketAddr) {
    self.probe_block.push((a, b));
  }

  /// Permanently drop every QUIC datagram in the ORDERED direction
  /// `from -> to` (reverse-direction QUIC and all memberlist frames continue
  /// to flow). Used to starve `to` of `from`'s ACK / MAX_STREAM_DATA frames,
  /// so any data `to` is sending (e.g. a backpressured push/pull response)
  /// never gets further flow-control credit — the connection itself stays
  /// alive (no graceful close, no `ConnectionLost` until quinn's much-later
  /// idle timeout), so the only thing that can reap a stuck `Done`-but-
  /// unflushed bridge within `stream_timeout` of its accept is the bridge's
  /// flush deadline. Public ONLY for tests that need fine-grained
  /// directional QUIC fault injection.
  pub fn drop_quic_directional(&mut self, from: SocketAddr, to: SocketAddr) {
    self.quic_block_directional.push((from, to));
  }

  /// Partition `group_a` from `group_b` (datagrams across the cut dropped).
  pub fn partition(&mut self, group_a: &[SocketAddr], group_b: &[SocketAddr]) {
    let mut map = HashMap::new();
    for &a in group_a {
      map.insert(a, 0usize);
    }
    for &b in group_b {
      map.insert(b, 1usize);
    }
    self.faults.partitions = Some(map);
  }

  /// `a` joins `b` via a QUIC push/pull, but every node is built with a TINY
  /// quinn per-stream receive window AND `a`'s push snapshot is pre-loaded
  /// with `extra_peers` extra Alive members. The join push/pull payload is
  /// therefore far larger than one stream-window's worth, so
  /// `Bridge::pump_out` must hit `WriteError::Blocked`, retain the unwritten
  /// remainder in `pending_out`, and retry it head-first on later ticks —
  /// the join must still complete with ZERO byte loss (every pushed peer
  /// crosses intact). The small window is set BEFORE the nodes are built and
  /// the extras are injected BEFORE `start_push_pull`, so the very first
  /// push/pull is already large and back-pressured (no live-coordinator
  /// rebuild — that would discard the in-flight dial).
  pub fn two_node_join_small_window(a: SocketAddr, b: SocketAddr, extra_peers: usize) -> Self {
    let mut c = Self::empty();
    c.shrink_window = true;
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      for i in 0..extra_peers {
        let paddr: SocketAddr = format!("203.0.113.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_packet(
          a,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("extra-{i}")), paddr),
          )),
          now,
        );
      }
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Like [`two_node_join_small_window`](Self::two_node_join_small_window) but
  /// every node is built with `algorithm` compression enabled. The
  /// many-member, back-pressured variant of
  /// [`two_node_join_compressed`](Self::two_node_join_compressed): the join
  /// push/pull is large and fragmented across QUIC stream-window credits, so
  /// the compression-enabled conformance run exercises the compressed reliable
  /// path under backpressure. `with_threshold(0)` forces every gossip datagram
  /// through the compressor.
  pub fn two_node_join_small_window_compressed(
    a: SocketAddr,
    b: SocketAddr,
    extra_peers: usize,
    algorithm: memberlist_proto::CompressAlgorithm,
  ) -> Self {
    let mut c = Self::empty();
    c.shrink_window = true;
    c.compression = memberlist_proto::CompressionOptions::new()
      .with_algorithm(algorithm)
      .with_threshold(0);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      for i in 0..extra_peers {
        let paddr: SocketAddr = format!("203.0.113.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_packet(
          a,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("extra-{i}")), paddr),
          )),
          now,
        );
      }
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Like [`two_node_join_small_window`](Self::two_node_join_small_window) but
  /// the RESPONDER (`b`) is the one pre-loaded with `extra_peers` extra Alive
  /// members, so it is `b`'s push/pull **response** snapshot that exceeds one
  /// tiny stream-window's worth. `b`'s `Bridge::pump_out` must therefore
  /// partial-accept then hit `WriteError::Blocked` on its inbound-response
  /// send half, retain the remainder in `pending_out`, and — because the
  /// memberlist `Stream` flips the inbound-response phase to `Done` the
  /// instant it hands the bytes to the bridge (before they reach quinn) — the
  /// bridge must NOT be reaped until that retained tail is fully written AND
  /// the send half is `finish()`ed (FIN). If it were reaped early the tail is
  /// truncated / no FIN is sent and `a` never receives the complete response
  /// (it would never see `b`'s extras Alive, never converge). `a` is also
  /// seeded with a few extras so its push is non-trivial; the subject here is
  /// the responder's large reply, so `b` carries the bulk.
  pub fn two_node_join_small_window_responder_heavy(
    a: SocketAddr,
    b: SocketAddr,
    extra_peers: usize,
  ) -> Self {
    let mut c = Self::empty();
    c.shrink_window = true;
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&b) {
      for i in 0..extra_peers {
        let paddr: SocketAddr = format!("198.51.100.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_packet(
          b,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("bextra-{i}")), paddr),
          )),
          now,
        );
      }
    }
    if let Some(n) = c.nodes.get_mut(&a) {
      // A small, non-trivial push from the initiator (a few ghosts) so the
      // request side is exercised too; the responder still carries the bulk.
      for i in 0..4 {
        let paddr: SocketAddr = format!("203.0.113.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_packet(
          a,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("aextra-{i}")), paddr),
          )),
          now,
        );
      }
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b` over QUIC with a SHORT `stream_timeout` AND a small per-
  /// stream quinn receive window, so:
  ///   - the bridge's original accept-time deadline (`accept_time +
  ///     stream_timeout`) is tight relative to the post-`stream_load_response`
  ///     response write window (`now + 5s`);
  ///   - `a_extras` extra Alive ghosts on the INITIATOR make the push
  ///     REQUEST several tiny-window worths so it takes a few ticks to
  ///     arrive at `b`, eating into the accept-time deadline;
  ///   - `b_extras` extra Alive ghosts on the RESPONDER make the push/pull
  ///     RESPONSE also several tiny-window worths so its backpressured
  ///     delivery strictly crosses the original accept-time deadline while
  ///     staying within the post-refresh `now + 5s` window.
  ///
  /// Regression coverage for response-deadline refresh: when
  /// `stream_load_response` writes the response window into the inner
  /// stream's deadline (`now + 5s`), `Bridge.self.deadline` must be
  /// refreshed to that same value. Otherwise the bridge would keep its
  /// original `accept_time + stream_timeout` deadline and the
  /// `Done`-but-unflushed abandon path would fire when virtual time
  /// crosses the stale deadline, resetting the backpressured response
  /// tail before `a` receives the high-index `bextra-*` entries. With the
  /// refreshed deadline the abandon holds until the actual response
  /// window elapses and the full reply delivers.
  pub fn two_node_join_short_stream_timeout_both_heavy(
    a: SocketAddr,
    b: SocketAddr,
    a_extras: usize,
    b_extras: usize,
    stream_timeout: Duration,
  ) -> Self {
    let mut c = Self::empty();
    c.shrink_window = true;
    c.stream_timeout = Some(stream_timeout);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&b) {
      for i in 0..b_extras {
        let paddr: SocketAddr = format!("198.51.100.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_packet(
          b,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("bextra-{i}")), paddr),
          )),
          now,
        );
      }
    }
    if let Some(n) = c.nodes.get_mut(&a) {
      for i in 0..a_extras {
        let paddr: SocketAddr = format!("203.0.113.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_packet(
          a,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("aextra-{i}")), paddr),
          )),
          now,
        );
      }
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// Begin a subsequent (post-join) push/pull from `host` to `peer`. Used to
  /// prove a later reliable exchange opens a fresh bidi stream on the SAME
  /// pooled connection (no new handshake) after an earlier back-pressured
  /// exchange on it has been reaped.
  pub fn trigger_push_pull(&mut self, host: SocketAddr, peer: SocketAddr) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.start_push_pull(peer, PushPullKind::Refresh, now);
    }
  }

  /// Begin a push/pull from `host` to `peer` via the HIGH-LEVEL
  /// [`QuicEndpoint::start_push_pull`] wrapper (not `endpoint_mut()`). The
  /// wrapper runs `service_dials(now)` AND the zero-time outbound flush
  /// before returning, so the dial's Initial datagram (fresh dial) or the
  /// freshly-opened bridge's request bytes (pooled-Established dial)
  /// emerge on the very next [`QuicEndpoint::poll_transmit`] — without
  /// requiring a same-instant `handle_timeout` pre-pump. Paired with
  /// [`Self::step_via_poll_timeout_only`] this lets a test prove a driver
  /// that uses ONLY the public Sans-I/O poll surface drives a `start_*`-
  /// initiated exchange to completion.
  pub fn start_push_pull_via_high_level_api(
    &mut self,
    host: SocketAddr,
    peer: SocketAddr,
    kind: PushPullKind,
  ) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.start_push_pull(peer, kind, now);
    }
  }

  /// Send a one-way reliable user message from `host` to `peer` over QUIC
  /// (delegates to [`Endpoint::start_user_message`]). The exchange opens an
  /// outbound bidi stream, writes the `UserData` frame, and is one-way (no
  /// reply): the inbound side decodes it, emits `Event::UserPacket`, and the
  /// inner `Stream` goes straight to `Done` with NO outbound bytes — the
  /// exact shape whose bridge must still `finish()` its (empty) send half and
  /// be reaped instead of leaking.
  pub fn start_user_message(&mut self, host: SocketAddr, peer: SocketAddr, payload: &[u8]) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      // Ignoring Err: a leaving/left host starts no new reliable user message;
      // the harness treats that as a no-op.
      let _ = n.start_user_message(peer, bytes::Bytes::copy_from_slice(payload), now);
    }
  }

  /// Advance virtual time and drive a probe round on `host` (mirrors the
  /// plain harness `trigger_probe`).
  pub fn trigger_probe(&mut self, host: SocketAddr) -> bool {
    let now = self.clock.now();
    match self.nodes.get_mut(&host) {
      Some(n) => n.start_probe(now),
      None => false,
    }
  }

  /// Inject a Suspect at `host` about `target` attributed to `from`.
  pub fn inject_suspect(&mut self, host: SocketAddr, target: SmolStr, from: SmolStr, inc: u32) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.handle_packet(host, Message::Suspect(Suspect::new(inc, target, from)), now);
    }
  }

  /// Inject an Alive at `host` about `peer` (e.g. a higher-incarnation
  /// refute).
  pub fn inject_alive(&mut self, host: SocketAddr, peer: SmolStr, paddr: SocketAddr, inc: u32) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.handle_packet(
        host,
        Message::Alive(Alive::new(inc, Node::new(peer, paddr))),
        now,
      );
    }
  }

  /// Encode an Ack for `seq` and feed it to `host`'s public UDP demux
  /// from `from` via [`QuicEndpoint::handle_udp`] ONLY — without
  /// running the driver's drain / decode / `handle_packet` /
  /// `handle_timeout` follow-up. Used by the buffered-only regression
  /// test that observes `handle_udp`'s side-effects in isolation: the
  /// contract is "buffered-only on `Class::Memberlist`" — no probe /
  /// gossip / push-pull scheduler may fire inside `handle_udp` before
  /// the codec-owning driver decodes and feeds the buffered datagram.
  pub fn inject_memberlist_handle_udp_only(
    &mut self,
    host: SocketAddr,
    from: SocketAddr,
    seq: u32,
  ) {
    let now = self.clock.now();
    let Some(node) = self.nodes.get_mut(&host) else {
      return;
    };
    let msg: Message<SmolStr, SocketAddr> = Message::Ack(Ack::new(seq));
    let Some(bytes) = encode_frame(&msg) else {
      return;
    };
    node.handle_udp(from, &bytes, now);
  }

  /// Drain every outbound datagram every coordinator currently has
  /// queued (both QUIC-class and memberlist-class) AND every queued
  /// inbound memberlist datagram, dropping all of them. Pairs with
  /// [`Self::inject_memberlist_handle_udp_only`] for the buffered-only
  /// regression test: after `advance` deliveries the queue may contain
  /// not-yet-delivered handshake / probe frames whose presence would
  /// otherwise confuse the buffered-only assertion below.
  pub fn drain_pending_transmits_for_test(&mut self) {
    let addrs: Vec<SocketAddr> = self.nodes.keys().copied().collect();
    for a in addrs {
      let Some(node) = self.nodes.get_mut(&a) else {
        continue;
      };
      while node.poll_transmit().is_some() {}
      while node.poll_memberlist_transmit().is_some() {}
      while node.poll_memberlist_ingress().is_some() {}
    }
    self.queue.clear();
  }

  /// Drain `host`'s public `poll_memberlist_ingress` queue, returning
  /// the count of raw datagrams the demux had buffered. Used by the
  /// buffered-only regression test to assert that
  /// `handle_udp(Memberlist)` buffered the inbound datagram (the SOLE
  /// observable side-effect of the buffered-only contract).
  pub fn drain_memberlist_ingress_count_for_test(&mut self, host: SocketAddr) -> usize {
    let Some(node) = self.nodes.get_mut(&host) else {
      return 0;
    };
    let mut count = 0usize;
    while node.poll_memberlist_ingress().is_some() {
      count += 1;
    }
    count
  }

  /// Drain `host`'s public `poll_transmit` AND `poll_memberlist_transmit`
  /// queues, returning the total count. Used by the buffered-only
  /// regression test to assert that `handle_udp(Memberlist)` did NOT
  /// advance time (which would have produced one or more outbound
  /// datagrams from any scheduler that fired in `run_tick`).
  pub fn drain_transmits_count_for_test(&mut self, host: SocketAddr) -> usize {
    let Some(node) = self.nodes.get_mut(&host) else {
      return 0;
    };
    let mut count = 0usize;
    while node.poll_transmit().is_some() {
      count += 1;
    }
    while node.poll_memberlist_transmit().is_some() {
      count += 1;
    }
    count
  }

  /// Begin a graceful leave on `host` (delegates to
  /// [`QuicEndpoint::leave`]).
  pub fn leave(&mut self, host: SocketAddr) -> Result<(), memberlist_proto::Error> {
    let now = self.clock.now();
    match self.nodes.get_mut(&host) {
      Some(n) => n.leave(now),
      None => Ok(()),
    }
  }

  /// Deliver a raw gossip datagram directly into `host`'s UDP demux,
  /// bypassing the datagram queue and fault injection. Used by tests that need
  /// to inject a crafted datagram (e.g. a compressed datagram with trailing
  /// junk) without going through the normal outbound path.
  pub fn inject_raw_gossip(&mut self, from: SocketAddr, host: SocketAddr, bytes: &[u8]) {
    let now = self.clock.now();
    self.deliver_one(from, host, bytes, now);
  }

  /// Advance virtual time by `by`, then deliver matured datagrams and tick
  /// every node (mirrors [`Cluster::advance`](crate::cluster::Cluster)).
  pub fn advance(&mut self, by: Duration) {
    self.clock.advance(by);
    let now = self.clock.now();
    self.deliver_ready(now);
    self.tick_all(now);
  }

  /// Advance the virtual clock by `by` but do NOT deliver matured
  /// datagrams or tick any node. Used by tests that need to take a
  /// public-API action AT a known clock instant where the next
  /// `handle_*` call is the one that should observe the new `now`.
  pub fn advance_clock_only(&mut self, by: Duration) {
    self.clock.advance(by);
  }

  /// Idle the simulation forward by `by` with NO traffic so every QUIC
  /// connection passes `max_idle_timeout` and is drained-reaped.
  pub fn idle_for(&mut self, by: Duration) {
    // Drop everything queued and block all delivery for this stretch, then
    // step the clock forward in coarse increments, ticking each node so its
    // quinn connections observe the idle timeout and wind down.
    self.queue.clear();
    let target = self.clock.now() + by;
    while self.clock.now() < target {
      self.clock.advance(Duration::from_secs(1));
      let now = self.clock.now().min(target);
      self.tick_all(now);
      // Discard any close/drain datagrams — the peer is "gone".
      self.queue.clear();
    }
  }

  /// Inject a stray QUIC datagram at `host` for a (now reaped) connection:
  /// must be a no-op (no panic).
  pub fn inject_stray_quic_datagram(&mut self, host: SocketAddr) {
    let now = self.clock.now();
    let bogus: SocketAddr = "198.51.100.7:4433".parse().unwrap();
    if let Some(n) = self.nodes.get_mut(&host) {
      // A short-header-shaped datagram (first byte >= 0x40) for a CID the
      // endpoint no longer knows: quinn must classify-and-drop it.
      n.handle_udp(bogus, &[0x40, 0x01, 0x02, 0x03, 0x04, 0x05], now);
    }
  }

  // ── Observation ───────────────────────────────────────────────────────────

  /// Virtual time elapsed since the clock origin (for bounding a step loop
  /// by simulated duration rather than a raw iteration count).
  pub fn elapsed(&self) -> Duration {
    self.clock.elapsed()
  }

  /// `true` if `host` currently sees `peer` Alive.
  pub fn sees_alive(&self, host: SocketAddr, peer: &SmolStr) -> bool {
    self.member_state(host, peer) == Some(memberlist_proto::typed::State::Alive)
  }

  /// `host`'s gossip-tracked liveness state for `peer`, if known.
  pub fn member_state(
    &self,
    host: SocketAddr,
    peer: &SmolStr,
  ) -> Option<memberlist_proto::typed::State> {
    self.nodes.get(&host)?.endpoint_ref().member_liveness(peer)
  }

  /// `true` if `host` has EVER observed `peer` in `Suspect` (scanned every
  /// tick, so a later-refuted transient Suspect is still caught).
  pub fn ever_suspected(&self, host: SocketAddr, peer: &SmolStr) -> bool {
    self.suspected.get(&host).is_some_and(|s| s.contains(peer))
  }

  /// `true` if `host` has EVER observed `peer` `Alive` (scanned every tick).
  /// Used to assert a D1 merge reached the `Endpoint` even if normal SWIM
  /// later transitioned the peer away after the connection was torn down.
  pub fn ever_saw_alive(&self, host: SocketAddr, peer: &SmolStr) -> bool {
    self.ever_alive.get(&host).is_some_and(|s| s.contains(peer))
  }

  /// `true` if `host` has EVER observed `peer` `Dead` or `Left` (scanned
  /// every tick). Used to assert a leave/death transition was observed even
  /// after the dead/left entry is later garbage-collected to absent.
  pub fn ever_saw_gone(&self, host: SocketAddr, peer: &SmolStr) -> bool {
    self.ever_gone.get(&host).is_some_and(|s| s.contains(peer))
  }

  /// `true` once `host` has emitted `Event::LeftCluster`.
  pub fn left_cluster(&self, host: SocketAddr) -> bool {
    self.left.contains(&host)
  }

  /// `true` if `host` has observed a reliable `Event::UserPacket` with
  /// exactly `payload` as its body, for asserting a one-way QUIC user message
  /// was delivered. Presence semantics (set dedup) — see [`user_packets`].
  pub fn received_user_payload(&self, host: SocketAddr, payload: &[u8]) -> bool {
    self
      .user_packets
      .get(&host)
      .is_some_and(|s| s.iter().any(|b| b.as_ref() == payload))
  }

  /// Number of live (non-reaped) QUIC connections `host` holds toward any
  /// known peer (`0` after a drained-reap).
  pub fn live_quic_connections(&self, host: SocketAddr) -> usize {
    let Some(node) = self.nodes.get(&host) else {
      return 0;
    };
    self
      .nodes
      .keys()
      .filter(|&&peer| peer != host)
      .map(|&peer| node.live_connections_to(peer))
      .sum()
  }

  /// Number of in-flight reliable-exchange bridges `host` currently holds
  /// (one per active push/pull or reliable-ping stream). After a warm-up
  /// join has fully settled this is `0` until a reliable-ping fallback (or a
  /// fresh push/pull) opens one — so a test can observe the fallback being
  /// genuinely exercised, then assert it completed without leaking a bridge.
  pub fn live_bridge_count(&self, host: SocketAddr) -> usize {
    self.nodes.get(&host).map_or(0, |n| n.live_bridge_count())
  }

  /// Every QUIC datagram the harness routed through the virtual reliable pipe
  /// so far. The encryption-conformance suite asserts that no entry begins
  /// with [`memberlist_proto::ENCRYPTED_TAG`] — the QUIC bridge skips
  /// reliable-path encryption (quinn-encrypted streams already provide
  /// confidentiality), and a quinn UDP datagram's first byte has
  /// `b & 0xC0 != 0` (>= `0x40`) by the QUIC long/short-header bit pattern,
  /// disjoint from the memberlist tag space (`1..=15`) that contains
  /// `ENCRYPTED_TAG`. Only compiled under the `__sim-encryption-aes-gcm`
  /// feature.
  #[cfg(feature = "__sim-encryption-aes-gcm")]
  pub fn observed_reliable_wire_bytes(&self) -> &[Vec<u8>] {
    &self.observed_reliable_wire_bytes
  }

  /// Simulate an external driver draining `host`'s [`QuicEndpoint::poll_event`]
  /// queue: every event popped is silently discarded (no re-queue). Returns
  /// the number of events drained.
  ///
  /// `DialRequested` is coordinator-internal — the public `poll_event` MUST
  /// sieve it into the private `dial_pending` deque before any external
  /// caller ever observes it. Calling this between a `handle_timeout`
  /// (which produces the `DialRequested`) and the next `service_dials`
  /// (which would normally consume it) is the precise sequence a real
  /// external driver might run to surface membership events to the
  /// application — if the sieve is missing, the dial retry token is
  /// silently lost and the pending stream intent orphans.
  pub fn external_poll_event_drain(&mut self, host: SocketAddr) -> usize {
    let mut n = 0;
    if let Some(node) = self.nodes.get_mut(&host) {
      while node.poll_event().is_some() {
        n += 1;
      }
    }
    n
  }

  /// Try to open a unidirectional QUIC stream from `host` to `peer` at the
  /// protocol layer (`quinn_proto::Streams::open(Dir::Uni)` on the pooled
  /// connection). Returns the would-be `StreamId` on success or `None` if
  /// the protocol-layer credit for remotely-initiated unidirectional
  /// streams is exhausted (`state.next[Uni] >= state.max[Uni]`).
  ///
  /// Regression coverage for the refusal of peer-attempted unidirectional
  /// streams: `QuicOptions::new` forces `max_concurrent_uni_streams = 0`
  /// on the shared transport config, so the peer's advertised
  /// `initial_max_streams_uni` is zero, `host`'s view of `state.max[Uni]`
  /// is zero, and the open returns `None` without generating any bytes
  /// on the wire. Without that enforcement, quinn's default (100
  /// concurrent uni streams) would let the open succeed.
  pub fn try_open_uni_stream(&mut self, host: SocketAddr, peer: SocketAddr) -> bool {
    let now = self.clock.now();
    self
      .nodes
      .get_mut(&host)
      .map(|n| n.try_open_uni_stream_to(peer, now))
      .unwrap_or(false)
  }

  /// Drain ONE quinn datagram from `host`'s
  /// [`QuicEndpoint::poll_transmit`] without enqueueing it. Returns the
  /// destination + raw bytes if one was queued. Used by the strict-poll
  /// regression test to assert that an Initial datagram (fresh dial) or
  /// the freshly-opened bridge's request bytes (pooled-Established dial)
  /// are queued on the driver-facing surface AT THE SAME INSTANT
  /// [`QuicEndpoint::start_push_pull`] returns — the same-instant
  /// transmit observable.
  pub fn pop_transmit_of(&mut self, host: SocketAddr) -> Option<(SocketAddr, bytes::Bytes)> {
    self.nodes.get_mut(&host)?.poll_transmit()
  }

  /// Probe `host`'s [`QuicEndpoint::poll_timeout`] directly (without
  /// running a step). Returns the same `Option<Instant>` an external
  /// driver would consume. Used by the no-orphan-dial regression to
  /// assert the immediate-due wake landed at `<= clock_before` after an
  /// external poll-event drain sieved an unattempted entry into
  /// `dial_pending`.
  pub fn poll_timeout_of(&mut self, host: SocketAddr) -> Option<Instant> {
    self.nodes.get_mut(&host)?.poll_timeout()
  }

  /// The current virtual-clock instant. Same `Instant` injected into
  /// `handle_*` calls inside the harness; load-bearing for the
  /// no-orphan-dial regression's `poll_timeout_of(a) <=
  /// clock_instant_now()` assertion (the immediate-due wake observable).
  pub fn clock_instant_now(&self) -> Instant {
    self.clock.now()
  }

  /// Run `handle_timeout(now)` on `host` ONLY. No drain, no delivery, no
  /// scan_events. Used by the leave-completion regression to advance
  /// `host`'s tick without touching `poll_memberlist_transmit` (so the
  /// dead-self leave tail produced by a preceding `leave(now)` stays
  /// queued inside the inner `Endpoint` and `Event::LeftCluster` is NOT
  /// yet observable through `poll_event`).
  pub fn tick_only(&mut self, host: SocketAddr) {
    let now = self.clock.now();
    if let Some(node) = self.nodes.get_mut(&host) {
      node.handle_timeout(now);
    }
  }

  /// Non-destructively peek whether `host`'s `poll_event` currently has
  /// `Event::LeftCluster` queued. Drains every event into a local vector,
  /// records the match, then requeues each event in its original FIFO
  /// order via [`QuicEndpoint::requeue_event`]. Used by the
  /// leave-completion regression to assert `LeftCluster` is NOT
  /// observable before the dead-self tail has crossed
  /// `poll_memberlist_transmit`.
  pub fn host_has_left_cluster_pending(&mut self, host: SocketAddr) -> bool {
    let now = self.clock.now();
    let Some(node) = self.nodes.get_mut(&host) else {
      return false;
    };
    let mut buf: Vec<Event<SmolStr, SocketAddr>> = Vec::new();
    while let Some(ev) = node.poll_event() {
      buf.push(ev);
    }
    let seen = buf.iter().any(|e| matches!(e, Event::LeftCluster));
    for ev in buf {
      node.requeue_event(ev, now);
    }
    seen
  }

  /// Drain ONE `poll_memberlist_transmit` from `host`, encode the typed
  /// Transmit (Packet or Compound) into a single plain-framed datagram,
  /// enqueue it (faults applied), and immediately advance the queue to
  /// deliver any datagram whose `deliver_at <= now`. Returns `true` if a
  /// Transmit was popped (the next call after the source queue empties
  /// returns `false`).
  ///
  /// The single-pop semantics are load-bearing for the leave-completion
  /// regression: it proves that each pop is the moment the
  /// leave-completion counter ticks, so the FINAL pop (and only the
  /// final pop) is what makes
  /// `Event::LeftCluster` observable through `poll_event` on the next
  /// read.
  pub fn drain_one_memberlist_transmit(&mut self, host: SocketAddr) -> bool {
    let now = self.clock.now();
    let popped = {
      let Some(node) = self.nodes.get_mut(&host) else {
        return false;
      };
      node.poll_memberlist_transmit()
    };
    let Some(tx) = popped else {
      return false;
    };
    let mut pending: Vec<(SocketAddr, bytes::Bytes)> = Vec::new();
    match tx {
      Transmit::Packet(p) => {
        let (to, message) = p.into_parts();
        if let Some(b) = encode_frame(&message) {
          pending.push((to, b));
        }
      }
      Transmit::Compound(cmp) => {
        let (to, messages) = cmp.into_parts();
        let mut buf = Vec::new();
        for m in &messages {
          if let Some(b) = encode_frame(m) {
            buf.extend_from_slice(&b);
          }
        }
        if !buf.is_empty() {
          pending.push((to, bytes::Bytes::from(buf)));
        }
      }
    }
    for (to, bytes) in pending {
      // Compress THEN encrypt to match the main `drain_and_enqueue` egress
      // shape (each identity when the corresponding option is disabled), so a
      // single-pop drain produces the same on-wire bytes as the normal path.
      // An encrypt failure (e.g. backend not built in) drops the gossip —
      // emitting plaintext on the configured-encrypted egress would bypass
      // authentication. SWIM gossip is lossy and self-healing.
      let node = self.nodes.get(&host).unwrap();
      let compressed = node.compress_gossip(&bytes);
      let on_wire = match node.encrypt_gossip(&compressed) {
        Ok(b) => b,
        Err(_) => continue,
      };
      self.enqueue(host, to, bytes::Bytes::from(on_wire), now);
    }
    self.deliver_ready(now);
    true
  }

  // ── Step loop (mirrors Cluster::step) ─────────────────────────────────────

  /// One simulation tick. Returns `true` if anything happened.
  ///
  /// 1. Drain transmits from every node — quinn datagrams via
  ///    [`QuicEndpoint::poll_transmit`], unreliable memberlist via
  ///    [`QuicEndpoint::poll_memberlist_transmit`] (then plain-frame
  ///    encoded). Enqueue with [`FaultConfig`].
  /// 2. Advance to the next deadline (`min` over every node's
  ///    [`QuicEndpoint::poll_timeout`] and the earliest queued datagram).
  /// 3. Deliver matured datagrams through the public first-byte demux
  ///    ([`QuicEndpoint::handle_udp`]): `>= 0x40` is demuxed into quinn;
  ///    `1..=15` is surfaced raw via
  ///    [`QuicEndpoint::poll_memberlist_ingress`], decoded here, and fed back
  ///    through [`QuicEndpoint::handle_packet`], then a coordinator tick.
  /// 4. Tick every node, then scan `poll_event` (record Suspect /
  ///    LeftCluster) and apply the one-shot D1 hooks.
  pub fn step(&mut self) -> bool {
    let now = self.clock.now();
    let mut progressed = false;
    let addrs: Vec<SocketAddr> = self.nodes.keys().copied().collect();

    // 0. Pump every coordinator at the current instant so a freshly-queued
    //    `DialRequested` (from `start_push_pull` / a fallback dial), an
    //    accept, every bridge byte-pump, and `collect_transmits` all run
    //    BEFORE the outbound datagrams are drained. `QuicEndpoint` produces
    //    `out`/`packet_out` only inside its `run_tick` (driven by
    //    `handle_timeout`), so without this a brand-new dial would never emit
    //    its handshake datagram and the simulation would wedge at step 0.
    //    (The plain `Cluster::step` gets this for free via its explicit
    //    `process_dial_requests` / `step_streams` phases — here it is one
    //    coordinator tick.)
    for a in &addrs {
      self.nodes.get_mut(a).unwrap().handle_timeout(now);
    }

    // 1. Drain every coordinator's outbound datagrams → enqueue.
    if self.drain_and_enqueue(&addrs, now) {
      progressed = true;
    }

    // 2. Advance to the next deadline = min(earliest queued datagram, every
    //    coordinator's unified poll_timeout). Advancing the clock toward a
    //    real pending deadline IS forward progress: the QUIC handshake /
    //    push-pull needs several virtual-time round-trips, and a tick whose
    //    only effect is producing the next flight (drained next iteration)
    //    must NOT look quiescent — otherwise a `while c.step()` loop would
    //    abort the exchange mid-handshake.
    let mut next: Option<Instant> = self.queue.iter().map(|d| d.deliver_at).min();
    for a in &addrs {
      if let Some(t) = self.nodes.get_mut(a).unwrap().poll_timeout() {
        next = Some(next.map_or(t, |b| b.min(t)));
      }
    }
    let Some(next) = next else {
      // No queued datagrams and no pending timer anywhere: truly idle.
      return progressed;
    };
    if next > now {
      self.clock.advance(next - now);
      progressed = true;
    }
    let now = self.clock.now();

    // 3. Deliver matured datagrams.
    if self.deliver_ready(now) > 0 {
      progressed = true;
    }

    // 4. Tick every node, scan events, apply the one-shot D1 hooks, then
    //    drain again so THIS step captures the datagrams its own final tick
    //    produced (no one-step pipeline lag → no spurious quiescence).
    self.tick_all(now);
    if self.scan_events() {
      progressed = true;
    }
    self.apply_reply_hooks(now);
    if self.drain_and_enqueue(&addrs, now) {
      progressed = true;
    }

    progressed
  }

  /// One simulation tick that ADVANCES ONLY by `QuicEndpoint::poll_timeout`.
  ///
  /// Mirrors [`Self::step`] except for the omitted "pump every coordinator
  /// before reading `poll_timeout`" stage (step 0). The wake instant is
  /// determined purely from each node's [`QuicEndpoint::poll_timeout`] and
  /// from the earliest queued datagram. When `poll_timeout` returns an
  /// instant `<= now` (the immediate-due wake of an unattempted
  /// `dial_pending` entry, or an already-elapsed deadline), the clock is
  /// NOT advanced and `tick_all(now)` fires the same logical instant.
  ///
  /// Used by the no-orphan-dial regression
  /// `external_poll_event_drain_then_advance_only_by_poll_timeout_does_not_orphan_dial`
  /// to prove that a `DialRequested` sieved out of the inner endpoint by
  /// `QuicEndpoint::poll_event` (an external poll-event drain between
  /// `start_push_pull` and the next `service_dials`) surfaces as an
  /// immediate-due wake out of `poll_timeout`, so a caller that does not
  /// unconditionally pre-pump every coordinator still services the dial
  /// in real time (no orphaning).
  pub fn step_via_poll_timeout_only(&mut self) -> bool {
    let now = self.clock.now();
    let mut progressed = false;
    let addrs: Vec<SocketAddr> = self.nodes.keys().copied().collect();

    // (No pre-pump.) Drain whatever each coordinator already had in
    // `out`/`packet_out` from a prior tick.
    if self.drain_and_enqueue(&addrs, now) {
      progressed = true;
    }

    let mut next: Option<Instant> = self.queue.iter().map(|d| d.deliver_at).min();
    for a in &addrs {
      if let Some(t) = self.nodes.get_mut(a).unwrap().poll_timeout() {
        next = Some(next.map_or(t, |b| b.min(t)));
      }
    }
    let Some(next) = next else {
      return progressed;
    };
    if next > now {
      self.clock.advance(next - now);
      progressed = true;
    }
    let now = self.clock.now();

    if self.deliver_ready(now) > 0 {
      progressed = true;
    }

    // The tick fires at `now` regardless of whether the clock advanced or
    // the wake was immediate-due: an unattempted `dial_pending` entry is
    // serviced here by `service_dials`.
    self.tick_all(now);
    if self.scan_events() {
      progressed = true;
    }
    self.apply_reply_hooks(now);
    if self.drain_and_enqueue(&addrs, now) {
      progressed = true;
    }

    progressed
  }

  /// Drain every coordinator's `poll_transmit` (quinn datagrams) and
  /// `poll_memberlist_transmit` (typed unreliable Transmit → plain-frame
  /// encoded), enqueueing each as ONE datagram. Returns `true` if anything
  /// was enqueued.
  fn drain_and_enqueue(&mut self, addrs: &[SocketAddr], now: Instant) -> bool {
    let mut any = false;
    for src in addrs {
      // Quinn transport datagrams (handshake / stream data / ACK / close) —
      // opaque quinn-proto bytes. The memberlist gossip compressor must not
      // touch these: they are not memberlist frames and their first byte does
      // not follow the memberlist tag space.
      let mut quic_pending: Vec<(SocketAddr, bytes::Bytes)> = Vec::new();
      // Memberlist gossip datagrams — plain-frame encoded, eligible for
      // compression through the source coordinator's configured codec.
      let mut gossip_pending: Vec<(SocketAddr, bytes::Bytes)> = Vec::new();
      let node = self.nodes.get_mut(src).unwrap();
      while let Some((to, bytes)) = node.poll_transmit() {
        quic_pending.push((to, bytes));
      }
      // Unreliable memberlist Transmit — drained one-at-a-time through the
      // coordinator's accessor (which calls `Endpoint::poll_transmit`
      // directly, so the leave-completion counter ticks at the exact moment
      // each datagram crosses to the driver — never on a coordinator-internal
      // buffer hop), then plain-frame encoded with the machine's own wire
      // codec so the first-byte demux classifies it `Memberlist`.
      while let Some(tx) = node.poll_memberlist_transmit() {
        match tx {
          Transmit::Packet(p) => {
            let (to, message) = p.into_parts();
            if let Some(b) = encode_frame(&message) {
              gossip_pending.push((to, b));
            }
          }
          Transmit::Compound(cmp) => {
            // A compound is ONE datagram: concatenate the plain frames so a
            // single fault/latency decision covers the whole bundle (a real
            // driver `encode_outgoing_compound`s into one `send_to`).
            let (to, messages) = cmp.into_parts();
            let mut buf = Vec::new();
            for m in &messages {
              if let Some(b) = encode_frame(m) {
                buf.extend_from_slice(&b);
              }
            }
            if !buf.is_empty() {
              gossip_pending.push((to, bytes::Bytes::from(buf)));
            }
          }
        }
      }
      for (to, bytes) in quic_pending {
        // Reliable-wire observation tap: every quinn UDP datagram is the
        // wire form of the QUIC reliable path (quinn streams ride atop these
        // datagrams). Append the bytes verbatim so the encrypted conformance
        // suite can pin that no inner `[Encrypted[..]]` wrapper ever surfaces
        // here — the QUIC bridge skips reliable-path encryption, so the
        // plaintext units handed to quinn never carry an `Encrypted` wrapper
        // either; quinn then encrypts them into datagrams whose first byte
        // has `b & 0xC0 != 0` (>= `0x40`), disjoint from the memberlist tag
        // space that contains `ENCRYPTED_TAG`.
        #[cfg(feature = "__sim-encryption-aes-gcm")]
        self.observed_reliable_wire_bytes.push(bytes.to_vec());
        if self.enqueue(*src, to, bytes, now) {
          any = true;
        }
      }
      for (to, bytes) in gossip_pending {
        // Compress THEN encrypt the outbound gossip datagram through the
        // source coordinator's configured transforms (each identity when
        // disabled). The on-wire byte order is
        // `[Encrypted[Compressed[frame]]]` when both are enabled; the
        // receiver reverses the order on ingress. The outermost wrapper tag
        // stays in the memberlist tag space (`1..=15`), so the first-byte
        // demux still classifies the datagram `Memberlist`. An encrypt
        // failure (e.g. backend not built in) drops the gossip — emitting
        // plaintext on the configured-encrypted egress would bypass
        // authentication. SWIM gossip is lossy and self-healing.
        let node = self.nodes.get(src).unwrap();
        let compressed = node.compress_gossip(&bytes);
        let on_wire = match node.encrypt_gossip(&compressed) {
          Ok(b) => b,
          Err(_) => continue,
        };
        if self.enqueue(*src, to, bytes::Bytes::from(on_wire), now) {
          any = true;
        }
      }
    }
    any
  }

  // ── Internals (mirror Network) ────────────────────────────────────────────

  /// Enqueue one datagram, applying fault injection. Returns `true` if it
  /// was actually queued (not dropped).
  fn enqueue(
    &mut self,
    from: SocketAddr,
    to: SocketAddr,
    bytes: bytes::Bytes,
    now: Instant,
  ) -> bool {
    let first = bytes.first().copied();
    let is_memberlist = matches!(first, Some(b) if (1..=15).contains(&b));
    let is_quic = matches!(first, Some(b) if b & 0xC0 != 0);
    // A memberlist plain frame between a probe-blocked pair is dropped —
    // models "direct + indirect UDP probes dropped" while leaving the QUIC
    // (`>= 0x40`) reliable path intact (so the reliable-ping fallback runs).
    if is_memberlist
      && self
        .probe_block
        .iter()
        .any(|&(x, y)| (x == from && y == to) || (x == to && y == from))
    {
      return false;
    }
    // A QUIC datagram between a quic-blocked pair is dropped while that
    // pair's memberlist frames keep flowing — models "the QUIC connection
    // died but unreliable SWIM continues" (the D1 hooks).
    if is_quic && self.is_quic_blocked_pair(from, to) {
      return false;
    }
    // Directional QUIC drop (credit starvation): only the exact `from -> to`
    // orientation is dropped, so the starved responder can still transmit
    // its doomed retries (it just never receives the credit/ACKs that would
    // unblock them).
    if is_quic
      && self
        .quic_block_directional
        .iter()
        .any(|&(x, y)| x == from && y == to)
    {
      return false;
    }
    if !self.faults.should_deliver(from, to) {
      return false;
    }
    let deliver_at = now + self.faults.latency;
    self.queue.push_back(PendingDatagram {
      deliver_at,
      from,
      to,
      bytes,
    });
    true
  }

  /// Deliver every datagram whose `deliver_at <= now`. Returns the count.
  fn deliver_ready(&mut self, now: Instant) -> usize {
    self.queue.make_contiguous().sort_by_key(|d| d.deliver_at);
    let mut delivered = 0;
    let mut remaining = VecDeque::new();
    while let Some(d) = self.queue.pop_front() {
      if d.deliver_at > now {
        remaining.push_back(d);
        continue;
      }
      if self.nodes.contains_key(&d.to) {
        self.deliver_one(d.from, d.to, &d.bytes, now);
        delivered += 1;
      }
    }
    self.queue = remaining;
    delivered
  }

  /// Deliver one datagram to its destination through the coordinator's
  /// PUBLIC first-byte-demux ingress (`handle_udp`) for BOTH transports:
  /// `>= 0x40` is demuxed into quinn; `1..=15` is classified `Memberlist` and
  /// surfaced raw via `poll_memberlist_ingress` (the coordinator has no codec
  /// dep, so it never decodes or silently drops them). The harness — the
  /// codec-owning layer here — drains that, decodes each plain frame, and
  /// feeds the typed message(s) back through the coordinator's public
  /// `handle_packet`, then runs a coordinator tick. The inner
  /// `Endpoint::handle_packet` is NEVER called directly: routing through the
  /// public ingress is what exercises the composed unit's real UDP path.
  fn deliver_one(&mut self, from: SocketAddr, to: SocketAddr, bytes: &[u8], now: Instant) {
    let Some(node) = self.nodes.get_mut(&to) else {
      return;
    };
    // One conceptual UDP socket: every inbound datagram (QUIC or memberlist)
    // enters through the public first-byte demux. A `Class::Memberlist`
    // datagram is buffered by the coordinator and surfaced via
    // `poll_memberlist_ingress` (never decoded in-crate, never dropped).
    node.handle_udp(from, bytes, now);
    // Drain every raw memberlist datagram the demux classified, decode its
    // plain frame(s), and feed each typed message back through the public
    // `handle_packet` pass-through (a compound datagram is several
    // concatenated frames in one buffer). Then tick the coordinator so the
    // endpoint processes the fed messages and produces its outputs.
    let mut fed = false;
    while let Some((src, raw)) = node.poll_memberlist_ingress() {
      // Single-call unwrap: `decrypt_gossip` is the encryption-aware
      // unwrap that consumes the Encrypted-then-Compressed wrapper stack
      // (each identity when the wrapper is absent) and applies strict-mode
      // rejection at the entry boundary when a keyring is configured. It is
      // the coordinator's single canonical ingress helper — one call covers
      // both transforms. A corrupt wrapper drops the datagram — gossip is
      // lossy and self-healing.
      let raw = match node.decrypt_gossip(&raw) {
        Ok(plain) => plain,
        Err(_) => continue,
      };
      // Decode the whole datagram before applying anything: a gossip datagram
      // that does not decode cleanly and completely is dropped wholesale (lossy
      // and self-healing), matching the real wire's atomic compound decode.
      // Partial application of a corrupt datagram is not a drop.
      let mut decoded = Vec::new();
      let mut off = 0;
      let mut clean = true;
      while off < raw.len() {
        match decode_frame(&raw[off..]) {
          Some((consumed, msg)) if consumed > 0 => {
            decoded.push(msg);
            off += consumed;
          }
          // A decode error, or a zero-length decode (which would spin) — the
          // datagram is malformed; drop it whole.
          _ => {
            clean = false;
            break;
          }
        }
      }
      if clean && off == raw.len() {
        for msg in decoded {
          node.handle_packet(src, msg, now);
          fed = true;
        }
      }
    }
    if fed {
      node.handle_timeout(now);
    }
  }

  /// `handle_timeout(now)` on every node (mirrors `Network::tick_all`).
  fn tick_all(&mut self, now: Instant) {
    for node in self.nodes.values_mut() {
      node.handle_timeout(now);
    }
  }

  /// Scan every node's `poll_event`, recording Suspect / LeftCluster, then
  /// re-queue every event (so future scans still see late ones). Returns
  /// `true` if any event was observed. Also records the gossip-tracked
  /// liveness so a transient Suspect is caught even without an event.
  fn scan_events(&mut self) -> bool {
    let now = self.clock.now();
    let addrs: Vec<SocketAddr> = self.nodes.keys().copied().collect();
    let mut any = false;
    for host in addrs {
      // Scan the live member view, recording every peer currently Suspect
      // or Alive (a transient state later refuted/transitioned would
      // otherwise be invisible to an end-state assertion).
      #[allow(clippy::type_complexity)]
      let (suspects, alives, gone): (Vec<SmolStr>, Vec<SmolStr>, Vec<SmolStr>) = {
        let node = self.nodes.get(&host).unwrap();
        let me = node.endpoint_ref().local_id_ref().clone();
        let mut sus = Vec::new();
        let mut alv = Vec::new();
        let mut gn = Vec::new();
        for ns in node
          .endpoint_ref()
          .members()
          .filter(|ns| ns.id_ref() != &me)
        {
          match node.endpoint_ref().member_liveness(ns.id_ref()) {
            Some(memberlist_proto::typed::State::Suspect) => sus.push(ns.id_ref().clone()),
            Some(memberlist_proto::typed::State::Alive) => alv.push(ns.id_ref().clone()),
            Some(memberlist_proto::typed::State::Dead)
            | Some(memberlist_proto::typed::State::Left) => gn.push(ns.id_ref().clone()),
            _ => {}
          }
        }
        (sus, alv, gn)
      };
      if !suspects.is_empty() {
        let set = self.suspected.entry(host).or_default();
        for s in suspects {
          set.insert(s);
        }
      }
      if !alives.is_empty() {
        let set = self.ever_alive.entry(host).or_default();
        for a in alives {
          set.insert(a);
        }
      }
      if !gone.is_empty() {
        let set = self.ever_gone.entry(host).or_default();
        for g in gone {
          set.insert(g);
        }
      }
      let node = self.nodes.get_mut(&host).unwrap();
      let mut deferred = Vec::new();
      while let Some(ev) = node.poll_event() {
        any = true;
        match &ev {
          Event::LeftCluster => {
            self.left.insert(host);
          }
          Event::UserPacket(p) if p.reliability().is_reliable() => {
            self
              .user_packets
              .entry(host)
              .or_default()
              .insert(p.data_ref().clone());
          }
          _ => {}
        }
        deferred.push(ev);
      }
      for ev in deferred {
        node.requeue_event(ev, now);
      }
    }
    any
  }

  /// Apply the one-shot D1 hooks. The tick `host` FIRST merges a peer (it
  /// sees the peer `Alive` — the join push/pull reply just reached its
  /// `Endpoint`), tear the QUIC connection down THE SAME TICK:
  /// - `d1_drop_quic`: drop only the QUIC (`>= 0x40`) datagrams between the
  ///   pair from now on (the unreliable memberlist path keeps flowing). The
  ///   bridge is mid-reap as the reply lands; if D1 holds the merge is
  ///   already applied and ongoing UDP SWIM keeps the membership alive.
  /// - `d1_lose_conn`: drop EVERY QUIC datagram to/from `host` so quinn
  ///   idle-times-out → `Event::ConnectionLost` → the coordinator marks
  ///   every bridge on it fatal (the StreamErrored teardown variant).
  ///
  /// Firing AFTER `host` already merged still exercises D1: the reply
  /// reached the `Endpoint` only because the bridge drained its
  /// `PushPullReplyReceived` BEFORE the connection/stream was reaped — the
  /// test asserts `ever_alive` (the merge happened), not the steady state.
  fn apply_reply_hooks(&mut self, _now: Instant) {
    let mut newly_blocked: Vec<(SocketAddr, SocketAddr)> = Vec::new();
    if let Some((host, false)) = self.d1_drop_quic {
      if self.merged_a_peer(host) {
        for peer in self.peers_of(host) {
          newly_blocked.push((host, peer));
        }
        self.d1_drop_quic = Some((host, true));
      }
    }
    if let Some((host, false)) = self.d1_lose_conn {
      if self.merged_a_peer(host) {
        for peer in self.peers_of(host) {
          newly_blocked.push((host, peer));
        }
        self.d1_lose_conn = Some((host, true));
      }
    }
    if newly_blocked.is_empty() {
      return;
    }
    self.quic_block.extend(newly_blocked.iter().copied());
    // Drop QUIC datagrams already queued for the newly-blocked pair(s) (the
    // reply has landed; the connection now "dies" the same tick).
    let blocked = std::mem::take(&mut self.quic_block);
    self.queue.retain(|d| {
      let is_quic = d.bytes.first().is_some_and(|b| b & 0xC0 != 0);
      let pair_blocked = blocked
        .iter()
        .any(|&(x, y)| (x == d.from && y == d.to) || (x == d.to && y == d.from));
      !(is_quic && pair_blocked)
    });
    self.quic_block = blocked;
  }

  fn is_quic_blocked_pair(&self, x: SocketAddr, y: SocketAddr) -> bool {
    self
      .quic_block
      .iter()
      .any(|&(a, b)| (a == x && b == y) || (a == y && b == x))
  }

  /// `true` if `host` currently sees ANY non-local peer Alive (the join
  /// reply has merged).
  fn merged_a_peer(&self, host: SocketAddr) -> bool {
    let Some(node) = self.nodes.get(&host) else {
      return false;
    };
    let me = node.endpoint_ref().local_id_ref().clone();
    node
      .endpoint_ref()
      .members()
      .filter(|ns| ns.id_ref() != &me)
      .any(|ns| {
        node.endpoint_ref().member_liveness(ns.id_ref())
          == Some(memberlist_proto::typed::State::Alive)
      })
  }

  fn peers_of(&self, host: SocketAddr) -> Vec<SocketAddr> {
    self.nodes.keys().copied().filter(|&a| a != host).collect()
  }
}

/// Encode one typed message into a plain frame (`[TAG][VARINT len][BODY]`) —
/// the EXACT bytes the machine's own `wire.rs` produces, so the first-byte
/// demux classifies it as `Memberlist`. Stream-only / un-bridgeable messages
/// yield `None` (they never ride the unreliable path).
fn encode_frame(msg: &Message<SmolStr, SocketAddr>) -> Option<bytes::Bytes> {
  let any = message_to_any::<SmolStr, SocketAddr>(msg).ok()?;
  framing::encode_message(&any).ok().map(bytes::Bytes::from)
}

/// Decode one leading plain frame, returning `(consumed, message)`.
fn decode_frame(buf: &[u8]) -> Option<(usize, Message<SmolStr, SocketAddr>)> {
  let (consumed, any) = framing::decode_message(buf).ok()?;
  let msg = message_from_any::<SmolStr, SocketAddr>(&any).ok()?;
  Some((consumed, msg))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn frame_roundtrips_and_is_memberlist_classified() {
    let m: Message<SmolStr, SocketAddr> = Message::Alive(Alive::new(
      7,
      Node::new(SmolStr::new("x"), "127.0.0.1:9000".parse().unwrap()),
    ));
    let b = encode_frame(&m).expect("alive encodes");
    assert!(
      matches!(b.first(), Some(t) if (1..=15).contains(t)),
      "encoded memberlist frame must be demux-classified Memberlist (first byte 1..=15)"
    );
    let (n, back) = decode_frame(&b).expect("decodes");
    assert_eq!(n, b.len());
    assert!(matches!(back, Message::Alive(_)));
  }
}
