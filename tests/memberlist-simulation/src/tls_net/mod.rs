//! [`TlsCluster`]: the `tls`-gated conformance harness.
//!
//! Mirrors [`Cluster`](crate::cluster::Cluster) / [`QuicCluster`](crate::quic_net::QuicCluster)
//! exactly, driving a [`StreamEndpoint`] over the rustls-over-TCP record layer
//! (`TlsRecords`) instead of the bare `Endpoint`: reliable exchanges
//! (join/anti-entropy push-pull, reliable-ping fallback) ride a real
//! rustls-over-TCP connection, while unreliable gossip rides a plain UDP
//! socket. TLS and UDP are SEPARATE transports here (no first-byte demux —
//! that was QUIC-specific, since quinn runs over UDP), so the harness carries
//! two queues:
//!
//! - **UDP gossip** — a datagram queue ([`PendingDatagram`]) byte-identical to
//!   the QUIC harness's memberlist path: outbound is drained through
//!   [`StreamEndpoint::poll_memberlist_transmit`], plain-frame encoded with the
//!   machine's own `memberlist-wire` codec, enqueued (faults applied once per
//!   datagram), and on delivery fed through [`StreamEndpoint::handle_gossip`] →
//!   [`StreamEndpoint::poll_memberlist_ingress`] → decode →
//!   [`StreamEndpoint::handle_packet`].
//! - **A deterministic virtual TCP** ([`TcpPipe`]) — per-connection ordered
//!   byte pipes (FIFO, no reordering, no loss within a connection: TCP is
//!   reliable + ordered) with clock-driven delivery latency, connect / accept /
//!   write / read(==0 on peer half-close) / reset, plus fault injection
//!   (connect-refused, mid-stream reset, half-close). Ciphertext is drained
//!   through [`StreamEndpoint::poll_transport_transmit`], delivered with
//!   latency, and fed back through [`StreamEndpoint::handle_transport_data`]
//!   with an explicit `eof: bool` (the out-of-band TCP FIN, distinct from the
//!   in-band `close_notify` alert rustls carries); transport directives are
//!   drained through [`StreamEndpoint::poll_action`].
//!
//! Virtual time (the shared [`Clock`]) is injected as `now` into both
//! machines, and the existing [`FaultConfig`] drop/delay/partition model is
//! reused unchanged — so the SWIM timing observed here is directly comparable
//! to the plain harness (the conformance-parity capstone).

use std::{
  collections::{HashMap, HashSet, VecDeque},
  net::SocketAddr,
  sync::Arc,
  time::Duration,
};

use memberlist_proto::{
  Endpoint, EndpointOptions, Event, Instant, LabelOptions, Labeled, PushPullKind, TlsOptions,
  TlsRecords, Transmit, framing, message_from_any, message_to_any,
  streams::{ExchangeId, StreamAction, StreamEndpoint},
  typed::{Alive, Message, Node, Suspect},
};
use rand::{SeedableRng, rngs::SmallRng};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use smol_str::SmolStr;

use crate::{clock::Clock, faults::FaultConfig, virtual_tcp::TcpPipe};

/// The concrete coordinator the harness drives: the cluster-label decorator
/// over the rustls-over-TCP record layer. The reliable plane is label-gated
/// (the label rides `LabelOptions`, never `TlsOptions`) then TLS-encrypted.
type Node1 = StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>>;

/// The rustls crypto provider the harness uses, selected by the active backend
/// feature. The entire conformance suite runs under whichever is chosen
/// (`tls` → ring, `tls-rustls-aws-lc-rs` → aws-lc-rs).
#[cfg(not(feature = "tls-rustls-aws-lc-rs"))]
pub fn sim_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
  Arc::new(rustls::crypto::ring::default_provider())
}

/// See [`sim_crypto_provider`]. aws-lc-rs variant.
#[cfg(feature = "tls-rustls-aws-lc-rs")]
pub fn sim_crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
  Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Accept-any server-cert verifier — sim/test only. The deployment-grade
/// cluster-CA verification policy is the operator's; behavioural determinism
/// in the sim is the virtual clock + the deterministic virtual TCP, not crypto.
#[derive(Debug)]
struct AnyServer(Arc<rustls::crypto::CryptoProvider>);

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
    self.0.signature_verification_algorithms.supported_schemes()
  }
}

/// A fresh self-signed leaf + PKCS#8 key with `"localhost"` as the SAN (the
/// same construction the memberlist-proto tls crypto tests use).
fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
  let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  (chain, key)
}

/// A self-signed [`TlsOptions`] bundle: server with `with_no_client_auth`,
/// client with the accept-any verifier (sim/test only). Trusted-network model.
fn sim_tls_config() -> TlsOptions {
  let (chain, key) = self_signed();
  let provider = sim_crypto_provider();
  let server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .unwrap();
  let client = rustls::ClientConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AnyServer(provider)))
    .with_no_client_auth();
  TlsOptions::new(server, client)
}

/// `b`'s mTLS-required [`TlsOptions`]: its server installs a `ClientCertVerifier`
/// rooted at a throwaway cluster CA that `a` is NOT signed by, so `a`'s
/// `with_no_client_auth` dial fails the MUTUAL handshake before any `Stream`
/// exists. Returns the responder's bundle (a's bundle is the plain
/// [`sim_tls_config`]).
fn sim_tls_config_mtls_required() -> TlsOptions {
  // A throwaway cluster CA whose roots `a`'s self-signed leaf does not chain
  // to. `b` requires a client cert verified against it; `a` presents none.
  let ca = rcgen::generate_simple_self_signed(vec!["cluster-ca".into()]).unwrap();
  let mut roots = rustls::RootCertStore::empty();
  roots
    .add(CertificateDer::from(ca.cert.der().to_vec()))
    .unwrap();
  let provider = sim_crypto_provider();
  let verifier =
    rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
      .build()
      .unwrap();

  let (chain, key) = self_signed();
  let server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_client_cert_verifier(verifier)
    .with_single_cert(chain, key)
    .unwrap();
  let client = rustls::ClientConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AnyServer(provider)))
    .with_no_client_auth();
  TlsOptions::new(server, client)
}

/// One in-flight UDP gossip datagram. Carries an encoded plain frame (or a
/// concatenation of frames for a compound). Fault/latency is applied ONCE per
/// datagram (one `send_to`).
struct PendingDatagram {
  deliver_at: Instant,
  from: SocketAddr,
  to: SocketAddr,
  bytes: bytes::Bytes,
}

/// `tls`-gated deterministic conformance harness. Accessor-only; no `pub`
/// fields. Mirrors [`Cluster`](crate::cluster::Cluster)'s step contract.
pub struct TlsCluster {
  nodes: HashMap<SocketAddr, Node1>,
  clock: Clock,
  faults: FaultConfig,
  /// UDP gossip queue.
  queue: VecDeque<PendingDatagram>,
  /// Per-side virtual-TCP pipes, keyed by the READER's `(addr, exchange)`.
  pipes: HashMap<(SocketAddr, ExchangeId), TcpPipe>,
  /// Dialer ↔ acceptor exchange-handle mapping, established when the dialer's
  /// `Connect` action is delivered (the harness calls `acceptor.accept_connection`
  /// and records the returned acceptor `ExchangeId`). Both orientations are
  /// stored so a write on either side resolves the peer's pipe key.
  peer_of: HashMap<(SocketAddr, ExchangeId), (SocketAddr, ExchangeId)>,
  /// Per-host record of every peer EVER observed in `Suspect` (scanned every
  /// tick — a transient Suspect later refuted would otherwise be invisible to
  /// an end-state assertion).
  suspected: HashMap<SocketAddr, HashSet<SmolStr>>,
  /// Per-host record of every peer EVER observed `Alive` (so a test can assert
  /// "the merge reached the Endpoint" even if normal SWIM later transitions the
  /// peer away).
  ever_alive: HashMap<SocketAddr, HashSet<SmolStr>>,
  /// Per-host record of every peer EVER observed `Dead` or `Left` (so a leave
  /// test can assert the transition was observed even after the dead/left entry
  /// is later GC'd to absent).
  ever_gone: HashMap<SocketAddr, HashSet<SmolStr>>,
  /// Per-host record of whether `Event::LeftCluster` has fired.
  left: HashSet<SocketAddr>,
  /// Hosts whose direct + indirect probe UDP datagrams are dropped (forces the
  /// reliable-ping fallback over TLS), as an unordered pair set.
  probe_block: Vec<(SocketAddr, SocketAddr)>,
  /// Pairs whose TCP connect is refused (the dialer's pipe is `reset` the
  /// instant its `Connect` action is delivered). Unordered.
  connect_refuse: Vec<(SocketAddr, SocketAddr)>,
  /// One-shot: arm `host`'s next inbound TCP pipe to half-close after its first
  /// delivered record (truncation mid-frame). `false` once armed onto a pipe.
  half_close_host: Option<SocketAddr>,
  /// When `Some`, every inbound TCP pipe created for this host withholds its
  /// read==0 FIN anchor: the peer's `close_notify` rides in-band as application
  /// ciphertext but the TCP FIN is deferred indefinitely, so this host must
  /// drain a coalesced first request on the SAME tick it is delivered (the
  /// small-coalesced post-promotion drain regression).
  withhold_fin_host: Option<SocketAddr>,
  /// Optional `(probe_interval, probe_timeout)` override applied to every node
  /// built via [`add_node`](Self::add_node). `None` uses the standard LAN knobs
  /// that match the plain harness (the conformance-parity invariant).
  probe_window: Option<(Duration, Duration)>,
  /// Per-connection virtual-TCP receive window in bytes applied to every pipe
  /// (the `large_state` backpressure knob). `None` = unbounded.
  tcp_window: Option<usize>,
  /// `b`'s config override: when `Some`, the node at this address is built with
  /// the mTLS-required responder bundle instead of the trusted-network bundle.
  mtls_responder: Option<SocketAddr>,
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
  /// reliable path always installs this through `with_encryption` — the
  /// `TlsRecords` bridge's `is_secure() == true` guarantee forces the
  /// reliable-side `EncryptionOptions` back to disabled inside the bridge,
  /// so this knob influences only the gossip codec on the harness side.
  encryption: memberlist_proto::EncryptionOptions,
  /// Reliable-wire observation tap. Every payload routed through the virtual
  /// reliable pipe is appended here so the encrypted conformance suite can
  /// assert the wire never carries an `Encrypted` wrapper (a TLS reliable
  /// record must NOT begin with [`memberlist_proto::ENCRYPTED_TAG`]: the bridge
  /// skips reliable-path encryption when `R::is_secure() == true`). Only
  /// compiled under the encryption-conformance feature — the standard suite
  /// stays byte-unchanged.
  #[cfg(feature = "__sim-aes-gcm")]
  observed_reliable_wire_bytes: Vec<Vec<u8>>,
}

impl TlsCluster {
  /// Empty cluster.
  fn empty() -> Self {
    Self {
      nodes: HashMap::new(),
      clock: Clock::new(),
      faults: FaultConfig::none(),
      queue: VecDeque::new(),
      pipes: HashMap::new(),
      peer_of: HashMap::new(),
      suspected: HashMap::new(),
      ever_alive: HashMap::new(),
      ever_gone: HashMap::new(),
      left: HashSet::new(),
      probe_block: Vec::new(),
      connect_refuse: Vec::new(),
      half_close_host: None,
      withhold_fin_host: None,
      probe_window: None,
      tcp_window: None,
      mtls_responder: None,
      compression: memberlist_proto::CompressionOptions::new(),
      encryption: memberlist_proto::EncryptionOptions::new(),
      #[cfg(feature = "__sim-aes-gcm")]
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
    let cfg = EndpointOptions::new(SmolStr::new(id), addr)
      .with_gossip_interval(Duration::from_millis(200))
      .with_push_pull_interval(Duration::from_secs(30))
      .with_probe_interval(probe_interval)
      .with_probe_timeout(probe_timeout)
      .with_suspicion_mult(4)
      .with_retransmit_mult(4);
    let now = self.clock.now();
    let mut ep = Endpoint::new_at(
      cfg,
      now,
      SmallRng::seed_from_u64(crate::rng_seed_from_addr(&addr)),
    );
    ep.start_scheduling(now);
    let tls = if self.mtls_responder == Some(addr) {
      sim_tls_config_mtls_required()
    } else {
      sim_tls_config()
    };
    // The reliable transport is the label decorator over the TLS record layer.
    // These scenarios exercise TLS mutual-auth (not the wire label) for cluster
    // isolation, so the label is `None` (unlabeled): the gate is a pure
    // passthrough and behaviour matches the pre-decorator TLS coordinator.
    let cfg = LabelOptions::new_in(None, tls);
    self.nodes.insert(
      addr,
      StreamEndpoint::with_compression(
        ep,
        cfg,
        Box::new(|_addr: &SocketAddr| Some("localhost".to_string())),
        Box::new(|addr: &SocketAddr| *addr),
        self.compression,
      )
      .with_encryption(self.encryption.clone()),
    );
  }

  /// Two nodes; `a` joins `b` via a TLS push/pull. `a` starts knowing NOTHING
  /// about `b` (no bootstrap alive — `start_push_pull` dials the address
  /// directly), so `a` learns `b` ONLY by merging the join reply and `b` learns
  /// `a` ONLY by merging `a`'s push: both sides converge Alive purely through
  /// the reliable TLS exchange.
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
  /// unencrypted run (encryption is transparent to SWIM). The bridge's
  /// `TlsRecords::is_secure() == true` selector also forces the reliable path
  /// to drop the `EncryptionOptions` to disabled inside the bridge — the
  /// reliable-wire conformance test pins that invariant.
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

  /// Two nodes pre-seeded Alive about each other (no join handshake), ready for
  /// a probe scenario.
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

  /// Like [`two_node_join`](Self::two_node_join) but with a SHORT direct
  /// `probe_timeout` (500 ms) and a LONG `probe_interval` (10 s). The probe's
  /// single cumulative deadline is `sent + probe_interval`, while the
  /// reliable-ping fallback is opened only AFTER the direct `probe_timeout`
  /// elapses — so the fallback has ample time to complete a real TLS
  /// reliable-ping round-trip. The SUBJECT of the reliable-fallback scenario is
  /// the fixed per-tick step order (a fallback-ping ack drained into the
  /// `Endpoint` in step (2) BEFORE the probe `handle_timeout` in step (3)), NOT
  /// SWIM timing parity.
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

  /// `a` joins `b` over TLS, but every virtual-TCP pipe is built with a TINY
  /// receive window AND `a`'s push snapshot is pre-loaded with `extra_peers`
  /// extra Alive members. The join push/pull ciphertext is therefore far larger
  /// than one window's worth, so the writer cannot drain in one tick: the
  /// harness defers the over-window chunks to later ticks (zero byte loss, TCP
  /// never drops in-connection) and the join must still complete with every
  /// pushed peer crossing intact. The small window is set BEFORE the nodes are
  /// built and the extras are injected BEFORE `start_push_pull`, so the very
  /// first push/pull is already large and back-pressured.
  pub fn two_node_join_small_window(a: SocketAddr, b: SocketAddr, extra_peers: usize) -> Self {
    let mut c = Self::empty();
    c.tcp_window = Some(1200);
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
  /// push/pull is large and fragmented across reads, so the compression-enabled
  /// conformance run exercises the compressed reliable path under a small
  /// virtual-TCP window. `with_threshold(0)` forces every gossip datagram
  /// through the compressor.
  pub fn two_node_join_small_window_compressed(
    a: SocketAddr,
    b: SocketAddr,
    extra_peers: usize,
    algorithm: memberlist_proto::CompressAlgorithm,
  ) -> Self {
    let mut c = Self::empty();
    c.tcp_window = Some(1200);
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

  /// `a` joins `b` over TLS with an UNCAPPED receive window (the default —
  /// every ciphertext buffer is delivered as ONE coalesced TCP read, NOT
  /// fragmented), but `b` (the RESPONDER) is pre-loaded with `extra_peers` extra
  /// Alive members. `b`'s push/pull RESPONSE therefore carries `b`'s whole
  /// member list and, once it exceeds rustls's 16 KiB received-plaintext limit,
  /// arrives at `a` as a single coalesced read larger than that limit. This is
  /// the path the small-window `large_state` test MASKS (its fragmentation
  /// drains the received buffer between sub-window chunks): the coordinator must
  /// interleave bounded record intake with plaintext draining so the response
  /// decodes intact and the join converges. `a` starts knowing nothing about
  /// `b`, so `a`'s push is small; the large coalesced read is `a` decrypting
  /// `b`'s response.
  pub fn two_node_join_large_response_coalesced(
    a: SocketAddr,
    b: SocketAddr,
    extra_peers: usize,
  ) -> Self {
    let mut c = Self::empty();
    // tcp_window stays None: the response crosses as one coalesced read.
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&b) {
      for i in 0..extra_peers {
        // Vary the third octet and the port so (octet, port) is unique for a
        // few thousand peers while both stay in valid ranges (octet 0..=255,
        // port 9000..=9199).
        let paddr: SocketAddr = format!("198.51.100.{}:{}", i / 200, 9000 + (i % 200))
          .parse()
          .unwrap();
        n.handle_packet(
          b,
          Message::Alive(Alive::new(
            1,
            Node::new(SmolStr::new(format!("extra-{i}")), paddr),
          )),
          now,
        );
      }
    }
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b` over TLS with an UNCAPPED receive window, and `a` (the
  /// DIALER) is pre-loaded with `extra_peers` extra Alive members so its join
  /// push REQUEST exceeds rustls's 16 KiB received-plaintext limit. Because `a`
  /// mints + pumps its outbound `Stream` the SAME tick its TLS handshake
  /// completes, `a`'s final handshake flight (TLS 1.3 client `Finished`) and the
  /// whole >16 KiB request are drained into ONE coalesced ciphertext buffer and
  /// delivered to `b` as ONE transport read while `b` is STILL `Handshaking` (it
  /// has not yet consumed `a`'s `Finished`). `b`'s record layer completes the
  /// handshake AND buffers the trailing app records in the SAME
  /// `process_new_packets`, and the >16 KiB request trips `read_tls`
  /// backpressure before a `Stream` exists — the pre-promotion coalescing path.
  /// `b` must retain the unconsumed ciphertext tail and replay it after minting
  /// its inbound `Stream` so the full request reassembles and the join
  /// converges. This is the path the large-RESPONSE-coalesced test does NOT
  /// cover (that one reads the large buffer Established, post-promotion).
  pub fn two_node_join_large_request_coalesced(
    a: SocketAddr,
    b: SocketAddr,
    extra_peers: usize,
  ) -> Self {
    let mut c = Self::empty();
    // tcp_window stays None: the request crosses as one coalesced read.
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      for i in 0..extra_peers {
        // Vary the third octet and the port so (octet, port) is unique for a
        // few thousand peers while both stay in valid ranges.
        let paddr: SocketAddr = format!("198.51.100.{}:{}", i / 200, 9000 + (i % 200))
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

  /// `a` joins `b` over TLS with an UNCAPPED receive window and a SMALL join
  /// request (no preloaded extras), but `b`'s inbound TCP pipe WITHHOLDS its
  /// read==0 FIN anchor. Because `a` mints + pumps its outbound `Stream` the
  /// SAME tick its TLS handshake completes, `a`'s final TLS flight (client
  /// `Finished`), its small join request, AND its `close_notify` (a push/pull
  /// half-closes its send side after the request so the peer can reply) drain
  /// into ONE coalesced ciphertext buffer delivered to `b` in ONE transport read
  /// WHILE `b` IS STILL `Handshaking`. The whole small read is consumed by
  /// rustls in ONE pass — NO backpressure, so NO ciphertext tail is retained —
  /// yet the decrypted request sits in `b`'s record-layer received-plaintext
  /// buffer and `peer_has_closed()` is latched.
  ///
  /// With the FIN anchor withheld, `b` gets NO later transport read to lean on:
  /// it MUST drain the buffered request on the SAME tick it mints + promotes its
  /// inbound `Stream` (the post-promotion drain) to merge `a`'s push and produce
  /// its reply. The post-promotion drain must run even on an empty tail:
  /// otherwise `b` never sees the request, never merges `a`, and never replies —
  /// so `a` never learns `b` either (the join is the only path) and the exchange
  /// stalls. This is the SMALL sibling of the large-request-coalesced case
  /// (which is forced through backpressure and therefore retains a tail).
  pub fn two_node_join_small_coalesced_no_fin(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    // tcp_window stays None: the small request crosses as one coalesced read.
    c.withhold_fin_host = Some(b);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b`, but `b` (the RESPONDER) requires a cluster-CA-signed client
  /// cert and `a` presents none → the MUTUAL TLS handshake fails server-side
  /// before any `Stream` exists. No merge, no `UserDataReceived`, no ack on
  /// either side; both failed-handshake bridges are torn down.
  pub fn two_node_join_mtls_required_responder(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.mtls_responder = Some(b);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  // ── Fault / scenario hooks ──────────────────────────────────────────────────

  /// Begin a subsequent (post-join) push/pull from `host` to `peer`.
  pub fn trigger_push_pull(&mut self, host: SocketAddr, peer: SocketAddr) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.start_push_pull(peer, PushPullKind::Refresh, now);
    }
  }

  /// Drive a probe round on `host` (mirrors the plain harness `trigger_probe`).
  pub fn trigger_probe(&mut self, host: SocketAddr) -> bool {
    let now = self.clock.now();
    match self.nodes.get_mut(&host) {
      Some(n) => n.start_probe(now),
      None => false,
    }
  }

  /// Drop every direct + indirect UDP probe datagram between `a` and `b`,
  /// forcing the reliable-ping fallback over TLS.
  pub fn drop_all_udp_probes_between(&mut self, a: SocketAddr, b: SocketAddr) {
    self.probe_block.push((a, b));
  }

  /// Refuse the TCP connect for the (unordered) pair `a`/`b`: the dialer's pipe
  /// is `reset` the instant its `Connect` action is delivered, so its next
  /// `handle_transport_data(id, &[], eof=true, now)` surfaces the connect
  /// failure.
  pub fn refuse_connect_between(&mut self, a: SocketAddr, b: SocketAddr) {
    self.connect_refuse.push((a, b));
  }

  /// Arm `host`'s next inbound TCP pipe to half-close after its first delivered
  /// record (read==0 mid-frame): the partial frame never completes, so no merge
  /// is applied.
  pub fn half_close_tcp_after_first_record(&mut self, host: SocketAddr) {
    self.half_close_host = Some(host);
  }

  /// Partition `group_a` from `group_b` (UDP datagrams across the cut dropped).
  pub fn partition(&mut self, group_a: &[SocketAddr], group_b: &[SocketAddr]) {
    let mut map = HashMap::new();
    for &x in group_a {
      map.insert(x, 0usize);
    }
    for &y in group_b {
      map.insert(y, 1usize);
    }
    self.faults.partitions = Some(map);
  }

  /// Inject a Suspect at `host` about `target` attributed to `from`.
  pub fn inject_suspect(&mut self, host: SocketAddr, target: SmolStr, from: SmolStr, inc: u32) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.handle_packet(host, Message::Suspect(Suspect::new(inc, target, from)), now);
    }
  }

  /// Inject an Alive at `host` about `peer` (e.g. a higher-incarnation refute).
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

  /// Begin a graceful leave on `host` (delegates to [`StreamEndpoint::leave`]).
  pub fn leave(&mut self, host: SocketAddr) -> Result<(), memberlist_proto::Error> {
    let now = self.clock.now();
    match self.nodes.get_mut(&host) {
      Some(n) => n.leave(now),
      None => Ok(()),
    }
  }

  /// Deliver a raw gossip datagram directly into `host`'s gossip ingress,
  /// bypassing the UDP queue and fault injection. Used by tests that need to
  /// inject a crafted datagram (e.g. a compressed datagram with trailing junk)
  /// without going through the normal outbound path.
  pub fn inject_raw_gossip(&mut self, from: SocketAddr, host: SocketAddr, bytes: &[u8]) {
    let now = self.clock.now();
    self.deliver_gossip(from, host, bytes, now);
  }

  /// Advance virtual time by `by`, then deliver matured UDP datagrams + matured
  /// TCP chunks and tick every node (mirrors
  /// [`Cluster::advance`](crate::cluster::Cluster)).
  pub fn advance(&mut self, by: Duration) {
    self.clock.advance(by);
    let now = self.clock.now();
    self.deliver_ready(now);
    self.deliver_tcp_ready(now);
    self.tick_all(now);
  }

  // ── Observation ─────────────────────────────────────────────────────────────

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
  pub fn ever_saw_alive(&self, host: SocketAddr, peer: &SmolStr) -> bool {
    self.ever_alive.get(&host).is_some_and(|s| s.contains(peer))
  }

  /// `true` if `host` has EVER observed `peer` `Dead` or `Left` (scanned every
  /// tick), even after the dead/left entry is later GC'd to absent.
  pub fn ever_saw_gone(&self, host: SocketAddr, peer: &SmolStr) -> bool {
    self.ever_gone.get(&host).is_some_and(|s| s.contains(peer))
  }

  /// `true` once `host` has emitted `Event::LeftCluster`.
  pub fn left_fired(&self, host: SocketAddr) -> bool {
    self.left.contains(&host)
  }

  /// Number of in-flight reliable-exchange bridges `host` currently holds (one
  /// per active push/pull, reliable-ping, or still-handshaking dial/accept).
  /// `0` after every exchange completes / its connection is torn down.
  pub fn live_bridge_count(&self, host: SocketAddr) -> usize {
    self.nodes.get(&host).map_or(0, |n| n.live_bridge_count())
  }

  /// Every payload the harness routed through the virtual reliable pipe so
  /// far. The encryption-conformance suite asserts that no entry begins with
  /// [`memberlist_proto::ENCRYPTED_TAG`] — the TLS bridge skips reliable-path
  /// encryption (`TlsRecords::is_secure() == true`), so the wire never carries
  /// an `[Encrypted[..]]` wrapper. Only compiled under the
  /// `__sim-aes-gcm` feature.
  #[cfg(feature = "__sim-aes-gcm")]
  pub fn observed_reliable_wire_bytes(&self) -> &[Vec<u8>] {
    &self.observed_reliable_wire_bytes
  }

  // ── Step loop (mirrors QuicCluster::step) ────────────────────────────────────

  /// One simulation tick. Returns `true` if anything happened.
  ///
  /// 0. Pre-pump every coordinator with `handle_timeout(now)` so a freshly-
  ///    queued dial/accept, every bridge byte-pump, and `collect_transmits` run
  ///    BEFORE the outbound directives/ciphertext/datagrams are drained.
  /// 1. Drain every node's gossip [`Transmit`] (plain-frame encoded → UDP
  ///    queue), transport directives ([`StreamAction`]), and outbound ciphertext
  ///    ([`StreamEndpoint::poll_transport_transmit`] → peer pipe) →
  ///    enqueue/route.
  /// 2. Advance to the next deadline = `min`(earliest queued UDP datagram,
  ///    earliest queued TCP chunk, every node's [`StreamEndpoint::poll_timeout`]).
  /// 3. Deliver matured UDP datagrams (gossip ingress + decode + handle_packet)
  ///    and matured TCP chunks ([`StreamEndpoint::handle_transport_data`]).
  /// 4. Tick every node, scan events, drain again so THIS tick captures the
  ///    directives/bytes its own final tick produced.
  pub fn step(&mut self) -> bool {
    let now = self.clock.now();
    let mut progressed = false;
    let addrs: Vec<SocketAddr> = self.nodes.keys().copied().collect();

    // 0. Pre-pump every coordinator at the current instant.
    for a in &addrs {
      self.nodes.get_mut(a).unwrap().handle_timeout(now);
    }

    // 1. Drain every coordinator's outbound directives + ciphertext + gossip.
    if self.drain_and_route(&addrs, now) {
      progressed = true;
    }

    // 2. Advance to the next deadline.
    let mut next: Option<Instant> = self.earliest_udp_deadline();
    if let Some(t) = self.earliest_tcp_deadline() {
      next = Some(next.map_or(t, |b| b.min(t)));
    }
    for a in &addrs {
      if let Some(t) = self.nodes.get_mut(a).unwrap().poll_timeout() {
        next = Some(next.map_or(t, |b| b.min(t)));
      }
    }
    let Some(next) = next else {
      // No queued traffic and no pending timer anywhere: truly idle.
      return progressed;
    };
    if next > now {
      self.clock.advance(next - now);
      progressed = true;
    }
    let now = self.clock.now();

    // 3. Deliver matured UDP datagrams + matured TCP chunks.
    if self.deliver_ready(now) > 0 {
      progressed = true;
    }
    if self.deliver_tcp_ready(now) > 0 {
      progressed = true;
    }

    // 4. Tick every node, scan events, drain again.
    self.tick_all(now);
    if self.scan_events() {
      progressed = true;
    }
    if self.drain_and_route(&addrs, now) {
      progressed = true;
    }

    progressed
  }

  /// Drain every coordinator's transport directives, outbound ciphertext, and
  /// gossip transmits → route them (mappings/pipes, peer pipes, UDP queue).
  ///
  /// Ordering is load-bearing: `Connect` directives are applied BEFORE
  /// ciphertext (so a fresh dial's pipe mapping exists before its ClientHello is
  /// routed), but `Shutdown`/`Close` directives are applied AFTER ciphertext
  /// (the FIN segment is the LAST thing on the wire — it must be armed after the
  /// tick's data fragments are queued, else the read==0 anchor could be timed
  /// before data the bridge already wrote, denying the peer a tick to generate
  /// its push/pull response). Teardowns are withheld by the coordinator while
  /// their exchange still has bytes in `poll_transport_transmit`; we re-poll
  /// after the ciphertext drain below to surface those released teardowns and
  /// apply them in the same step. Returns `true` if anything was routed.
  fn drain_and_route(&mut self, addrs: &[SocketAddr], now: Instant) -> bool {
    let mut any = false;
    // Drain every node's currently-available actions, partitioning Connect
    // (pre-ciphertext) from Shutdown/Close (post-ciphertext).
    let mut connects: Vec<(SocketAddr, StreamAction)> = Vec::new();
    let mut teardowns: Vec<(SocketAddr, StreamAction)> = Vec::new();
    for src in addrs {
      let node = self.nodes.get_mut(src).unwrap();
      while let Some(action) = node.poll_action() {
        match &action {
          StreamAction::Connect(_) => connects.push((*src, action)),
          StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_) => {
            teardowns.push((*src, action))
          }
        }
      }
    }
    // (a) Connect directives → establish mappings + pipes.
    for (src, action) in connects {
      if self.apply_action(src, action, now) {
        any = true;
      }
    }
    // (b) Outbound ciphertext → peer pipe.
    for src in addrs {
      let mut chunks: Vec<(ExchangeId, SocketAddr, bytes::Bytes)> = Vec::new();
      let node = self.nodes.get_mut(src).unwrap();
      while let Some((id, peer, bytes)) = node.poll_transport_transmit() {
        chunks.push((id, peer, bytes));
      }
      for (id, peer, bytes) in chunks {
        if self.route_ciphertext(*src, id, peer, bytes, now) {
          any = true;
        }
      }
    }
    // (b.5) Re-poll actions: the coordinator gates per-exchange teardowns
    // behind the exchange's bytes, so a `Shutdown`/`Close` enqueued the same
    // tick as its trailing ciphertext surfaces only AFTER the ciphertext is
    // drained. Appending the released teardowns here keeps the "FIN after
    // data" wire order (arm_fin runs in step (c) below, after route_ciphertext
    // has already queued every chunk).
    let mut late_connects: Vec<(SocketAddr, StreamAction)> = Vec::new();
    for src in addrs {
      let node = self.nodes.get_mut(src).unwrap();
      while let Some(action) = node.poll_action() {
        match &action {
          StreamAction::Connect(_) => late_connects.push((*src, action)),
          StreamAction::Shutdown(_) | StreamAction::Close(_) | StreamAction::Abort(_) => {
            teardowns.push((*src, action))
          }
        }
      }
    }
    // No new Connect should be emitted by a ciphertext drain alone; if one
    // appears (e.g. from a same-tick reliable-fallback) apply it before its
    // ciphertext is produced. Drained in producer order.
    for (src, action) in late_connects {
      if self.apply_action(src, action, now) {
        any = true;
      }
    }
    // (c) Shutdown/Close directives → arm the FIN AFTER this tick's data.
    for (src, action) in teardowns {
      if self.apply_action(src, action, now) {
        any = true;
      }
    }
    // (d) Gossip transmits → UDP queue.
    for src in addrs {
      let mut pending: Vec<(SocketAddr, bytes::Bytes)> = Vec::new();
      let node = self.nodes.get_mut(src).unwrap();
      while let Some(tx) = node.poll_memberlist_transmit() {
        match tx {
          Transmit::Packet(p) => {
            let (to, message) = p.into_parts();
            if let Some(b) = encode_frame(&message) {
              pending.push((to, b));
            }
          }
          Transmit::Compound(cmp) => {
            // A compound is ONE datagram: concatenate the plain frames so a
            // single fault/latency decision covers the whole bundle.
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
      }
      for (to, bytes) in pending {
        // Compress THEN encrypt the outbound datagram through the source
        // coordinator's configured transforms (each identity when disabled).
        // The on-wire byte order is `[Encrypted[Compressed[frame]]]` when both
        // are enabled; the receiver reverses the order on ingress. An
        // encrypt failure (e.g. backend not built in) drops the gossip —
        // emitting plaintext on the configured-encrypted egress would bypass
        // authentication. SWIM gossip is lossy and self-healing.
        let node = self.nodes.get(src).unwrap();
        let compressed = node.compress_gossip(&bytes);
        let on_wire = match node.encrypt_gossip(&compressed) {
          Ok(b) => b,
          Err(_) => continue,
        };
        if self.enqueue_udp(*src, to, bytes::Bytes::from(on_wire), now) {
          any = true;
        }
      }
    }
    any
  }

  /// The virtual-TCP segment stride: the spacing between successive matured
  /// fragments and between the last data byte and the FIN. Tied to the network
  /// latency, but a zero latency still needs a strictly-positive stride so the
  /// clock advances between releases (otherwise everything would mature at the
  /// same `now` and neither the window nor the post-data FIN would be modeled).
  fn tcp_stride(&self) -> Duration {
    if self.faults.latency.is_zero() {
      Duration::from_millis(1)
    } else {
      self.faults.latency
    }
  }

  /// Apply one transport directive from `src`. Returns `true` if it changed
  /// connection state.
  fn apply_action(&mut self, src: SocketAddr, action: StreamAction, now: Instant) -> bool {
    match action {
      StreamAction::Connect(info) => {
        let dialer_exch = info.id();
        let peer = info.peer();
        // Refuse if the peer node is unknown or the pair is connect-refused:
        // mark the dialer's own pipe `reset` so no ciphertext ever flows. The
        // dialer's bridge stays `Handshaking` (the empty-slice anchor is a
        // no-op before a `Stream` exists), so it is reaped when its dial
        // deadline elapses — the bridge's handshake-deadline guard
        // terminalizes it, which drives `dial_failed` through the coordinator.
        let refused = !self.nodes.contains_key(&peer)
          || self
            .connect_refuse
            .iter()
            .any(|&(x, y)| (x == src && y == peer) || (x == peer && y == src));
        if refused {
          let mut pipe = TcpPipe::new(self.tcp_window);
          pipe.reset = true;
          self.pipes.insert((src, dialer_exch), pipe);
          return true;
        }
        // Accept on the peer: it allocates its own exchange handle. `None` means
        // the peer did NOT admit the connection (leaving, the inbound-stream cap,
        // or a config error) — treat it like a refusal: reset the dialer's pipe so
        // no bytes flow and its bridge is reaped on the dial deadline.
        let Some(acceptor_exch) = self
          .nodes
          .get_mut(&peer)
          .unwrap()
          .accept_connection(src, now)
        else {
          let mut pipe = TcpPipe::new(self.tcp_window);
          pipe.reset = true;
          self.pipes.insert((src, dialer_exch), pipe);
          return true;
        };
        // Record the bidirectional mapping and create both reader pipes.
        self
          .peer_of
          .insert((src, dialer_exch), (peer, acceptor_exch));
        self
          .peer_of
          .insert((peer, acceptor_exch), (src, dialer_exch));
        self
          .pipes
          .insert((src, dialer_exch), TcpPipe::new(self.tcp_window));
        let mut acceptor_pipe = TcpPipe::new(self.tcp_window);
        // Arm the one-shot half-close on the acceptor's INBOUND pipe (`peer`
        // reads the dialer's exchange here): after the acceptor receives its
        // first record, the connection is half-closed (read==0 mid-frame), so the
        // dialer's request frame never completes on the acceptor side. The
        // acceptor therefore never merges the dialer (no `PushPullRequestReceived`
        // is decoded), and — with no membership entry to gossip — the dialer
        // never learns the acceptor through ANY path. (`half_close_host` names the
        // ACCEPTOR, the side whose read is truncated.)
        if self.half_close_host == Some(peer) {
          acceptor_pipe.half_close_after_first = true;
          self.half_close_host = None;
        }
        // Withhold the acceptor's read==0 FIN anchor: the dialer's `close_notify`
        // still rides in-band (application ciphertext), but no later TCP read==0
        // ever arrives, so the acceptor must drain the coalesced first request on
        // the same tick it is delivered (post-promotion), not on a later read.
        if self.withhold_fin_host == Some(peer) {
          acceptor_pipe.withhold_fin = true;
        }
        self.pipes.insert((peer, acceptor_exch), acceptor_pipe);
        true
      }
      StreamAction::Shutdown(r) => {
        // Half-close `src`'s write side (TCP FIN): the PEER reads read==0 after
        // draining its buffered inbound. The FIN is a separate segment arriving
        // after the last queued data byte (`arm_fin`), so the peer keeps a tick
        // to generate its push/pull response before honoring the EOF.
        let stride = self.tcp_stride();
        let key = (src, r.id());
        if let Some(&(peer, peer_exch)) = self.peer_of.get(&key) {
          if let Some(pipe) = self.pipes.get_mut(&(peer, peer_exch)) {
            pipe.arm_fin(now, stride);
            return true;
          }
        }
        false
      }
      StreamAction::Close(r) => {
        // Tear the connection down: forget the mapping and arm the FIN on BOTH
        // sides so any already-queued inbound bytes still drain (TCP delivers
        // bytes sent before a clean close), then each reader observes read==0.
        // A clean Close is NOT an RST — already-sent bytes are never discarded.
        let stride = self.tcp_stride();
        let key = (src, r.id());
        if let Some(pipe) = self.pipes.get_mut(&key) {
          pipe.arm_fin(now, stride);
        }
        if let Some((peer, peer_exch)) = self.peer_of.remove(&key) {
          self.peer_of.remove(&(peer, peer_exch));
          if let Some(pipe) = self.pipes.get_mut(&(peer, peer_exch)) {
            pipe.arm_fin(now, stride);
          }
        }
        true
      }
      StreamAction::Abort(r) => {
        // RST the connection: the exchange FAILED (dial failure, label /
        // encryption rejection, or an elapsed exchange deadline). Unlike `Close`,
        // which arms the FIN so already-queued inbound ciphertext still drains, an
        // abort DISCARDS any queued-but-unsent inbound ciphertext in BOTH
        // directions — it is stale, and a reader holding only a partial record
        // stream rejects it rather than applying it. `inbound.clear()` is the
        // discard; `arm_fin` then surfaces a terminal read==0 so the reader's
        // exchange ends (it cannot hang awaiting records that will never come),
        // and the mapping is forgotten so no later write routes into either side.
        //
        // The discard is the only behavioral difference from `Close`; the read==0
        // terminator is shared because the transport surface has no signal
        // distinct from EOF for a reset — the receiver sees the read stream end
        // either way, and the truncated ciphertext buffer is what makes a reset
        // observably different from a clean drain.
        let stride = self.tcp_stride();
        let key = (src, r.id());
        if let Some(pipe) = self.pipes.get_mut(&key) {
          pipe.inbound.clear();
          pipe.arm_fin(now, stride);
        }
        if let Some((peer, peer_exch)) = self.peer_of.remove(&key) {
          self.peer_of.remove(&(peer, peer_exch));
          if let Some(pipe) = self.pipes.get_mut(&(peer, peer_exch)) {
            pipe.inbound.clear();
            pipe.arm_fin(now, stride);
          }
        }
        true
      }
    }
  }

  /// Route one outbound ciphertext buffer from `src`'s exchange into the peer's
  /// reader pipe. With no receive window the whole buffer is one chunk delivered
  /// after `latency`. With a window the buffer is fragmented into `window`-sized
  /// pieces released one round-trip apart — modeling a tiny TCP receive window
  /// that lets the reader pull ~`window` bytes per round-trip, so a large
  /// transfer drains across many ticks (NOT dropped — TCP never drops
  /// in-connection; rustls reassembles fragments split at arbitrary byte
  /// boundaries, so there is zero byte loss). A buffer whose connection has no
  /// mapping (the peer tore it down) is dropped on the floor — the bridge that
  /// produced it is already being reaped. Returns `true` if anything was queued.
  fn route_ciphertext(
    &mut self,
    src: SocketAddr,
    exch: ExchangeId,
    _peer_socket: SocketAddr,
    bytes: bytes::Bytes,
    now: Instant,
  ) -> bool {
    let latency = self.faults.latency;
    let stride = self.tcp_stride();
    let Some(&(peer, peer_exch)) = self.peer_of.get(&(src, exch)) else {
      return false;
    };
    // Pipe gating: a reset pipe, a pipe whose reader side has half-closed
    // (FIN armed), or an empty write is silently dropped. Once the read half
    // is closed the writer can send no more bytes that the reader will ever
    // observe (the harness's forced-truncation hooks arm the FIN to drop the
    // remainder of the frame). Inspected via `.get` so the observation tap
    // below can take its own `&mut self` borrow before the `get_mut`.
    let droppable = self
      .pipes
      .get(&(peer, peer_exch))
      .map(|p| p.reset || p.fin_at.is_some())
      .unwrap_or(true);
    if droppable || bytes.is_empty() {
      return false;
    }
    // Reliable-wire observation tap: append every non-empty chunk routed
    // through the virtual reliable pipe verbatim, so the encrypted conformance
    // suite can pin that no `[Encrypted[..]]` wrapper ever surfaces here. On
    // TLS these bytes are TLS records (`0x14..=0x17` record-type leading
    // byte), never `ENCRYPTED_TAG` — the bridge's `R::is_secure() == true`
    // selector forces reliable-path encryption off, so the plaintext units
    // the bridge writes never carry an `Encrypted` wrapper either.
    #[cfg(feature = "__sim-aes-gcm")]
    self.observed_reliable_wire_bytes.push(bytes.to_vec());
    let pipe = self
      .pipes
      .get_mut(&(peer, peer_exch))
      .expect("pipe presence already gated above");
    // The release cadence: a fragment matures one `stride` after the previous
    // queued fragment so the reader pulls one window's worth per round-trip.
    let base = now + latency;
    match pipe.window {
      Some(w) if bytes.len() > w => {
        // Fragment into `w`-sized pieces, each released a `stride` after the
        // last already-queued fragment (or `base` for the first), preserving
        // FIFO order.
        let mut next = pipe
          .inbound
          .back()
          .map(|(t, _)| *t + stride)
          .unwrap_or(base)
          .max(base);
        let mut off = 0;
        while off < bytes.len() {
          let end = (off + w).min(bytes.len());
          pipe.inbound.push_back((next, bytes.slice(off..end)));
          off = end;
          next += stride;
        }
      }
      _ => {
        // No window (or buffer fits): one chunk after latency, ordered after
        // any already-queued fragment.
        let deliver_at = pipe
          .inbound
          .back()
          .map(|(t, _)| (*t).max(base))
          .unwrap_or(base);
        pipe.inbound.push_back((deliver_at, bytes));
      }
    }
    true
  }

  /// Enqueue one UDP gossip datagram, applying fault injection. Returns `true`
  /// if it was actually queued (not dropped).
  fn enqueue_udp(
    &mut self,
    from: SocketAddr,
    to: SocketAddr,
    bytes: bytes::Bytes,
    now: Instant,
  ) -> bool {
    // A gossip datagram between a probe-blocked pair is dropped — models
    // "direct + indirect UDP probes dropped" while leaving the TLS reliable
    // path intact (so the reliable-ping fallback runs).
    if self
      .probe_block
      .iter()
      .any(|&(x, y)| (x == from && y == to) || (x == to && y == from))
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

  /// Earliest delivery deadline among all queued UDP datagrams.
  fn earliest_udp_deadline(&self) -> Option<Instant> {
    self.queue.iter().map(|d| d.deliver_at).min()
  }

  /// Earliest deadline among all queued TCP chunks, FIN segments, and any owed
  /// reset anchor (a reset is immediately due — it surfaces as `now`).
  fn earliest_tcp_deadline(&self) -> Option<Instant> {
    let mut best: Option<Instant> = None;
    let fold = |t: Instant, best: &mut Option<Instant>| {
      *best = Some(best.map_or(t, |b: Instant| b.min(t)));
    };
    for pipe in self.pipes.values() {
      if let Some((t, _)) = pipe.inbound.front() {
        fold(*t, &mut best);
      }
      // The FIN segment matures at `fin_at` (strictly after the last data byte)
      // and is delivered once the buffer ahead of it has drained. A withheld
      // FIN is never delivered, so it contributes no deadline.
      if let Some(fin_at) = pipe.fin_at {
        if !pipe.eof_anchor_sent && !pipe.withhold_fin {
          fold(fin_at, &mut best);
        }
      }
      // A reset connection owes an immediate anchor.
      if pipe.reset && !pipe.reset_anchor_sent {
        fold(self.clock.now(), &mut best);
      }
    }
    best
  }

  /// Deliver every UDP datagram whose `deliver_at <= now` through the
  /// coordinator's gossip ingress (`handle_gossip` → `poll_memberlist_ingress`
  /// → decode → `handle_packet` → `handle_timeout`). Returns the count.
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
        self.deliver_gossip(d.from, d.to, &d.bytes, now);
        delivered += 1;
      }
    }
    self.queue = remaining;
    delivered
  }

  /// Deliver one UDP datagram to its destination through the coordinator's
  /// gossip ingress. The inner `Endpoint::handle_packet` is NEVER called
  /// directly: routing through the public ingress exercises the composed unit's
  /// real UDP path (the codec-owning layer here decodes each plain frame).
  fn deliver_gossip(&mut self, from: SocketAddr, to: SocketAddr, bytes: &[u8], now: Instant) {
    let Some(node) = self.nodes.get_mut(&to) else {
      return;
    };
    node.handle_gossip(from, bytes, now);
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

  /// Deliver every matured TCP chunk and any owed read==0 / reset anchor.
  /// Returns the number of `handle_transport_data` calls made.
  ///
  /// The unified `handle_transport_data` carries an explicit `eof: bool`. On
  /// TLS the peer's in-band `close_notify` alert (carried as application
  /// ciphertext) is the close anchor the record layer latches, so the harness
  /// never piggybacks the transport FIN onto a data chunk: each delivery is a
  /// tuple `(payload, eof)` where a matured ciphertext chunk is
  /// `(Some(bytes), false)` (a normal byte delivery — its EOF, if any, rides
  /// in-band) and a separate FIN/reset anchor is `(None, true)` (the
  /// out-of-band virtual-TCP read==0).
  fn deliver_tcp_ready(&mut self, now: Instant) -> usize {
    // Collect the deliveries first (key, payload, eof) so the
    // `handle_transport_data` calls — which mutate the nodes — do not alias the
    // pipe borrow. One chunk per pipe per pass keeps FIFO/anchor ordering tight
    // across repeated passes within the same `now`.
    let stride = self.tcp_stride();
    let mut count = 0;
    // Pipes that delivered a data chunk during THIS invocation: their FIN is
    // held until a LATER invocation (a later clock instant), so the reader never
    // sees read==0 in the same `deliver_tcp_ready` pass as its last data byte —
    // it keeps a tick to act on that data (e.g. generate a push/pull response)
    // before honoring the EOF.
    let mut data_this_call: HashSet<(SocketAddr, ExchangeId)> = HashSet::new();
    loop {
      let mut deliveries: Vec<((SocketAddr, ExchangeId), Option<bytes::Bytes>, bool)> = Vec::new();
      for (&key, pipe) in self.pipes.iter_mut() {
        // A reset connection surfaces its anchor exactly once, immediately.
        if pipe.reset {
          if !pipe.reset_anchor_sent {
            pipe.reset_anchor_sent = true;
            deliveries.push((key, None, true));
          }
          continue;
        }
        // A matured ciphertext chunk.
        if let Some((t, _)) = pipe.inbound.front() {
          if *t <= now {
            let (_, b) = pipe.inbound.pop_front().unwrap();
            // One-shot truncation: after the first delivered record, ARM the FIN
            // (one stride later) so the next read is read==0 mid-frame — the FIN
            // lands strictly after this partial record, never in the same pass.
            if pipe.half_close_after_first {
              pipe.half_close_after_first = false;
              pipe.arm_fin(now, stride);
            }
            data_this_call.insert(key);
            // A ciphertext chunk is a pure byte delivery: `eof = false`. TLS
            // closure rides the in-band `close_notify` the chunk may contain,
            // not the out-of-band transport FIN.
            deliveries.push((key, Some(b), false));
            continue;
          }
        }
        // The FIN segment: deliver read==0 once, only after every queued data
        // byte has drained, the FIN's own arrival instant has matured, AND no
        // data was delivered to this pipe in this same invocation. A pipe that
        // withholds its FIN never surfaces the read==0 anchor (the peer's TCP
        // write-side FIN is deferred indefinitely; only its in-band
        // `close_notify` is delivered, with the data chunk above).
        if let Some(fin_at) = pipe.fin_at {
          if !pipe.eof_anchor_sent
            && !pipe.withhold_fin
            && pipe.inbound.is_empty()
            && fin_at <= now
            && !data_this_call.contains(&key)
          {
            pipe.eof_anchor_sent = true;
            deliveries.push((key, None, true));
          }
        }
      }
      if deliveries.is_empty() {
        break;
      }
      for ((addr, exch), payload, eof) in deliveries {
        if let Some(node) = self.nodes.get_mut(&addr) {
          match payload {
            Some(b) => node.handle_transport_data(exch, &b, eof, now),
            None => node.handle_transport_data(exch, &[], eof, now),
          }
          count += 1;
        }
      }
    }
    count
  }

  /// `handle_timeout(now)` on every node.
  fn tick_all(&mut self, now: Instant) {
    for node in self.nodes.values_mut() {
      node.handle_timeout(now);
    }
  }

  /// Scan every node's live member view + `poll_event`, recording Suspect /
  /// Alive / Dead-or-Left / LeftCluster, then re-queue every event (so future
  /// scans still see late ones). Returns `true` if any event was observed.
  fn scan_events(&mut self) -> bool {
    let now = self.clock.now();
    let addrs: Vec<SocketAddr> = self.nodes.keys().copied().collect();
    let mut any = false;
    for host in addrs {
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
        if matches!(ev, Event::LeftCluster) {
          self.left.insert(host);
        }
        deferred.push(ev);
      }
      for ev in deferred {
        node.requeue_event(ev, now);
      }
    }
    any
  }
}

/// Encode one typed message into a plain frame (`[TAG][VARINT len][BODY]`) —
/// the EXACT bytes the machine's own `wire.rs` produces. Stream-only /
/// un-bridgeable messages yield `None` (they never ride the unreliable path).
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
mod tests;
