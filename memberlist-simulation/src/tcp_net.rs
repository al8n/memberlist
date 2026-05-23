//! [`TcpCluster`]: the `tcp`-gated conformance harness.
//!
//! Mirrors [`TlsCluster`](crate::tls_net::TlsCluster) method-for-method,
//! driving a [`StreamEndpoint`] over the plain-TCP record layer (`RawRecords`)
//! instead of the rustls-over-TCP record layer (`TlsRecords`):
//! reliable exchanges (join/anti-entropy push-pull, reliable-ping fallback)
//! ride a per-exchange plain TCP connection with a wire-label prefix, while
//! unreliable gossip rides a plain UDP socket. The cluster-isolation gate is
//! the [`TcpOptions`] label (matched at the inbound stream's first read), not a
//! TLS handshake; there is no crypto layer at all. Two queues mirror the TLS
//! harness:
//!
//! - **UDP gossip** — a datagram queue ([`PendingDatagram`]) byte-identical to
//!   the TLS harness's memberlist path: outbound is drained through
//!   [`StreamEndpoint::poll_memberlist_transmit`], plain-frame encoded with the
//!   machine's own `memberlist-wire` codec, enqueued (faults applied once per
//!   datagram), and on delivery fed through [`StreamEndpoint::handle_gossip`] →
//!   [`StreamEndpoint::poll_memberlist_ingress`] → decode →
//!   [`StreamEndpoint::handle_packet`].
//! - **A deterministic virtual TCP** (the shared
//!   [`TcpPipe`](crate::virtual_tcp::TcpPipe)) — per-connection ordered byte
//!   pipes (FIFO, no reordering, no loss within a connection; TCP is reliable +
//!   ordered) with clock-driven delivery latency, connect / accept / write /
//!   read(==0 on peer half-close) / reset, plus fault injection (connect-
//!   refused, mid-stream reset, half-close). Plain bytes are drained through
//!   [`StreamEndpoint::poll_transport_transmit`], delivered with latency, and
//!   fed back through [`StreamEndpoint::handle_transport_data`] with an explicit
//!   `eof: bool` (the out-of-band FIN signal that replaces TLS's in-band
//!   `close_notify` alert); transport directives are drained through
//!   [`StreamEndpoint::poll_action`].
//!
//! Virtual time (the shared [`Clock`]) is injected as `now` into both
//! machines, and the existing [`FaultConfig`] drop/delay/partition model is
//! reused unchanged — so the SWIM timing observed here is directly comparable
//! to the plain harness and to the sibling TLS harness (the conformance-parity
//! capstone).

use std::{
  collections::{HashMap, HashSet, VecDeque},
  net::SocketAddr,
  time::{Duration, Instant},
};

use memberlist_machine::{
  AddrBridge, Endpoint, EndpointConfig, Event, PushPullKind, RawRecords, TcpOptions, Transmit,
  streams::{ExchangeId, StreamAction, StreamEndpoint},
};
use memberlist_wire::{
  framing, message_from_any, message_to_any,
  typed::{Alive, Message, Node, Suspect},
};
use smol_str::SmolStr;

use crate::{clock::Clock, faults::FaultConfig, virtual_tcp::TcpPipe};

/// Identity [`AddrBridge`] for `A = SocketAddr`: the membership address *is*
/// the wire `SocketAddr`, so both directions are the identity (a production
/// driver would map its resolved/advertised address here instead). The
/// `server_name` accessor is unused on plain TCP (no certificate verification)
/// but must be supplied for the trait.
struct IdentityBridge;

impl AddrBridge<SocketAddr> for IdentityBridge {
  type ServerName = str;

  fn to_socket(addr: &SocketAddr) -> SocketAddr {
    *addr
  }
  fn from_socket(socket: SocketAddr) -> SocketAddr {
    socket
  }
  fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
    None
  }
}

/// The concrete coordinator the harness drives.
type Node1 = StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords>;

/// The default cluster label every node is built with unless the harness has
/// overridden it (the label-mismatch scenario installs a different label on the
/// responder). Both the dialer and the legitimate responder write this same
/// prefix at stream start, so a healthy join passes the inbound check.
const DEFAULT_LABEL: &[u8] = b"sim-cluster";

/// One in-flight UDP gossip datagram. Carries an encoded plain frame (or a
/// concatenation of frames for a compound). Fault/latency is applied ONCE per
/// datagram (one `send_to`).
struct PendingDatagram {
  deliver_at: Instant,
  from: SocketAddr,
  to: SocketAddr,
  bytes: bytes::Bytes,
}

/// `tcp`-gated deterministic conformance harness. Accessor-only; no `pub`
/// fields. Mirrors [`Cluster`](crate::cluster::Cluster)'s step contract and the
/// sibling [`TlsCluster`](crate::tls_net::TlsCluster) method-for-method.
pub struct TcpCluster {
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
  /// reliable-ping fallback over TCP), as an unordered pair set.
  probe_block: Vec<(SocketAddr, SocketAddr)>,
  /// Pairs whose TCP connect is refused (the dialer's pipe is `reset` the
  /// instant its `Connect` action is delivered). Unordered.
  connect_refuse: Vec<(SocketAddr, SocketAddr)>,
  /// One-shot: arm `host`'s next inbound TCP pipe to half-close after its first
  /// delivered record (truncation mid-frame). `false` once armed onto a pipe.
  half_close_host: Option<SocketAddr>,
  /// When `Some`, every inbound TCP pipe created for this host withholds its
  /// read==0 FIN anchor: the dialer's request is delivered in one coalesced
  /// read with no later FIN, so the responder must drain it on the same tick
  /// it is delivered (the small-coalesced post-promotion drain regression —
  /// structurally analogous to the TLS sibling's `close_notify`-then-no-FIN
  /// case, even though plain TCP carries no in-band close signal).
  withhold_fin_host: Option<SocketAddr>,
  /// Optional `(probe_interval, probe_timeout)` override applied to every node
  /// built via [`add_node`](Self::add_node). `None` uses the standard LAN knobs
  /// that match the plain harness (the conformance-parity invariant).
  probe_window: Option<(Duration, Duration)>,
  /// Per-connection virtual-TCP receive window in bytes applied to every pipe
  /// (the `large_state` backpressure knob). `None` = unbounded.
  tcp_window: Option<usize>,
  /// Per-host label override: when an entry maps `addr` to a label, the node
  /// at that address is built with that label instead of [`DEFAULT_LABEL`].
  /// Used by the label-mismatch scenario to install a different label on the
  /// responder so the inbound check rejects the dialer's stream.
  label_overrides: HashMap<SocketAddr, Vec<u8>>,
  /// Hosts built unlabeled (`TcpOptions::new(None)`): no outbound label
  /// prefix is queued; only required for the dialer-side
  /// response-label-rejection scenario.
  unlabeled_hosts: HashSet<SocketAddr>,
  /// Hosts built with `skip_inbound_label_check=true`: the inbound check
  /// suppresses the missing-but-expected mismatch (an unlabeled inbound is
  /// accepted as plaintext). Faithful to memberlist-core's option of the
  /// same name.
  skip_inbound_label_check_hosts: HashSet<SocketAddr>,
  /// Cross-transport compression applied to every coordinator built via
  /// [`add_node`](Self::add_node). [`new`](memberlist_wire::CompressionOptions::new)
  /// by default — the standard conformance suite drives gossip through
  /// `compress_gossip` on egress and the single canonical `decrypt_gossip`
  /// unwrap on ingress regardless (which strips both the Encrypted and the
  /// Compressed wrapper in one pass, each identity when absent), and a
  /// disabled configuration makes the egress compression an identity, so
  /// the suite stays byte-unchanged. The `*_compressed` constructors
  /// install an enabled configuration.
  compression: memberlist_wire::CompressionOptions,
  /// Cross-transport encryption applied to every coordinator built via
  /// [`add_node`](Self::add_node). [`new`](memberlist_wire::EncryptionOptions::new)
  /// by default — the standard conformance suite drives gossip through
  /// `encrypt_gossip` / `decrypt_gossip` regardless, and a disabled
  /// configuration makes both identity, so the suite stays byte-unchanged.
  /// The `*_encrypted` constructors install an enabled configuration.
  encryption: memberlist_wire::EncryptionOptions,
}

impl TcpCluster {
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
      label_overrides: HashMap::new(),
      unlabeled_hosts: HashSet::new(),
      skip_inbound_label_check_hosts: HashSet::new(),
      compression: memberlist_wire::CompressionOptions::new(),
      encryption: memberlist_wire::EncryptionOptions::new(),
    }
  }

  /// Build one coordinator with the SAME membership knobs the plain
  /// [`Cluster::add_node_with`](crate::cluster::Cluster) and the sibling
  /// [`TlsCluster::add_node`](crate::tls_net::TlsCluster) use, so SWIM timing
  /// is directly comparable (the conformance-parity invariant).
  fn add_node(&mut self, id: &str, addr: SocketAddr) {
    let (probe_interval, probe_timeout) = self
      .probe_window
      .unwrap_or((Duration::from_millis(1000), Duration::from_millis(500)));
    let cfg = EndpointConfig::new(SmolStr::new(id), addr)
      .with_gossip_interval(Duration::from_millis(200))
      .with_push_pull_interval(Duration::from_secs(30))
      .with_probe_interval(probe_interval)
      .with_probe_timeout(probe_timeout)
      .with_suspicion_mult(4)
      .with_retransmit_mult(4)
      .with_rng_seed(Some(addr.port() as u64));
    let mut ep = Endpoint::new(cfg);
    ep.start_scheduling(self.clock.now());
    let configured_label = if self.unlabeled_hosts.contains(&addr) {
      None
    } else {
      Some(
        self
          .label_overrides
          .get(&addr)
          .cloned()
          .unwrap_or_else(|| DEFAULT_LABEL.to_vec()),
      )
    };
    let mut tcp_cfg = TcpOptions::new(configured_label);
    if self.skip_inbound_label_check_hosts.contains(&addr) {
      tcp_cfg = tcp_cfg.with_skip_inbound_label_check(true);
    }
    self.nodes.insert(
      addr,
      StreamEndpoint::with_compression(ep, tcp_cfg, self.compression)
        .with_encryption(self.encryption.clone()),
    );
  }

  /// Two nodes; `a` joins `b` via a plain-TCP push/pull. `a` starts knowing
  /// NOTHING about `b` (no bootstrap alive — `start_push_pull` dials the
  /// address directly), so `a` learns `b` ONLY by merging the join reply and
  /// `b` learns `a` ONLY by merging `a`'s push: both sides converge Alive
  /// purely through the reliable TCP exchange.
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
    algorithm: memberlist_wire::CompressAlgorithm,
  ) -> Self {
    let mut c = Self::empty();
    c.compression = memberlist_wire::CompressionOptions::new()
      .with_algorithm(Some(algorithm))
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
  /// unencrypted run (encryption is transparent to SWIM).
  pub fn two_node_join_encrypted(
    a: SocketAddr,
    b: SocketAddr,
    primary_key: memberlist_wire::SecretKey,
  ) -> Self {
    let mut c = Self::empty();
    c.encryption = memberlist_wire::EncryptionOptions::new()
      .with_keyring(memberlist_wire::Keyring::new(primary_key));
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
    algorithm: memberlist_wire::CompressAlgorithm,
    primary_key: memberlist_wire::SecretKey,
  ) -> Self {
    let mut c = Self::empty();
    c.compression = memberlist_wire::CompressionOptions::new()
      .with_algorithm(Some(algorithm))
      .with_threshold(0);
    c.encryption = memberlist_wire::EncryptionOptions::new()
      .with_keyring(memberlist_wire::Keyring::new(primary_key));
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
      n.handle_alive(a, Alive::new(1, Node::new(SmolStr::new("b"), b)), now);
    }
    if let Some(n) = c.nodes.get_mut(&b) {
      n.handle_alive(b, Alive::new(1, Node::new(SmolStr::new("a"), a)), now);
    }
    c
  }

  /// Like [`two_node_join`](Self::two_node_join) but with a SHORT direct
  /// `probe_timeout` (500 ms) and a LONG `probe_interval` (10 s). The probe's
  /// single cumulative deadline is `sent + probe_interval`, while the
  /// reliable-ping fallback is opened only AFTER the direct `probe_timeout`
  /// elapses — so the fallback has ample time to complete a real TCP
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

  /// `a` joins `b` over plain TCP, but every virtual-TCP pipe is built with a
  /// TINY receive window AND `a`'s push snapshot is pre-loaded with
  /// `extra_peers` extra Alive members. The join push/pull bytes are therefore
  /// far larger than one window's worth, so the writer cannot drain in one
  /// tick: the harness defers the over-window chunks to later ticks (zero byte
  /// loss, TCP never drops in-connection) and the join must still complete
  /// with every pushed peer crossing intact. The small window is set BEFORE
  /// the nodes are built and the extras are injected BEFORE `start_push_pull`,
  /// so the very first push/pull is already large and back-pressured.
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
        n.handle_alive(
          a,
          Alive::new(1, Node::new(SmolStr::new(format!("extra-{i}")), paddr)),
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
    algorithm: memberlist_wire::CompressAlgorithm,
  ) -> Self {
    let mut c = Self::empty();
    c.tcp_window = Some(1200);
    c.compression = memberlist_wire::CompressionOptions::new()
      .with_algorithm(Some(algorithm))
      .with_threshold(0);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      for i in 0..extra_peers {
        let paddr: SocketAddr = format!("203.0.113.{}:9{:03}", (i % 250) + 1, i)
          .parse()
          .unwrap();
        n.handle_alive(
          a,
          Alive::new(1, Node::new(SmolStr::new(format!("extra-{i}")), paddr)),
          now,
        );
      }
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b` over plain TCP with an UNCAPPED receive window, but `b`
  /// (the RESPONDER) is pre-loaded with `extra_peers` extra Alive members.
  /// `b`'s push/pull RESPONSE therefore carries `b`'s whole member list and
  /// arrives at `a` as a single coalesced TCP read, exercising the
  /// large-buffer reassembly path of the plain-TCP record layer without the
  /// small-window fragmentation that the `large_state` scenario uses.
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
        n.handle_alive(
          b,
          Alive::new(1, Node::new(SmolStr::new(format!("extra-{i}")), paddr)),
          now,
        );
      }
    }
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b` over plain TCP with an UNCAPPED receive window, and `a`
  /// (the DIALER) is pre-loaded with `extra_peers` extra Alive members so its
  /// join push REQUEST exceeds typical read-buffer thresholds. `a` writes the
  /// label prefix and the whole request as ONE coalesced byte buffer
  /// delivered to `b` in one transport read; the bridge must strip the label,
  /// buffer the trailing plaintext while still `Handshaking`, and replay it
  /// the same tick it mints + promotes its inbound `Stream`. This is the
  /// plain-TCP analogue of the TLS `large_request_coalesced_with_handshake_flight`
  /// case — TCP has no separate handshake, but the label prefix plays the same
  /// "pre-promotion buffered prefix" role.
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
        n.handle_alive(
          a,
          Alive::new(1, Node::new(SmolStr::new(format!("extra-{i}")), paddr)),
          now,
        );
      }
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b` over plain TCP with an UNCAPPED receive window and a SMALL
  /// join request (no preloaded extras), but `b`'s inbound TCP pipe WITHHOLDS
  /// its read==0 FIN anchor. `a` writes the label prefix and the small request
  /// as ONE coalesced byte buffer delivered to `b` in one transport read; with
  /// the FIN withheld `b` has no later read to lean on and must drain the
  /// buffered request on the SAME tick it mints + promotes its inbound
  /// `Stream` (the post-promotion drain). The small (no-retained-tail) sibling
  /// of the large-request-coalesced case, structurally analogous to the TLS
  /// `small_request_coalesced_with_handshake_flight_no_fin` scenario — TCP's
  /// FIN is the out-of-band analogue of TLS's in-band `close_notify` alert.
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

  /// `a` joins `b`, but `b` (the RESPONDER) is configured with a label that
  /// differs from `a`'s. The inbound label-check on `b` rejects `a`'s stream
  /// before any memberlist message is decoded; no merge runs on either side
  /// and the bridge is torn down. The structural analogue of the TLS
  /// `mtls_required_responder` case — both reject an inbound stream before any
  /// payload is decoded, the only difference being the cluster-isolation gate
  /// (TLS mutual-auth vs wire label).
  pub fn two_node_join_label_mismatch_responder(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.label_overrides.insert(b, b"other-cluster".to_vec());
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b`. `a` is built UNLABELED so its outbound request carries
  /// no `[12][len][label]` prefix; `b` is labeled `cluster-b` with
  /// `skip_inbound_label_check = true` so it suppresses the
  /// missing-but-expected mismatch and accepts `a`'s unlabeled request as
  /// plaintext (faithful to memberlist-core's option). `b` then responds
  /// with `[12][len][cluster-b][response]` — its own outbound label, queued
  /// at construction symmetric with the dialer. `a` is unlabeled with the
  /// default `skip = false`, so per the inbound truth table a labeled `[12]`
  /// header on an unlabeled receiver is rejected as a double label
  /// (`memberlist::codec`'s rule): `a` never merges `b`'s response. With
  /// bidirectional wire labels (each side queues an outbound prefix and
  /// validates the inbound), neither side ever applies a cross-cluster
  /// merge. The dialer-side analogue of
  /// [`two_node_join_label_mismatch_responder`](Self::two_node_join_label_mismatch_responder).
  pub fn two_node_join_label_mismatch_responder_reply(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    c.unlabeled_hosts.insert(a);
    c.label_overrides.insert(b, b"cluster-b".to_vec());
    c.skip_inbound_label_check_hosts.insert(b);
    c.add_node("a", a);
    c.add_node("b", b);
    let now = c.clock.now();
    if let Some(n) = c.nodes.get_mut(&a) {
      n.start_push_pull(b, PushPullKind::Join, now);
    }
    c
  }

  /// `a` joins `b` over plain TCP with a TINY receive window, so the dialer's
  /// coalesced `[label||request]` buffer is fragmented across multiple TCP
  /// reads. The companion [`half_close_tcp_after_first_record`](Self::half_close_tcp_after_first_record)
  /// hook arms b's pipe to read==0 after the FIRST fragment; the rest of the
  /// request never reaches b → b's frame decode never reaches `Done` → no
  /// merge runs. The structural analogue of the TLS `truncation_mid_frame`
  /// case (TLS naturally splits the handshake from the application records,
  /// so a small window is unnecessary there; plain TCP has no application-
  /// level framing on the wire so the window is the only way to guarantee
  /// fragmentation).
  pub fn two_node_join_truncated_mid_frame(a: SocketAddr, b: SocketAddr) -> Self {
    let mut c = Self::empty();
    // 8 bytes guarantees the label header (`[12][len][label]`, 13+ bytes for
    // the default label) is itself split into multiple fragments, so the
    // truncation cuts the wire BEFORE the request body has been delivered.
    c.tcp_window = Some(8);
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
  /// forcing the reliable-ping fallback over TCP.
  pub fn drop_all_udp_probes_between(&mut self, a: SocketAddr, b: SocketAddr) {
    self.probe_block.push((a, b));
  }

  /// Refuse the TCP connect for the (unordered) pair `a`/`b`: the dialer's pipe
  /// is `reset` the instant its `Connect` action is delivered, so its next
  /// `handle_transport_data(id, &[], eof=true, now)` surfaces the connect failure.
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
      n.handle_suspect(host, Suspect::new(inc, target, from), now);
    }
  }

  /// Inject an Alive at `host` about `peer` (e.g. a higher-incarnation refute).
  pub fn inject_alive(&mut self, host: SocketAddr, peer: SmolStr, paddr: SocketAddr, inc: u32) {
    let now = self.clock.now();
    if let Some(n) = self.nodes.get_mut(&host) {
      n.handle_alive(host, Alive::new(inc, Node::new(peer, paddr)), now);
    }
  }

  /// Begin a graceful leave on `host` (delegates to [`StreamEndpoint::leave`]).
  pub fn leave(&mut self, host: SocketAddr) -> Result<(), memberlist_machine::Error> {
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
    self.member_state(host, peer) == Some(memberlist_wire::typed::State::Alive)
  }

  /// `host`'s gossip-tracked liveness state for `peer`, if known.
  pub fn member_state(
    &self,
    host: SocketAddr,
    peer: &SmolStr,
  ) -> Option<memberlist_wire::typed::State> {
    self.nodes.get(&host)?.endpoint().member_liveness(peer)
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

  /// Iterate every node's coordinator — test-only, for asserting the
  /// compression configuration the `*_compressed` constructors threaded.
  #[cfg(test)]
  fn nodes_for_test(&self) -> impl Iterator<Item = &Node1> {
    self.nodes.values()
  }

  // ── Step loop (mirrors TlsCluster::step) ────────────────────────────────────

  /// One simulation tick. Returns `true` if anything happened.
  ///
  /// 0. Pre-pump every coordinator with `handle_timeout(now)` so a freshly-
  ///    queued dial/accept, every bridge byte-pump, and `collect_transmits` run
  ///    BEFORE the outbound directives/bytes/datagrams are drained.
  /// 1. Drain every node's gossip [`Transmit`] (plain-frame encoded → UDP
  ///    queue), transport directives ([`StreamAction`]), and outbound bytes
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

    // 1. Drain every coordinator's outbound directives + bytes + gossip.
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

  /// Drain every coordinator's transport directives, outbound bytes, and
  /// gossip transmits → route them (mappings/pipes, peer pipes, UDP queue).
  ///
  /// Ordering is load-bearing: `Connect` directives are applied BEFORE bytes
  /// (so a fresh dial's pipe mapping exists before its label prefix is
  /// routed), but `Shutdown`/`Close` directives are applied AFTER bytes (the
  /// FIN segment is the LAST thing on the wire — it must be armed after the
  /// tick's data fragments are queued, else the read==0 anchor could be timed
  /// before data the bridge already wrote, denying the peer a tick to generate
  /// its push/pull response). Returns `true` if anything was routed.
  fn drain_and_route(&mut self, addrs: &[SocketAddr], now: Instant) -> bool {
    let mut any = false;
    // Drain every node's currently-available actions, partitioning Connect
    // (pre-bytes) from Shutdown/Close (post-bytes). Teardowns are withheld by
    // the coordinator while their exchange still has bytes in
    // `poll_transport_transmit`; we re-poll after the byte drain below to
    // surface those released teardowns and apply them in the same step.
    let mut connects: Vec<(SocketAddr, StreamAction)> = Vec::new();
    let mut teardowns: Vec<(SocketAddr, StreamAction)> = Vec::new();
    for src in addrs {
      let node = self.nodes.get_mut(src).unwrap();
      while let Some(action) = node.poll_action() {
        match &action {
          StreamAction::Connect(_) => connects.push((*src, action)),
          StreamAction::Shutdown(_) | StreamAction::Close(_) => teardowns.push((*src, action)),
        }
      }
    }
    // (a) Connect directives → establish mappings + pipes.
    for (src, action) in connects {
      if self.apply_action(src, action, now) {
        any = true;
      }
    }
    // (b) Outbound bytes → peer pipe.
    for src in addrs {
      let mut chunks: Vec<(ExchangeId, SocketAddr, bytes::Bytes)> = Vec::new();
      let node = self.nodes.get_mut(src).unwrap();
      while let Some((id, peer, bytes)) = node.poll_transport_transmit() {
        chunks.push((id, peer, bytes));
      }
      for (id, peer, bytes) in chunks {
        if self.route_bytes(*src, id, peer, bytes, now) {
          any = true;
        }
      }
    }
    // (b.5) Re-poll actions: the coordinator gates per-exchange teardowns
    // behind the exchange's bytes, so a `Shutdown`/`Close` enqueued the same
    // tick as its trailing bytes surfaces only AFTER the bytes are drained.
    // Appending the released teardowns here keeps the "FIN after data" wire
    // order (arm_fin runs in step (c) below, after route_bytes has already
    // queued every chunk).
    let mut late_connects: Vec<(SocketAddr, StreamAction)> = Vec::new();
    for src in addrs {
      let node = self.nodes.get_mut(src).unwrap();
      while let Some(action) = node.poll_action() {
        match &action {
          StreamAction::Connect(_) => late_connects.push((*src, action)),
          StreamAction::Shutdown(_) | StreamAction::Close(_) => teardowns.push((*src, action)),
        }
      }
    }
    // No new Connect should be emitted by a byte drain alone; if one appears
    // (e.g. from a same-tick reliable-fallback) apply it before its bytes are
    // produced. Drained in producer order.
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
            if let Some(b) = encode_frame(&p.message) {
              pending.push((p.to, b));
            }
          }
          Transmit::Compound(cmp) => {
            // A compound is ONE datagram: concatenate the plain frames so a
            // single fault/latency decision covers the whole bundle.
            let mut buf = Vec::new();
            for m in &cmp.messages {
              if let Some(b) = encode_frame(m) {
                buf.extend_from_slice(&b);
              }
            }
            if !buf.is_empty() {
              pending.push((cmp.to, bytes::Bytes::from(buf)));
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
        // mark the dialer's own pipe `reset` so no bytes ever flow. The
        // dialer's bridge stays awaiting the FIN-anchor (the empty-slice
        // anchor surfaces the connect failure on its next delivery pass), so
        // it is reaped when the dial deadline elapses.
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
        // Accept on the peer: it allocates its own exchange handle.
        let acceptor_exch = self
          .nodes
          .get_mut(&peer)
          .unwrap()
          .accept_connection(src, now);
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
        // first record, the connection is half-closed (read==0 mid-frame), so
        // the dialer's request frame never completes on the acceptor side.
        // The acceptor therefore never merges the dialer (no
        // `PushPullRequestReceived` is decoded), and — with no membership
        // entry to gossip — the dialer never learns the acceptor through ANY
        // path. (`half_close_host` names the ACCEPTOR, the side whose read is
        // truncated.)
        if self.half_close_host == Some(peer) {
          acceptor_pipe.half_close_after_first = true;
          self.half_close_host = None;
        }
        // Withhold the acceptor's read==0 FIN anchor: the dialer's bytes still
        // arrive (the coalesced first request rides one TCP read), but no
        // later read==0 ever surfaces, so the acceptor must drain the buffered
        // request on the same tick it is delivered (post-promotion).
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
    }
  }

  /// Route one outbound buffer from `src`'s exchange into the peer's reader
  /// pipe. With no receive window the whole buffer is one chunk delivered
  /// after `latency`. With a window the buffer is fragmented into
  /// `window`-sized pieces released one round-trip apart — modeling a tiny TCP
  /// receive window that lets the reader pull ~`window` bytes per round-trip,
  /// so a large transfer drains across many ticks (NOT dropped — TCP never
  /// drops in-connection). A buffer whose connection has no mapping (the peer
  /// tore it down) is dropped on the floor — the bridge that produced it is
  /// already being reaped. Returns `true` if anything was queued.
  fn route_bytes(
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
    let Some(pipe) = self.pipes.get_mut(&(peer, peer_exch)) else {
      return false;
    };
    // No delivery into a reset pipe, an empty write, or a pipe whose reader
    // side has half-closed (FIN armed): once the read half is closed the writer
    // can send no more bytes that the reader will ever observe (the harness's
    // forced-truncation hooks arm the FIN to drop the remainder of the frame).
    if pipe.reset || pipe.fin_at.is_some() || bytes.is_empty() {
      return false;
    }
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
    // "direct + indirect UDP probes dropped" while leaving the TCP reliable
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
  /// The plain-TCP coordinator's `handle_transport_data` carries an explicit
  /// `eof: bool` argument (TLS's in-band `close_notify` is absent on plain
  /// TCP, so the FIN signal must ride the same call as the bytes). Each
  /// delivery is a tuple `(payload, eof)`: a matured data chunk is
  /// `(Some(bytes), false)` (FIN follows on a later pass), a matured
  /// separate FIN/reset anchor is `(None, true)`, and — for a pipe whose
  /// `withhold_fin` flag is set — the FIN piggybacks with the LAST queued
  /// data chunk as `(Some(bytes), true)`, modeling a TCP read that returns
  /// both buffered bytes and `read==0` on the same wake. That is the
  /// structural analog of the TLS sibling's `close_notify` riding in-band
  /// with the trailing ciphertext: the receiver must drain the bytes and
  /// honor the EOF on the SAME tick the bridge mints + promotes its
  /// `Stream`, with no later transport read available to lean on.
  fn deliver_tcp_ready(&mut self, now: Instant) -> usize {
    // Collect the deliveries first (key, payload, eof) so the
    // `handle_transport_data` calls — which mutate the nodes — do not alias
    // the pipe borrow. One chunk per pipe per pass keeps FIFO/anchor ordering
    // tight across repeated passes within the same `now`.
    let stride = self.tcp_stride();
    let mut count = 0;
    // Pipes that delivered a data chunk during THIS invocation WITHOUT
    // piggybacking the FIN: their separate FIN anchor (if any) is held until
    // a LATER invocation (a later clock instant), so the reader keeps a tick
    // to act on the data (e.g. generate a push/pull response) before
    // honoring the EOF. A piggybacked FIN bypasses this gate because the
    // EOF rode with the data in one TCP read.
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
        // A matured data chunk.
        if let Some((t, _)) = pipe.inbound.front() {
          if *t <= now {
            let (_, b) = pipe.inbound.pop_front().unwrap();
            // One-shot truncation: after the first delivered record, drop the
            // remaining queued data (the reader's recv half has just been
            // closed — the FIN that follows is the read==0 anchor and any
            // further buffered bytes are lost; the harness models the
            // half-close as terminating the read stream immediately) and ARM
            // the FIN one stride later so the read==0 lands strictly after
            // this partial record.
            if pipe.half_close_after_first {
              pipe.half_close_after_first = false;
              pipe.inbound.clear();
              pipe.arm_fin(now, stride);
            }
            // FIN-piggyback: when `withhold_fin` is set AND the writer's FIN
            // is armed AND this drain emptied the inbound queue, the EOF
            // signal rides WITH this chunk — modeling a TCP read that
            // returns both data and read==0 on the same wake, the plain-TCP
            // structural analog of the TLS sibling's in-band `close_notify`
            // arriving with the trailing ciphertext.
            let piggyback_eof =
              pipe.withhold_fin && pipe.fin_at.is_some() && pipe.inbound.is_empty();
            if piggyback_eof {
              pipe.eof_anchor_sent = true;
            } else {
              data_this_call.insert(key);
            }
            deliveries.push((key, Some(b), piggyback_eof));
            continue;
          }
        }
        // The FIN segment: deliver read==0 once, only after every queued data
        // byte has drained, the FIN's own arrival instant has matured, AND no
        // data was delivered to this pipe in this same invocation. A pipe
        // that withholds its FIN never surfaces a separate read==0 anchor
        // (its EOF rode piggybacked with the last data chunk above).
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
        let me = node.endpoint().local_id().clone();
        let mut sus = Vec::new();
        let mut alv = Vec::new();
        let mut gn = Vec::new();
        for ns in node.endpoint().members().filter(|ns| ns.id() != &me) {
          match node.endpoint().member_liveness(ns.id()) {
            Some(memberlist_wire::typed::State::Suspect) => sus.push(ns.id().clone()),
            Some(memberlist_wire::typed::State::Alive) => alv.push(ns.id().clone()),
            Some(memberlist_wire::typed::State::Dead)
            | Some(memberlist_wire::typed::State::Left) => gn.push(ns.id().clone()),
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
mod tests {
  use super::*;

  #[test]
  fn frame_roundtrips() {
    let m: Message<SmolStr, SocketAddr> = Message::Alive(Alive::new(
      7,
      Node::new(SmolStr::new("x"), "127.0.0.1:9000".parse().unwrap()),
    ));
    let b = encode_frame(&m).expect("alive encodes");
    let (n, back) = decode_frame(&b).expect("decodes");
    assert_eq!(n, b.len());
    assert!(matches!(back, Message::Alive(_)));
  }

  #[test]
  fn identity_bridge_is_identity() {
    let a: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    assert_eq!(IdentityBridge::to_socket(&a), a);
    assert_eq!(IdentityBridge::from_socket(a), a);
  }

  #[test]
  fn tcp_cluster_compression_mode_configures_nodes() {
    use memberlist_wire::CompressAlgorithm;
    let a = "127.0.0.1:9301".parse().unwrap();
    let b = "127.0.0.1:9302".parse().unwrap();
    let c = TcpCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
    // Every node carries the configured algorithm.
    for node in c.nodes_for_test() {
      assert_eq!(node.compression().algorithm(), Some(CompressAlgorithm::Lz4));
    }
  }
}
