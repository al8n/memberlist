//! `EndpointConfig` — construction-time settings for the Endpoint state machine.
//!
//! The design uses event-driven decisions (no callbacks): the application
//! drains [`Event::PendingAlive`](crate::event::Event::PendingAlive) /
//! [`Event::PendingMerge`](crate::event::Event::PendingMerge) events from
//! `Endpoint::poll_event` and responds via `decide_alive` / `decide_merge`.
//! State the application wants to push to the Endpoint (e.g. ack payloads,
//! per-target reliable-ping opt-out) flows through dedicated setters.

use core::time::Duration;

use bytes::Bytes;
use memberlist_wire::typed::{DelegateVersion, Meta, ProtocolVersion};

/// Default value for [`EndpointConfig::gossip_mtu`]: 1400 bytes — just under
/// a typical 1500-byte Ethernet MTU, matching the legacy memberlist default
/// and keeping UDP gossip un-fragmented on IPv4/IPv6 + UDP-header headroom.
/// The compound-frame budget reserves its own framing from this; a lone
/// `Packet` is bounded directly by it.
pub const DEFAULT_GOSSIP_MTU: usize = 1400;

/// Construction-time settings for [`Endpoint`](crate::endpoint::Endpoint).
#[derive(Debug, Clone)]
pub struct EndpointConfig<I, A> {
  local_id: I,
  advertise_addr: A,
  initial_meta: Meta,
  initial_local_state: Bytes,
  suspicion_mult: u32,
  suspicion_max_timeout_mult: u32,
  probe_interval: Duration,
  gossip_to_the_dead_time: Duration,
  dead_node_reclaim_time: Duration,
  awareness_max_multiplier: u32,
  indirect_checks: u32,
  probe_timeout: Duration,
  /// Deadline for a complete stream exchange (dial + request + response).
  /// Default: 10 seconds. Used for push/pull, reliable ping, and user messages.
  stream_timeout: Duration,
  /// Hard cap on a single inbound reliable-stream frame
  /// (`[tag][varint len][body]`). A frame whose declared total exceeds
  /// this is rejected the moment the length varint is decoded — BEFORE
  /// the body is buffered — so a peer cannot exhaust memory by declaring a
  /// huge length and dribbling bytes. Default: 64 MiB (generous enough for
  /// a very large push/pull snapshot; tune up for huge clusters).
  max_stream_frame_size: usize,
  /// Maximum plaintext byte size for an outbound gossip datagram, before
  /// any codec-layer transform (compression and/or encryption) is applied.
  /// The default (1400) leaves headroom for IPv4/IPv6 + UDP headers plus
  /// the encryption wrapper overhead on typical 1500-byte-MTU networks.
  ///
  /// When encryption is enabled, the on-wire datagram size can exceed
  /// `gossip_mtu` by [`memberlist_wire::ENCRYPTED_WRAPPER_OVERHEAD`] (30
  /// bytes — wrapper header + 12-byte nonce + 16-byte AEAD auth tag).
  /// Operators on tight path-MTU networks should size this accordingly:
  /// `gossip_mtu = path_mtu - ip_udp_headers - 30` to stay under the
  /// path's UDP-safe ceiling.
  ///
  /// Parallel to [`Self::max_stream_frame_size`] (the reliable-path bound).
  gossip_mtu: usize,
  /// How often to gossip broadcasts to `gossip_nodes` random peers.
  /// `Duration::ZERO` disables gossip. Default: 200 ms (LAN).
  gossip_interval: Duration,
  /// Number of random peers selected per gossip round. Default: 3.
  gossip_nodes: usize,
  /// How often to run a full push/pull anti-entropy exchange with one
  /// random peer. `Duration::ZERO` disables push/pull. Default: 30 s (LAN).
  push_pull_interval: Duration,
  retransmit_mult: u32,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
  rng_seed: Option<u64>,
}

impl<I, A> EndpointConfig<I, A> {
  /// Construct a new config with sensible LAN defaults.
  pub fn new(local_id: I, advertise_addr: A) -> Self {
    Self {
      local_id,
      advertise_addr,
      initial_meta: Meta::empty(),
      initial_local_state: Bytes::new(),
      suspicion_mult: 4,
      suspicion_max_timeout_mult: 6,
      probe_interval: Duration::from_secs(1),
      gossip_to_the_dead_time: Duration::from_secs(30),
      dead_node_reclaim_time: Duration::ZERO,
      awareness_max_multiplier: 8,
      indirect_checks: 3,
      probe_timeout: Duration::from_millis(500),
      stream_timeout: Duration::from_secs(10),
      max_stream_frame_size: 64 * 1024 * 1024,
      gossip_mtu: DEFAULT_GOSSIP_MTU,
      gossip_interval: Duration::from_millis(200),
      gossip_nodes: 3,
      push_pull_interval: Duration::from_secs(30),
      retransmit_mult: 4,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
      rng_seed: None,
    }
  }

  /// Builder: set initial node metadata.
  pub fn with_initial_meta(mut self, meta: Meta) -> Self {
    self.initial_meta = meta;
    self
  }

  /// Builder: set initial local-state snapshot (for push/pull).
  pub fn with_initial_local_state(mut self, state: Bytes) -> Self {
    self.initial_local_state = state;
    self
  }

  /// Builder: set suspicion multiplier (timeout = mult * log(N+1) * probe_interval).
  pub fn with_suspicion_mult(mut self, mult: u32) -> Self {
    self.suspicion_mult = mult;
    self
  }

  /// Builder: set suspicion max-timeout multiplier.
  pub fn with_suspicion_max_timeout_mult(mut self, mult: u32) -> Self {
    self.suspicion_max_timeout_mult = mult;
    self
  }

  /// Builder: set probe interval.
  pub fn with_probe_interval(mut self, d: Duration) -> Self {
    self.probe_interval = d;
    self
  }

  /// Builder: set gossip-to-the-dead time.
  pub fn with_gossip_to_the_dead_time(mut self, d: Duration) -> Self {
    self.gossip_to_the_dead_time = d;
    self
  }

  /// Builder: set dead-node-reclaim time. `0` disables reclaim.
  pub fn with_dead_node_reclaim_time(mut self, d: Duration) -> Self {
    self.dead_node_reclaim_time = d;
    self
  }

  /// Builder: set awareness max multiplier.
  pub fn with_awareness_max_multiplier(mut self, m: u32) -> Self {
    self.awareness_max_multiplier = m;
    self
  }

  /// Builder: set the number of indirect peers used as fallback.
  pub fn with_indirect_checks(mut self, n: u32) -> Self {
    self.indirect_checks = n;
    self
  }

  /// Builder: set the direct-ping timeout.
  pub fn with_probe_timeout(mut self, d: Duration) -> Self {
    self.probe_timeout = d;
    self
  }

  /// Sets the per-stream exchange timeout (builder pattern).
  pub fn with_stream_timeout(mut self, d: Duration) -> Self {
    self.stream_timeout = d;
    self
  }

  /// Builder: set the hard cap on a single inbound reliable-stream frame.
  pub fn with_max_stream_frame_size(mut self, n: usize) -> Self {
    self.max_stream_frame_size = n;
    self
  }

  /// Builder: set the plaintext-byte ceiling for an outbound gossip
  /// datagram. The on-wire datagram may exceed this by
  /// [`memberlist_wire::ENCRYPTED_WRAPPER_OVERHEAD`] when encryption is
  /// enabled — see the field doc on `gossip_mtu`.
  pub fn with_gossip_mtu(mut self, n: usize) -> Self {
    self.gossip_mtu = n;
    self
  }

  /// Builder: set gossip interval (`Duration::ZERO` disables gossip).
  pub fn with_gossip_interval(mut self, d: Duration) -> Self {
    self.gossip_interval = d;
    self
  }

  /// Builder: set the number of peers to gossip to per round.
  pub fn with_gossip_nodes(mut self, n: usize) -> Self {
    self.gossip_nodes = n;
    self
  }

  /// Builder: set push/pull interval (`Duration::ZERO` disables push/pull).
  pub fn with_push_pull_interval(mut self, d: Duration) -> Self {
    self.push_pull_interval = d;
    self
  }

  /// Builder: set broadcast retransmit multiplier.
  pub fn with_retransmit_mult(mut self, m: u32) -> Self {
    self.retransmit_mult = m;
    self
  }

  /// Builder: set protocol version.
  pub fn with_protocol_version(mut self, v: ProtocolVersion) -> Self {
    self.protocol_version = v;
    self
  }

  /// Builder: set delegate version.
  pub fn with_delegate_version(mut self, v: DelegateVersion) -> Self {
    self.delegate_version = v;
    self
  }

  /// Builder: set the deterministic RNG seed (`None` ⇒ OS entropy).
  pub const fn with_rng_seed(mut self, seed: Option<u64>) -> Self {
    self.rng_seed = seed;
    self
  }

  // Getters.

  /// The local node's id.
  pub const fn local_id(&self) -> &I {
    &self.local_id
  }

  /// The local node's advertise address.
  pub const fn advertise_addr(&self) -> &A {
    &self.advertise_addr
  }

  /// The initial metadata for the local node.
  pub const fn initial_meta(&self) -> &Meta {
    &self.initial_meta
  }

  /// The initial local-state snapshot (for push/pull).
  pub const fn initial_local_state(&self) -> &Bytes {
    &self.initial_local_state
  }

  /// Suspicion multiplier.
  pub const fn suspicion_mult(&self) -> u32 {
    self.suspicion_mult
  }

  /// Suspicion max-timeout multiplier.
  pub const fn suspicion_max_timeout_mult(&self) -> u32 {
    self.suspicion_max_timeout_mult
  }

  /// Probe interval.
  pub const fn probe_interval(&self) -> Duration {
    self.probe_interval
  }

  /// Gossip-to-the-dead time.
  pub const fn gossip_to_the_dead_time(&self) -> Duration {
    self.gossip_to_the_dead_time
  }

  /// Dead-node-reclaim time.
  pub const fn dead_node_reclaim_time(&self) -> Duration {
    self.dead_node_reclaim_time
  }

  /// Awareness max multiplier.
  pub const fn awareness_max_multiplier(&self) -> u32 {
    self.awareness_max_multiplier
  }

  /// Number of indirect peers used as fallback for failed direct pings.
  pub const fn indirect_checks(&self) -> u32 {
    self.indirect_checks
  }

  /// Direct-ping timeout before fallback to indirect.
  pub const fn probe_timeout(&self) -> Duration {
    self.probe_timeout
  }

  /// Returns the per-stream exchange timeout.
  pub const fn stream_timeout(&self) -> Duration {
    self.stream_timeout
  }

  /// Returns the hard cap on a single inbound reliable-stream frame.
  pub const fn max_stream_frame_size(&self) -> usize {
    self.max_stream_frame_size
  }

  /// Returns the plaintext-byte ceiling for an outbound gossip datagram.
  /// On-wire datagrams may exceed this by
  /// [`memberlist_wire::ENCRYPTED_WRAPPER_OVERHEAD`] when encryption is
  /// enabled — see the field doc on `gossip_mtu`.
  pub const fn gossip_mtu(&self) -> usize {
    self.gossip_mtu
  }

  /// How often gossip rounds fire. `Duration::ZERO` means disabled.
  pub const fn gossip_interval(&self) -> Duration {
    self.gossip_interval
  }

  /// Number of random peers selected per gossip round.
  pub const fn gossip_nodes(&self) -> usize {
    self.gossip_nodes
  }

  /// How often push/pull anti-entropy fires. `Duration::ZERO` means disabled.
  pub const fn push_pull_interval(&self) -> Duration {
    self.push_pull_interval
  }

  /// Broadcast retransmit multiplier.
  pub const fn retransmit_mult(&self) -> u32 {
    self.retransmit_mult
  }

  /// Protocol version.
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  /// Delegate version.
  pub const fn delegate_version(&self) -> DelegateVersion {
    self.delegate_version
  }

  /// Optional deterministic RNG seed.
  pub const fn rng_seed(&self) -> Option<u64> {
    self.rng_seed
  }
}
