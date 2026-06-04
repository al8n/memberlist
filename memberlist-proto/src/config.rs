//! `EndpointOptions` — construction-time settings for the Endpoint state machine.
//!
//! The design uses event-driven decisions (no callbacks): the application
//! drains [`Event::PendingAlive`](crate::event::Event::PendingAlive) /
//! [`Event::PendingMerge`](crate::event::Event::PendingMerge) events from
//! `Endpoint::poll_event` and responds via `decide_alive` / `decide_merge`.
//! State the application wants to push to the Endpoint (e.g. ack payloads,
//! per-target reliable-ping opt-out) flows through dedicated setters.

use core::time::Duration;

use crate::typed::{DelegateVersion, Meta, ProtocolVersion};
use bytes::Bytes;

/// Default value for [`EndpointOptions::gossip_mtu`]: 1400 bytes — just under
/// a typical 1500-byte Ethernet MTU, matching the legacy memberlist default
/// and keeping UDP gossip un-fragmented on IPv4/IPv6 + UDP-header headroom.
/// The compound-frame budget reserves its own framing from this; a lone
/// `Packet` is bounded directly by it.
pub const DEFAULT_GOSSIP_MTU: usize = 1400;

/// Default value for [`EndpointOptions::meta_max_size`]: 512 bytes —
/// the legacy memberlist limit, retained as the default to ease
/// migration from Go memberlist clusters. The absolute upper bound
/// (the wire-construction ceiling on `Meta`) is
/// [`crate::typed::Meta::MAX_SIZE`] (`u16::MAX`); operators
/// who want larger node metadata can raise this via
/// [`EndpointOptions::with_meta_max_size`] up to that wire ceiling.
pub const DEFAULT_META_MAX_SIZE: usize = 512;

/// Default value for [`EndpointOptions::max_stream_frame_size`]: 64 MiB.
/// The hard cap on a single inbound reliable-stream frame
/// (`[tag][varint len][body]`); a declared frame larger than this is
/// rejected the moment the length varint is decoded. Generous enough for a
/// very large push/pull snapshot; tune up for huge clusters.
pub const DEFAULT_MAX_STREAM_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Default value for [`EndpointOptions::accept_handshake_deadline`]:
/// 10 seconds. Bounds the time a server-side bridge spends in
/// `Handshaking` (label or TLS handshake step) before the
/// coordinator reaps it as failed. Long enough to ride out typical
/// TLS handshake latency over a busy WAN; short enough to prevent
/// half-open server sockets from accumulating under a
/// connect-but-never-send attacker.
pub const DEFAULT_ACCEPT_HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// Construction-time settings for [`Endpoint`](crate::endpoint::Endpoint).
#[derive(Debug, Clone)]
pub struct EndpointOptions<I, A> {
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
  /// `gossip_mtu` by [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] (30
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
  /// Per-endpoint cap on the LOCAL node's `Meta` byte length.
  /// Enforced at [`Self::with_initial_meta`] construction-time (via
  /// `Endpoint::new` debug_assert) and on every outgoing
  /// `Endpoint::update_meta` call (returns
  /// `Error::MetaExceedsCap`). Bounded above by
  /// [`crate::typed::Meta::MAX_SIZE`] (the absolute wire
  /// ceiling). Default [`DEFAULT_META_MAX_SIZE`] (512, matching Go
  /// memberlist).
  ///
  /// This cap is LOCAL-ONLY — incoming Alives from peers carry
  /// whatever Meta size their own configuration allows (bounded
  /// above by `Meta::MAX_SIZE`). A node sets this knob to limit
  /// its OWN meta-broadcast size, not to refuse peers with larger
  /// metas; refusing peers would cause silent cluster
  /// fragmentation that hides bootstrap / config-skew failures.
  meta_max_size: usize,
  /// Deadline for a server-side bridge's `Handshaking` step (label
  /// validation for plain TCP; TLS handshake for TLS records). A
  /// peer that connects but never sends the label / completes the
  /// handshake within this window is reaped as failed. Default
  /// [`DEFAULT_ACCEPT_HANDSHAKE_DEADLINE`] (10 s).
  accept_handshake_deadline: Duration,
  /// How often to run a full push/pull anti-entropy exchange with one
  /// random peer. `Duration::ZERO` disables push/pull. Default: 30 s (LAN).
  push_pull_interval: Duration,
  retransmit_mult: u32,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
  rng_seed: Option<u64>,
}

impl<I, A> EndpointOptions<I, A> {
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
      max_stream_frame_size: DEFAULT_MAX_STREAM_FRAME_SIZE,
      gossip_mtu: DEFAULT_GOSSIP_MTU,
      gossip_interval: Duration::from_millis(200),
      gossip_nodes: 3,
      meta_max_size: DEFAULT_META_MAX_SIZE,
      accept_handshake_deadline: DEFAULT_ACCEPT_HANDSHAKE_DEADLINE,
      push_pull_interval: Duration::from_secs(30),
      retransmit_mult: 4,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
      rng_seed: None,
    }
  }

  /// Builder: set initial node metadata.
  #[must_use]
  #[inline(always)]
  pub fn with_initial_meta(mut self, meta: Meta) -> Self {
    self.initial_meta = meta;
    self
  }

  /// Builder: set initial local-state snapshot (for push/pull).
  #[must_use]
  #[inline(always)]
  pub fn with_initial_local_state(mut self, state: Bytes) -> Self {
    self.initial_local_state = state;
    self
  }

  /// Builder: set suspicion multiplier (timeout = mult * log(N+1) * probe_interval).
  #[must_use]
  #[inline(always)]
  pub const fn with_suspicion_mult(mut self, mult: u32) -> Self {
    self.suspicion_mult = mult;
    self
  }

  /// Builder: set suspicion max-timeout multiplier.
  #[must_use]
  #[inline(always)]
  pub const fn with_suspicion_max_timeout_mult(mut self, mult: u32) -> Self {
    self.suspicion_max_timeout_mult = mult;
    self
  }

  /// Builder: set probe interval.
  #[must_use]
  #[inline(always)]
  pub const fn with_probe_interval(mut self, d: Duration) -> Self {
    self.probe_interval = d;
    self
  }

  /// Builder: set gossip-to-the-dead time.
  #[must_use]
  #[inline(always)]
  pub const fn with_gossip_to_the_dead_time(mut self, d: Duration) -> Self {
    self.gossip_to_the_dead_time = d;
    self
  }

  /// Builder: set dead-node-reclaim time. `0` disables reclaim.
  #[must_use]
  #[inline(always)]
  pub const fn with_dead_node_reclaim_time(mut self, d: Duration) -> Self {
    self.dead_node_reclaim_time = d;
    self
  }

  /// Builder: set awareness max multiplier.
  #[must_use]
  #[inline(always)]
  pub const fn with_awareness_max_multiplier(mut self, m: u32) -> Self {
    self.awareness_max_multiplier = m;
    self
  }

  /// Builder: set the number of indirect peers used as fallback.
  #[must_use]
  #[inline(always)]
  pub const fn with_indirect_checks(mut self, n: u32) -> Self {
    self.indirect_checks = n;
    self
  }

  /// Builder: set the direct-ping timeout.
  #[must_use]
  #[inline(always)]
  pub const fn with_probe_timeout(mut self, d: Duration) -> Self {
    self.probe_timeout = d;
    self
  }

  /// Sets the per-stream exchange timeout (builder pattern).
  #[must_use]
  #[inline(always)]
  pub const fn with_stream_timeout(mut self, d: Duration) -> Self {
    self.stream_timeout = d;
    self
  }

  /// Builder: set the hard cap on a single inbound reliable-stream frame.
  #[must_use]
  #[inline(always)]
  pub const fn with_max_stream_frame_size(mut self, n: usize) -> Self {
    self.max_stream_frame_size = n;
    self
  }

  /// Builder: set the plaintext-byte ceiling for an outbound gossip
  /// datagram. The on-wire datagram may exceed this by
  /// [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] when encryption is
  /// enabled — see the field doc on `gossip_mtu`.
  #[must_use]
  #[inline(always)]
  pub const fn with_gossip_mtu(mut self, n: usize) -> Self {
    self.gossip_mtu = n;
    self
  }

  /// Builder: set gossip interval (`Duration::ZERO` disables gossip).
  #[must_use]
  #[inline(always)]
  pub const fn with_gossip_interval(mut self, d: Duration) -> Self {
    self.gossip_interval = d;
    self
  }

  /// Builder: set the number of peers to gossip to per round.
  #[must_use]
  #[inline(always)]
  pub const fn with_gossip_nodes(mut self, n: usize) -> Self {
    self.gossip_nodes = n;
    self
  }

  /// Builder: set the per-endpoint LOCAL `Meta` size cap. Capped
  /// above by [`crate::typed::Meta::MAX_SIZE`] (the
  /// absolute wire ceiling); operators wanting to broadcast larger
  /// node metadata than the default 512 (Go memberlist parity) can
  /// raise this up to that ceiling.
  ///
  /// This cap limits the LOCAL node's broadcasts only — incoming
  /// peer Alives are accepted regardless of their Meta size (up to
  /// the wire ceiling). Refusing peers with larger metas would
  /// cause silent cluster fragmentation that hides bootstrap and
  /// config-skew failures, so the FSM does not gate inbound Alives
  /// on this cap. Operators should still keep the cap consistent
  /// cluster-wide if they raise it, so every peer can broadcast
  /// the same maximum size.
  #[must_use]
  #[inline(always)]
  pub const fn with_meta_max_size(mut self, n: usize) -> Self {
    self.meta_max_size = n;
    self
  }

  /// Builder: set the server-side accept handshake deadline.
  /// Bounds the time a bridge spends in `Handshaking` (label step
  /// for plain TCP / TLS handshake for TLS records) before the
  /// coordinator reaps it as failed.
  #[must_use]
  #[inline(always)]
  pub const fn with_accept_handshake_deadline(mut self, d: Duration) -> Self {
    self.accept_handshake_deadline = d;
    self
  }

  /// Builder: set push/pull interval (`Duration::ZERO` disables push/pull).
  #[must_use]
  #[inline(always)]
  pub const fn with_push_pull_interval(mut self, d: Duration) -> Self {
    self.push_pull_interval = d;
    self
  }

  /// Builder: set broadcast retransmit multiplier.
  #[must_use]
  #[inline(always)]
  pub const fn with_retransmit_mult(mut self, m: u32) -> Self {
    self.retransmit_mult = m;
    self
  }

  /// Builder: set protocol version.
  #[must_use]
  #[inline(always)]
  pub const fn with_protocol_version(mut self, v: ProtocolVersion) -> Self {
    self.protocol_version = v;
    self
  }

  /// Builder: set delegate version.
  #[must_use]
  #[inline(always)]
  pub const fn with_delegate_version(mut self, v: DelegateVersion) -> Self {
    self.delegate_version = v;
    self
  }

  /// Setter: enable the deterministic RNG seed in place.
  #[inline(always)]
  pub const fn set_rng_seed(&mut self, seed: u64) -> &mut Self {
    self.rng_seed = Some(seed);
    self
  }

  /// Builder: set the RNG seed (consuming). Fixes the seed for deterministic
  /// replay. Use [`maybe_rng_seed`](Self::maybe_rng_seed) to assign the raw
  /// `Option<u64>` (including `None` for OS entropy).
  #[must_use]
  #[inline(always)]
  pub const fn with_rng_seed(mut self, seed: u64) -> Self {
    self.rng_seed = Some(seed);
    self
  }

  /// Setter: assign the raw `Option<u64>` RNG seed in place.
  /// `None` reverts to OS entropy; `Some(seed)` fixes the seed.
  #[inline(always)]
  pub const fn update_rng_seed(&mut self, seed: Option<u64>) -> &mut Self {
    self.rng_seed = seed;
    self
  }

  /// Builder: assign the raw `Option<u64>` RNG seed (consuming, alias for
  /// when the raw-wrapper intent is clearest).
  #[must_use]
  #[inline(always)]
  pub const fn maybe_rng_seed(mut self, seed: Option<u64>) -> Self {
    self.rng_seed = seed;
    self
  }

  /// Setter: clear the RNG seed (revert to OS entropy) in place.
  #[inline(always)]
  pub const fn clear_rng_seed(&mut self) -> &mut Self {
    self.rng_seed = None;
    self
  }

  /// The local node's id.
  #[inline(always)]
  pub const fn local_id_ref(&self) -> &I {
    &self.local_id
  }

  /// The local node's advertise address.
  #[inline(always)]
  pub const fn advertise_addr_ref(&self) -> &A {
    &self.advertise_addr
  }

  /// The initial metadata for the local node.
  #[inline(always)]
  pub const fn initial_meta_ref(&self) -> &Meta {
    &self.initial_meta
  }

  /// The initial local-state snapshot (for push/pull) as a byte slice.
  #[inline(always)]
  pub fn initial_local_state(&self) -> &[u8] {
    self.initial_local_state.as_ref()
  }

  /// Return a cheap clone of the initial local-state snapshot buffer.
  #[inline(always)]
  pub fn initial_local_state_bytes(&self) -> Bytes {
    self.initial_local_state.clone()
  }

  /// Suspicion multiplier.
  #[inline(always)]
  pub const fn suspicion_mult(&self) -> u32 {
    self.suspicion_mult
  }

  /// Suspicion max-timeout multiplier.
  #[inline(always)]
  pub const fn suspicion_max_timeout_mult(&self) -> u32 {
    self.suspicion_max_timeout_mult
  }

  /// Probe interval.
  #[inline(always)]
  pub const fn probe_interval(&self) -> Duration {
    self.probe_interval
  }

  /// Gossip-to-the-dead time.
  #[inline(always)]
  pub const fn gossip_to_the_dead_time(&self) -> Duration {
    self.gossip_to_the_dead_time
  }

  /// Dead-node-reclaim time.
  #[inline(always)]
  pub const fn dead_node_reclaim_time(&self) -> Duration {
    self.dead_node_reclaim_time
  }

  /// Awareness max multiplier.
  #[inline(always)]
  pub const fn awareness_max_multiplier(&self) -> u32 {
    self.awareness_max_multiplier
  }

  /// Number of indirect peers used as fallback for failed direct pings.
  #[inline(always)]
  pub const fn indirect_checks(&self) -> u32 {
    self.indirect_checks
  }

  /// Direct-ping timeout before fallback to indirect.
  #[inline(always)]
  pub const fn probe_timeout(&self) -> Duration {
    self.probe_timeout
  }

  /// Returns the per-stream exchange timeout.
  #[inline(always)]
  pub const fn stream_timeout(&self) -> Duration {
    self.stream_timeout
  }

  /// Returns the hard cap on a single inbound reliable-stream frame.
  #[inline(always)]
  pub const fn max_stream_frame_size(&self) -> usize {
    self.max_stream_frame_size
  }

  /// Returns the plaintext-byte ceiling for an outbound gossip datagram.
  /// On-wire datagrams may exceed this by
  /// [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] when encryption is
  /// enabled — see the field doc on `gossip_mtu`.
  #[inline(always)]
  pub const fn gossip_mtu(&self) -> usize {
    self.gossip_mtu
  }

  /// How often gossip rounds fire. `Duration::ZERO` means disabled.
  #[inline(always)]
  pub const fn gossip_interval(&self) -> Duration {
    self.gossip_interval
  }

  /// Number of random peers selected per gossip round.
  #[inline(always)]
  pub const fn gossip_nodes(&self) -> usize {
    self.gossip_nodes
  }

  /// Per-endpoint `Meta` byte-length cap. See
  /// [`Self::with_meta_max_size`].
  #[inline(always)]
  pub const fn meta_max_size(&self) -> usize {
    self.meta_max_size
  }

  /// Server-side accept handshake deadline. See
  /// [`Self::with_accept_handshake_deadline`].
  #[inline(always)]
  pub const fn accept_handshake_deadline(&self) -> Duration {
    self.accept_handshake_deadline
  }

  /// How often push/pull anti-entropy fires. `Duration::ZERO` means disabled.
  #[inline(always)]
  pub const fn push_pull_interval(&self) -> Duration {
    self.push_pull_interval
  }

  /// Broadcast retransmit multiplier.
  #[inline(always)]
  pub const fn retransmit_mult(&self) -> u32 {
    self.retransmit_mult
  }

  /// Protocol version.
  #[inline(always)]
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  /// Delegate version.
  #[inline(always)]
  pub const fn delegate_version(&self) -> DelegateVersion {
    self.delegate_version
  }

  /// Optional deterministic RNG seed.
  #[inline(always)]
  pub const fn rng_seed(&self) -> Option<u64> {
    self.rng_seed
  }
}
