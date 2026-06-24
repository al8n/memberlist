//! `EndpointOptions` — construction-time settings for the Endpoint state machine.
//!
//! The design uses event-driven decisions (no callbacks): the application
//! drains [`Event::PendingAlive`](crate::event::Event::PendingAlive) /
//! [`Event::PendingMerge`](crate::event::Event::PendingMerge) events from
//! `Endpoint::poll_event` and responds via `decide_alive` / `decide_merge`.
//! State the application wants to push to the Endpoint (e.g. ack payloads,
//! per-target reliable-ping opt-out) flows through dedicated setters.

use core::{num::NonZeroU8, time::Duration};

use crate::typed::{DelegateVersion, Meta, ProtocolVersion};
use bytes::Bytes;

/// Default value for [`EndpointOptions::gossip_mtu`]: 1400 bytes — just under
/// a typical 1500-byte Ethernet MTU, matching the legacy memberlist default
/// and keeping UDP gossip un-fragmented on IPv4/IPv6 + UDP-header headroom.
/// The compound-frame budget reserves its own framing from this; a lone
/// `Packet` is bounded directly by it.
pub const DEFAULT_GOSSIP_MTU: usize = 1400;

/// The hard upper bound on [`EndpointOptions::gossip_mtu`]: 65507 bytes, the
/// maximum IPv4 UDP datagram payload (65535 − 20 B IP − 8 B UDP headers). A
/// gossip packet is one UDP datagram, so a plaintext `gossip_mtu` above this
/// could never fit the wire. This is the transport-agnostic raw ceiling the
/// machine enforces at construction; a driver that wraps the datagram (checksum
/// / encryption) enforces its own tighter ceiling that also subtracts that
/// overhead.
pub const MAX_GOSSIP_MTU: usize = 65507;

/// The lower bound on [`EndpointOptions::gossip_mtu`]: 512 bytes. A gossip
/// packet carries SWIM's mandatory single-datagram control messages — the probe
/// `Ping`, its `Ack`, and a minimal self-`Alive` — each emitted as one UDP
/// datagram with no split point, so a `gossip_mtu` below the largest of them
/// makes normal probes deterministically rejected → false suspicion. 512 covers
/// the framed mandatory packets (~8 B Ack, ~28 B Alive, ~70 B Ping over IPv6)
/// with generous headroom and sits far below the 1400-byte default. Rejected,
/// not clamped, so the operator learns and fixes the misconfiguration.
pub const GOSSIP_MTU_MIN: usize = 512;

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

/// Default value for [`EndpointOptions::max_indirect_forwards`]: 256. A node
/// relays at most this many concurrent indirect pings on others' behalf; a
/// healthy cluster never approaches it (a probe fans out to `indirect_checks`
/// peers for one `probe_timeout`), so it is purely a flood backstop bounding the
/// relay state a peer can induce.
pub const DEFAULT_MAX_INDIRECT_FORWARDS: usize = 256;

// Per-field default values. Each is the single source of truth for one
// field's default: `new()` builds the struct from these, the serde
// `default = "…"` attributes fill missing config-file fields with them, and the
// clap mirror reuses the scalar/bool/binary ones. The `Duration` and version
// defaults additionally appear as `default_value` strings on the clap mirror —
// kept in sync with these by the round-trip tests.

#[inline]
fn default_initial_meta() -> Meta {
  Meta::empty()
}

#[inline]
fn default_initial_local_state() -> Bytes {
  Bytes::new()
}

#[inline]
const fn default_suspicion_mult() -> u32 {
  4
}

#[inline]
const fn default_suspicion_max_timeout_mult() -> u32 {
  6
}

#[inline]
const fn default_probe_interval() -> Duration {
  Duration::from_secs(1)
}

#[inline]
const fn default_gossip_to_the_dead_time() -> Duration {
  Duration::from_secs(30)
}

#[inline]
const fn default_dead_node_reclaim_time() -> Duration {
  Duration::ZERO
}

#[inline]
const fn default_awareness_max_multiplier() -> u32 {
  8
}

#[inline]
const fn default_indirect_checks() -> u32 {
  3
}

#[inline]
const fn default_max_indirect_forwards() -> usize {
  DEFAULT_MAX_INDIRECT_FORWARDS
}

#[inline]
const fn default_max_members() -> Option<usize> {
  None
}

#[inline]
const fn default_ack_payload_to_members_only() -> bool {
  false
}

#[inline]
const fn default_max_inbound_streams() -> Option<usize> {
  None
}

#[inline]
const fn default_probe_timeout() -> Duration {
  Duration::from_millis(500)
}

#[inline]
const fn default_stream_timeout() -> Duration {
  Duration::from_secs(10)
}

#[inline]
const fn default_max_stream_frame_size() -> usize {
  DEFAULT_MAX_STREAM_FRAME_SIZE
}

#[inline]
const fn default_gossip_mtu() -> usize {
  DEFAULT_GOSSIP_MTU
}

#[inline]
const fn default_gossip_interval() -> Duration {
  Duration::from_millis(200)
}

#[inline]
const fn default_gossip_nodes() -> usize {
  3
}

#[inline]
const fn default_meta_max_size() -> usize {
  DEFAULT_META_MAX_SIZE
}

#[inline]
const fn default_accept_handshake_deadline() -> Duration {
  DEFAULT_ACCEPT_HANDSHAKE_DEADLINE
}

#[inline]
const fn default_push_pull_interval() -> Duration {
  Duration::from_secs(30)
}

#[inline]
const fn default_retransmit_mult() -> u32 {
  4
}

#[inline]
const fn default_user_broadcast_tiers() -> NonZeroU8 {
  NonZeroU8::MIN
}

#[inline]
const fn default_protocol_version() -> ProtocolVersion {
  ProtocolVersion::V1
}

#[inline]
const fn default_delegate_version() -> DelegateVersion {
  DelegateVersion::V1
}

#[inline]
const fn default_initial_incarnation() -> u32 {
  1
}

/// Deserialize and validate `initial_incarnation` against the same `u32::MAX / 2`
/// bound `with_initial_incarnation` asserts, so a config value cannot seed an
/// out-of-range incarnation that later wraps on a refute / update bump.
#[cfg(feature = "serde")]
fn deserialize_initial_incarnation<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
  D: serde::Deserializer<'de>,
{
  let value = <u32 as serde::Deserialize>::deserialize(deserializer)?;
  if value > u32::MAX / 2 {
    return Err(serde::de::Error::custom(
      "initial_incarnation must be at most u32::MAX / 2",
    ));
  }
  Ok(value)
}

/// Construction-time settings for [`Endpoint`](crate::endpoint::Endpoint).
///
/// `serde` serializes every value knob (the `Duration` fields render as a
/// humantime string via `humantime-serde`); `local_id` / `advertise_addr` are
/// required (no default), and the two runtime binary fields (`initial_meta`,
/// `initial_local_state`) are skipped — they default to empty and are populated
/// at runtime, not from a config file.
///
/// `clap` exposes every value knob as a CLI flag + `MEMBERLIST_*` env var via a
/// private mirror that carries the `FromStr` bounds `local_id` / `advertise_addr`
/// need; the two binary fields are not CLI-settable.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct EndpointOptions<I, A> {
  local_id: I,
  advertise_addr: A,
  #[cfg_attr(feature = "serde", serde(skip, default = "default_initial_meta"))]
  initial_meta: Meta,
  #[cfg_attr(
    feature = "serde",
    serde(skip, default = "default_initial_local_state")
  )]
  initial_local_state: Bytes,
  #[cfg_attr(feature = "serde", serde(default = "default_suspicion_mult"))]
  suspicion_mult: u32,
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_suspicion_max_timeout_mult")
  )]
  suspicion_max_timeout_mult: u32,
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_probe_interval", with = "humantime_serde")
  )]
  probe_interval: Duration,
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_gossip_to_the_dead_time", with = "humantime_serde")
  )]
  gossip_to_the_dead_time: Duration,
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_dead_node_reclaim_time", with = "humantime_serde")
  )]
  dead_node_reclaim_time: Duration,
  #[cfg_attr(feature = "serde", serde(default = "default_awareness_max_multiplier"))]
  awareness_max_multiplier: u32,
  #[cfg_attr(feature = "serde", serde(default = "default_indirect_checks"))]
  indirect_checks: u32,
  /// Hard cap on concurrent indirect-ping relays this node tracks on behalf of
  /// other probers. A fresh IndirectPing is dropped (no forwarded Ping, no
  /// registry/forward state, no Nack-on-expiry) when the cap is reached, so a
  /// peer cannot grow this node's relay state without bound. Default
  /// [`DEFAULT_MAX_INDIRECT_FORWARDS`].
  #[cfg_attr(feature = "serde", serde(default = "default_max_indirect_forwards"))]
  max_indirect_forwards: usize,
  /// Optional admission ceiling on total cluster membership. When `Some(n)`, an
  /// Alive for a NEW id is rejected once the node already tracks `n` members, so
  /// an open (unauthenticated) network cannot grow membership — and every
  /// per-member structure (timers, broadcast queue, O(n) scans) — without bound.
  /// `None` (the default) preserves unlimited open-join. Existing members and
  /// state transitions of known ids are never rejected.
  #[cfg_attr(feature = "serde", serde(default = "default_max_members"))]
  max_members: Option<usize>,
  /// When `true`, the optional ack payload (set via `Endpoint::set_ack_payload`)
  /// is attached to an outgoing Ack ONLY when the Ping's source id is a known
  /// member; an Ack to an unknown (e.g. spoofed-source) Ping carries no payload.
  /// Bounds the byte amplification a spoofed Ping can elicit toward a victim.
  /// Default `false` (the payload is always attached, matching upstream).
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_ack_payload_to_members_only")
  )]
  ack_payload_to_members_only: bool,
  /// Optional ceiling on concurrently accepted INBOUND reliable-stream
  /// exchanges. When `Some(n)`, a fresh inbound connection beyond `n` live
  /// inbound exchanges builds no bridge (the driver closes it), so a peer cannot
  /// grow inbound bridge state — each pins up to ~3x `max_stream_frame_size`
  /// transiently — without bound. `None` (the default) is unlimited. Outbound
  /// (self-initiated) exchanges are never gated by this.
  #[cfg_attr(feature = "serde", serde(default = "default_max_inbound_streams"))]
  max_inbound_streams: Option<usize>,
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_probe_timeout", with = "humantime_serde")
  )]
  probe_timeout: Duration,
  /// Deadline for a complete stream exchange (dial + request + response).
  /// Default: 10 seconds. Used for push/pull, reliable ping, and user messages.
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_stream_timeout", with = "humantime_serde")
  )]
  stream_timeout: Duration,
  /// Hard cap on a single inbound reliable-stream frame
  /// (`[tag][varint len][body]`). A frame whose declared total exceeds
  /// this is rejected the moment the length varint is decoded — BEFORE
  /// the body is buffered — so a peer cannot exhaust memory by declaring a
  /// huge length and dribbling bytes. Default: 64 MiB (generous enough for
  /// a very large push/pull snapshot; tune up for huge clusters).
  #[cfg_attr(feature = "serde", serde(default = "default_max_stream_frame_size"))]
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
  #[cfg_attr(feature = "serde", serde(default = "default_gossip_mtu"))]
  gossip_mtu: usize,
  /// How often to gossip broadcasts to `gossip_nodes` random peers.
  /// `Duration::ZERO` disables gossip. Default: 200 ms (LAN).
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_gossip_interval", with = "humantime_serde")
  )]
  gossip_interval: Duration,
  /// Number of random peers selected per gossip round. Default: 3.
  #[cfg_attr(feature = "serde", serde(default = "default_gossip_nodes"))]
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
  #[cfg_attr(feature = "serde", serde(default = "default_meta_max_size"))]
  meta_max_size: usize,
  /// Deadline for a server-side bridge's `Handshaking` step (label
  /// validation for plain TCP; TLS handshake for TLS records). A
  /// peer that connects but never sends the label / completes the
  /// handshake within this window is reaped as failed. Default
  /// [`DEFAULT_ACCEPT_HANDSHAKE_DEADLINE`] (10 s).
  #[cfg_attr(
    feature = "serde",
    serde(
      default = "default_accept_handshake_deadline",
      with = "humantime_serde"
    )
  )]
  accept_handshake_deadline: Duration,
  /// How often to run a full push/pull anti-entropy exchange with one
  /// random peer. `Duration::ZERO` disables push/pull. Default: 30 s (LAN).
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_push_pull_interval", with = "humantime_serde")
  )]
  push_pull_interval: Duration,
  #[cfg_attr(feature = "serde", serde(default = "default_retransmit_mult"))]
  retransmit_mult: u32,
  /// Number of independent priority tiers for the user-data broadcast queue.
  /// Tier `0` is the highest priority and drains first under the gossip residual
  /// budget; higher tier numbers are lower priority. The default `1` is a single
  /// retransmit-counted `BroadcastQueue` tier (no cross-tier priority). Send order
  /// is NOT FIFO: like Go's `TransmitLimitedQueue` it orders by `(fewest
  /// transmits, larger size, newer id)`, so a newer or larger payload can be
  /// gossiped — and retransmitted — ahead of an older one. An embedder that needs
  /// ranked user gossip (e.g. intent > query > event — see the strict-priority
  /// contract on `queue_user_broadcast_ranked`) raises this and queues via
  /// [`Endpoint::queue_user_broadcast_ranked`](crate::endpoint::Endpoint::queue_user_broadcast_ranked).
  #[cfg_attr(feature = "serde", serde(default = "default_user_broadcast_tiers"))]
  user_broadcast_tiers: NonZeroU8,
  #[cfg_attr(feature = "serde", serde(default = "default_protocol_version"))]
  protocol_version: ProtocolVersion,
  #[cfg_attr(feature = "serde", serde(default = "default_delegate_version"))]
  delegate_version: DelegateVersion,
  #[cfg_attr(
    feature = "serde",
    serde(
      default = "default_initial_incarnation",
      deserialize_with = "deserialize_initial_incarnation"
    )
  )]
  initial_incarnation: u32,
}

impl<I, A> EndpointOptions<I, A> {
  /// Construct a new config with sensible LAN defaults.
  pub fn new(local_id: I, advertise_addr: A) -> Self {
    Self {
      local_id,
      advertise_addr,
      initial_meta: default_initial_meta(),
      initial_local_state: default_initial_local_state(),
      suspicion_mult: default_suspicion_mult(),
      suspicion_max_timeout_mult: default_suspicion_max_timeout_mult(),
      probe_interval: default_probe_interval(),
      gossip_to_the_dead_time: default_gossip_to_the_dead_time(),
      dead_node_reclaim_time: default_dead_node_reclaim_time(),
      awareness_max_multiplier: default_awareness_max_multiplier(),
      indirect_checks: default_indirect_checks(),
      max_indirect_forwards: default_max_indirect_forwards(),
      max_members: default_max_members(),
      ack_payload_to_members_only: default_ack_payload_to_members_only(),
      max_inbound_streams: default_max_inbound_streams(),
      probe_timeout: default_probe_timeout(),
      stream_timeout: default_stream_timeout(),
      max_stream_frame_size: default_max_stream_frame_size(),
      gossip_mtu: default_gossip_mtu(),
      gossip_interval: default_gossip_interval(),
      gossip_nodes: default_gossip_nodes(),
      meta_max_size: default_meta_max_size(),
      accept_handshake_deadline: default_accept_handshake_deadline(),
      push_pull_interval: default_push_pull_interval(),
      retransmit_mult: default_retransmit_mult(),
      user_broadcast_tiers: default_user_broadcast_tiers(),
      protocol_version: default_protocol_version(),
      delegate_version: default_delegate_version(),
      initial_incarnation: default_initial_incarnation(),
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

  /// Builder: set the concurrent indirect-forward relay cap. See
  /// [`Self::max_indirect_forwards`].
  #[must_use]
  #[inline(always)]
  pub const fn with_max_indirect_forwards(mut self, n: usize) -> Self {
    self.max_indirect_forwards = n;
    self
  }

  /// Builder: set the optional membership admission ceiling. `None` (the
  /// default) leaves open-join unbounded. See [`Self::max_members`].
  #[must_use]
  #[inline(always)]
  pub const fn with_max_members(mut self, n: Option<usize>) -> Self {
    self.max_members = n;
    self
  }

  /// Builder: restrict the ack payload to known members. See
  /// [`Self::ack_payload_to_members_only`].
  #[must_use]
  #[inline(always)]
  pub const fn with_ack_payload_to_members_only(mut self, yes: bool) -> Self {
    self.ack_payload_to_members_only = yes;
    self
  }

  /// Builder: set the optional concurrent inbound-stream ceiling. `None` (the
  /// default) is unlimited. See [`Self::max_inbound_streams`].
  #[must_use]
  #[inline(always)]
  pub const fn with_max_inbound_streams(mut self, n: Option<usize>) -> Self {
    self.max_inbound_streams = n;
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

  /// Builder: set the number of user-data broadcast priority tiers (default 1).
  /// Tier `0` is the highest priority. An out-of-range rank passed to
  /// [`Endpoint::queue_user_broadcast_ranked`](crate::endpoint::Endpoint::queue_user_broadcast_ranked)
  /// saturates to the lowest tier.
  #[must_use]
  #[inline(always)]
  pub const fn with_user_broadcast_tiers(mut self, tiers: NonZeroU8) -> Self {
    self.user_broadcast_tiers = tiers;
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

  /// Builder: set the local node's initial incarnation (default 1). A driver
  /// that restarts a crashed node uses this to supersede the incarnation peers
  /// still hold, ensuring the restarted node's Alive wins over any stale Dead
  /// or lower-incarnation Alive already in circulation.
  ///
  /// # Panics
  ///
  /// Panics unless `incarnation <= u32::MAX / 2`. Incarnation bumps (self-refute,
  /// `update_meta`) increment a `u32` that wraps at its boundary; once a node
  /// wraps past a value peers already observed, its lower wrapped incarnation is
  /// rejected as stale and it can no longer supersede its own terminal state. A
  /// starting incarnation is therefore confined to the lower half of the range,
  /// reserving the upper half — 2^31 increments — as overflow headroom, orders
  /// of magnitude beyond the bumps any node performs in its lifetime. A real
  /// cluster seeds small incarnations; the bound only rejects pathological
  /// near-overflow seeds (rejecting `u32::MAX` alone would still bless values a
  /// few bumps short of wrapping).
  #[must_use]
  #[inline(always)]
  pub const fn with_initial_incarnation(mut self, incarnation: u32) -> Self {
    assert!(
      incarnation <= u32::MAX / 2,
      "initial incarnation must lie in the lower half of the u32 range: incarnation \
       bumps wrap, so the upper half is reserved as overflow headroom"
    );
    self.initial_incarnation = incarnation;
    self
  }

  /// The configured initial incarnation.
  #[inline(always)]
  pub const fn initial_incarnation(&self) -> u32 {
    self.initial_incarnation
  }

  /// Maps the advertise-address type while keeping every other field. The driver
  /// layer uses this to turn an unresolved advertise address into the resolved
  /// `SocketAddr` the engine consumes.
  #[must_use]
  pub fn map_advertise<B>(self, f: impl FnOnce(A) -> B) -> EndpointOptions<I, B> {
    EndpointOptions {
      advertise_addr: f(self.advertise_addr),
      local_id: self.local_id,
      initial_meta: self.initial_meta,
      initial_local_state: self.initial_local_state,
      suspicion_mult: self.suspicion_mult,
      suspicion_max_timeout_mult: self.suspicion_max_timeout_mult,
      probe_interval: self.probe_interval,
      gossip_to_the_dead_time: self.gossip_to_the_dead_time,
      dead_node_reclaim_time: self.dead_node_reclaim_time,
      awareness_max_multiplier: self.awareness_max_multiplier,
      indirect_checks: self.indirect_checks,
      max_indirect_forwards: self.max_indirect_forwards,
      max_members: self.max_members,
      ack_payload_to_members_only: self.ack_payload_to_members_only,
      max_inbound_streams: self.max_inbound_streams,
      probe_timeout: self.probe_timeout,
      stream_timeout: self.stream_timeout,
      max_stream_frame_size: self.max_stream_frame_size,
      gossip_mtu: self.gossip_mtu,
      gossip_interval: self.gossip_interval,
      gossip_nodes: self.gossip_nodes,
      meta_max_size: self.meta_max_size,
      accept_handshake_deadline: self.accept_handshake_deadline,
      push_pull_interval: self.push_pull_interval,
      retransmit_mult: self.retransmit_mult,
      user_broadcast_tiers: self.user_broadcast_tiers,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
      initial_incarnation: self.initial_incarnation,
    }
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

  /// The concurrent indirect-forward relay cap. See
  /// [`Self::max_indirect_forwards`].
  #[inline(always)]
  pub const fn max_indirect_forwards(&self) -> usize {
    self.max_indirect_forwards
  }

  /// The optional membership admission ceiling. See [`Self::max_members`].
  #[inline(always)]
  pub const fn max_members(&self) -> Option<usize> {
    self.max_members
  }

  /// Whether the ack payload is restricted to known members. See
  /// [`Self::ack_payload_to_members_only`].
  #[inline(always)]
  pub const fn ack_payload_to_members_only(&self) -> bool {
    self.ack_payload_to_members_only
  }

  /// The optional concurrent inbound-stream ceiling. See
  /// [`Self::max_inbound_streams`].
  #[inline(always)]
  pub const fn max_inbound_streams(&self) -> Option<usize> {
    self.max_inbound_streams
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

  /// Number of user-data broadcast priority tiers (default 1, tier 0 highest).
  #[inline(always)]
  pub const fn user_broadcast_tiers(&self) -> NonZeroU8 {
    self.user_broadcast_tiers
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
}

// `clap::Args` cannot derive directly on `EndpointOptions<I, A>`: `clap`'s
// `value_parser!` cannot resolve a parser for an unbounded generic, and adding
// an `I: FromStr` / `A: FromStr` bound on the struct itself would cascade those
// bounds onto the whole machine. Instead a private mirror carries the parse
// bounds and derives `Args`; `EndpointOptions` delegates its `Args` /
// `FromArgMatches` to the mirror and rebuilds itself from the parsed mirror.
// The two runtime binary fields (`initial_meta`, `initial_local_state`) are not
// CLI-settable and default when rebuilding.
#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};
  use core::str::FromStr;

  fn parse_protocol_version(s: &str) -> Result<ProtocolVersion, core::num::ParseIntError> {
    s.parse::<u8>().map(ProtocolVersion::from)
  }

  fn parse_delegate_version(s: &str) -> Result<DelegateVersion, core::num::ParseIntError> {
    s.parse::<u8>().map(DelegateVersion::from)
  }

  #[derive(Args)]
  struct EndpointOptionsCli<I, A>
  where
    I: FromStr + Clone + Send + Sync + 'static,
    <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    A: FromStr + Clone + Send + Sync + 'static,
    <A as FromStr>::Err: std::error::Error + Send + Sync + 'static,
  {
    #[arg(
      id = "endpoint-local-id",
      long = "local-id",
      env = "MEMBERLIST_LOCAL_ID"
    )]
    local_id: I,
    #[arg(
      id = "endpoint-advertise-addr",
      long = "advertise-addr",
      env = "MEMBERLIST_ADVERTISE_ADDR"
    )]
    advertise_addr: A,
    #[arg(
      id = "endpoint-suspicion-mult",
      long = "suspicion-mult",
      env = "MEMBERLIST_SUSPICION_MULT",
      default_value_t = default_suspicion_mult()
    )]
    suspicion_mult: u32,
    #[arg(
      id = "endpoint-suspicion-max-timeout-mult",
      long = "suspicion-max-timeout-mult",
      env = "MEMBERLIST_SUSPICION_MAX_TIMEOUT_MULT",
      default_value_t = default_suspicion_max_timeout_mult()
    )]
    suspicion_max_timeout_mult: u32,
    #[arg(
      id = "endpoint-probe-interval",
      long = "probe-interval",
      env = "MEMBERLIST_PROBE_INTERVAL",
      value_parser = humantime::parse_duration,
      default_value = "1s"
    )]
    probe_interval: Duration,
    #[arg(
      id = "endpoint-gossip-to-the-dead-time",
      long = "gossip-to-the-dead-time",
      env = "MEMBERLIST_GOSSIP_TO_THE_DEAD_TIME",
      value_parser = humantime::parse_duration,
      default_value = "30s"
    )]
    gossip_to_the_dead_time: Duration,
    #[arg(
      id = "endpoint-dead-node-reclaim-time",
      long = "dead-node-reclaim-time",
      env = "MEMBERLIST_DEAD_NODE_RECLAIM_TIME",
      value_parser = humantime::parse_duration,
      default_value = "0s"
    )]
    dead_node_reclaim_time: Duration,
    #[arg(
      id = "endpoint-awareness-max-multiplier",
      long = "awareness-max-multiplier",
      env = "MEMBERLIST_AWARENESS_MAX_MULTIPLIER",
      default_value_t = default_awareness_max_multiplier()
    )]
    awareness_max_multiplier: u32,
    #[arg(
      id = "endpoint-indirect-checks",
      long = "indirect-checks",
      env = "MEMBERLIST_INDIRECT_CHECKS",
      default_value_t = default_indirect_checks()
    )]
    indirect_checks: u32,
    #[arg(
      id = "endpoint-max-indirect-forwards",
      long = "max-indirect-forwards",
      env = "MEMBERLIST_MAX_INDIRECT_FORWARDS",
      default_value_t = default_max_indirect_forwards()
    )]
    max_indirect_forwards: usize,
    #[arg(
      id = "endpoint-max-members",
      long = "max-members",
      env = "MEMBERLIST_MAX_MEMBERS"
    )]
    max_members: Option<usize>,
    #[arg(
      id = "endpoint-ack-payload-to-members-only",
      long = "ack-payload-to-members-only",
      env = "MEMBERLIST_ACK_PAYLOAD_TO_MEMBERS_ONLY",
      default_value_t = default_ack_payload_to_members_only()
    )]
    ack_payload_to_members_only: bool,
    #[arg(
      id = "endpoint-max-inbound-streams",
      long = "max-inbound-streams",
      env = "MEMBERLIST_MAX_INBOUND_STREAMS"
    )]
    max_inbound_streams: Option<usize>,
    #[arg(
      id = "endpoint-probe-timeout",
      long = "probe-timeout",
      env = "MEMBERLIST_PROBE_TIMEOUT",
      value_parser = humantime::parse_duration,
      default_value = "500ms"
    )]
    probe_timeout: Duration,
    #[arg(
      id = "endpoint-stream-timeout",
      long = "stream-timeout",
      env = "MEMBERLIST_STREAM_TIMEOUT",
      value_parser = humantime::parse_duration,
      default_value = "10s"
    )]
    stream_timeout: Duration,
    #[arg(
      id = "endpoint-max-stream-frame-size",
      long = "max-stream-frame-size",
      env = "MEMBERLIST_MAX_STREAM_FRAME_SIZE",
      default_value_t = default_max_stream_frame_size()
    )]
    max_stream_frame_size: usize,
    #[arg(
      id = "endpoint-gossip-mtu",
      long = "gossip-mtu",
      env = "MEMBERLIST_GOSSIP_MTU",
      default_value_t = default_gossip_mtu()
    )]
    gossip_mtu: usize,
    #[arg(
      id = "endpoint-gossip-interval",
      long = "gossip-interval",
      env = "MEMBERLIST_GOSSIP_INTERVAL",
      value_parser = humantime::parse_duration,
      default_value = "200ms"
    )]
    gossip_interval: Duration,
    #[arg(
      id = "endpoint-gossip-nodes",
      long = "gossip-nodes",
      env = "MEMBERLIST_GOSSIP_NODES",
      default_value_t = default_gossip_nodes()
    )]
    gossip_nodes: usize,
    #[arg(
      id = "endpoint-meta-max-size",
      long = "meta-max-size",
      env = "MEMBERLIST_META_MAX_SIZE",
      default_value_t = default_meta_max_size()
    )]
    meta_max_size: usize,
    #[arg(
      id = "endpoint-accept-handshake-deadline",
      long = "accept-handshake-deadline",
      env = "MEMBERLIST_ACCEPT_HANDSHAKE_DEADLINE",
      value_parser = humantime::parse_duration,
      default_value = "10s"
    )]
    accept_handshake_deadline: Duration,
    #[arg(
      id = "endpoint-push-pull-interval",
      long = "push-pull-interval",
      env = "MEMBERLIST_PUSH_PULL_INTERVAL",
      value_parser = humantime::parse_duration,
      default_value = "30s"
    )]
    push_pull_interval: Duration,
    #[arg(
      id = "endpoint-retransmit-mult",
      long = "retransmit-mult",
      env = "MEMBERLIST_RETRANSMIT_MULT",
      default_value_t = default_retransmit_mult()
    )]
    retransmit_mult: u32,
    #[arg(
      id = "endpoint-user-broadcast-tiers",
      long = "user-broadcast-tiers",
      env = "MEMBERLIST_USER_BROADCAST_TIERS",
      default_value_t = default_user_broadcast_tiers()
    )]
    user_broadcast_tiers: NonZeroU8,
    #[arg(
      id = "endpoint-protocol-version",
      long = "protocol-version",
      env = "MEMBERLIST_PROTOCOL_VERSION",
      value_parser = parse_protocol_version,
      default_value = "1"
    )]
    protocol_version: ProtocolVersion,
    #[arg(
      id = "endpoint-delegate-version",
      long = "delegate-version",
      env = "MEMBERLIST_DELEGATE_VERSION",
      value_parser = parse_delegate_version,
      default_value = "1"
    )]
    delegate_version: DelegateVersion,
    #[arg(
      id = "endpoint-initial-incarnation",
      long = "initial-incarnation",
      env = "MEMBERLIST_INITIAL_INCARNATION",
      default_value_t = default_initial_incarnation(),
      value_parser = clap::value_parser!(u32).range(..=(u32::MAX as i64 / 2))
    )]
    initial_incarnation: u32,
  }

  impl<I, A> From<EndpointOptionsCli<I, A>> for EndpointOptions<I, A>
  where
    I: FromStr + Clone + Send + Sync + 'static,
    <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    A: FromStr + Clone + Send + Sync + 'static,
    <A as FromStr>::Err: std::error::Error + Send + Sync + 'static,
  {
    fn from(c: EndpointOptionsCli<I, A>) -> Self {
      Self {
        local_id: c.local_id,
        advertise_addr: c.advertise_addr,
        initial_meta: default_initial_meta(),
        initial_local_state: default_initial_local_state(),
        suspicion_mult: c.suspicion_mult,
        suspicion_max_timeout_mult: c.suspicion_max_timeout_mult,
        probe_interval: c.probe_interval,
        gossip_to_the_dead_time: c.gossip_to_the_dead_time,
        dead_node_reclaim_time: c.dead_node_reclaim_time,
        awareness_max_multiplier: c.awareness_max_multiplier,
        indirect_checks: c.indirect_checks,
        max_indirect_forwards: c.max_indirect_forwards,
        max_members: c.max_members,
        ack_payload_to_members_only: c.ack_payload_to_members_only,
        max_inbound_streams: c.max_inbound_streams,
        probe_timeout: c.probe_timeout,
        stream_timeout: c.stream_timeout,
        max_stream_frame_size: c.max_stream_frame_size,
        gossip_mtu: c.gossip_mtu,
        gossip_interval: c.gossip_interval,
        gossip_nodes: c.gossip_nodes,
        meta_max_size: c.meta_max_size,
        accept_handshake_deadline: c.accept_handshake_deadline,
        push_pull_interval: c.push_pull_interval,
        retransmit_mult: c.retransmit_mult,
        user_broadcast_tiers: c.user_broadcast_tiers,
        protocol_version: c.protocol_version,
        delegate_version: c.delegate_version,
        initial_incarnation: c.initial_incarnation,
      }
    }
  }

  impl<I, A> Args for EndpointOptions<I, A>
  where
    I: FromStr + Clone + Send + Sync + 'static,
    <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    A: FromStr + Clone + Send + Sync + 'static,
    <A as FromStr>::Err: std::error::Error + Send + Sync + 'static,
  {
    fn augment_args(cmd: Command) -> Command {
      EndpointOptionsCli::<I, A>::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      EndpointOptionsCli::<I, A>::augment_args_for_update(cmd)
    }
  }

  impl<I, A> FromArgMatches for EndpointOptions<I, A>
  where
    I: FromStr + Clone + Send + Sync + 'static,
    <I as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    A: FromStr + Clone + Send + Sync + 'static,
    <A as FromStr>::Err: std::error::Error + Send + Sync + 'static,
  {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      EndpointOptionsCli::<I, A>::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // Apply ONLY operator-supplied overrides — args whose value came from the
      // command line or an env var, not a clap default. A bare derived update
      // treats every `default_value` arg as present and would reset unset fields;
      // the mirror-skipped `initial_meta` / `initial_local_state` are not touched
      // here, so they survive too.
      macro_rules! take {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            if let Some(v) = m.get_one::<$ty>($id) {
              self.$field = v.clone();
            }
          }
        };
      }
      macro_rules! take_opt {
        ($id:literal, $field:ident) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            self.$field = m.get_one::<usize>($id).copied();
          }
        };
      }
      take!("endpoint-local-id", local_id, I);
      take!("endpoint-advertise-addr", advertise_addr, A);
      take!("endpoint-suspicion-mult", suspicion_mult, u32);
      take!(
        "endpoint-suspicion-max-timeout-mult",
        suspicion_max_timeout_mult,
        u32
      );
      take!("endpoint-probe-interval", probe_interval, Duration);
      take!(
        "endpoint-gossip-to-the-dead-time",
        gossip_to_the_dead_time,
        Duration
      );
      take!(
        "endpoint-dead-node-reclaim-time",
        dead_node_reclaim_time,
        Duration
      );
      take!(
        "endpoint-awareness-max-multiplier",
        awareness_max_multiplier,
        u32
      );
      take!("endpoint-indirect-checks", indirect_checks, u32);
      take!(
        "endpoint-max-indirect-forwards",
        max_indirect_forwards,
        usize
      );
      take_opt!("endpoint-max-members", max_members);
      take!(
        "endpoint-ack-payload-to-members-only",
        ack_payload_to_members_only,
        bool
      );
      take_opt!("endpoint-max-inbound-streams", max_inbound_streams);
      take!("endpoint-probe-timeout", probe_timeout, Duration);
      take!("endpoint-stream-timeout", stream_timeout, Duration);
      take!(
        "endpoint-max-stream-frame-size",
        max_stream_frame_size,
        usize
      );
      take!("endpoint-gossip-mtu", gossip_mtu, usize);
      take!("endpoint-gossip-interval", gossip_interval, Duration);
      take!("endpoint-gossip-nodes", gossip_nodes, usize);
      take!("endpoint-meta-max-size", meta_max_size, usize);
      take!(
        "endpoint-accept-handshake-deadline",
        accept_handshake_deadline,
        Duration
      );
      take!("endpoint-push-pull-interval", push_pull_interval, Duration);
      take!("endpoint-retransmit-mult", retransmit_mult, u32);
      take!(
        "endpoint-user-broadcast-tiers",
        user_broadcast_tiers,
        NonZeroU8
      );
      take!(
        "endpoint-protocol-version",
        protocol_version,
        ProtocolVersion
      );
      take!(
        "endpoint-delegate-version",
        delegate_version,
        DelegateVersion
      );
      take!("endpoint-initial-incarnation", initial_incarnation, u32);
      Ok(())
    }
  }
};

#[cfg(test)]
mod tests;
