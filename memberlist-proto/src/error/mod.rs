//! Error types surfaced by the [`Endpoint`].

use core::net::SocketAddr;
use std::borrow::Cow;

use thiserror::Error;

/// Errors returned by [`Endpoint`](crate::endpoint::Endpoint) operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
  /// The endpoint has already left the cluster (or never joined).
  #[error("endpoint is not running (already left or shut down)")]
  NotRunning,

  /// An incoming message had a state value the local node doesn't recognise.
  /// The payload is the raw state identifier received from the wire.
  #[error("unknown peer state: {0}")]
  UnknownPeerState(Cow<'static, str>),

  /// A caller-supplied `Meta` exceeded the per-endpoint
  /// [`EndpointOptions::meta_max_size`](crate::config::EndpointOptions::meta_max_size)
  /// cap. Carries the supplied size and the cap (see [`SizeExceeded`]).
  #[error("meta size {} exceeds per-endpoint cap {}", _0.0, _0.1)]
  MetaExceedsCap(SizeExceeded),

  /// A caller-supplied ack payload, once framed, would not fit a single
  /// gossip datagram. Acks are emitted as one UDP datagram on the gossip
  /// socket, so an over-budget ack is deterministically unsendable: every
  /// probe reply would silently fail (`send_to` errors are dropped under
  /// the lossy-gossip policy), peers would receive no ack and falsely
  /// suspect this node. Rejected at the setter so the payload is never
  /// stored.
  #[error("encoded ack ({} bytes) exceeds the gossip packet budget ({} bytes)", _0.0, _0.1)]
  AckPayloadExceedsMtu(SizeExceeded),

  /// A caller-supplied local-state snapshot, once framed into a PushPull,
  /// would not fit the reliable-stream frame cap. The snapshot rides every
  /// push/pull exchange as the PushPull `user_data`, and receivers reject any
  /// stream frame whose declared length exceeds
  /// [`EndpointOptions::max_stream_frame_size`](crate::config::EndpointOptions::max_stream_frame_size)
  /// the moment the length varint is decoded. A snapshot whose minimal framed
  /// PushPull already exceeds that cap (after reserving a framing budget for
  /// the co-resident membership-state list) is deterministically untransmittable:
  /// every push/pull carrying it is rejected and the application state never
  /// reaches any peer. Rejected at the setter so the snapshot is never stored.
  /// The limit is `max_stream_frame_size` minus the reserved membership-state
  /// headroom.
  #[error(
    "framed local-state snapshot ({} bytes) exceeds the reliable-stream frame budget ({} bytes)",
    _0.0,
    _0.1
  )]
  LocalStateExceedsFrame(SizeExceeded),

  /// A caller-supplied user-broadcast payload exceeds the gossip **compound-part
  /// selection budget** (`gossip_mtu` minus the compound header and a conservative
  /// per-part overhead). The gossip drain selects user payloads through the
  /// compound-part tier walk, so an over-budget payload is never selected and
  /// would sit queued forever; it is rejected at the setter, by raw length, so it
  /// is never stored. (A *selected* payload is emitted as a lone packet when it is
  /// the tick's only message, or as a compound part when several share the
  /// datagram.) This is a *selection* budget, not a physical wire limit — the
  /// conservative overhead can reject a payload that would still fit a lone
  /// `gossip_mtu` datagram.
  #[error("user broadcast payload ({} bytes) exceeds the gossip compound-part budget ({} bytes)", _0.0, _0.1)]
  UserBroadcastExceedsMtu(SizeExceeded),

  /// A caller-supplied directed user packet (or multi-packet compound),
  /// once framed including compound framing overhead, would not fit a single
  /// gossip datagram. Directed user packets are emitted as one UDP datagram
  /// and a compound whose assembled framed size exceeds the gossip MTU is
  /// deterministically unsendable.
  #[error("framed user packet ({} bytes) exceeds the packet MTU ({} bytes)", _0.0, _0.1)]
  UserPacketExceedsMtu(SizeExceeded),

  /// A caller-supplied [`Endpoint::ping`](crate::endpoint::Endpoint::ping)
  /// target, once framed into a `Ping`, would not fit a single gossip datagram.
  /// A direct application ping rides one UDP datagram on the gossip socket, so
  /// an over-budget Ping is deterministically unsendable: the peer receives no
  /// Ping, never acks, and the probe would fail on its deadline. Node ids are
  /// unbounded, so a large target id can push the framed Ping past the budget;
  /// it is rejected before any probe/ack state is registered rather than begun
  /// as a probe doomed to time out. Carries the framed size and the budget
  /// (see [`SizeExceeded`]).
  #[error("framed ping ({} bytes) exceeds the gossip packet budget ({} bytes)", _0.0, _0.1)]
  PingExceedsMtu(SizeExceeded),

  /// A caller-supplied [`Endpoint::ping`](crate::endpoint::Endpoint::ping)
  /// target whose id or advertise address cannot be encoded on the compact wire
  /// layout — e.g. a scoped or flow-labelled IPv6 `SocketAddr`, whose nonzero
  /// `scope_id` / `flowinfo` the compact encoder rejects — so the probe `Ping`
  /// to it could never be framed for the wire. Unlike a wire-decoded address
  /// (whose `scope_id` / `flowinfo` always decode to 0, so it always re-encodes),
  /// a caller-supplied target can carry these fields; it is rejected before any
  /// probe/ack state is registered rather than begun as a probe that can never
  /// be sent. Use a wire-representable target.
  #[error("the ping target's identity is not wire-encodable, so its Ping is unsendable")]
  UnencodablePingTarget,

  /// The per-peer reliable user-message dial backlog to this peer is full: the
  /// QUIC coordinator already holds its configured
  /// `QuicOptions::max_pending_user_dials_per_peer` limit of OUTSTANDING
  /// (still-dialing) reliable user-message intents to the peer, so a further
  /// `QuicEndpoint::start_user_message` is refused at the call site as visible
  /// self-backpressure rather than parking yet another intent toward its dial
  /// deadline. This is admission control on the node's OWN application load —
  /// backpressure, not a delivery failure: retry once the peer establishes or
  /// grants stream credit and the backlog drains. Push/pull and reliable-ping
  /// dials are exempt (protocol-paced and liveness-critical) and never counted
  /// against this bound. Carries the peer and the limit (see
  /// [`UserDialBacklogFull`]).
  #[error(
    "reliable user-message dial backlog to {} is full ({} outstanding intents); backpressure, not failure — retry once the peer drains",
    _0.peer(),
    _0.limit()
  )]
  UserDialBacklogFull(UserDialBacklogFull),
}

/// Payload for [`Error`]'s size-limit variants: a measured size in bytes that
/// exceeded a limit in bytes. The variant names the specific size and limit
/// (a per-endpoint cap, a gossip-packet budget, or a reliable-stream frame
/// budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeExceeded(usize, usize);

impl SizeExceeded {
  /// Build from the measured size and the limit, both in bytes.
  #[inline]
  pub const fn new(size: usize, limit: usize) -> Self {
    Self(size, limit)
  }

  /// The measured size in bytes.
  #[inline(always)]
  pub const fn size(&self) -> usize {
    self.0
  }

  /// The limit — cap, budget, or MTU — in bytes.
  #[inline(always)]
  pub const fn limit(&self) -> usize {
    self.1
  }
}

/// Payload for [`Error::UserDialBacklogFull`]: the peer whose per-peer reliable
/// user-message dial backlog is full and the configured
/// `QuicOptions::max_pending_user_dials_per_peer` limit it reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserDialBacklogFull {
  peer: SocketAddr,
  limit: usize,
}

impl UserDialBacklogFull {
  /// Build from the target peer and the per-peer outstanding-dial limit.
  #[inline]
  pub const fn new(peer: SocketAddr, limit: usize) -> Self {
    Self { peer, limit }
  }

  /// The peer whose reliable user-message dial backlog is full.
  #[inline(always)]
  pub const fn peer(&self) -> SocketAddr {
    self.peer
  }

  /// The configured per-peer outstanding-dial limit that was reached.
  #[inline(always)]
  pub const fn limit(&self) -> usize {
    self.limit
  }
}

/// Error constructing an [`Endpoint`](crate::endpoint::Endpoint) via the
/// fallible [`try_new`](crate::endpoint::Endpoint::try_new) /
/// [`try_new_at`](crate::endpoint::Endpoint::try_new_at) constructors.
#[derive(Debug, Error, derive_more::From)]
#[non_exhaustive]
pub enum EndpointInitError {
  /// `EndpointOptions::initial_meta` is larger than the configured
  /// `meta_max_size`, so the local Alive broadcast would carry a meta that
  /// peers reject. Fix the builder configuration: shrink `initial_meta` or
  /// raise `with_meta_max_size`.
  #[error(
    "initial_meta ({} bytes) exceeds meta_max_size ({} bytes)",
    .0.meta_len(),
    .0.max()
  )]
  MetaTooLarge(MetaTooLarge),
  /// `with_awareness_max_multiplier` was set to 0. The Lifeguard awareness
  /// score is clamped to `[0, max - 1]`, which is empty when `max == 0`, so a
  /// zero multiplier is rejected rather than constructing an unusable tracker.
  #[error("awareness_max_multiplier must be >= 1")]
  AwarenessMultiplierZero,
  /// `EndpointOptions::gossip_mtu` is below
  /// [`GOSSIP_MTU_MIN`](crate::config::GOSSIP_MTU_MIN), so the mandatory
  /// single-datagram control packets (probe Ping / Ack / self-Alive) cannot fit
  /// and normal probes would be deterministically rejected → false suspicion.
  /// Raise `with_gossip_mtu`.
  #[error(
    "gossip_mtu ({} bytes) is below the minimum ({} bytes) for the mandatory control packets",
    .0.configured(),
    .0.bound()
  )]
  #[from(skip)]
  GossipMtuTooSmall(GossipMtuBound),
  /// `EndpointOptions::gossip_mtu` exceeds
  /// [`MAX_GOSSIP_MTU`](crate::config::MAX_GOSSIP_MTU), the maximum UDP datagram
  /// payload, so a gossip packet could never fit one datagram. Lower
  /// `with_gossip_mtu`.
  #[error(
    "gossip_mtu ({} bytes) exceeds the maximum UDP datagram payload ({} bytes)",
    .0.configured(),
    .0.bound()
  )]
  #[from(skip)]
  GossipMtuTooLarge(GossipMtuBound),
  /// `EndpointOptions::max_stream_frame_size` is 0 or above the u32 wire limit.
  /// A zero ceiling rejects every reliable frame (no push/pull, no reliable user
  /// message); reliable frame lengths are u32-encoded, so a ceiling above that
  /// is unreachable as a receive gate and a locally-built frame above it would
  /// fail to encode. Set it within `1..=u32::MAX`.
  #[error("max_stream_frame_size ({0}) must be in 1..=u32::MAX")]
  #[from(skip)]
  InvalidMaxStreamFrameSize(usize),
  /// `EndpointOptions::initial_local_state` is too large to travel in a single
  /// reliable PushPull frame under the configured `max_stream_frame_size` (less
  /// the membership-state reserve), so every join / anti-entropy exchange
  /// carrying it would be rejected by the reliable-frame gate, silently blocking
  /// application-state propagation. Shrink the snapshot or raise
  /// `with_max_stream_frame_size`.
  #[error(
    "initial_local_state's framed PushPull ({} bytes) exceeds the frame budget ({} bytes)",
    .0.size(),
    .0.limit()
  )]
  #[from(skip)]
  LocalStateExceedsFrame(SizeExceeded),
  /// The local node's identity — its id or advertise address — cannot be encoded
  /// on the compact wire layout (e.g. a scoped or flow-labelled IPv6 `SocketAddr`,
  /// whose nonzero `scope_id` / `flowinfo` the compact encoder rejects), so the
  /// node could not encode the mandatory control packets it must broadcast about
  /// itself (self-`Alive`, probe `Ping`). Use a wire-representable advertise
  /// address.
  #[error(
    "the local node's identity is not wire-encodable, so its mandatory control packets (self-Alive / Ping) are unsendable"
  )]
  UnencodableLocalIdentity,
  /// `EndpointOptions::max_stream_frame_size` is too small to carry the local
  /// node's minimal push/pull frame — its own state sized for the worst-case
  /// meta the node could ever broadcast (its `meta_max_size` ceiling) — so every
  /// join / anti-entropy exchange would be rejected by the receiver's
  /// frame-length gate, leaving the node unable to complete membership exchange.
  /// Raise `with_max_stream_frame_size`.
  #[error(
    "the local node's minimal push/pull frame ({} bytes) exceeds max_stream_frame_size ({} bytes)",
    .0.size(),
    .0.limit()
  )]
  #[from(skip)]
  MaxStreamFrameSizeTooSmall(SizeExceeded),
}

/// Payload for [`EndpointInitError::MetaTooLarge`]: the configured
/// `initial_meta` length and the `meta_max_size` it exceeded, both in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaTooLarge {
  /// The configured `initial_meta` length in bytes.
  meta_len: usize,
  /// The configured `meta_max_size` ceiling in bytes.
  max: usize,
}

impl MetaTooLarge {
  /// Build from the `initial_meta` length and the `meta_max_size` ceiling, both
  /// in bytes.
  #[inline(always)]
  pub const fn new(meta_len: usize, max: usize) -> Self {
    Self { meta_len, max }
  }

  /// The configured `initial_meta` length in bytes.
  #[inline(always)]
  pub const fn meta_len(&self) -> usize {
    self.meta_len
  }

  /// The configured `meta_max_size` ceiling in bytes.
  #[inline(always)]
  pub const fn max(&self) -> usize {
    self.max
  }
}

/// Payload for [`EndpointInitError::GossipMtuTooSmall`] /
/// [`EndpointInitError::GossipMtuTooLarge`]: the configured `gossip_mtu` and the
/// bound it violated, both in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GossipMtuBound {
  /// The configured `gossip_mtu` in bytes.
  configured: usize,
  /// The bound it violated — the floor for `TooSmall`, the ceiling for `TooLarge`.
  bound: usize,
}

impl GossipMtuBound {
  /// Build from the configured `gossip_mtu` and the violated bound, both in bytes.
  #[inline(always)]
  pub const fn new(configured: usize, bound: usize) -> Self {
    Self { configured, bound }
  }

  /// The configured `gossip_mtu` in bytes.
  #[inline(always)]
  pub const fn configured(&self) -> usize {
    self.configured
  }

  /// The bound it violated, in bytes.
  #[inline(always)]
  pub const fn bound(&self) -> usize {
    self.bound
  }
}

/// Error from a per-stream reliable-exchange state machine.
///
/// `Clone` so a fatal error can be both stored in the terminal
/// `StreamPhase::Failed` and returned to the driver from the same
/// `handle_data` call. All variants are value types.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
  /// The stream deadline elapsed before the exchange completed.
  #[error("stream timed out")]
  Timeout,
  /// The driver reported that the dial failed.
  /// The payload is the free-form OS/network error description.
  #[error("dial failed: {0}")]
  DialFailed(Cow<'static, str>),
  /// The remote peer sent an unexpected or malformed message.
  /// The payload is a free-form description of what was unexpected.
  #[error("unexpected message from peer: {0}")]
  UnexpectedMessage(Cow<'static, str>),
  /// The inner message frame failed to decode (or encode) — wire structure,
  /// compression, encryption, or checksum. Carries the typed `FrameError`.
  #[error("frame error: {0}")]
  Frame(#[from] crate::framing::FrameError),
  /// The typed-to-buffa message bridge failed. Carries the typed `BridgeError`.
  #[error("bridge error: {0}")]
  Bridge(#[from] crate::BridgeError),
  /// A protocol-level decode condition described free-form (e.g. an inbound
  /// frame exceeding the buffer cap) — distinct from the typed `Frame` /
  /// `Bridge` wire-codec errors above.
  #[error("decode error: {0}")]
  Decode(Cow<'static, str>),
  /// The peer closed the stream before sending a response.
  #[error("peer closed stream unexpectedly")]
  PeerClosed,
}

#[cfg(test)]
mod tests;
