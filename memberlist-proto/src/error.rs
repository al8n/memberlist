//! Error types surfaced by the [`Endpoint`].

use std::borrow::Cow;

use thiserror::Error;

/// Errors returned by [`Endpoint`](crate::endpoint::Endpoint) operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
  /// The endpoint has already left the cluster (or never joined).
  #[error("endpoint is not running (already left or shut down)")]
  NotRunning,

  /// The endpoint received a message of an unexpected type for the current
  /// state (e.g. a `PushPull` arriving on the UDP path).
  #[error("unexpected message type: {0}")]
  UnexpectedMessage(&'static str),

  /// An incoming message had a state value the local node doesn't recognise.
  /// The payload is the raw state identifier received from the wire.
  #[error("unknown peer state: {0}")]
  UnknownPeerState(Cow<'static, str>),

  /// A caller-supplied `Meta` exceeded the per-endpoint
  /// [`EndpointConfig::meta_max_size`](crate::config::EndpointConfig::meta_max_size)
  /// cap. Payload: `(supplied_len, cap)`.
  #[error("meta size {0} exceeds per-endpoint cap {1}")]
  MetaExceedsCap(usize, usize),

  /// A caller-supplied ack payload, once framed, would not fit a single
  /// gossip datagram. Acks are emitted as one UDP datagram on the gossip
  /// socket, so an over-budget ack is deterministically unsendable: every
  /// probe reply would silently fail (`send_to` errors are dropped under
  /// the lossy-gossip policy), peers would receive no ack and falsely
  /// suspect this node. Rejected at the setter so the payload is never
  /// stored. Payload: `(encoded_ack_len, gossip_mtu)`.
  #[error("encoded ack ({0} bytes) exceeds the gossip packet budget ({1} bytes)")]
  AckPayloadExceedsMtu(usize, usize),

  /// A caller-supplied local-state snapshot, once framed into a PushPull,
  /// would not fit the reliable-stream frame cap. The snapshot rides every
  /// push/pull exchange as the PushPull `user_data`, and receivers reject any
  /// stream frame whose declared length exceeds
  /// [`EndpointConfig::max_stream_frame_size`](crate::config::EndpointConfig::max_stream_frame_size)
  /// the moment the length varint is decoded. A snapshot whose minimal framed
  /// PushPull already exceeds that cap (after reserving a framing budget for
  /// the co-resident membership-state list) is deterministically untransmittable:
  /// every push/pull carrying it is rejected and the application state never
  /// reaches any peer. Rejected at the setter so the snapshot is never stored.
  /// Payload: `(minimal_framed_pushpull_len, frame_budget)`, where the budget
  /// is `max_stream_frame_size` minus the reserved membership-state headroom.
  #[error(
    "framed local-state snapshot ({0} bytes) exceeds the reliable-stream frame budget ({1} bytes)"
  )]
  LocalStateExceedsFrame(usize, usize),

  /// A caller-supplied user-broadcast payload, once framed as a lone
  /// `UserData` packet, would not fit a single gossip datagram. User
  /// broadcasts ride outgoing gossip; a lone payload is emitted as one UDP
  /// datagram, so a payload whose minimal lone frame already exceeds the
  /// gossip packet budget is deterministically untransmittable — it can never
  /// be gossiped and would otherwise sit queued until a gossip tick discards
  /// it. Rejected at the setter so the payload is never stored. Payload:
  /// `(encoded_userdata_len, gossip_mtu)`.
  #[error("framed user broadcast ({0} bytes) exceeds the gossip packet budget ({1} bytes)")]
  UserBroadcastExceedsMtu(usize, usize),

  /// A caller-supplied directed user packet (or multi-packet compound),
  /// once framed including compound framing overhead, would not fit a single
  /// gossip datagram. Directed user packets are emitted as one UDP datagram
  /// and a compound whose assembled framed size exceeds the gossip MTU is
  /// deterministically unsendable. Payload: `(framed_len, gossip_mtu)`.
  #[error("framed user packet ({0} bytes) exceeds the packet MTU ({1} bytes)")]
  UserPacketExceedsMtu(usize, usize),
}

/// Error constructing an [`Endpoint`](crate::endpoint::Endpoint) via the
/// fallible [`try_new`](crate::endpoint::Endpoint::try_new) /
/// [`try_new_at`](crate::endpoint::Endpoint::try_new_at) constructors.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EndpointInitError {
  /// The config carried no RNG seed and the platform entropy source failed
  /// while seeding the gossip RNG. Recoverable: the driver can retry, or
  /// supply a seed via
  /// [`EndpointConfig::with_rng_seed`](crate::config::EndpointConfig::with_rng_seed)
  /// (e.g. from a hardware RNG) to avoid platform entropy entirely. On no_std
  /// targets this reflects an integrator-provided getrandom backend that
  /// errored or was not yet ready.
  #[error("entropy source failed while seeding the gossip RNG")]
  Entropy,
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
  /// The peer sent bytes that could not be decoded.
  /// The payload is the free-form wire-decode error reason.
  #[error("decode error: {0}")]
  Decode(Cow<'static, str>),
  /// The peer closed the stream before sending a response.
  #[error("peer closed stream unexpectedly")]
  PeerClosed,
}
