//! Application-facing event and transmit types emitted by [`Endpoint`].

use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use memberlist_wire::typed::{Message, NodeState, PushNodeState};

/// A coordinator-allocated handle for one in-flight reliable exchange,
/// stable across the `Handshaking → Established` promotion (unlike
/// [`StreamId`], which does not exist until the record-layer step
/// settles). Newtype over `u64`.
///
/// Defined here so the [`Event::PushPullCompleted`] variant is
/// uniformly available across feature configurations; the streams
/// coordinator (`crate::streams`) re-exports this type from its
/// own module path for back-compat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeId(u64);

impl ExchangeId {
  /// Construct from a raw u64. Crate-internal: the streams
  /// coordinator's allocator is the only legitimate producer.
  /// `dead_code` allow guards builds without the streams module
  /// (no `tcp` / `tls` feature) — the type itself stays compiled so
  /// [`PushPullCompleted`] is feature-uniform, but the constructor
  /// has no caller in those builds.
  #[allow(dead_code)]
  #[inline(always)]
  pub(crate) const fn new(raw: u64) -> Self {
    Self(raw)
  }

  /// The raw monotonic handle. The driver keys its per-exchange
  /// stream-transport connection on this value.
  #[inline(always)]
  pub const fn get(self) -> u64 {
    self.0
  }
}

/// Transport reliability hint. Tagged so call sites self-document at the
/// API boundary without bool flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reliability {
  /// In-order, retransmitted delivery (TCP / QUIC stream).
  Reliable,
  /// Best-effort datagram delivery (UDP).
  Unreliable,
}

impl Reliability {
  /// Returns `true` if this is `Reliable`.
  #[inline(always)]
  pub const fn is_reliable(self) -> bool {
    matches!(self, Reliability::Reliable)
  }
}

/// Why a push/pull state exchange is happening.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushPullKind {
  /// Initial state exchange triggered by a node joining the cluster.
  Join,
  /// Periodic state-anti-entropy refresh.
  Refresh,
}

impl PushPullKind {
  /// Returns `true` if this is `Join`.
  #[inline(always)]
  pub const fn is_join(self) -> bool {
    matches!(self, PushPullKind::Join)
  }
}

/// Which reliable-stream initiator produced an outbound exchange. The
/// streams coordinator tags each [`ExchangeId`] with this so the bridge
/// reap path can route a per-kind terminal event (currently only
/// [`Event::PushPullCompleted`] is exposed publicly — the other kinds
/// fire `Endpoint`'s existing kind-specific events).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeKind {
  /// `start_push_pull` — Join or Refresh state exchange.
  PushPull,
  /// `start_reliable_ping` — TCP/TLS-fallback probe ping.
  ReliablePing,
  /// `start_user_message` — one-way reliable user-message delivery.
  UserMessage,
}

impl ExchangeKind {
  /// Returns `true` if this exchange originated from `start_push_pull`.
  #[inline(always)]
  pub const fn is_push_pull(self) -> bool {
    matches!(self, ExchangeKind::PushPull)
  }
}

/// Terminal outcome of one per-exchange reliable push/pull, observed by
/// the driver via [`Event::PushPullCompleted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PushPullOutcome {
  /// The exchange validated end-to-end (frame decoder, record layer,
  /// payload merge) and the peer's state was applied to membership.
  /// For an outbound (we-initiated) exchange this implies the peer's
  /// push/pull response was processed; for an inbound (peer-initiated)
  /// exchange it implies our response was sent and the peer's request
  /// was applied.
  Succeeded,
  /// The exchange terminated without a validated push/pull (dial
  /// failure, label / handshake rejection, frame decode error, record
  /// layer fault, deadline elapsed mid-flight, transport I/O error).
  Failed,
}

impl PushPullOutcome {
  /// Returns `true` if this is `Succeeded`.
  #[inline(always)]
  pub const fn is_succeeded(self) -> bool {
    matches!(self, PushPullOutcome::Succeeded)
  }
}

/// Payload for [`Event::PushPullCompleted`]: the terminal outcome of
/// one per-exchange reliable push/pull, emitted from the coordinator's
/// bridge reap path. Drivers correlate `eid` with their own
/// `ExchangeId`-keyed waiter table to drive synchronous completion
/// semantics (e.g. a `join_with` "actually-contacted" count) without
/// having to infer success from membership-state side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushPullCompleted {
  eid: ExchangeId,
  peer: SocketAddr,
  outcome: PushPullOutcome,
}

impl PushPullCompleted {
  /// Construct a new payload. Crate-internal: the coordinator's bridge
  /// reap path is the only legitimate producer. `dead_code` allow
  /// guards builds without the streams module — the type itself stays
  /// compiled (so the [`Event::PushPullCompleted`] variant is
  /// feature-uniform) but the constructor has no caller.
  #[allow(dead_code)]
  #[inline(always)]
  pub(crate) const fn new(eid: ExchangeId, peer: SocketAddr, outcome: PushPullOutcome) -> Self {
    Self { eid, peer, outcome }
  }

  /// The opaque exchange handle the event refers to.
  #[inline(always)]
  pub const fn eid(&self) -> ExchangeId {
    self.eid
  }

  /// The peer the exchange targeted.
  #[inline(always)]
  pub const fn peer(&self) -> SocketAddr {
    self.peer
  }

  /// The terminal outcome.
  #[inline(always)]
  pub const fn outcome(&self) -> PushPullOutcome {
    self.outcome
  }
}

/// Opaque handle for a stream-oriented exchange (push/pull, reliable ping,
/// reliable user-message).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct StreamId(u64);

impl StreamId {
  /// Construct from a raw u64. Crate-internal: the `Endpoint`'s allocator is
  /// the only legitimate producer of `StreamId`s.
  #[allow(dead_code)]
  pub(crate) const fn from_raw(raw: u64) -> Self {
    Self(raw)
  }

  /// The raw u64 representation. Drivers may use this to map `StreamId`s
  /// to their own connection handles.
  #[inline(always)]
  pub const fn as_u64(self) -> u64 {
    self.0
  }
}

/// A single message destined for one peer (one plain-frame datagram).
#[derive(Debug)]
pub struct PacketTransmit<I, A> {
  to: A,
  message: Message<I, A>,
}

impl<I, A> PacketTransmit<I, A> {
  /// Construct a new packet-transmit directive.
  #[inline(always)]
  pub const fn new(to: A, message: Message<I, A>) -> Self {
    Self { to, message }
  }

  /// Borrow the destination address.
  #[inline(always)]
  pub const fn to_ref(&self) -> &A {
    &self.to
  }

  /// Borrow the message.
  #[inline(always)]
  pub const fn message_ref(&self) -> &Message<I, A> {
    &self.message
  }

  /// Consume and return the (destination, message) pair.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Message<I, A>) {
    (self.to, self.message)
  }
}

/// Two or more messages destined for one peer, sent as ONE datagram
/// (SWIM piggyback / compound frame). Never constructed with fewer than
/// two messages — a single message is a [`PacketTransmit`] so its bytes
/// stay a byte-identical plain frame (matches legacy memberlist
/// `makeCompoundMessage`, which only compounds when parts > 1).
#[derive(Debug)]
pub struct CompoundTransmit<I, A> {
  to: A,
  messages: Vec<Message<I, A>>,
}

impl<I, A> CompoundTransmit<I, A> {
  /// Construct a new compound-transmit directive. Panics if `messages.len() < 2`
  /// (single-message sends use [`PacketTransmit`]).
  #[inline(always)]
  pub fn new(to: A, messages: Vec<Message<I, A>>) -> Self {
    assert!(
      messages.len() >= 2,
      "CompoundTransmit requires >= 2 messages"
    );
    Self { to, messages }
  }

  /// Borrow the destination address.
  #[inline(always)]
  pub const fn to_ref(&self) -> &A {
    &self.to
  }

  /// Borrow the messages as a slice.
  #[inline(always)]
  pub const fn messages_slice(&self) -> &[Message<I, A>] {
    self.messages.as_slice()
  }

  /// Consume and return the (destination, messages) pair.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Vec<Message<I, A>>) {
    (self.to, self.messages)
  }
}

/// Outgoing I/O directive emitted by [`Endpoint::poll_transmit`].
///
/// The driver encodes the payload and sends it on the unreliable (UDP)
/// packet path. A Compound carries a SWIM piggyback set (gossip broadcast
/// batch, or probe + buddy Suspect) in one datagram; a Packet is a single
/// message whose bytes are a plain frame.
#[derive(Debug)]
pub enum Transmit<I, A> {
  /// Send one message via the unreliable (UDP) packet path.
  Packet(PacketTransmit<I, A>),
  /// Send two or more messages to one peer as a single compound datagram.
  Compound(CompoundTransmit<I, A>),
}

/// Application-facing event drained from [`Endpoint::poll_event`].
#[derive(Debug)]
pub enum Event<I, A> {
  /// A peer transitioned `Dead`/`Left` → `Alive`, or appeared for the first time.
  NodeJoined(Arc<NodeState<I, A>>),
  /// A peer transitioned to `Dead` or `Left`.
  NodeLeft(Arc<NodeState<I, A>>),
  /// A peer's metadata changed (id+address unchanged, but `Meta`/version updated).
  NodeUpdated(Arc<NodeState<I, A>>),
  /// Two peers claim the same id but advertise different addresses. The local
  /// node will not adopt the new address (unless the existing state is `Dead`/`Left`
  /// and `dead_node_reclaim_time` has elapsed — that path emits `NodeUpdated`).
  NodeConflict {
    /// The locally-tracked peer.
    existing: Arc<NodeState<I, A>>,
    /// The conflicting peer reported via incoming Alive.
    other: Arc<NodeState<I, A>>,
  },
  /// An application-level user-data packet arrived.
  UserPacket {
    /// Source address.
    from: A,
    /// Payload bytes.
    data: Bytes,
    /// Transport reliability of the delivery path.
    reliability: Reliability,
  },
  /// The local node has finished its own leave broadcast.
  LeftCluster,
  /// A ping (issued by the local probe FSM) completed with a
  /// measured round-trip-time and the peer's optional ack payload.
  /// Replaces legacy `PingDelegate::notify_ping_complete`.
  PingCompleted {
    /// The peer that responded.
    node: Arc<NodeState<I, A>>,
    /// Measured round-trip-time.
    rtt: core::time::Duration,
    /// Bytes the peer attached to its Ack via `set_ack_payload`.
    payload: Bytes,
  },
  /// A decode error was reported by an upstream component.
  DecodeError {
    /// Source address.
    from: A,
    /// Human-readable error message.
    err: String,
  },
  /// The Endpoint requests that the driver dial `peer` on a fresh stream.
  /// The driver should call `Endpoint::dial_succeeded(id, now)` on success
  /// or `Endpoint::dial_failed(id, err, now)` on failure.
  DialRequested {
    /// The stream ID to report back in `dial_succeeded` / `dial_failed`.
    id: StreamId,
    /// The peer to dial.
    peer: A,
    /// Deadline by which the dial (and full exchange) must complete.
    deadline: std::time::Instant,
  },
  /// A per-exchange reliable push/pull terminated, with the validated
  /// outcome (see [`PushPullOutcome`]). Fired from the coordinator's
  /// bridge reap path for BOTH outbound (we-initiated) and inbound
  /// (peer-initiated) exchanges regardless of which direction reached
  /// the terminal phase first. Drivers correlate `eid` with their own
  /// `ExchangeId`-keyed waiter tables to drive synchronous completion
  /// semantics directly (no need to infer success from
  /// `NodeJoined` / `NodeUpdated` / membership snapshots).
  PushPullCompleted(PushPullCompleted),
}

/// Event from a [`Stream`](super::endpoint) back to the [`Endpoint`].
#[derive(Debug)]
pub enum EndpointEvent<I, A> {
  /// An inbound (peer-initiated) push/pull stream decoded the peer's request.
  /// The Endpoint should buffer a merge AND respond with our own state via
  /// `StreamCommand::SendPushPullResponse`.
  PushPullRequestReceived {
    /// Peer that initiated the exchange.
    peer: A,
    /// Their snapshot of cluster state.
    states: Vec<PushNodeState<I, A>>,
    /// Application-level payload they attached.
    user_data: Bytes,
    /// Whether they consider this a join or periodic anti-entropy.
    kind: PushPullKind,
  },
  /// An outbound (we-initiated) push/pull stream decoded the peer's reply.
  /// The Endpoint should buffer a merge. No response is needed — we wrote ours
  /// before they replied.
  PushPullReplyReceived {
    /// Peer that replied.
    peer: A,
    /// Their snapshot of cluster state.
    states: Vec<PushNodeState<I, A>>,
    /// Application-level payload they attached.
    user_data: Bytes,
    /// Whether the original outbound exchange was a join or refresh.
    kind: PushPullKind,
  },
  /// A reliable-fallback ping was acknowledged.
  ReliablePingAcked {
    /// The sequence number of the original ping.
    seq: u32,
    /// When the ack was observed.
    at: std::time::Instant,
  },
  /// A reliable-fallback ping failed (timeout, dial error, peer reset, …).
  ReliablePingFailed {
    /// The sequence number of the original ping.
    seq: u32,
  },
  /// The driver is informing the Endpoint that a stream completed cleanly.
  StreamClosed {
    /// The stream that closed.
    id: StreamId,
  },
  /// The driver is informing the Endpoint that a stream errored.
  StreamErrored {
    /// The stream that errored.
    id: StreamId,
    /// A human-readable error description.
    err: String,
  },
  /// A reliable-stream carried a user-data payload addressed to the application.
  UserDataReceived {
    /// Peer that sent the data.
    peer: A,
    /// The payload bytes.
    data: Bytes,
  },
}

/// Command [`Endpoint::handle_stream_event`] hands back to the driver to route
/// to the originating stream.
#[derive(Debug)]
pub enum StreamCommand<I, A> {
  /// Send this snapshot back to the peer (response to inbound push/pull).
  SendPushPullResponse {
    /// The local node's view of cluster state to ship back, in wire-format
    /// `PushNodeState` (carries incarnation, state, protocol/delegate
    /// versions). The Endpoint builds this from each `LocalNodeState`
    /// before handing it off; the driver passes it directly to
    /// `Endpoint::encode_push_pull_response`.
    local_states: Vec<PushNodeState<I, A>>,
    /// Application-level payload to ship alongside.
    user_data: Bytes,
  },
  /// Force-close the stream (e.g. `allow_merge` returned false).
  Close,
}

/// App-visible stream lifecycle event, drained from `Stream::poll_event`.
#[derive(Debug)]
pub enum StreamEvent {
  /// Peer closed the stream cleanly.
  Closed,
  /// Stream errored; the wrapped string is a driver-reported reason.
  Failed(String),
}
