//! Application-facing event and transmit types emitted by [`Endpoint`].

use std::sync::Arc;
#[cfg(not(feature = "std"))]
use std::{string::String, vec::Vec};

use crate::typed::{Message, NodeState, PushNodeState};
use bytes::Bytes;

/// A coordinator-allocated handle for one in-flight reliable exchange,
/// stable across the `Handshaking → Established` promotion (unlike
/// [`StreamId`], which does not exist until the record-layer step
/// settles). Newtype over `u64`.
///
/// Defined here so the [`Event::ExchangeCompleted`] variant is
/// uniformly available across feature configurations; the streams
/// coordinator (`crate::streams`) re-exports this type from its
/// own module path for back-compat.
///
/// Each backend sources its `ExchangeId` from its natural identifier
/// domain. The [`StreamEndpoint`](crate::streams::StreamEndpoint)
/// backend allocates `ExchangeId`s explicitly because the bridge
/// predates the wire stream (`Handshaking → Established` two-phase
/// lifecycle). The [`QuicEndpoint`](crate::quic::QuicEndpoint) backend
/// coerces from the machine-level [`StreamId`] (`From<StreamId> for
/// ExchangeId` below) because the bridge and the stream are born
/// together. Within a single endpoint's lifetime, the token is unique
/// per bridge — both producers preserve that uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeId(u64);

impl ExchangeId {
  /// Construct from a raw u64. Crate-internal: the streams
  /// coordinator's allocator and the [`From<StreamId>`] coercion are
  /// the only legitimate producers. `dead_code` allow guards builds
  /// without the streams module (no `tcp` / `tls` feature) — the type
  /// itself stays compiled so [`ExchangeCompleted`] is feature-uniform,
  /// but the constructor has no caller in those builds.
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

impl From<StreamId> for ExchangeId {
  /// Coerce a machine [`StreamId`] into an [`ExchangeId`].
  ///
  /// The [`QuicEndpoint`](crate::quic::QuicEndpoint) backend uses this
  /// to surface its bridge-reap event through the uniform
  /// [`Event::ExchangeCompleted`] payload. In the QUIC backend the
  /// bridge is keyed by the machine `StreamId` (allocated by the
  /// inner `Endpoint` at `start_push_pull` / `start_reliable_ping` /
  /// `start_user_message` time) and the stream is born with the
  /// bridge — so there is no separate `ExchangeId` allocator and the
  /// `StreamId` itself is the natural correlation token. Both
  /// identifiers are monotonic per-endpoint u64s, so the coercion
  /// preserves uniqueness within a single endpoint's lifetime.
  #[inline(always)]
  fn from(id: StreamId) -> Self {
    Self(id.as_u64())
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
/// coordinator tags each [`ExchangeId`] with this so the bridge-reap
/// path can carry the originating kind on the uniform
/// [`Event::ExchangeCompleted`] terminal event. Consumers filter by
/// [`ExchangeCompleted::kind`] to focus on the bridges they care about
/// (e.g. synchronous join consumes only `ExchangeKind::PushPull`
/// completions; the other kinds remain observable for future
/// monitoring uses).
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

/// Terminal outcome of one reliable exchange, observed by the driver
/// via [`Event::ExchangeCompleted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeOutcome {
  /// The exchange validated end-to-end (frame decoder, record layer,
  /// payload merge for push/pull, ack-decode for reliable ping, etc.)
  /// and any membership/probe side effects were applied. For an
  /// outbound (we-initiated) push/pull this implies the peer's
  /// response was processed; for an inbound (peer-initiated) one it
  /// implies our response was sent and the peer's request was applied.
  Succeeded,
  /// The exchange terminated without success (dial failure,
  /// label / handshake rejection, frame decode error, record-layer
  /// fault, deadline elapsed mid-flight, transport I/O error).
  Failed,
}

impl ExchangeOutcome {
  /// Returns `true` if this is `Succeeded`.
  #[inline(always)]
  pub const fn is_succeeded(self) -> bool {
    matches!(self, ExchangeOutcome::Succeeded)
  }
}

/// Payload for [`Event::ExchangeCompleted`]: the terminal outcome of
/// one reliable exchange, emitted from the coordinator's bridge-reap
/// path for ALL outbound bridge kinds (push/pull, reliable ping,
/// reliable user message). Consumers filter by [`Self::kind`] to focus
/// on the bridges they care about — synchronous `join` consumes only
/// `ExchangeKind::PushPull` completions; the other kinds are
/// observable for future monitoring use.
///
/// Drivers correlate `eid` with their own `ExchangeId`-keyed waiter
/// table to drive synchronous completion semantics directly (e.g. a
/// `join_with` "actually-contacted" count) without having to infer
/// success from membership-state side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeCompleted<A> {
  eid: ExchangeId,
  peer: A,
  outcome: ExchangeOutcome,
  kind: ExchangeKind,
}

impl<A> ExchangeCompleted<A> {
  /// Construct a new payload. Crate-internal: the coordinator's
  /// bridge-reap path is the only legitimate producer.
  #[allow(dead_code)]
  #[inline(always)]
  pub(crate) const fn new(
    eid: ExchangeId,
    peer: A,
    outcome: ExchangeOutcome,
    kind: ExchangeKind,
  ) -> Self {
    Self {
      eid,
      peer,
      outcome,
      kind,
    }
  }

  /// The opaque exchange handle the event refers to.
  #[inline(always)]
  pub const fn eid(&self) -> ExchangeId {
    self.eid
  }

  /// The peer the exchange targeted.
  #[inline(always)]
  pub const fn peer(&self) -> &A {
    &self.peer
  }

  /// The terminal outcome.
  #[inline(always)]
  pub const fn outcome(&self) -> ExchangeOutcome {
    self.outcome
  }

  /// The originating exchange kind.
  #[inline(always)]
  pub const fn kind(&self) -> ExchangeKind {
    self.kind
  }
}

/// Correlation token for an application ping (`Endpoint::ping`), echoed on the
/// terminal `Event::PingCompleted` / `Event::PingFailed`. Mirrors how
/// `StreamId` correlates reliable exchanges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PingId(u32);

impl PingId {
  /// Construct from the machine-allocated sequence number.
  #[inline(always)]
  pub(crate) const fn new(seq: u32) -> Self {
    Self(seq)
  }

  /// The underlying sequence number.
  #[inline(always)]
  pub const fn get(&self) -> u32 {
    self.0
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
  pub fn messages_slice(&self) -> &[Message<I, A>] {
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

/// Payload for [`Event::NodeConflict`]: two peers claiming the same id but
/// advertising different addresses.
#[derive(Debug)]
pub struct NodeConflict<I, A> {
  existing: Arc<NodeState<I, A>>,
  other: Arc<NodeState<I, A>>,
}

impl<I, A> NodeConflict<I, A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(existing: Arc<NodeState<I, A>>, other: Arc<NodeState<I, A>>) -> Self {
    Self { existing, other }
  }

  /// The locally-tracked peer.
  #[inline(always)]
  pub fn existing_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.existing
  }

  /// The conflicting peer reported via incoming Alive.
  #[inline(always)]
  pub fn other_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.other
  }
}

/// Payload for [`Event::UserPacket`]: an application-level user-data packet.
#[derive(Debug)]
pub struct UserPacket<A> {
  from: A,
  data: Bytes,
  reliability: Reliability,
}

impl<A> UserPacket<A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(from: A, data: Bytes, reliability: Reliability) -> Self {
    Self {
      from,
      data,
      reliability,
    }
  }

  /// Source address.
  #[inline(always)]
  pub const fn from_ref(&self) -> &A {
    &self.from
  }

  /// Payload bytes.
  #[inline(always)]
  pub const fn data_ref(&self) -> &Bytes {
    &self.data
  }

  /// Transport reliability of the delivery path.
  #[inline(always)]
  pub const fn reliability(&self) -> Reliability {
    self.reliability
  }

  /// Consume the payload into its (from, data, reliability) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Bytes, Reliability) {
    (self.from, self.data, self.reliability)
  }
}

/// Payload for [`Event::RemoteStateReceived`]: a peer's application-level
/// push-pull state snapshot arrived during anti-entropy / join sync.
/// Surfaces the `user_data` the FSM layer does not itself consult, so the
/// driver can hand it to the application (the Sans-I/O analog of Go
/// memberlist's `MergeRemoteState`).
#[derive(Debug)]
pub struct RemoteStateReceived<A> {
  peer: A,
  user_data: Bytes,
  join: bool,
}

impl<A> RemoteStateReceived<A> {
  /// Construct a new payload. Crate-internal: only the `Endpoint` emits this
  /// event; consumers read it through the accessors.
  #[inline(always)]
  pub(crate) const fn new(peer: A, user_data: Bytes, join: bool) -> Self {
    Self {
      peer,
      user_data,
      join,
    }
  }

  /// The peer that supplied the state.
  #[inline(always)]
  pub const fn peer_ref(&self) -> &A {
    &self.peer
  }

  /// The application state bytes the peer sent.
  #[inline(always)]
  pub const fn user_data_ref(&self) -> &Bytes {
    &self.user_data
  }

  /// `true` if this push-pull was part of an initial join (vs periodic
  /// anti-entropy).
  #[inline(always)]
  pub const fn join(&self) -> bool {
    self.join
  }

  /// Consume the payload into its (peer, user_data, join) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Bytes, bool) {
    (self.peer, self.user_data, self.join)
  }
}

/// Payload for [`Event::PingCompleted`]: a probe ping completed with RTT.
#[derive(Debug)]
pub struct PingCompleted<I, A> {
  ping_id: PingId,
  node: Arc<NodeState<I, A>>,
  rtt: core::time::Duration,
  payload: Bytes,
}

impl<I, A> PingCompleted<I, A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(
    ping_id: PingId,
    node: Arc<NodeState<I, A>>,
    rtt: core::time::Duration,
    payload: Bytes,
  ) -> Self {
    Self {
      ping_id,
      node,
      rtt,
      payload,
    }
  }

  /// The correlation token from `Endpoint::ping`.
  #[inline(always)]
  pub const fn ping_id(&self) -> PingId {
    self.ping_id
  }

  /// The peer that responded.
  #[inline(always)]
  pub fn node_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.node
  }

  /// Measured round-trip-time.
  #[inline(always)]
  pub const fn rtt(&self) -> core::time::Duration {
    self.rtt
  }

  /// Bytes the peer attached to its Ack via `set_ack_payload`.
  #[inline(always)]
  pub const fn payload_ref(&self) -> &Bytes {
    &self.payload
  }
}

/// Payload for [`Event::PingFailed`]: an application ping timed out (no Ack
/// within `probe_timeout`).
#[derive(Debug)]
pub struct PingFailed<I, A> {
  ping_id: PingId,
  node: Arc<NodeState<I, A>>,
}

impl<I, A> PingFailed<I, A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(ping_id: PingId, node: Arc<NodeState<I, A>>) -> Self {
    Self { ping_id, node }
  }

  /// The correlation token from `Endpoint::ping`.
  #[inline(always)]
  pub const fn ping_id(&self) -> PingId {
    self.ping_id
  }

  /// The peer that did not respond.
  #[inline(always)]
  pub fn node_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.node
  }
}

/// Payload for [`Event::DecodeError`]: a decode error from an upstream component.
#[derive(Debug)]
pub struct DecodeError<A> {
  from: A,
  err: String,
}

impl<A> DecodeError<A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(from: A, err: String) -> Self {
    Self { from, err }
  }

  /// Source address.
  #[inline(always)]
  pub const fn from_ref(&self) -> &A {
    &self.from
  }

  /// Human-readable error message.
  #[inline(always)]
  pub fn err(&self) -> &str {
    &self.err
  }
}

/// Payload for [`Event::DialRequested`]: the Endpoint requests that the driver
/// dial `peer` on a fresh stream. The driver should call
/// `Endpoint::dial_succeeded(id, now)` on success or
/// `Endpoint::dial_failed(id, err, now)` on failure.
#[derive(Debug)]
pub struct DialRequested<A> {
  id: StreamId,
  peer: A,
  deadline: crate::Instant,
}

impl<A> DialRequested<A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(id: StreamId, peer: A, deadline: crate::Instant) -> Self {
    Self { id, peer, deadline }
  }

  /// The stream ID to report back in `dial_succeeded` / `dial_failed`.
  #[inline(always)]
  pub const fn id(&self) -> StreamId {
    self.id
  }

  /// The peer to dial.
  #[inline(always)]
  pub const fn peer_ref(&self) -> &A {
    &self.peer
  }

  /// Deadline by which the dial (and full exchange) must complete.
  #[inline(always)]
  pub const fn deadline(&self) -> crate::Instant {
    self.deadline
  }

  /// Consume the payload into its (id, peer, deadline) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (StreamId, A, crate::Instant) {
    (self.id, self.peer, self.deadline)
  }
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
  NodeConflict(NodeConflict<I, A>),
  /// An application-level user-data packet arrived.
  UserPacket(UserPacket<A>),
  /// A peer's push-pull application state arrived (non-empty). The Sans-I/O
  /// analog of Go memberlist's `Delegate::MergeRemoteState`.
  RemoteStateReceived(RemoteStateReceived<A>),
  /// The local node has finished its own leave broadcast.
  LeftCluster,
  /// A ping (issued by the local probe FSM) completed with a
  /// measured round-trip-time and the peer's optional ack payload.
  /// Replaces legacy `PingDelegate::notify_ping_complete`.
  PingCompleted(PingCompleted<I, A>),
  /// An application ping (issued via `Endpoint::ping`) timed out: no Ack
  /// arrived within `probe_timeout`. Carries the same [`PingId`] token
  /// returned by `Endpoint::ping`.
  PingFailed(PingFailed<I, A>),
  /// A decode error was reported by an upstream component.
  DecodeError(DecodeError<A>),
  /// The Endpoint requests that the driver dial `peer` on a fresh stream.
  /// The driver should call `Endpoint::dial_succeeded(id, now)` on success
  /// or `Endpoint::dial_failed(id, err, now)` on failure.
  DialRequested(DialRequested<A>),
  /// A reliable exchange terminated, with the validated outcome (see
  /// [`ExchangeOutcome`]). Fired from the coordinator's bridge-reap
  /// path for ALL outbound bridge kinds (push/pull, reliable ping,
  /// reliable user message); consumers filter by
  /// [`ExchangeCompleted::kind`] to focus on the bridges they care
  /// about. Drivers correlate `eid` with their own `ExchangeId`-keyed
  /// waiter tables to drive synchronous completion semantics directly
  /// (no need to infer success from `NodeJoined` / `NodeUpdated` /
  /// membership snapshots). Inbound (peer-initiated) bridges do NOT
  /// emit this event — synchronous-join drivers observe only their
  /// own outbound exchange's terminal outcome.
  ExchangeCompleted(ExchangeCompleted<A>),
}

/// Payload for [`EndpointEvent::PushPullRequestReceived`]: an inbound
/// (peer-initiated) push/pull stream decoded the peer's request.
#[derive(Debug)]
pub struct PushPullRequestReceived<I, A> {
  peer: A,
  states: Vec<PushNodeState<I, A>>,
  user_data: Bytes,
  kind: PushPullKind,
}

impl<I, A> PushPullRequestReceived<I, A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(
    peer: A,
    states: Vec<PushNodeState<I, A>>,
    user_data: Bytes,
    kind: PushPullKind,
  ) -> Self {
    Self {
      peer,
      states,
      user_data,
      kind,
    }
  }

  /// Peer that initiated the exchange.
  #[inline(always)]
  pub const fn peer_ref(&self) -> &A {
    &self.peer
  }

  /// Their snapshot of cluster state.
  #[inline(always)]
  pub fn states_slice(&self) -> &[PushNodeState<I, A>] {
    self.states.as_slice()
  }

  /// Application-level payload they attached.
  #[inline(always)]
  pub const fn user_data_ref(&self) -> &Bytes {
    &self.user_data
  }

  /// Whether they consider this a join or periodic anti-entropy.
  #[inline(always)]
  pub const fn kind(&self) -> PushPullKind {
    self.kind
  }

  /// Consume the payload into its (peer, states, user_data, kind) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Vec<PushNodeState<I, A>>, Bytes, PushPullKind) {
    (self.peer, self.states, self.user_data, self.kind)
  }
}

/// Payload for [`EndpointEvent::PushPullReplyReceived`]: an outbound
/// (we-initiated) push/pull stream decoded the peer's reply.
#[derive(Debug)]
pub struct PushPullReplyReceived<I, A> {
  peer: A,
  states: Vec<PushNodeState<I, A>>,
  user_data: Bytes,
  kind: PushPullKind,
}

impl<I, A> PushPullReplyReceived<I, A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(
    peer: A,
    states: Vec<PushNodeState<I, A>>,
    user_data: Bytes,
    kind: PushPullKind,
  ) -> Self {
    Self {
      peer,
      states,
      user_data,
      kind,
    }
  }

  /// Peer that replied.
  #[inline(always)]
  pub const fn peer_ref(&self) -> &A {
    &self.peer
  }

  /// Their snapshot of cluster state.
  #[inline(always)]
  pub fn states_slice(&self) -> &[PushNodeState<I, A>] {
    self.states.as_slice()
  }

  /// Application-level payload they attached.
  #[inline(always)]
  pub const fn user_data_ref(&self) -> &Bytes {
    &self.user_data
  }

  /// Whether the original outbound exchange was a join or refresh.
  #[inline(always)]
  pub const fn kind(&self) -> PushPullKind {
    self.kind
  }

  /// Consume the payload into its (peer, states, user_data, kind) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Vec<PushNodeState<I, A>>, Bytes, PushPullKind) {
    (self.peer, self.states, self.user_data, self.kind)
  }
}

/// Payload for [`EndpointEvent::ReliablePingAcked`]: a reliable-fallback ping
/// was acknowledged.
#[derive(Debug)]
pub struct ReliablePingAcked {
  seq: u32,
  at: crate::Instant,
}

impl ReliablePingAcked {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(seq: u32, at: crate::Instant) -> Self {
    Self { seq, at }
  }

  /// The sequence number of the original ping.
  #[inline(always)]
  pub const fn seq(&self) -> u32 {
    self.seq
  }

  /// When the ack was observed.
  #[inline(always)]
  pub const fn at(&self) -> crate::Instant {
    self.at
  }
}

/// Payload for [`EndpointEvent::ReliablePingFailed`]: a reliable-fallback ping
/// failed (timeout, dial error, peer reset, …).
#[derive(Debug)]
pub struct ReliablePingFailed {
  seq: u32,
}

impl ReliablePingFailed {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(seq: u32) -> Self {
    Self { seq }
  }

  /// The sequence number of the original ping.
  #[inline(always)]
  pub const fn seq(&self) -> u32 {
    self.seq
  }
}

/// Payload for [`EndpointEvent::StreamClosed`]: a stream completed cleanly.
#[derive(Debug)]
pub struct StreamClosed {
  id: StreamId,
}

impl StreamClosed {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(id: StreamId) -> Self {
    Self { id }
  }

  /// The stream that closed.
  #[inline(always)]
  pub const fn id(&self) -> StreamId {
    self.id
  }
}

/// Payload for [`EndpointEvent::StreamErrored`]: a stream errored.
#[derive(Debug)]
pub struct StreamErrored {
  id: StreamId,
  err: String,
}

impl StreamErrored {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(id: StreamId, err: String) -> Self {
    Self { id, err }
  }

  /// The stream that errored.
  #[inline(always)]
  pub const fn id(&self) -> StreamId {
    self.id
  }

  /// A human-readable error description.
  #[inline(always)]
  pub fn err(&self) -> &str {
    &self.err
  }
}

/// Payload for [`EndpointEvent::UserDataReceived`]: a reliable stream carried
/// a user-data payload addressed to the application.
#[derive(Debug)]
pub struct UserDataReceived<A> {
  peer: A,
  data: Bytes,
}

impl<A> UserDataReceived<A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(peer: A, data: Bytes) -> Self {
    Self { peer, data }
  }

  /// Peer that sent the data.
  #[inline(always)]
  pub const fn peer_ref(&self) -> &A {
    &self.peer
  }

  /// The payload bytes.
  #[inline(always)]
  pub const fn data_ref(&self) -> &Bytes {
    &self.data
  }

  /// Consume the payload into its (peer, data) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (A, Bytes) {
    (self.peer, self.data)
  }
}

/// Event from a [`Stream`](super::endpoint) back to the [`Endpoint`].
#[derive(Debug)]
pub enum EndpointEvent<I, A> {
  /// An inbound (peer-initiated) push/pull stream decoded the peer's request.
  /// The Endpoint should buffer a merge AND respond with our own state via
  /// `StreamCommand::SendPushPullResponse`.
  PushPullRequestReceived(PushPullRequestReceived<I, A>),
  /// An outbound (we-initiated) push/pull stream decoded the peer's reply.
  /// The Endpoint should buffer a merge. No response is needed — we wrote ours
  /// before they replied.
  PushPullReplyReceived(PushPullReplyReceived<I, A>),
  /// A reliable-fallback ping was acknowledged.
  ReliablePingAcked(ReliablePingAcked),
  /// A reliable-fallback ping failed (timeout, dial error, peer reset, …).
  ReliablePingFailed(ReliablePingFailed),
  /// The driver is informing the Endpoint that a stream completed cleanly.
  StreamClosed(StreamClosed),
  /// The driver is informing the Endpoint that a stream errored.
  StreamErrored(StreamErrored),
  /// A reliable-stream carried a user-data payload addressed to the application.
  UserDataReceived(UserDataReceived<A>),
}

/// Payload for [`StreamCommand::SendPushPullResponse`]: the local node's state
/// snapshot to ship back as the response to an inbound push/pull.
#[derive(Debug)]
pub struct SendPushPullResponse<I, A> {
  local_states: Vec<PushNodeState<I, A>>,
  user_data: Bytes,
}

impl<I, A> SendPushPullResponse<I, A> {
  /// Construct a new payload.
  #[inline(always)]
  pub const fn new(local_states: Vec<PushNodeState<I, A>>, user_data: Bytes) -> Self {
    Self {
      local_states,
      user_data,
    }
  }

  /// The local node's view of cluster state to ship back, in wire-format
  /// `PushNodeState` (carries incarnation, state, protocol/delegate
  /// versions). The Endpoint builds this from each `LocalNodeState`
  /// before handing it off; the driver passes it directly to
  /// `Endpoint::encode_push_pull_response`.
  #[inline(always)]
  pub fn local_states_slice(&self) -> &[PushNodeState<I, A>] {
    self.local_states.as_slice()
  }

  /// Application-level payload to ship alongside.
  #[inline(always)]
  pub const fn user_data_ref(&self) -> &Bytes {
    &self.user_data
  }

  /// Consume the payload into its (local_states, user_data) parts.
  #[inline(always)]
  pub fn into_parts(self) -> (Vec<PushNodeState<I, A>>, Bytes) {
    (self.local_states, self.user_data)
  }
}

/// Command [`Endpoint::handle_stream_event`] hands back to the driver to route
/// to the originating stream.
#[derive(Debug)]
pub enum StreamCommand<I, A> {
  /// Send this snapshot back to the peer (response to inbound push/pull).
  SendPushPullResponse(SendPushPullResponse<I, A>),
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
