//! [`Endpoint`] — the Sans-I/O SWIM state machine.

use crate::Instant;
use core::{marker::PhantomData, time::Duration};

use std::{boxed::Box, collections::VecDeque, sync::Arc, vec::Vec};

use crate::{
  CheapClone, Data, FxHashMap, FxHashSet, Id, MaybeOwned, Node,
  typed::{
    Ack, Alive, Dead, IndirectPing, Message, Meta, Nack, NodeState, Ping, PushNodeState, PushPull,
    State, Suspect,
  },
};
use bytes::Bytes;
use derive_more::IsVariant;
use rand::{Rng, RngExt, rngs::SmallRng, seq::IteratorRandom};
use smallvec_wrapper::{SmallVec, TinyVec};

use crate::{
  AckEntry, AckKind, EndpointEvent, ForwardAck, PushPullKind, StreamCommand, StreamId,
  ack::AckRegistry,
  awareness::Awareness,
  broadcast::{BroadcastQueue, BytesBroadcast, MemberlistBroadcast, NoId, UserBroadcasts},
  config::EndpointOptions,
  delegate::{AliveDelegate, MergeDelegate},
  error::{EndpointInitError, Error, GossipMtuBound, MetaTooLarge, SizeExceeded},
  event::{
    CompoundTransmit, DialRequested, Event, NodeConflict, PacketTransmit, PingCompleted,
    PingFailed, PingId, Reliability, RemoteStateReceived, SendPushPullResponse, Transmit,
    UserPacket,
  },
  members::{LocalNodeState, Member, Members},
  metrics::Metrics,
  probe::{AwaitingIndirect, Probe, ProbeKind, ProbePhase},
  stream::{OutboundKind, Stream, StreamPhase},
  wire::{COMPOUND_MAX_COUNT_PREFIX_LEN, COMPOUND_MAX_PART_PREFIX_LEN, COMPOUND_TAG_LEN},
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod swim_parity_tests;

/// Endpoint lifecycle state. Mutually exclusive: an Endpoint is in exactly
/// one of these states. Replaces the legacy twin booleans `leaving` + `left`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IsVariant, derive_more::Display)]
#[display("{}", self.as_str())]
pub enum Lifecycle {
  /// Normal operation. Probes, gossip, push/pull all proceed.
  Running,
  /// The local node has called `leave()`. We've broadcast our Dead message
  /// and are draining outgoing queues, but are still polling.
  Leaving,
  /// The local node has completed leaving. Most operations are no-ops.
  Left,
}

impl Lifecycle {
  /// String representation for diagnostics and debugging.
  #[inline(always)]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Running => "running",
      Self::Leaving => "leaving",
      Self::Left => "left",
    }
  }
}

/// Maximum size of node-meta payload that can be carried in an `Alive` message.
pub const META_MAX_SIZE: usize = 512;

/// Headroom reserved below [`EndpointOptions::max_stream_frame_size`] for the
/// membership-state list that rides the SAME PushPull frame as the local-state
/// snapshot. [`set_local_state_snapshot`](Endpoint::set_local_state_snapshot)
/// (and the construction-time `initial_local_state` check) bound the snapshot's
/// minimal framed PushPull to `max_stream_frame_size - LOCAL_STATE_FRAME_BUDGET`
/// so a snapshot sized right at the cap cannot crowd out the membership states
/// it must travel with. 1 MiB comfortably holds the framing for thousands of
/// `PushNodeState` entries; an operator running BOTH a near-cap snapshot AND a
/// cluster whose membership framing exceeds this reserve should raise
/// `max_stream_frame_size` accordingly.
pub const LOCAL_STATE_FRAME_BUDGET: usize = 1024 * 1024;

/// Validate a candidate local-state snapshot against the reliable-stream frame
/// cap. Returns [`Error::LocalStateExceedsFrame`](crate::error::Error::LocalStateExceedsFrame)
/// when the snapshot's minimal framed PushPull (the snapshot carried as
/// `user_data`, with an empty membership-state list) would exceed
/// `max_stream_frame_size - LOCAL_STATE_FRAME_BUDGET`.
///
/// Shared by the runtime setter
/// ([`Endpoint::set_local_state_snapshot`]) and the construction-time
/// `initial_local_state` check so both reject identically. Charges the EXACT
/// framed size of a PushPull carrying only this snapshot — the same
/// real-`encode_message` idiom [`Endpoint::set_ack_payload`] uses (no estimated
/// upper bound); the `LOCAL_STATE_FRAME_BUDGET` reserve then leaves room for the
/// membership states the real PushPull also carries.
pub fn validate_local_state_snapshot<I, A>(
  snapshot: &Bytes,
  max_stream_frame_size: usize,
) -> Result<(), Error>
where
  I: Data,
  A: Data,
{
  local_state_snapshot_overage::<I, A>(snapshot, max_stream_frame_size)
    .map_err(Error::LocalStateExceedsFrame)
}

/// The shared size check behind [`validate_local_state_snapshot`] and the
/// construction-time `initial_local_state` gate: charge the snapshot's minimal
/// framed PushPull (empty membership states, snapshot as `user_data`; `join`
/// does not change the framed length) and return the [`SizeExceeded`] payload
/// when it overflows the `max_stream_frame_size - LOCAL_STATE_FRAME_BUDGET`
/// budget, so each caller can wrap it in its own error type.
fn local_state_snapshot_overage<I, A>(
  snapshot: &Bytes,
  max_stream_frame_size: usize,
) -> Result<(), SizeExceeded>
where
  I: Data,
  A: Data,
{
  // An empty snapshot carries no application state — there is nothing to size
  // against the budget, so a node with no snapshot (or one clearing it) must
  // succeed under any frame cap, including one below the membership reserve.
  if snapshot.is_empty() {
    return Ok(());
  }
  let candidate: PushPull<I, A> =
    PushPull::new(true, core::iter::empty()).with_user_data(snapshot.cheap_clone());
  let encoded_len = crate::wire::encode_message::<I, A>(&Message::PushPull(candidate))
    .expect("locally-built PushPull always bridges to wire form")
    .len();
  let budget = max_stream_frame_size.saturating_sub(LOCAL_STATE_FRAME_BUDGET);
  if encoded_len > budget {
    return Err(SizeExceeded::new(encoded_len, budget));
  }
  Ok(())
}

/// A pending outbound stream dial that has not yet connected.
#[derive(Debug)]
struct PendingStreamIntent<I, A> {
  peer: A,
  kind: crate::stream::OutboundKind,
  deadline: Instant,
  /// For push/pull: the encoded request bytes are stored here until
  /// dial_succeeded fires and we transfer them into a Stream's output_buf.
  encoded: Vec<u8>,
  _marker: PhantomData<fn() -> I>,
}

/// State for one indirect-ping we're routing on behalf of another node.
/// When the target acks our forwarded Ping, we relay an Ack to the original
/// requester. When the deadline elapses without an ack, we send a Nack instead.
#[derive(Debug)]
struct IndirectForward<A> {
  /// The original requester's address — the VALIDATED transport source
  /// (`handle_indirect_ping` already proved `from == ind.source()
  /// .address()`), i.e. the exact same value the relay-Ack path stores in
  /// `AckKind::Forward(ForwardAck { reply_to })`. The timeout Nack is sent
  /// directly here, NOT via a lossy id→members lookup: under asymmetric or
  /// stale local membership the requester id may be absent/outdated, which
  /// would silently drop the Nack (or misroute it) and corrupt the
  /// requester's `expected_nacks - seen` Lifeguard accounting. memberlist-
  /// core likewise Nacks `ind.source().address()`.
  reply_to_addr: A,
  /// The forwarded target's address. The Ack to our forwarded Ping must
  /// originate from this node; `handle_ack` validates the source against
  /// it before consuming the registry slot so a peer that guesses our
  /// allocated seq cannot relay-forge a success.
  target_addr: A,
  /// The original sequence number from the requester's IndirectPing
  /// (for the Ack/Nack we eventually send back).
  requester_seq: u32,
  /// Deadline after which we send a Nack.
  deadline: Instant,
}

/// Sans-I/O SWIM state machine for one local node: membership
/// (alive/suspect/dead/refute/merge), the probe FSM, Lifeguard awareness,
/// suspicion timers, and the gossip broadcast queue. No I/O, no clock.
///
/// **Driving it** (see the crate-level docs for the full contract):
/// * **Inputs** — feed decoded inbound messages with the observed
///   transport source + `now`: [`handle_packet`](Self::handle_packet)
///   (dispatches to the typed `handle_ping`/`handle_ack`/`handle_nack`/
///   `handle_alive`/`handle_suspect`/`handle_dead`/`handle_indirect_ping`/
///   `handle_user_data`); advance time with
///   [`handle_timeout(now)`](Self::handle_timeout).
/// * **Reliable exchanges** — [`start_push_pull`](Self::start_push_pull) /
///   [`start_reliable_ping`](Self::start_reliable_ping) /
///   [`start_user_message`](Self::start_user_message) emit a
///   `DialRequested`; the driver dials and calls
///   [`dial_succeeded`](Self::dial_succeeded) (→ a [`Stream`]) or
///   [`dial_failed`](Self::dial_failed). Inbound connections become a
///   [`Stream`] via [`accept_stream`](Self::accept_stream). Route every
///   `Stream::poll_endpoint_event` back in through
///   [`handle_stream_event`](Self::handle_stream_event).
/// * **Outputs** — drain [`poll_transmit`](Self::poll_transmit) and
///   [`poll_event`](Self::poll_event) until `None`; schedule the next
///   wake from [`poll_timeout`](Self::poll_timeout).
///
/// Correct under any input ordering (late/out-of-order input self-heals,
/// never corrupts); delivery promptness/ordering is the driver's lever on
/// *quality* (failure-detection latency, transient-suspect window), not a
/// correctness dependency. The machine never compensates for the driver.
pub struct Endpoint<I, A, R = SmallRng> {
  cfg: EndpointOptions<I, A>,
  rng: R,

  // Membership state.
  members: Members<I, A>,
  /// The next membership-instance generation token to hand out (monotonic;
  /// never `0`, which is the "unset" sentinel). Bumped at each site that
  /// creates a NEW membership instance — a brand-new id, or a Left/Dead id
  /// reclaimed / re-admitted — so a fresh instance never inherits a departed
  /// one's token even when it reuses the same `(id, address, incarnation)`. See
  /// [`Member::generation`](crate::members::Member::generation) for the invariant.
  next_member_generation: u64,

  // Lifeguard.
  awareness: Awareness,

  // Gossip queue (uses MemberlistBroadcast as the concrete Broadcast impl).
  broadcast: BroadcastQueue<I, MemberlistBroadcast<I, A>>,

  // Probe FSM.
  ack_registry: AckRegistry<A>,
  probes: FxHashMap<u32, Probe<I, A>>,
  /// Indirect-ping forwarders we're currently routing on behalf of other nodes.
  indirect_forwards: FxHashMap<u32, IndirectForward<A>>,
  /// Monotonically-increasing sequence number for outgoing Pings.
  next_seq: u32,
  /// Bumped whenever a snapshot-relevant fact changes: membership (every
  /// `NodeJoined` / `NodeUpdated` / `NodeLeft`), a Suspect transition, a
  /// `reset_nodes` removal, or a health-score change. A driver caches the last
  /// value it published and rebuilds + republishes its membership snapshot only
  /// when this differs, instead of on every productive poll. Wraps (only
  /// equality matters).
  snapshot_version: u64,
  /// Cumulative operational counters bumped as the machine sheds load at its
  /// bounds. Read by drivers via [`Self::metrics`].
  metrics: Metrics,
  /// Round-robin index into `members` for `start_probe`'s target selection.
  probe_index: usize,
  /// Probe ticks since the last `reset_nodes` sweep. Once it reaches the
  /// member count (one full round-robin pass), `reset_nodes` is invoked to
  /// prune long-dead members. Production drivers only call `handle_timeout`,
  /// so this is the sole GC trigger; without it Dead/Left entries persist
  /// forever.
  probes_since_reset: usize,

  // Bookkeeping.
  incarnation: u32,
  local_state_snapshot: Bytes,
  lifecycle: Lifecycle,

  // Output queues (drained via `poll_*`).
  pending_events: VecDeque<Event<I, A>>,
  pending_transmits: VecDeque<Transmit<I, A>>,

  // Explicit leave-completion boundary. `leave()` sets `Some(n)` = the
  // number of `poll_transmit` pops after which the last dead-self notice
  // it queued has been handed to the I/O layer; `poll_transmit` counts
  // down and emits `Event::LeftCluster` when it reaches zero. `None` once
  // signaled / not leaving. When there are no live peers `leave()` emits
  // `LeftCluster` immediately and never sets this — so a zero-live-peer
  // leave is NOT delayed by unrelated traffic already in
  // `pending_transmits`. The count is the queue length captured right
  // after the dead-self fan-out: FIFO guarantees those packets occupy the
  // tail, so later (post-leave) transmits sit behind the boundary and
  // cannot delay completion.
  leave_flush_remaining: Option<usize>,

  // Synchronous admission delegates. Called INLINE while processing an
  // inbound Alive / join push-pull — no deferral, so there is no
  // decision-boundary gap for ordering/timing races. `None` = accept all.
  alive_delegate: Option<Box<dyn AliveDelegate<I, A>>>,
  merge_delegate: Option<Box<dyn MergeDelegate<I, A>>>,

  /// Monotonically-increasing counter for allocating `StreamId`s.
  next_stream_id: u64,

  /// Pending outbound dial intents, keyed by StreamId. Populated by
  /// `start_push_pull`, `start_reliable_ping`, `start_user_message`.
  pending_stream_intents: FxHashMap<StreamId, PendingStreamIntent<I, A>>,

  // App-pushed state (replaces EndpointHooks::ack_payload + disable_reliable_pings).
  ack_payload: Bytes,
  reliable_pings_disabled: FxHashSet<I>,

  // App-pushed user-data ride-along queue (legacy NodeDelegate::broadcast_messages
  // analog). The gossip scheduler drains it via drain_user_broadcasts. Each
  // payload is retransmit-counted across `retransmit_mult * ceil(log10(N+1))`
  // gossip rounds, not sent once. Default 1 tier; an embedder can configure N
  // priority tiers (tier 0 highest) via `EndpointOptions::with_user_broadcast_tiers`.
  user_broadcasts: UserBroadcasts<NoId, BytesBroadcast>,

  // ── Scheduler deadlines ───────────────────────────────────────────────────
  // Set at `new` time with random stagger. `None` means the scheduler is
  // disabled (zero interval) or the node has left.
  next_probe: Option<Instant>,
  next_gossip: Option<Instant>,
  next_pushpull: Option<Instant>,
}

impl<I, A, R> Endpoint<I, A, R> {
  /// The local node's Lifeguard health score (`0` = fully healthy; higher is
  /// worse). Mirrors `memberlist-core`'s `health_score`.
  #[inline]
  pub const fn health_score(&self) -> usize {
    self.awareness.health_score() as usize
  }

  /// Record a health failure of the given `severity`, raising the health score
  /// toward the worst value. Used for test setup that needs an initial degraded
  /// state without running a full failed probe.
  pub fn degrade_health(&mut self, severity: u32) {
    self.awareness.record_failure(severity);
    self.bump_snapshot_version();
  }

  /// Record one health success, lowering the health score toward fully healthy.
  pub fn improve_health(&mut self) {
    self.awareness.record_success();
    self.bump_snapshot_version();
  }

  /// The current lifecycle state of this endpoint.
  #[inline(always)]
  pub const fn lifecycle(&self) -> Lifecycle {
    self.lifecycle
  }

  /// Returns `true` if this endpoint is in normal operation (not leaving or left).
  #[inline(always)]
  pub const fn is_running(&self) -> bool {
    self.lifecycle.is_running()
  }

  /// The single post-leave guard for the fallible runtime operations: `Ok`
  /// while running, `Err(NotRunning)` once leaving or left. Every config setter,
  /// data send, and reliable/ping initiator that returns a `Result` funnels its
  /// lifecycle check through here so the leave contract is named in one place.
  #[inline(always)]
  const fn ensure_running(&self) -> Result<(), crate::error::Error> {
    if self.is_running() {
      Ok(())
    } else {
      Err(crate::error::Error::NotRunning)
    }
  }

  /// Whether the local node has finished leaving (after `leave()` and the
  /// dead-self broadcast has been emitted).
  #[inline(always)]
  pub const fn is_left(&self) -> bool {
    self.lifecycle.is_left()
  }

  /// Whether the local node is in the process of leaving.
  #[inline(always)]
  pub const fn is_leaving(&self) -> bool {
    self.lifecycle.is_leaving()
  }

  /// The configured maximum reliable stream frame size. The composed
  /// stream-transport coordinators read this as the reliable-unit /
  /// decompressed-payload ceiling so the limit always tracks
  /// `EndpointOptions::max_stream_frame_size` rather than a separate constant.
  #[cfg(any(feature = "tls", feature = "tcp", feature = "quic"))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "tls", feature = "tcp", feature = "quic")))
  )]
  pub(crate) const fn max_stream_frame_size(&self) -> usize {
    self.cfg.max_stream_frame_size()
  }

  /// The configured plaintext-byte ceiling for an outbound gossip datagram.
  /// The composed stream-transport coordinators read this as the
  /// decompressed / decrypted-payload ceiling so the limit always tracks
  /// [`EndpointOptions::gossip_mtu`] rather than a separate constant. The
  /// on-wire datagram may exceed this by
  /// [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] when encryption is
  /// enabled — the ceiling bounds the FSM's plaintext budget, not the
  /// post-encryption wire size.
  #[inline(always)]
  pub const fn gossip_mtu(&self) -> usize {
    self.cfg.gossip_mtu()
  }

  /// The configured server-side accept handshake deadline. Used by the
  /// streams coordinator when bounding a freshly-accepted bridge's
  /// label / TLS-handshake step. See
  /// [`EndpointOptions::with_accept_handshake_deadline`]. Gated on a
  /// feature config that compiles the `streams` module — under
  /// QUIC-only or default builds the accessor has no callers.
  #[inline(always)]
  pub const fn accept_handshake_deadline(&self) -> Duration {
    self.cfg.accept_handshake_deadline()
  }

  pub(crate) fn emit_event(&mut self, ev: Event<I, A>) {
    // Membership-content changes always flow through one of these three events
    // (insert -> Joined, meta/addr/incarnation update -> Updated, dead/left ->
    // Left), so bumping here covers them; the silent snapshot-relevant changes
    // (Suspect transition, reset_nodes removal, health) bump explicitly.
    if matches!(
      ev,
      Event::NodeJoined(_) | Event::NodeUpdated(_) | Event::NodeLeft(_)
    ) {
      self.bump_snapshot_version();
    }
    self.pending_events.push_back(ev);
  }

  /// Mark the membership/health snapshot dirty so a driver republishes it.
  #[inline(always)]
  pub(crate) const fn bump_snapshot_version(&mut self) {
    self.snapshot_version = self.snapshot_version.wrapping_add(1);
  }

  /// A monotonically-changing version of the snapshot-relevant state
  /// (membership + health). A driver caches the last value it published and
  /// rebuilds its snapshot only when this differs. See [`Self::snapshot_version`
  /// field docs](Endpoint).
  #[inline(always)]
  pub const fn snapshot_version(&self) -> u64 {
    self.snapshot_version
  }

  /// A snapshot of the machine's cumulative operational counters (load shed at
  /// the membership / ingress / amplification bounds). See [`crate::metrics`].
  #[inline]
  pub const fn metrics(&self) -> &Metrics {
    &self.metrics
  }

  /// Mutable access for the machine's own bound-shedding sites to bump a counter.
  #[inline(always)]
  pub const fn metrics_mut(&mut self) -> &mut Metrics {
    &mut self.metrics
  }

  /// increment + return the next incarnation.
  ///
  /// Wraps at `u32::MAX`. The u32 incarnation space is the wire protocol;
  /// a `u32::MAX` accusation is unrefutable on wrap (it becomes 0, rejected
  /// by peers as `0 < MAX`). Diverging with a saturating clamp or a widened
  /// type would break wire compatibility.
  pub(crate) fn next_incarnation(&mut self) -> u32 {
    self.incarnation = self.incarnation.wrapping_add(1);
    self.incarnation
  }

  /// advance the incarnation past `accused_inc` and return it. Wraps at
  /// `u32::MAX` (same wire-format reasoning as [`next_incarnation`]).
  pub(crate) fn skip_incarnation_past(&mut self, accused_inc: u32) -> u32 {
    if self.incarnation <= accused_inc {
      self.incarnation = accused_inc.wrapping_add(1);
    }
    self.incarnation
  }

  /// Hand out the next membership-instance generation token and advance the
  /// counter, skipping `0` (the reserved "unset" value). Called at every site
  /// that admits a new instance under an id. The `u64` space is not a wire
  /// field and a token is drawn only on a membership replacement, so a wrap is
  /// astronomically unreachable; the `0`-skip is defensive so the sentinel is
  /// never handed to a tracked instance. See
  /// [`Member::generation`](crate::members::Member::generation).
  pub(crate) const fn next_member_generation(&mut self) -> u64 {
    let g = self.next_member_generation;
    self.next_member_generation = match self.next_member_generation.wrapping_add(1) {
      0 => 1,
      n => n,
    };
    g
  }

  /// Returns the mutable access to the RNG.
  #[inline(always)]
  pub const fn rng_mut(&mut self) -> &mut R {
    &mut self.rng
  }

  /// Allocate a fresh `StreamId` for a new reliable stream exchange.
  pub(crate) fn allocate_stream_id(&mut self) -> StreamId {
    let id = StreamId::from_raw(self.next_stream_id);
    self.next_stream_id += 1;
    id
  }

  /// Install the synchronous [`AliveDelegate`](crate::delegate::AliveDelegate)
  /// admission filter. Called inline for every inbound alive; `None` (the
  /// default) admits all. The delegate must be pure/non-blocking — see the
  /// [`delegate`](crate::delegate) module contract.
  pub fn set_alive_delegate(&mut self, d: impl AliveDelegate<I, A>) {
    self.alive_delegate = Some(Box::new(d));
  }

  /// Install the synchronous [`MergeDelegate`](crate::delegate::MergeDelegate)
  /// filter, consulted for every push/pull (a join and an anti-entropy
  /// refresh alike). `None` (the default) admits all.
  pub fn set_merge_delegate(&mut self, d: impl MergeDelegate<I, A>) {
    self.merge_delegate = Some(Box::new(d));
  }

  /// The local node's id.
  #[inline(always)]
  pub const fn local_id_ref(&self) -> &I {
    self.cfg.local_id_ref()
  }

  /// The local node's advertise address.
  #[inline(always)]
  pub const fn advertise_ref(&self) -> &A {
    self.cfg.advertise_addr_ref()
  }

  /// Driver feeds an incoming application user-data payload.
  ///
  /// For `reliability: Unreliable` (UDP datagram), emits
  /// [`Event::UserPacket`] directly. For `reliability: Reliable` (stream),
  /// the driver should instead use the stream FSM path:
  /// `accept_stream` → feed bytes via `handle_data` →
  /// drain `EndpointEvent::UserDataReceived` → call `handle_stream_event`.
  /// This method is retained for the UDP path only.
  pub(crate) fn handle_user_data(&mut self, from: A, data: Bytes, reliability: Reliability) {
    self.emit_event(Event::UserPacket(UserPacket::new(from, data, reliability)));
  }

  /// Drop pending stream-dial intents whose deadline has elapsed without a
  /// `dial_succeeded` / `dial_failed` callback. A correct driver always reports
  /// a dial outcome, but a lost result would otherwise leak the intent — which
  /// for a push/pull holds a full encoded membership snapshot — forever. This
  /// machine-side backstop mirrors `dial_failed` for each swept intent: a
  /// reliable-ping fallback is retired so a late event cannot match it; a
  /// push/pull or user-message intent is dropped silently.
  fn fire_expired_stream_intents(&mut self, now: Instant) {
    let expired: Vec<StreamId> = self
      .pending_stream_intents
      .iter()
      .filter(|(_, intent)| now >= intent.deadline)
      .map(|(id, _)| *id)
      .collect();
    for id in expired {
      if let Some(intent) = self.pending_stream_intents.remove(&id) {
        if let OutboundKind::ReliablePing(probe_seq) = intent.kind {
          self.retire_reliable_fallback(probe_seq);
        }
      }
    }
  }

  /// Send Nack for any indirect-ping forwards whose deadline has expired.
  fn fire_expired_forwards(&mut self, now: Instant) {
    let expired: Vec<u32> = self
      .indirect_forwards
      .iter()
      .filter(|(_, f)| f.deadline <= now)
      .map(|(s, _)| *s)
      .collect();
    for seq in expired {
      let Some(forward) = self.indirect_forwards.remove(&seq) else {
        continue;
      };
      // Remove THIS forward's AckRegistry entry by seq. `poll_expired`
      // would pop the globally-oldest expired entry instead — and direct
      // probe entries are intentionally left registered past their direct
      // deadline so relayed/indirect Acks can still match. Evicting one of
      // those here would drop a later Ack as untracked and cause a false
      // probe failure / spurious Suspect.
      // Ignoring Err: idempotent removal — the entry may already be gone.
      let _ = self.ack_registry.remove(seq);
      // Nack the original requester at its VALIDATED address — the same
      // value the relay-Ack path uses, NOT an id→members lookup. A lossy
      // lookup would silently drop the Nack when the requester id is
      // absent from local membership (asymmetric membership) or misroute
      // it when stale, corrupting the requester's
      // `expected_nacks - seen` Lifeguard accounting.
      let nack = Nack::new(forward.requester_seq);
      self
        .pending_transmits
        .push_back(Transmit::Packet(PacketTransmit::new(
          forward.reply_to_addr,
          Message::Nack(nack),
        )));
    }
  }

  /// Drain the next application-facing event, or `None` if no event is queued.
  pub fn poll_event(&mut self) -> Option<Event<I, A>> {
    self.pending_events.pop_front()
  }

  /// Re-enqueue an event at the back of the pending-events buffer.
  ///
  /// Used by the simulation harness after handling `PendingAlive` decisions:
  /// any non-`PendingAlive` events that surfaced during the decision loop are
  /// re-enqueued so that callers can observe them via [`poll_event`](Self::poll_event).
  pub fn requeue_event(&mut self, ev: Event<I, A>) {
    // A leaving/left node re-admits no dial: drop a requeued DialRequested so a
    // held event cannot tell a raw driver to dial after leave.
    if !self.is_running() && matches!(ev, Event::DialRequested(_)) {
      return;
    }
    self.pending_events.push_back(ev);
  }

  /// Drain the next outgoing transmit, or `None` if nothing is queued.
  pub fn poll_transmit(&mut self) -> Option<Transmit<I, A>> {
    let tx = self.pending_transmits.pop_front();
    // Leave-completion signal: count down the explicit boundary set by
    // `leave()`. When the last dead-self notice has been returned (handed
    // to the I/O layer), emit `Event::LeftCluster`. Drivers wait for
    // `LeftCluster` (with their own timeout → `LeaveTimeout`) before
    // reporting the leave done / tearing down the socket. Only decrement
    // on an actual pop; the boundary counts the dead-self tail plus any
    // stale prefix, never trailing post-leave traffic, so unrelated
    // packets cannot trigger or delay it.
    if tx.is_some() {
      if let Some(rem) = self.leave_flush_remaining {
        // `checked_sub` so a future invariant break cannot wrap the count to
        // `usize::MAX` and silently suppress `LeftCluster` forever (leaving
        // drivers to hit their leave timeout). A reached-zero count emits.
        match rem.checked_sub(1) {
          Some(0) | None => {
            self.emit_event(Event::LeftCluster);
            self.leave_flush_remaining = None;
          }
          Some(rem) => {
            self.leave_flush_remaining = Some(rem);
          }
        }
      }
    }
    tx
  }

  /// Earliest deadline the driver should call `handle_timeout(now)` for.
  ///
  /// Returns the minimum across:
  /// - Active suspicion timers on Members.
  /// - In-flight probe FSM deadlines (`AwaitingDirectAck` / `AwaitingIndirect`).
  /// - Outstanding indirect-ping forward deadlines.
  ///
  /// Returns `None` if all three sources are empty.
  pub fn poll_timeout(&self) -> Option<Instant> {
    // A Leaving/Left node reports no SWIM deadline: its probes, suspicions, and
    // indirect forwards are inert (handle_timeout fires none of them), so
    // surfacing their now-stale deadlines would spin the driver through
    // immediate, no-progress wakeups during the drain.
    if self.lifecycle != Lifecycle::Running {
      return None;
    }
    let suspicion_deadline = self
      .members
      .iter()
      .filter_map(|m| m.suspicion().map(|s| s.deadline()))
      .min();
    let probe_deadline = self.probes.values().map(|p| p.deadline()).min();
    let forward_deadline = self.indirect_forwards.values().map(|f| f.deadline).min();
    let intent_deadline = self
      .pending_stream_intents
      .values()
      .map(|i| i.deadline)
      .min();
    [
      suspicion_deadline,
      probe_deadline,
      forward_deadline,
      intent_deadline,
      self.next_probe,
      self.next_gossip,
      self.next_pushpull,
    ]
    .into_iter()
    .flatten()
    .min()
  }

  /// Whether the endpoint has an in-progress SWIM operation that will, on its
  /// own deadline, advance protocol state: an active suspicion (→ `Dead`), an
  /// in-flight probe (a direct ping, or one escalating to the reliable-ping
  /// fallback → ack / suspect), an outstanding indirect-probe forward (→ nack),
  /// or a pending stream-dial intent (→ bridge / dial-failure). Returns `false`
  /// for a non-`Running` (leaving / left) endpoint, which runs no operations.
  ///
  /// This is the OPERATIONAL subset of [`poll_timeout`](Self::poll_timeout): it
  /// deliberately EXCLUDES the periodic gossip / probe / push-pull schedule,
  /// which a Running cluster holds forever. A driver that drives a deterministic
  /// simulation to quiescence uses it to distinguish "a SWIM operation is still
  /// settling" (not quiescent) from "only the routine periodic timers remain"
  /// (quiescent) — a distinction `poll_timeout` cannot make (it folds both into
  /// one deadline), and which no other public signal exposes (a direct UDP probe
  /// is not a reliable bridge, so the bridge count alone cannot see it).
  pub fn has_pending_operation(&self) -> bool {
    if self.lifecycle != Lifecycle::Running {
      return false;
    }
    !self.probes.is_empty()
      || !self.indirect_forwards.is_empty()
      || !self.pending_stream_intents.is_empty()
      || self.members.iter().any(|m| m.suspicion().is_some())
  }

  /// Number of broadcasts currently in the gossip queue.
  pub fn broadcast_queue_len(&self) -> usize {
    self.broadcast.num_queued()
  }

  /// Number of transmits queued for `poll_transmit`. A driver can read this to
  /// shed or apply back-pressure when the machine's outbound backlog grows
  /// (it advances one or two entries per inbound packet while the driver lags).
  #[inline(always)]
  pub fn pending_transmits_len(&self) -> usize {
    self.pending_transmits.len()
  }

  /// Number of events queued for `poll_event`. A driver can read this to shed
  /// or apply back-pressure when the machine's event backlog grows.
  #[inline(always)]
  pub fn pending_events_len(&self) -> usize {
    self.pending_events.len()
  }

  /// The configured full-exchange stream timeout. The stream coordinator uses it
  /// to bound an inbound push/pull RESPONSE window, so a large response on a slow
  /// link is not cut by a hardcoded deadline shorter than the request side's.
  #[cfg(any(feature = "tls", feature = "tcp"))]
  #[inline(always)]
  pub(crate) fn stream_timeout(&self) -> Duration {
    self.cfg.stream_timeout()
  }

  /// The optional concurrent inbound-stream ceiling. The stream and QUIC
  /// coordinators use it to admission-gate inbound exchanges.
  #[cfg(any(feature = "tls", feature = "tcp", feature = "quic"))]
  #[inline(always)]
  pub(crate) fn max_inbound_streams(&self) -> Option<usize> {
    self.cfg.max_inbound_streams()
  }

  /// Read the current ack payload as a byte slice.
  #[inline(always)]
  pub fn ack_payload(&self) -> &[u8] {
    self.ack_payload.as_ref()
  }

  /// Return a cheap clone of the current ack payload buffer.
  #[inline(always)]
  pub fn ack_payload_bytes(&self) -> Bytes {
    self.ack_payload.clone()
  }

  /// Read the current local-state snapshot as a byte slice.
  #[inline(always)]
  pub fn local_state_snapshot(&self) -> &[u8] {
    self.local_state_snapshot.as_ref()
  }

  /// Return a cheap clone of the current local-state snapshot buffer.
  #[inline(always)]
  pub fn local_state_snapshot_bytes(&self) -> Bytes {
    self.local_state_snapshot.clone()
  }

  /// Number of user-data payloads currently queued for piggyback gossip,
  /// summed across all priority tiers.
  pub fn user_broadcast_queue_len(&self) -> usize {
    self.user_broadcasts.num_queued()
  }

  /// Conservative upper bound on the bytes a single drained user payload of
  /// length `L` adds to the assembled gossip compound. The true per-part
  /// framing is deeper than this constant: a `Message::UserData` bridges to
  /// `pb::UserData { bytes data = 1 }` (memberlist-wire `bridge.rs`), so the
  /// full worst-case chain is
  ///   compound `inner_len` varint (u32 LEB128 ≤ 5)
  /// + `UserData` plain-frame tag (1)
  /// + plain-frame body-len varint (u32 LEB128 ≤ 5)
  /// + protobuf field-1 tag (1)
  /// + protobuf `data` length varint (u32 LEB128 ≤ 5)
  ///   = 17 in the unbounded worst case.
  ///
  /// 11 is a SOUND conservative charge for every *admissible* payload at any
  /// configured `gossip_mtu`. A payload is only drained when `L +
  /// USER_PART_OVERHEAD <= user_budget <= compound_budget = gossip_mtu -
  /// (COMPOUND_TAG_LEN + COMPOUND_MAX_COUNT_PREFIX_LEN)`. Since `gossip_mtu <=
  /// MAX_GOSSIP_MTU` (65507), an admissible `L` and each of the three length
  /// values framing it stay under `2^21`, so every length varint above is ≤ 3
  /// bytes and the true assembled-part overhead is at most `3 + 1 + 3 + 1 + 3 =
  /// 11`. (At the default 1400 MTU `L <= 1383`, the varints are ≤ 2 bytes and the
  /// true overhead is only 8.) Do NOT "tighten" toward 8 — that bound holds only
  /// at small MTUs; 11 stays correct up to `MAX_GOSSIP_MTU`.
  const USER_PART_OVERHEAD: usize = COMPOUND_MAX_PART_PREFIX_LEN + 1 + 5;

  /// Drain user-data payloads up to `limit` total bytes, in priority-tier then
  /// retransmit order. Used by the gossip scheduler. Returns the drained
  /// payloads and the total bytes the selection charged (each payload's length
  /// plus [`USER_PART_OVERHEAD`](Self::USER_PART_OVERHEAD)). Each selected
  /// payload's retransmit counter is bumped and it is dropped once it reaches
  /// `retransmit_mult * ceil(log10(num_nodes + 1))`; payloads that don't fit
  /// remain in the queue for a later tick. `num_nodes` is the tracked member
  /// count (`members.len()`, including retained Dead/Left until reaped — the
  /// memberlist-core retransmit basis), recomputed per drain.
  pub(crate) fn drain_user_broadcasts(
    &mut self,
    num_nodes: u32,
    limit: usize,
  ) -> (Vec<Bytes>, usize) {
    self
      .user_broadcasts
      .take_broadcasts_measured(num_nodes, Self::USER_PART_OVERHEAD, limit)
  }

  fn allocate_seq(&mut self) -> u32 {
    // Skip 0 (reserved) and any seq still live in a probe / ack-registry /
    // indirect-forward slot, so a `u32` wrap cannot silently replace an
    // in-flight probe's slot and orphan it into a spurious suspect. Bounded:
    // the live maps are tiny relative to the 2^32 space, so this almost never
    // iterates more than once.
    loop {
      self.next_seq = self.next_seq.wrapping_add(1);
      if self.next_seq == 0 {
        continue;
      }
      if !self.probes.contains_key(&self.next_seq)
        && !self.indirect_forwards.contains_key(&self.next_seq)
        && !self.ack_registry.contains(self.next_seq)
      {
        return self.next_seq;
      }
    }
  }

  /// The driver failed to dial the peer for stream `id`. Removes the pending
  /// intent. For `ReliablePing` streams the probe FSM is driven to failure
  /// (the peer is marked Suspect). `PushPull` and `UserMessage` dial failures
  /// are silent at the machine level; the driver surfaces them through its own
  /// channel.
  pub fn dial_failed(&mut self, id: StreamId, _err: crate::error::StreamError, now: Instant) {
    let Some(intent) = self.pending_stream_intents.remove(&id) else {
      return;
    };
    if let OutboundKind::ReliablePing(probe_seq) = intent.kind {
      // Reliable-fallback dial failed. This does NOT fail the probe: the
      // fallback runs concurrently with the indirect pings, and a
      // fallback failure is just a "did not make contact" — the indirect
      // path keeps racing the single cumulative deadline. Just retire the
      // fallback stream so a late event can't match it.
      // `now` is part of the public signature for symmetry with `dial_succeeded`;
      // discard it explicitly to keep the unused-parameter lint quiet.
      let _ = now;
      self.retire_reliable_fallback(probe_seq);
    }
    // PushPull and UserMessage dial failures are silent at the machine level;
    // the application surfaces the error through its own channel.
  }

  /// Clear the concurrent reliable-ping fallback from probe `seq` (if it is
  /// in `AwaitingIndirect` with that fallback armed). The probe is left
  /// running so the indirect path can still succeed or time out — a
  /// fallback failure must never short-circuit the probe to failure.
  fn retire_reliable_fallback(&mut self, seq: u32) {
    if let Some(probe) = self.probes.get_mut(&seq) {
      if let ProbePhase::AwaitingIndirect(AwaitingIndirect {
        reliable_stream_id, ..
      }) = &mut probe.phase
      {
        *reliable_stream_id = None;
      }
    }
  }

  /// Encode a `StreamCommand::SendPushPullResponse` into raw bytes suitable
  /// for loading into a `Stream::output_buf`. The driver calls this after
  /// receiving the command from `handle_stream_event`, then calls
  /// `stream_load_response(stream, bytes, deadline)`.
  ///
  /// Takes pre-built `PushNodeState` entries so incarnation numbers are
  /// available (they live in `LocalNodeState`, not `NodeState`). The driver
  /// may build them from the `StreamCommand::SendPushPullResponse.local_states`
  /// slice using the incarnation values it tracks separately.
  ///
  /// Standalone associated function; does not read or modify `Endpoint` state.
  pub fn encode_push_pull_response(
    local_states: &[PushNodeState<I, A>],
    user_data: bytes::Bytes,
    join: bool,
  ) -> Vec<u8>
  where
    I: Data + CheapClone,
    A: Data + CheapClone,
  {
    let pp = PushPull::new(join, local_states.iter().map(|pns| pns.cheap_clone()))
      .with_user_data(user_data);
    let msg = Message::PushPull(pp);
    crate::wire::encode_message::<I, A>(&msg)
      .expect("PushPull encode cannot fail for well-formed data")
  }

  /// Pre-load a [`Stream`]'s output buffer with a response payload and set a
  /// write deadline. After this call `stream.poll_transmit()` will drain the
  /// encoded bytes. The stream must be in `InboundSendingResponse` phase when
  /// this is called.
  pub fn stream_load_response(stream: &mut Stream<I, A>, encoded: Vec<u8>, deadline: Instant) {
    stream.output_buf.extend(encoded);
    stream.deadline = Some(deadline);
  }
}

impl<I, A, R> Endpoint<I, A, R>
where
  I: Eq + core::hash::Hash,
{
  /// Iterate over all known members' wire-format `NodeState`.
  pub fn members(&self) -> impl Iterator<Item = Arc<NodeState<I, A>>> + '_ {
    self.members.iter().map(|m| m.state_ref().server_arc())
  }

  /// Look up a member by id.
  pub fn member(&self, id: &I) -> Option<Arc<NodeState<I, A>>> {
    self.members.get(id).map(|m| m.state_ref().server_arc())
  }

  /// Return the gossip-tracked liveness state for a member.
  ///
  /// Unlike [`member`](Self::member), which returns the wire-protocol
  /// `NodeState` (whose `state` field is fixed at insertion), this method
  /// returns the value maintained by the gossip state machine and reflects
  /// Suspect / Dead transitions.
  ///
  /// Returns `None` if `id` is not known to this endpoint.
  pub fn member_liveness(&self, id: &I) -> Option<State> {
    self.members.get(id).map(|m| m.state_ref().state())
  }

  /// Number of tracked members.
  pub fn num_members(&self) -> usize {
    self.members.len()
  }

  /// Compute the (min, max) suspicion timeouts for the current cluster size.
  fn suspicion_timeouts(&self) -> (Duration, Duration) {
    let n = self.num_members().max(1) as f64;
    let node_scale = crate::mathf::log10(n).max(1.0);
    let interval = self.cfg.probe_interval();
    // `probe_interval.as_millis()` truncates a sub-millisecond interval to 0,
    // which would collapse `min` (and hence `max`) to zero — `Suspicion::new`
    // then arms a deadline equal to `now`, an immediate false death the instant
    // a node is suspected. Floor a NONZERO interval to a 1ms minimum BEFORE it
    // is scaled by `suspicion_mult` and the node-count scale, so the mult and
    // scale still apply to a small-but-nonzero interval (a 500µs interval with
    // mult 4 and node_scale 1.0 yields 4ms, not 1ms). Flooring the interval
    // rather than the final product keeps two behaviors exact: a >= 1ms interval
    // already has `as_millis() >= 1`, so the floor is a no-op and this path stays
    // byte-identical to the unscaled formula; and a zero `suspicion_mult` (or a
    // zero, probing-disabled interval) still yields a zero timeout, matching Go
    // `suspicionTimeout` (which returns 0 when the mult or interval is 0).
    let interval_ms = if interval.is_zero() {
      0.0
    } else {
      (interval.as_millis() as f64).max(1.0)
    };
    let min_ms = (interval_ms * self.cfg.suspicion_mult() as f64 * node_scale) as u64;
    let min = Duration::from_millis(min_ms);
    // `checked_mul` to degrade a pathologically-large configured product to the
    // unscaled `min` instead of panicking on `Duration * u32` overflow (the same
    // discipline as `Awareness::scale_timeout`); unreachable for any sane config.
    let max = min
      .checked_mul(self.cfg.suspicion_max_timeout_mult())
      .unwrap_or(min);
    (min, max)
  }

  /// Last state-change [`Instant`] for `peer`, or `None` if unknown.
  pub fn node_state_change(&self, peer: &I) -> Option<Instant> {
    self.members.get(peer).map(|m| m.state_ref().state_change())
  }

  /// Current incarnation number for `peer`, or `None` if unknown.
  pub fn node_incarnation(&self, peer: &I) -> Option<u32> {
    self.members.get(peer).map(|m| m.state_ref().incarnation())
  }

  /// Incarnation number for the local node (i.e. `self.local_id_ref()`).
  pub fn local_incarnation(&self) -> u32 {
    self
      .members
      .get(self.cfg.local_id_ref())
      .map(|m| m.state_ref().incarnation())
      .unwrap_or(0)
  }

  /// Roll `peer`'s `state_change` timestamp back by `delta`.
  ///
  /// Used by simulation tests to age a peer's state without sleeping, so
  /// that suspicion-timeout logic fires immediately on the next tick.
  /// No-op if `peer` is not a known member.
  pub fn age_member(&mut self, peer: &I, delta: core::time::Duration) {
    if let Some(m) = self.members.get_mut(peer) {
      let old = m.state_ref().state_change();
      // Saturating subtraction: Instant cannot go below its origin.
      m.state_mut().set_state_change(old - delta);
    }
  }

  /// Disable reliable-stream pings to this target. Future probes will use
  /// best-effort packet probes only. No-op if already disabled.
  pub fn disable_reliable_ping(&mut self, target: I) {
    self.reliable_pings_disabled.insert(target);
  }

  /// Re-enable reliable-stream pings to this target. No-op if not currently
  /// disabled.
  pub fn enable_reliable_ping(&mut self, target: &I) {
    self.reliable_pings_disabled.remove(target);
  }

  /// Returns `true` if reliable-stream pings are enabled for this target.
  /// The default is enabled; this returns `false` only after
  /// `disable_reliable_ping(target)`.
  pub fn is_reliable_ping_enabled(&self, target: &I) -> bool {
    !self.reliable_pings_disabled.contains(target)
  }
}

// Construct/encode wire types, mutate membership, and route broadcasts — the
// identity-bearing methods that never draw randomness. Bounds match what the
// downstream wrapper types (`Members`, `MemberlistBroadcast`, `wire::encode`)
// demand; the gossip RNG is not among them.
impl<I, A, R> Endpoint<I, A, R>
where
  I: Id,
  A: CheapClone + Data + PartialEq + 'static,
{
  /// Fallibly construct a new endpoint at the driver-supplied `now`. Inserts
  /// the local node as Alive at incarnation 1, stamping its initial state at
  /// `now`, and enqueues the initial Alive broadcast. The machine never reads a
  /// clock itself, so a virtual- or embedded-clock driver stays internally
  /// consistent.
  ///
  /// The gossip RNG `rng` (peer selection, timing jitter — not key material) is
  /// injected by the driver, which owns entropy: a std driver seeds it from the
  /// OS, an embedded driver from a hardware RNG or a fixed seed. The machine
  /// never draws platform entropy itself. Fallible only for the operator-time
  /// config guards below (an oversized initial meta or a zero awareness
  /// multiplier); see [`Endpoint::new_at`] for the panicking convenience.
  pub fn try_new_at(
    cfg: EndpointOptions<I, A>,
    now: Instant,
    rng: R,
  ) -> Result<Self, EndpointInitError> {
    // Operator-time misconfiguration guards (release-mode, fallible): the
    // local Alive broadcast would otherwise carry a meta peers reject, or the
    // awareness tracker would be unconstructable. Reject at construction rather
    // than panicking or silently shipping a mismatch.
    if cfg.initial_meta_ref().len() > cfg.meta_max_size() {
      return Err(MetaTooLarge::new(cfg.initial_meta_ref().len(), cfg.meta_max_size()).into());
    }
    if cfg.awareness_max_multiplier() == 0 {
      return Err(EndpointInitError::AwarenessMultiplierZero);
    }
    // The gossip MTU must hold the mandatory single-datagram control packets
    // (floor) and fit one UDP datagram (raw ceiling), and the reliable-frame
    // ceiling must be a nonzero value within the u32 wire limit. A driver layers
    // its own tighter, transform-aware bounds on top; these are the machine's
    // backstop for the direct (driver-less) config path.
    let gossip_mtu = cfg.gossip_mtu();
    if gossip_mtu < crate::config::GOSSIP_MTU_MIN {
      return Err(EndpointInitError::GossipMtuTooSmall(GossipMtuBound::new(
        gossip_mtu,
        crate::config::GOSSIP_MTU_MIN,
      )));
    }
    if gossip_mtu > crate::config::MAX_GOSSIP_MTU {
      return Err(EndpointInitError::GossipMtuTooLarge(GossipMtuBound::new(
        gossip_mtu,
        crate::config::MAX_GOSSIP_MTU,
      )));
    }
    let max_stream_frame_size = cfg.max_stream_frame_size();
    if max_stream_frame_size == 0 || max_stream_frame_size > u32::MAX as usize {
      return Err(EndpointInitError::InvalidMaxStreamFrameSize(
        max_stream_frame_size,
      ));
    }
    let local_node = Node::new(
      cfg.local_id_ref().cheap_clone(),
      cfg.advertise_addr_ref().cheap_clone(),
    );
    // Identity-aware gossip-MTU floor: the gossip MTU must hold the largest of
    // the mandatory single-datagram control packets the node emits about itself
    // — the probe `Ping`, its `Ack`, and a minimal self-`Alive` carrying the
    // configured `initial_meta`. Node ids are unbounded, so a long `local_id` or
    // a large meta can push these past the constant floor while still nominally
    // valid; encoding them against the configured identity catches that. The
    // probe `Ping`'s peer slot reuses the local node — the dominant cost, a long
    // `local_id`, appears in both slots regardless of the peer's concrete address.
    {
      let mandatory: [Message<I, A>; 3] = [
        Message::Ping(Ping::new(
          u32::MAX,
          local_node.cheap_clone(),
          local_node.cheap_clone(),
        )),
        Message::Alive(
          Alive::new(u32::MAX, local_node.cheap_clone())
            .with_meta(cfg.initial_meta_ref().cheap_clone()),
        ),
        Message::Ack(Ack::new(u32::MAX)),
      ];
      let mut required = 0usize;
      for msg in &mandatory {
        // A mandatory local-node packet that cannot be encoded means the node
        // could never broadcast its own membership (e.g. a scoped / flow-labelled
        // IPv6 advertise address the compact encoder rejects) — reject the
        // identity rather than construct a silently-undiscoverable node.
        let bytes = crate::codec::encode_outgoing(msg, &crate::codec::EncodeOptions::default())
          .map_err(|_| EndpointInitError::UnencodableLocalIdentity)?;
        required = required.max(bytes.len());
      }
      if gossip_mtu < required {
        return Err(EndpointInitError::GossipMtuTooSmall(GossipMtuBound::new(
          gossip_mtu, required,
        )));
      }

      // Identity-aware reliable-frame floor: the node's own state is the minimum
      // any join / anti-entropy PushPull carries. Size it for the WORST-CASE meta
      // the node could ever broadcast — its `meta_max_size` ceiling (capped at
      // the wire `Meta::MAX_SIZE`), not just `initial_meta`, since `update_meta`
      // admits any later metadata up to that cap — and a worst-case incarnation.
      // A `max_stream_frame_size` below it has every join / anti-entropy frame
      // rejected by the receiver's frame-length gate.
      let meta_cap = cfg.meta_max_size().min(Meta::MAX_SIZE);
      let worst_case_meta = Meta::try_from(Bytes::from(vec![0u8; meta_cap])).unwrap_or_default();
      let minimal_push_pull: Message<I, A> = Message::PushPull(PushPull::new(
        true,
        core::iter::once(
          PushNodeState::new(
            u32::MAX,
            cfg.local_id_ref().cheap_clone(),
            cfg.advertise_addr_ref().cheap_clone(),
            State::Alive,
          )
          .with_meta(worst_case_meta),
        ),
      ));
      let frame =
        crate::codec::encode_outgoing(&minimal_push_pull, &crate::codec::EncodeOptions::default())
          .map_err(|_| EndpointInitError::UnencodableLocalIdentity)?;
      if frame.len() > max_stream_frame_size {
        return Err(EndpointInitError::MaxStreamFrameSizeTooSmall(
          SizeExceeded::new(frame.len(), max_stream_frame_size),
        ));
      }
    }
    let mut members = Members::new(local_node);
    let awareness = Awareness::new(cfg.awareness_max_multiplier());
    let broadcast = BroadcastQueue::<I, MemberlistBroadcast<I, A>>::new(cfg.retransmit_mult());
    let user_broadcasts =
      UserBroadcasts::new(cfg.user_broadcast_tiers().into(), cfg.retransmit_mult());

    // Insert the local node as Alive.
    let local_node_state = NodeState::new(
      cfg.local_id_ref().cheap_clone(),
      cfg.advertise_addr_ref().cheap_clone(),
      State::Alive,
    )
    .with_meta(cfg.initial_meta_ref().cheap_clone())
    .with_protocol_version(cfg.protocol_version())
    .with_delegate_version(cfg.delegate_version());
    // The local node's initial state is stamped at the driver-supplied `now`.
    let mut local_state = LocalNodeState::new(local_node_state, now);
    local_state.set_incarnation(cfg.initial_incarnation());
    // The local node is a membership instance too, so it draws the first
    // generation token (`1`); the counter advances to the next value handed to
    // the first admitted peer. Keeping the local record generation-stamped keeps
    // any self-probe path consistent with peers.
    let local_generation: u64 = 1;
    let next_member_generation: u64 = local_generation + 1;
    // Ignoring Err: a fresh `Members` is empty, so the insert returns
    // `None`. Asserting it is the caller-owned invariant we just upheld.
    let _ = members.insert(Member::new(local_state.clone()).with_generation(local_generation));

    let local_state_snapshot = cfg.initial_local_state_bytes();
    // The configured initial snapshot must fit a single reliable PushPull frame
    // under `max_stream_frame_size` — the same budget the runtime setter
    // enforces — or every join / anti-entropy exchange carrying it is rejected
    // by the reliable-frame gate, silently blocking application-state propagation.
    local_state_snapshot_overage::<I, A>(&local_state_snapshot, max_stream_frame_size)
      .map_err(EndpointInitError::LocalStateExceedsFrame)?;
    let initial_incarnation = cfg.initial_incarnation();

    let mut endpoint = Self {
      cfg,
      rng,
      members,
      next_member_generation,
      awareness,
      broadcast,
      ack_registry: AckRegistry::new(),
      probes: FxHashMap::default(),
      indirect_forwards: FxHashMap::default(),
      next_seq: 0,
      snapshot_version: 0,
      metrics: Metrics::default(),
      probe_index: 0,
      probes_since_reset: 0,
      incarnation: initial_incarnation,
      local_state_snapshot,
      lifecycle: Lifecycle::Running,
      pending_events: VecDeque::new(),
      pending_transmits: VecDeque::new(),
      leave_flush_remaining: None,
      alive_delegate: None,
      merge_delegate: None,
      next_stream_id: 1,
      pending_stream_intents: FxHashMap::default(),
      ack_payload: Bytes::new(),
      reliable_pings_disabled: FxHashSet::default(),
      user_broadcasts,
      next_probe: None,
      next_gossip: None,
      next_pushpull: None,
    };

    // Enqueue the initial Alive broadcast so peers learn about us.
    endpoint.broadcast_alive(&local_state);
    // Emit the local node's join as the endpoint's first event, mirroring Go
    // memberlist's `NotifyJoin(localNode)` on Create. Go reaches it through
    // `setAlive` -> `aliveNode(bootstrap=true)`, which inserts the local node
    // fresh as `Dead` then transitions it `Alive` (a `Dead`->`Alive` transition
    // fires `NotifyJoin`). The Sans-I/O path inserts the local node `Alive`
    // directly, so emit its join explicitly here with the same `Arc<NodeState>`
    // payload the peer-join path uses.
    endpoint.emit_event(Event::NodeJoined(local_state.server_arc()));
    Ok(endpoint)
  }

  /// Construct a new endpoint at the driver-supplied `now`, panicking if the
  /// configuration is invalid. Convenience over [`Endpoint::try_new_at`] for
  /// drivers whose configuration is a static constant known to be valid.
  ///
  /// # Panics
  /// When the configuration is rejected (e.g. `initial_meta` exceeds
  /// `meta_max_size`). Use [`Endpoint::try_new_at`] to handle that case without
  /// panicking.
  pub fn new_at(cfg: EndpointOptions<I, A>, now: Instant, rng: R) -> Self {
    Self::try_new_at(cfg, now, rng).expect("endpoint construction failed (invalid options)")
  }

  /// Fallibly construct a new endpoint, stamping the local node at the current
  /// std monotonic clock. Std convenience over [`Endpoint::try_new_at`];
  /// virtual- or embedded-clock drivers use `try_new_at` with their own `now`
  /// so the machine never reads a clock the driver does not own.
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  pub fn try_new(cfg: EndpointOptions<I, A>, rng: R) -> Result<Self, EndpointInitError> {
    Self::try_new_at(cfg, Instant::now(), rng)
  }

  /// Construct a new endpoint, stamping the local node at the current std
  /// monotonic clock. Convenience for std drivers on the real clock; virtual-
  /// or embedded-clock drivers use [`Endpoint::new_at`] with their own `now`
  /// so the machine never reads a clock the driver does not own.
  ///
  /// # Panics
  /// Panics on a seedless config when the platform entropy source is
  /// unavailable — see [`Endpoint::try_new`].
  #[cfg(feature = "std")]
  #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
  pub fn new(cfg: EndpointOptions<I, A>, rng: R) -> Self {
    Self::new_at(cfg, Instant::now(), rng)
  }

  pub(crate) fn broadcast_alive(&mut self, state: &LocalNodeState<I, A>) {
    let alive = Alive::new(
      state.incarnation(),
      Node::new(
        state.id_ref().cheap_clone(),
        state.address_ref().cheap_clone(),
      ),
    )
    .with_meta(state.server_ref().meta_ref().cheap_clone())
    .with_protocol_version(state.server_ref().protocol_version())
    .with_delegate_version(state.server_ref().delegate_version());
    self.broadcast.queue_broadcast(MemberlistBroadcast::new(
      state.id_ref().cheap_clone(),
      Message::Alive(alive),
    ));
  }

  pub(crate) fn broadcast_message(&mut self, node: I, msg: Message<I, A>) {
    self
      .broadcast
      .queue_broadcast(MemberlistBroadcast::new(node, msg));
  }

  /// Self-refute path: bump our incarnation past `accused_inc`, decrement
  /// our health score, and broadcast a fresh Alive about ourselves.
  ///
  /// Always bumps the local incarnation: first call `next_incarnation`,
  /// then if `accused_inc >=` the result, additionally skip past it.
  pub(crate) fn refute(&mut self, accused_inc: u32) {
    // No-op once leaving/left: refuting bumps our incarnation and
    // broadcasts a higher-incarnation Alive, which would resurrect a
    // node that has already left. Self-defense only applies while Running.
    if !self.lifecycle.is_running() {
      return;
    }
    let mut new_inc = self.next_incarnation();
    if accused_inc >= new_inc {
      new_inc = self.skip_incarnation_past(accused_inc);
    }
    self.awareness.record_failure(1);
    self.bump_snapshot_version();
    if let Some(local) = self.members.get_mut(self.cfg.local_id_ref()) {
      local.state_mut().set_incarnation(new_inc);

      let local_state = local.state_ref();
      let id = local_state.id_ref().cheap_clone();
      let addr = local_state.address_ref().cheap_clone();

      let local_server = local_state.server_ref();
      let meta = local_server.meta_ref().cheap_clone();
      let pv = local_server.protocol_version();
      let dv = local_server.delegate_version();
      let alive = Alive::new(new_inc, Node::new(id.cheap_clone(), addr))
        .with_meta(meta)
        .with_protocol_version(pv)
        .with_delegate_version(dv);
      self.broadcast_message(id, Message::Alive(alive));
    }
  }

  /// Apply an incoming Suspect to local state. Branches:
  /// 1. Unknown id: ignore.
  /// 2. Older incarnation: ignore.
  /// 3. Existing suspicion timer: forward to `confirm`. Re-broadcast on new info.
  /// 4. Non-Alive node: ignore.
  /// 5. Self → refute, return.
  /// 6. Otherwise: install a fresh Suspicion, transition to Suspect,
  ///    enqueue broadcast.
  pub(crate) fn process_suspect(&mut self, suspect: Suspect<I>, now: Instant) {
    // A leaving/left node makes no membership change from a Suspect — whether
    // from gossip, a confirming Suspect, an expired suspicion timer, or a failed
    // detection probe. The self-Suspect refute path is already inert (see
    // refute); the graceful-leave drain must not mutate membership.
    if self.lifecycle != Lifecycle::Running {
      return;
    }
    let target = suspect.node_ref().cheap_clone();
    let from = suspect.from_ref().cheap_clone();
    let inc = suspect.incarnation();

    // Unknown id → ignore. No pending-decision buffering is needed: an
    // inbound Alive is now applied or dropped synchronously before the
    // next message, so a Suspect can never race ahead of an in-flight
    // Alive decision.
    if !self.members.contains(&target) {
      return;
    }

    let local_id = self.cfg.local_id_ref().cheap_clone();
    let is_self = target == local_id;

    let (local_inc, current_state, has_suspicion) = {
      let m = self.members.get(&target).unwrap();
      (
        m.state_ref().incarnation(),
        m.state_ref().state(),
        m.suspicion().is_some(),
      )
    };
    if inc < local_inc {
      return;
    }
    if has_suspicion {
      let confirmed = match self
        .members
        .get_mut(&target)
        .and_then(|m| m.suspicion_mut())
      {
        Some(s) => s.confirm(&from, now),
        None => crate::suspicion::Confirmation::Ignored,
      };
      if confirmed.is_accepted() {
        self.broadcast_message(
          target.cheap_clone(),
          Message::Suspect(Suspect::new(inc, target, from)),
        );
      }
      return;
    }

    if current_state != State::Alive {
      return;
    }

    if is_self {
      self.refute(inc);
      return;
    }

    let (min, max) = self.suspicion_timeouts();
    let suspicion_mult = self.cfg.suspicion_mult();
    let n = self.num_members() as u32;
    // k = max(0, suspicion_mult - 2), but if the cluster is too small
    // (n < suspicion_mult), the legacy formula collapses k to 0.
    let k = if n >= suspicion_mult {
      suspicion_mult.saturating_sub(2)
    } else {
      0
    };
    let suspicion = crate::suspicion::Suspicion::new(from.cheap_clone(), k, min, max, now);
    let member = self.members.get_mut(&target).unwrap();
    member.set_suspicion(Some(suspicion));
    let member_state = member.state_mut();
    member_state.set_incarnation(inc);
    member_state.set_state(State::Suspect, now);
    // The Alive -> Suspect transition emits no membership event, so bump the
    // snapshot version explicitly.
    self.bump_snapshot_version();

    self.broadcast_message(
      target.cheap_clone(),
      Message::Suspect(Suspect::new(inc, target, from)),
    );
  }

  /// Apply an incoming Dead to local state. Branches:
  /// 1. Unknown id: ignore.
  /// 2. Older incarnation: ignore.
  /// 3. Already Dead/Left: ignore.
  /// 4. Self + not leaving: refute, return.
  /// 5. Self + leaving: mark Left.
  /// 6. Otherwise: mark Dead.
  /// 7. Clear suspicion, broadcast, emit NodeLeft.
  pub(crate) fn process_dead(&mut self, dead: Dead<I>, now: Instant) {
    let target = dead.node_ref().cheap_clone();
    let from = dead.from_ref().cheap_clone();
    let inc = dead.incarnation();

    // Unknown id → ignore. No pending-decision buffering: Alive is
    // applied/dropped synchronously before the next message, so a Dead
    // cannot race an in-flight Alive.
    if !self.members.contains(&target) {
      return;
    }

    let local_id = self.cfg.local_id_ref().cheap_clone();
    let is_self = target == local_id;
    // "self-marked-itself-dead" sentinel: target == from.
    let self_marked = target == from;

    let (local_inc, current_state) = {
      let m = self.members.get(&target).unwrap();
      (m.state_ref().incarnation(), m.state_ref().state())
    };
    if inc < local_inc {
      return;
    }
    if matches!(current_state, State::Dead | State::Left) {
      return;
    }

    if is_self {
      if !self.lifecycle.is_leaving() {
        self.refute(inc);
        return;
      }
      {
        let m = self.members.get_mut(&target).unwrap();
        m.set_suspicion(None);
        m.state_mut().set_incarnation(inc);
        m.state_mut().set_state(State::Left, now);
      }
      self.broadcast_message(
        target.cheap_clone(),
        Message::Dead(Dead::new(inc, target.cheap_clone(), from)),
      );
      let server = self.members.get(&target).unwrap().state_ref().server_arc();
      self.emit_event(Event::NodeLeft(server));
      self.lifecycle = Lifecycle::Left;
      // `LeftCluster` is the leave-*completion* signal and must NOT fire
      // here at the state transition while the dead-self is still only
      // queued. `leave()` owns that boundary: it computes
      // `leave_flush_remaining` after the dead-self fan-out (or emits
      // `LeftCluster` immediately when there are no live peers).
      return;
    }

    // A leaving/left node makes no remote membership change. The self-Dead
    // transition above drives leave to completion; an inbound or
    // expired-suspicion Dead about a peer must not mark it Dead/Left or emit
    // NodeLeft while we are draining.
    if !self.lifecycle.is_running() {
      return;
    }

    if let Some(m) = self.members.get_mut(&target) {
      m.set_suspicion(None);
      let current_state = m.state_mut();
      current_state.set_incarnation(inc);
      let new_state = if self_marked {
        State::Left
      } else {
        State::Dead
      };
      current_state.set_state(new_state, now);
    }

    self.broadcast_message(
      target.cheap_clone(),
      Message::Dead(Dead::new(inc, target.cheap_clone(), from)),
    );

    if let Some(server) = self
      .members
      .get(&target)
      .map(|m| m.state_ref().server_arc())
    {
      self.emit_event(Event::NodeLeft(server));
    }
  }

  /// Build the `NodeState` view passed to
  /// [`MergeDelegate::notify_merge`](crate::delegate::MergeDelegate) from
  /// the remote push/pull `states`.
  fn merge_peers_view(states: &[PushNodeState<I, A>]) -> Vec<NodeState<I, A>> {
    states
      .iter()
      .map(|p| {
        NodeState::new(
          p.id_ref().cheap_clone(),
          p.address_ref().cheap_clone(),
          p.state(),
        )
        .with_meta(p.meta_ref().cheap_clone())
        .with_protocol_version(p.protocol_version())
        .with_delegate_version(p.delegate_version())
      })
      .collect()
  }

  /// Whether a push/pull merge of `states` is admitted. The
  /// [`MergeDelegate`](crate::delegate::MergeDelegate) is consulted for
  /// **every** push/pull — a join AND a periodic anti-entropy refresh — so a
  /// peer the delegate rejects can never slip its membership or state in
  /// through a later refresh; with no delegate, all merges are admitted.
  /// Called inline (synchronous) — no deferral.
  ///
  /// This deliberately tightens Go memberlist, whose `mergeRemoteState` gates
  /// `NotifyMerge` on the join flag only and so still merges a rejected peer's
  /// state on anti-entropy — an incomplete split-brain guard.
  fn merge_admitted(&self, states: &[PushNodeState<I, A>]) -> bool {
    match &self.merge_delegate {
      None => true,
      Some(d) => d.notify_merge(MaybeOwned::from(Self::merge_peers_view(states))),
    }
  }

  /// Driver feeds an incoming Suspect message.
  pub(crate) fn handle_suspect(&mut self, _from: A, suspect: Suspect<I>, at: Instant) {
    self.process_suspect(suspect, at);
  }

  /// Driver feeds an incoming Dead message.
  pub(crate) fn handle_dead(&mut self, _from: A, dead: Dead<I>, at: Instant) {
    self.process_dead(dead, at);
  }

  /// Driver feeds an incoming Ping. Replies with an Ack via
  /// `pending_transmits`. Misrouted Pings (not addressed to the local node)
  /// are silently dropped.
  pub(crate) fn handle_ping(&mut self, from: A, ping: Ping<I, A>, _now: Instant) {
    // Verify the Ping is addressed to us.
    if ping.target_ref().id_ref() != self.cfg.local_id_ref() {
      return;
    }

    // Build the Ack carrying our current ack_payload. When configured to
    // restrict the payload to known members, omit it unless the Ping's claimed
    // source id maps to a tracked member WHOSE ADDRESS matches the transport
    // source we observed (`from`, where the Ack is sent). Binding the claimed id
    // to the observed address — the same address-as-identity model as the
    // indirect-ping relay guard — is what bounds the amplification: a Ping that
    // spoofs a member's id from a victim's address (so the large Ack reflects to
    // the victim) fails the address check, while a genuine member ping passes.
    let include_payload = !self.cfg.ack_payload_to_members_only()
      || self
        .members
        .get(ping.source_ref().id_ref())
        .is_some_and(|m| m.state_ref().address_ref() == &from);
    let ack = if include_payload {
      Ack::new(ping.sequence_number()).with_payload(self.ack_payload.clone())
    } else {
      self.metrics.ack_payloads_withheld += 1;
      Ack::new(ping.sequence_number())
    };

    self
      .pending_transmits
      .push_back(Transmit::Packet(PacketTransmit::new(
        from,
        Message::Ack(ack),
      )));
  }

  /// Driver feeds an incoming Ack. Resolves the matching probe or
  /// relays the Ack to the original requester if we were forwarding an
  /// indirect ping. Untracked sequence numbers are silently dropped.
  pub(crate) fn handle_ack(&mut self, from: A, ack: Ack, now: Instant) {
    let seq = ack.sequence_number();

    // Peek the entry kind WITHOUT consuming the slot. Keying ack handlers
    // purely by the monotonic (guessable) u32 seq and accepting an Ack
    // from ANY source is a forgery footgun: an off-path node that
    // observes/guesses the seq could complete a probe (forging success /
    // `PingCompleted`) or relay-forge a forward, and crucially EVICT the
    // registry slot so the genuine Ack is then dropped. Validate the
    // responder before consuming the slot, symmetric to the Nack allowlist.
    let kind = match self.ack_registry.get(seq) {
      Some(e) => e.kind_ref().clone(),
      None => return, // untracked seq
    };
    if !self.ack_source_is_valid(seq, &kind, &from) {
      // Wrong/forged source: drop WITHOUT removing the entry so the
      // genuine responder's Ack can still resolve it before the deadline.
      return;
    }

    // Forward-relay deadline authority. Probe/Ping Acks are
    // deadline-bounded by `complete_probe_success`'s
    // `now >= failure_deadline` cutoff (the consolidated single source).
    // A `Forward` Ack never goes through `complete_probe_success` — it
    // just relays — so without this it has NO timeout authority: drivers
    // process received packets BEFORE firing `handle_timeout`, so an Ack
    // delivered at/after the forward's deadline would be relayed and
    // suppress the Nack `fire_expired_forwards` is about to send,
    // falsely completing the requester's indirect probe of an
    // unresponsive target. Enforce the SAME boundary
    // `fire_expired_forwards` uses (`deadline <= now`): too late ⇒ drop
    // WITHOUT consuming the registry slot or the forward record, leaving
    // BOTH for the next `handle_timeout` → `fire_expired_forwards` to
    // emit the Nack and clean up.
    if let AckKind::Forward(_) = &kind {
      if let Some(fwd) = self.indirect_forwards.get(&seq) {
        if now >= fwd.deadline {
          return;
        }
      }
    }

    let payload = ack.payload_bytes();
    let resolution = match self.ack_registry.handle_ack(seq, payload, now) {
      Some(r) => r,
      None => return, // Untracked seq (race — already resolved).
    };
    let resolution_payload = resolution.payload_bytes().unwrap_or_default();
    match kind {
      AckKind::Probe => {
        // A direct ack comes from the probe target itself; an indirect-relayed
        // ack is relayed by a peer, so its `from` is that relay, not the target.
        let direct_target_ack = self
          .probes
          .get(&seq)
          .is_some_and(|p| p.target.address_ref() == &from);
        self.complete_probe_success(seq, direct_target_ack, resolution_payload, now);
      }
      AckKind::Forward(fa) => {
        let reply_to = fa.into_reply_to();
        // Relay an Ack to the original requester.
        if let Some(forward) = self.indirect_forwards.remove(&seq) {
          let relay = Ack::new(forward.requester_seq).with_payload(resolution_payload);
          self
            .pending_transmits
            .push_back(Transmit::Packet(PacketTransmit::new(
              reply_to,
              Message::Ack(relay),
            )));
        }
      }
      AckKind::Ping => {
        // An application `Ping` is direct-only: its ack always comes from the
        // target, so it always notifies completion.
        let direct_target_ack = self
          .probes
          .get(&seq)
          .is_some_and(|p| p.target.address_ref() == &from);
        self.complete_probe_success(seq, direct_target_ack, resolution_payload, now);
      }
    }
  }

  /// Whether an incoming Ack's source address is an acceptable responder
  /// for the pending registry entry `seq`. Validated BEFORE the slot is
  /// consumed so a spoofed Ack can neither forge an outcome nor evict the
  /// entry the genuine responder still needs.
  ///
  /// - `Probe`/`Ping`: the probe target. While `AwaitingIndirect` a probe
  ///   Ack may instead arrive *relayed* by one of the chosen indirect
  ///   peers — it carries that peer's source address, not the target's —
  ///   so the same allowlist used for Nack dedup is accepted.
  ///   `Ping` is direct-only and never reaches `AwaitingIndirect`, so only
  ///   the target is accepted for it.
  /// - `Forward`: we forwarded a Ping to exactly one target; only that
  ///   target may Ack our allocated seq.
  ///
  /// An entry with no backing probe/forward state is conservatively
  /// rejected — they are registered together, so this only happens if the
  /// outcome was already decided; there is nothing to validate against and
  /// nothing for a "success" to complete. Any registry slot is reaped by its
  /// paired `remove` when the probe/forward terminates, not by a sweep.
  fn ack_source_is_valid(&self, seq: u32, kind: &AckKind<A>, from: &A) -> bool {
    match kind {
      AckKind::Probe | AckKind::Ping => {
        let Some(probe) = self.probes.get(&seq) else {
          return false;
        };
        if probe.target.address_ref() == from {
          return true;
        }
        matches!(
          &probe.phase,
          ProbePhase::AwaitingIndirect(AwaitingIndirect { indirect_peers, .. })
            if indirect_peers.contains(from)
        )
      }
      AckKind::Forward(_) => self
        .indirect_forwards
        .get(&seq)
        .is_some_and(|f| &f.target_addr == from),
    }
  }

  /// Common terminal-success path for both Detection probes and Pings.
  /// Removes the probe from `probes`, ticks Awareness (Detection only),
  /// emits PingCompleted (Ping always; Detection only on the direct ack).
  fn complete_probe_success(
    &mut self,
    seq: u32,
    direct_target_ack: bool,
    payload: bytes::Bytes,
    now: Instant,
  ) {
    // An Ack arriving at/after the probe's authoritative failure deadline
    // must not rescue it → route to failure. That deadline is the single
    // kind-aware, sent_at-anchored, phase-INDEPENDENT value defined by
    // `Probe::failure_deadline` (Detection: sent_at + scale_timeout(
    // probe_interval) — the awareness-scaled probe interval snapshotted at
    // sent, also the AwaitingIndirect phase deadline; Ping: sent_at +
    // probe_timeout — direct-only). Routing through the one source
    // eliminates per-phase packet-vs-timer cutoff races.
    let cutoff = match self.probes.get(&seq) {
      None => return,
      Some(p) => p.failure_deadline(),
    };
    if now >= cutoff {
      self.probe_terminate_failure(seq, now);
      return;
    }
    let Some(probe) = self.probes.remove(&seq) else {
      return;
    };
    // Drop the original direct-ping AckRegistry entry registered at
    // `start_probe`. The direct/indirect-relayed paths reach here via
    // `handle_ack`, which already removed it; the reliable-fallback
    // success path (`handle_reliable_ping_response`) does NOT, so without
    // this a UDP-degraded / TCP-working cluster leaks one entry per
    // fallback success and keeps a stale seq live for late-Ack dispatch.
    // Targeted by seq + idempotent, symmetric with `probe_terminate_failure`.
    // Ignoring Err: idempotent removal; the entry may already be gone via
    // `handle_ack` on the direct path.
    let _ = self.ack_registry.remove(seq);
    let rtt = now.saturating_duration_since(probe.sent_at);
    match probe.kind {
      ProbeKind::Detection => {
        // Any successful probe — direct or indirect-relayed — improves our
        // self-awareness (`awareness_delta = -1`, persisting through
        // indirect-relayed Ack arrivals).
        self.awareness.record_success();
        self.bump_snapshot_version();
        // A passive node observes ping completions from ordinary periodic SWIM
        // traffic, but only on a direct ack from the probe target — not an
        // indirect-relayed ack, nor a reliable-fallback success. The caller
        // classifies the source (the ack's `from` versus the probe target), not
        // the probe phase: a direct target ack can arrive after the probe has
        // moved to `AwaitingIndirect` and still completes the ping.
        if direct_target_ack {
          self.emit_event(Event::PingCompleted(PingCompleted::new(
            PingId::new(seq),
            probe.target,
            rtt,
            payload,
          )));
        }
      }
      ProbeKind::Ping => {
        self.emit_event(Event::PingCompleted(PingCompleted::new(
          PingId::new(seq),
          probe.target,
          rtt,
          payload,
        )));
      }
    }
  }

  /// Fire any expired suspicion timers, transitioning the peer to Dead.
  fn fire_expired_suspicions(&mut self, now: Instant) {
    // Collect ids whose suspicion timer has expired. We do this in two
    // passes so we don't hold a borrow on members while calling
    // process_dead.
    let expired_ids: Vec<I> = self
      .members
      .iter()
      .filter_map(|m| {
        m.suspicion()
          .filter(|s| s.deadline() <= now)
          .map(|_| m.state_ref().id_ref().cheap_clone())
      })
      .collect();
    for id in expired_ids {
      // Fetch the suspicion's incarnation before transitioning. We use
      // local_id as the dead-message origin since we observed the
      // timeout ourselves.
      let inc = self
        .members
        .get(&id)
        .map(|m| m.state_ref().incarnation())
        .unwrap_or(0);
      let dead = Dead::new(inc, id, self.cfg.local_id_ref().cheap_clone());
      self.process_dead(dead, now);
    }
  }

  /// AwaitingIndirect deadline (or AwaitingDirect with no indirect peers
  /// available): terminate the probe as failure. For Detection, mark the
  /// target as Suspect and apply Awareness delta per Lifeguard rules.
  /// For Ping, emit `Event::PingFailed` carrying the ping's correlation
  /// token (no escalation, no awareness penalty).
  fn probe_terminate_failure(&mut self, seq: u32, now: Instant) {
    let Some(probe) = self.probes.remove(&seq) else {
      return;
    };
    // Remove the AckRegistry entry for this seq (probe is being
    // terminated, not awaiting an Ack anymore). Targeted by seq so we
    // don't disturb other still-registered probe/forward entries.
    // Ignoring Err: idempotent removal; the entry may already be gone.
    let _ = self.ack_registry.remove(seq);
    // Drop the concurrent reliable-fallback's pending dial intent (if it
    // never dialed) so a late `dial_succeeded` cannot promote it into an
    // orphan stream after the probe has already failed. An already-live
    // fallback stream is harmless: a late `ReliablePingAcked`
    // hits `complete_probe_success` for a now-absent probe (no-op) and a
    // late `ReliablePingFailed` hits `retire_reliable_fallback` (no-op).
    let nack_stats = match &probe.phase {
      ProbePhase::AwaitingIndirect(AwaitingIndirect {
        expected_nacks,
        nacked_by,
        reliable_stream_id,
        ..
      }) => {
        if let Some(rid) = reliable_stream_id {
          self.pending_stream_intents.remove(rid);
        }
        // Deduped distinct responders: a duplicate or late Nack never
        // entered `nacked_by`, so `expected - seen` cannot be driven to
        // 0 by a Nack flood to suppress the awareness penalty.
        Some((*expected_nacks, nacked_by.len()))
      }
      _ => None,
    };
    match probe.kind {
      ProbeKind::Detection => {
        // A probe that dispatched NO datagram at all — an over-MTU direct Ping,
        // no IndirectPing queued, and no reliable fallback opened — reflects a
        // purely LOCAL MTU limit (a node id is unbounded, so a large id can push
        // every Ping/IndirectPing past `gossip_mtu`), NOT peer loss. Abort
        // cleanly: the probe + ack entry are already removed above; charge NO
        // awareness penalty and emit NO Suspect. `dispatched` is the MONOTONIC
        // record of whether ANY attempt (direct Ping, IndirectPing, or reliable
        // dial) was ever made — read from history, NOT reconstructed from the
        // current `reliable_stream_id`, which a fallback retirement
        // (`dial_failed` / `ReliablePingFailed`) clears while the probe is still
        // live (so an over-MTU peer whose sole reliable attempt then fails would
        // otherwise evade suspicion forever). A probe that DID dispatch and then
        // timed out is a genuine failure and takes the penalty + suspicion path.
        if !probe.dispatched {
          return;
        }
        let severity = match nack_stats {
          Some((expected, seen)) if expected > 0 => {
            // Missing nacks = our health is suspect. Saturate to 0 in the
            // (defensive) case where seen > expected, which can't happen
            // under the FSM but the arithmetic should still be safe.
            expected.saturating_sub(seen) as u32
          }
          _ => 1, // No indirect peers available; penalize ourselves.
        };
        self.awareness.record_failure(severity);
        self.bump_snapshot_version();

        // Suspect the exact instance that was probed, at the incarnation
        // snapshotted when the probe started — NOT whatever now holds the id.
        // Only the EXACT membership instance may be suspected: while the probe
        // was in flight the id could have been REPLACED by a different instance,
        // and `process_suspect`'s `inc < local_inc` guard does not catch every
        // such case on its own — a `reset_nodes` removal + re-admit at the SAME
        // address and an EQUAL (or lower) incarnation yields `snapshot_inc >=
        // current_inc`, so the guard would let a stale Suspect evict the fresh
        // instance. The generation token settles this precisely: it identifies
        // one instance across its whole lifetime and changes on any replacement
        // (new id, or Left/Dead reclaim at any address/incarnation), so an exact
        // match is true ONLY for the probed instance. An in-place refutation
        // keeps the SAME generation but raises the incarnation, so the match
        // still holds and the snapshot-incarnation Suspect below is then dropped
        // by `process_suspect`'s `inc < local_inc` — matching Go memberlist
        // `suspectNode`, which suspects `node.Incarnation` and lets the newer
        // live incarnation reject it. A `0` snapshot (an untracked probe target)
        // never equals a tracked member's nonzero generation, so it can suspect
        // no one. The awareness penalty above stands regardless — our own failure
        // to confirm the probed peer is a real observation whichever instance
        // now holds the id.
        let target_id = probe.target.id_ref().cheap_clone();
        let same_probed_instance = probe.target_generation != 0
          && self
            .members
            .get(&target_id)
            .is_some_and(|m| m.generation() == probe.target_generation);
        if same_probed_instance {
          let local_id = self.cfg.local_id_ref().cheap_clone();
          let suspect = Suspect::new(probe.target_incarnation, target_id, local_id);
          self.process_suspect(suspect, now);
        }
      }
      ProbeKind::Ping => {
        // Notify the application that its directed ping timed out.
        self.emit_event(Event::PingFailed(PingFailed::new(
          PingId::new(seq),
          probe.target,
        )));
      }
    }
  }

  /// Driver feeds an incoming Nack. Records the responder against the
  /// matching `AwaitingIndirect` probe. Untracked sequence numbers, probes
  /// not in AwaitingIndirect, late Nacks, Nacks from a node we did not
  /// IndirectPing, and duplicate Nacks from the same responder are all
  /// silently dropped.
  ///
  /// This handler is edge-triggered, so it MUST enforce the invariants
  /// explicitly — deadline-bound, allowlisted, deduped — or a
  /// duplicate/late/forged Nack would inflate the count and suppress the
  /// Lifeguard health penalty in `probe_terminate_failure`.
  pub(crate) fn handle_nack(&mut self, from: A, nack: Nack, now: Instant) {
    let seq = nack.sequence_number();
    let Some(probe) = self.probes.get_mut(&seq) else {
      return;
    };
    // A Nack at/after the probe's authoritative failure deadline is too
    // late to influence the outcome — the probe terminates at that instant
    // and a relayed Ack is likewise no longer accepted past it (symmetric
    // to the `complete_probe_success` cutoff). Dropping it also stops a
    // post-deadline Nack flood from masking the awareness penalty.
    // `failure_deadline` is the single absolute source of truth (anchored
    // at `sent`, never the injected `now`).
    if now >= probe.failure_deadline() {
      return;
    }
    if let ProbePhase::AwaitingIndirect(AwaitingIndirect {
      indirect_peers,
      nacked_by,
      ..
    }) = &mut probe.phase
    {
      // Count a Nack ONLY from a peer we actually sent an IndirectPing to
      // (a Nack carries just the seq; an off-path node guessing it, or a
      // stale/forged Nack, must not be able to mark the indirect probe
      // answered), and only ONCE per responder (a duplicate must not
      // inflate the count and thereby drive `expected_nacks -
      // nacked_by.len()` to 0 in `probe_terminate_failure`).
      if indirect_peers.contains(&from) && !nacked_by.contains(&from) {
        nacked_by.push(from);
      }
    }
  }

  /// Drain all queued broadcasts and return their messages.
  ///
  /// Useful in tests that need to inspect the content (e.g. verify an Alive
  /// refute was queued after the buddy-system Suspect piggyback). Calling this
  /// consumes the broadcasts from the queue (each one still increments its
  /// retransmit counter via `take_broadcasts`).
  ///
  /// Uses the actual member count for the retransmit-limit calculation, but
  /// a practically unlimited byte budget so no message is left behind.
  pub fn drain_broadcasts(&mut self) -> Vec<Message<I, A>> {
    let num_nodes = self.num_members() as u32;
    // No compound assembly here (caller inspects messages individually) and
    // no MTU — pass overhead 0 / unlimited budget.
    self.broadcast.take_broadcasts(num_nodes, 0, usize::MAX)
  }

  // ────────────────────────── Application API ──────────────────────────────

  /// Set the bytes to attach to outgoing Acks. Replaces the legacy
  /// `PingDelegate::ack_payload` callback.
  ///
  /// An Ack is emitted as ONE UDP datagram on the gossip socket
  /// ([`handle_ping`](Self::handle_ping)), so its framed size must fit the
  /// node's gossip packet budget ([`gossip_mtu`](EndpointOptions::gossip_mtu)).
  /// A payload whose framed Ack would exceed that budget is rejected with
  /// [`Error::AckPayloadExceedsMtu`](crate::error::Error::AckPayloadExceedsMtu)
  /// and NOT stored: an over-budget Ack is deterministically unsendable
  /// (`send_to` errors are dropped under the lossy-gossip policy), so every
  /// probe reply would silently fail and peers would falsely suspect this
  /// node. Mirrors the fail-fast `update_meta` cap rather than storing a
  /// payload that can never leave the node.
  pub fn set_ack_payload(&mut self, payload: Bytes) -> Result<(), crate::error::Error> {
    // Reject once leaving/left, like update_meta: a node that has begun tearing
    // down rejects further config mutations rather than report a false success.
    self.ensure_running()?;
    // Charge the exact framed size of the Ack that would carry this payload.
    // `handle_ping` builds `Ack::new(seq).with_payload(...)`; the sequence
    // number's protobuf varint is 1–5 bytes wide, so validate against the
    // widest seq (`u32::MAX`) — the framed Ack for any real seq is then
    // guaranteed `<= budget`. Same real-`encode_message` idiom the probe
    // compound uses for its MTU fit check (no estimated upper bound).
    let candidate = Ack::new(u32::MAX).with_payload(payload.cheap_clone());
    let encoded_len = crate::wire::encode_message::<I, A>(&Message::Ack(candidate))
      .expect("locally-built Ack always bridges to wire form")
      .len();
    let budget = self.gossip_mtu();
    if encoded_len > budget {
      return Err(crate::error::Error::AckPayloadExceedsMtu(
        crate::error::SizeExceeded::new(encoded_len, budget),
      ));
    }
    self.ack_payload = payload;
    Ok(())
  }

  /// Update the local-state snapshot used for outgoing push/pull messages.
  /// Replaces legacy `NodeDelegate::local_state` — instead of the state
  /// machine pulling at send-time, the application pushes the freshest
  /// snapshot whenever its local state changes.
  ///
  /// The bytes are returned to the peer when the gossip layer constructs a
  /// push/pull response.
  ///
  /// The snapshot rides every push/pull exchange as the PushPull `user_data`,
  /// and receivers reject any reliable-stream frame whose declared length
  /// exceeds [`max_stream_frame_size`](EndpointOptions::max_stream_frame_size).
  /// A snapshot whose minimal framed PushPull would exceed that cap (after
  /// reserving headroom for the co-resident membership-state list) is rejected
  /// with [`Error::LocalStateExceedsFrame`](crate::error::Error::LocalStateExceedsFrame)
  /// and NOT stored: such a snapshot is deterministically untransmittable —
  /// every push/pull carrying it would be rejected and the application state
  /// would never reach any peer. Mirrors the fail-fast
  /// [`set_ack_payload`](Self::set_ack_payload) cap rather than storing a
  /// snapshot that can never leave the node.
  ///
  /// **Limitation:** legacy `local_state(join: bool)` could return different
  /// bytes for initial-join vs anti-entropy. This setter takes a single
  /// snapshot used for both contexts.
  pub fn set_local_state_snapshot(&mut self, bytes: Bytes) -> Result<(), crate::error::Error> {
    // Reject once leaving/left, like update_meta: a node that has begun tearing
    // down rejects further config mutations rather than report a false success.
    self.ensure_running()?;
    validate_local_state_snapshot::<I, A>(&bytes, self.cfg.max_stream_frame_size())?;
    self.local_state_snapshot = bytes;
    Ok(())
  }

  /// Reject the broadcast if it is leaving/left, or if `data` would not fit a
  /// compound part of the gossip datagram; otherwise `Ok`. Shared fail-fast
  /// guard for the `queue_user_broadcast*` setters.
  fn check_user_broadcast(&self, data: &Bytes) -> Result<(), crate::error::Error> {
    // Reject once leaving/left, like update_meta: a node that has begun tearing
    // down rejects further config mutations rather than report a false success.
    self.ensure_running()?;
    // Gate on the compound-part budget — the inverse of "will the gossip tier
    // walk ever select this payload". The drain charges `data.len() +
    // USER_PART_OVERHEAD` against the compound budget (`gossip_mtu - compound
    // header`), so a payload is sendable iff `data.len() <= compound_budget -
    // USER_PART_OVERHEAD`; reject a larger one here rather than store bytes no
    // tick can ship. `BytesBroadcast::encoded_len == data.len()`, so charge the
    // length directly, off the stable `gossip_mtu`, not a per-tick budget.
    let limit = self
      .gossip_mtu()
      .saturating_sub(COMPOUND_TAG_LEN + COMPOUND_MAX_COUNT_PREFIX_LEN)
      .saturating_sub(Self::USER_PART_OVERHEAD);
    let len = data.len();
    if len > limit {
      return Err(crate::error::Error::UserBroadcastExceedsMtu(
        crate::error::SizeExceeded::new(len, limit),
      ));
    }
    Ok(())
  }

  /// Queue an opaque user-data payload to ride along with outgoing gossip
  /// packets. The payload is retransmit-counted: the gossip scheduler emits it
  /// across `retransmit_mult * ceil(log10(N + 1))` gossip rounds (epidemic
  /// dissemination), prioritizing freshly-queued payloads, then drops it. This
  /// is the highest-priority tier (rank `0`); see
  /// [`queue_user_broadcast_ranked`](Self::queue_user_broadcast_ranked) for
  /// lower-priority tiers.
  ///
  /// Replaces legacy `NodeDelegate::broadcast_messages`. Unlike the legacy
  /// callback (which was pulled by the gossip scheduler each round), our
  /// API is push-based — the application enqueues bytes ahead of time.
  ///
  /// A payload whose raw `data.len()` exceeds the gossip compound-part budget
  /// ([`gossip_mtu`](EndpointOptions::gossip_mtu) minus the compound header and a
  /// conservative per-part framing overhead) is rejected with
  /// [`Error::UserBroadcastExceedsMtu`](crate::error::Error::UserBroadcastExceedsMtu)
  /// and NOT stored: the gossip drain selects user broadcasts through the
  /// compound-part tier walk, so an over-budget payload is never selected and
  /// would otherwise sit queued forever. A *selected* payload is emitted as a lone
  /// [`Transmit::Packet`] when it is the tick's only message, or a
  /// [`Transmit::Compound`] part when several share the datagram. This is a
  /// *selection* budget, not a physical wire limit — the conservative overhead can
  /// reject a payload that would still fit a lone `gossip_mtu` datagram; route
  /// larger payloads over the directed / reliable-stream path. Mirrors the
  /// fail-fast [`set_ack_payload`](Self::set_ack_payload) cap.
  ///
  /// **No automatic dedup:** the application is responsible for tracking
  /// which broadcasts are still relevant. To bound queue growth, check
  /// `user_broadcast_queue_len()` and avoid pushing if too many are pending.
  pub fn queue_user_broadcast(&mut self, data: Bytes) -> Result<(), crate::error::Error> {
    self.check_user_broadcast(&data)?;
    self
      .user_broadcasts
      .queue_ranked(0, BytesBroadcast::new(data));
    Ok(())
  }

  /// As [`queue_user_broadcast`](Self::queue_user_broadcast), but enqueues at
  /// priority `rank` (`0` = highest). A rank at or beyond the configured tier
  /// count ([`with_user_broadcast_tiers`](EndpointOptions::with_user_broadcast_tiers),
  /// default 1) saturates to the lowest tier — a larger rank is demoted, never
  /// promoted.
  ///
  /// **Strict priority, no cross-tier fairness.** Higher tiers drain fully before
  /// lower ones under the shared gossip budget, so under sustained higher-priority
  /// traffic that fills the per-tick budget, lower-priority payloads are deferred
  /// indefinitely and do NOT advance their retransmit counters — those ranks have
  /// **no eventual-delivery guarantee**. Intentional; mirrors Go serf's strict
  /// intent > query > event queues. Tiers are unbounded by default; observe depth
  /// via [`user_broadcast_queue_len`](Self::user_broadcast_queue_len) and bound it
  /// embedder-side if a high tier can be flooded.
  pub fn queue_user_broadcast_ranked(
    &mut self,
    rank: u8,
    data: Bytes,
  ) -> Result<(), crate::error::Error> {
    self.check_user_broadcast(&data)?;
    self
      .user_broadcasts
      .queue_ranked(rank as usize, BytesBroadcast::new(data));
    Ok(())
  }

  /// Enqueue a directed unreliable user message to `to` (no gossip, no
  /// delivery guarantee). Mirrors `memberlist-core`'s `send`. Validates the
  /// framed `UserData` size against the gossip MTU and emits one
  /// `Transmit::Packet`; the driver encodes it with the live policy.
  pub fn send_user_packet(&mut self, to: A, data: Bytes) -> Result<(), crate::error::Error> {
    // Reject once leaving/left: a departing node starts no new gossip-plane I/O.
    self.ensure_running()?;
    let encoded_len = crate::wire::encode_message::<I, A>(&Message::UserData(data.cheap_clone()))
      .map(|b| b.len())
      .unwrap_or(usize::MAX);
    let budget = self.gossip_mtu();
    if encoded_len > budget {
      return Err(crate::error::Error::UserPacketExceedsMtu(
        crate::error::SizeExceeded::new(encoded_len, budget),
      ));
    }
    self
      .pending_transmits
      .push_back(Transmit::Packet(PacketTransmit::new(
        to,
        Message::UserData(data),
      )));
    Ok(())
  }

  /// Enqueue several directed unreliable user messages to `to`. Mirrors
  /// `send_many`. Two-or-more messages are compound-packed into one datagram
  /// (the driver splits at the MTU); a single message degrades to
  /// `send_user_packet`.
  pub fn send_user_packets(
    &mut self,
    to: A,
    payloads: &[Bytes],
  ) -> Result<(), crate::error::Error> {
    // Reject once leaving/left: a departing node starts no new gossip-plane I/O.
    self.ensure_running()?;
    match payloads {
      [] => Ok(()),
      [one] => self.send_user_packet(to, one.cheap_clone()),
      many => {
        let mut assembled = COMPOUND_TAG_LEN + COMPOUND_MAX_COUNT_PREFIX_LEN;
        let mut msgs = TinyVec::new();
        for p in many {
          let m = Message::UserData(p.cheap_clone());
          let part_len = crate::wire::encode_message::<I, A>(&m)
            .map(|b| b.len())
            .unwrap_or(usize::MAX);
          assembled =
            assembled.saturating_add(COMPOUND_MAX_PART_PREFIX_LEN.saturating_add(part_len));
          msgs.push(m);
        }
        let budget = self.gossip_mtu();
        if assembled > budget {
          return Err(crate::error::Error::UserPacketExceedsMtu(
            crate::error::SizeExceeded::new(assembled, budget),
          ));
        }
        self
          .pending_transmits
          .push_back(Transmit::Compound(CompoundTransmit::new(to, msgs)));
        Ok(())
      }
    }
  }

  /// Re-broadcast our own Alive with updated metadata.
  pub fn update_meta(&mut self, meta: Meta) -> Result<(), crate::error::Error> {
    // Reject once leaving/left/shutdown: a post-leave meta update would bump
    // the incarnation and broadcast a higher-incarnation Alive that peers
    // accept over the dead-self leave, resurrecting the node.
    self.ensure_running()?;
    // Enforce the per-endpoint Meta cap. Wire's `Meta::MAX_SIZE` is the
    // absolute ceiling at construction; here we apply the tighter
    // configured `meta_max_size` so the local broadcast stays within
    // the cluster's agreed limit. `update_meta` is the public
    // boundary the operator drives, so a hard error is the right
    // signal — silent truncation would hide config drift.
    let cap = self.cfg.meta_max_size();
    if meta.len() > cap {
      return Err(crate::error::Error::MetaExceedsCap(
        crate::error::SizeExceeded::new(meta.len(), cap),
      ));
    }
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let local_addr = self.cfg.advertise_addr_ref().cheap_clone();
    let inc = self.next_incarnation();
    // Update local server.
    let new_server = Arc::new(
      NodeState::new(
        local_id.cheap_clone(),
        local_addr.cheap_clone(),
        State::Alive,
      )
      .with_meta(meta.cheap_clone())
      .with_protocol_version(self.cfg.protocol_version())
      .with_delegate_version(self.cfg.delegate_version()),
    );
    if let Some(local) = self.members.get_mut(&local_id) {
      local.state_mut().set_incarnation(inc);
      local.state_mut().set_server(new_server.clone());
    }
    // Broadcast.
    let alive = Alive::new(inc, Node::new(local_id.cheap_clone(), local_addr))
      .with_meta(meta)
      .with_protocol_version(self.cfg.protocol_version())
      .with_delegate_version(self.cfg.delegate_version());
    self.broadcast_message(local_id, Message::Alive(alive));
    // Emit NodeUpdated for the local node.
    self.emit_event(Event::NodeUpdated(new_server));
    Ok(())
  }

  /// Initiate a SWIM failure-detection probe. Returns `true` if a probe was
  /// initiated, `false` if no eligible target exists (cluster has only the
  /// local node, all peers are dead/leaving, etc.). The periodic scheduler
  /// calls this every `probe_interval` (scaled by Awareness).
  pub fn start_probe(&mut self, now: Instant) -> bool {
    // A leaving/left node starts no failure-detection probe (no direct Ping I/O).
    if !self.is_running() {
      return false;
    }
    let Some(target_id) = self.next_probe_target() else {
      return false;
    };
    let target_member = self
      .members
      .get(&target_id)
      .expect("target id came from members iteration");
    let target_arc = target_member.state_ref().server_arc();
    // Snapshot the probed instance now: a later failure suspects THIS
    // incarnation (not whatever the member holds at expiry) and only while the
    // live member still carries THIS generation token (so a rejoin under the
    // same id — even at the same address/incarnation — is not mistaken for it).
    let target_incarnation = target_member.state_ref().incarnation();
    let target_generation = target_member.generation();

    let seq = self.allocate_seq();
    let pt = self.cfg.probe_timeout();
    // Detection failure deadline = `sent + awareness.scale_timeout(
    // probe_interval)`, snapshotted at probe start. This is the
    // Lifeguard-scaled SWIM period, NOT `2*probe_timeout`: a degraded
    // local node (high health score) waits proportionally longer before
    // suspecting peers.
    let failure_deadline = now + self.awareness.scale_timeout(self.cfg.probe_interval());
    let probe = Probe::new_direct(
      target_arc.cheap_clone(),
      target_incarnation,
      target_generation,
      now,
      ProbeKind::Detection,
      pt,
      failure_deadline,
    );
    // AckEntry deadline derived from the SAME source as the probe FSM.
    let direct_deadline = probe.direct_deadline(pt);

    // Register the probe in the FSM map.
    self.probes.insert(seq, probe);

    // Register the AckRegistry entry so handle_ack can look up the probe.
    self
      .ack_registry
      .register(seq, AckEntry::new(now, direct_deadline, AckKind::Probe));

    // Build the Ping.
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let local_addr = self.cfg.advertise_addr_ref().cheap_clone();
    let target_addr = target_arc.address_ref().cheap_clone();
    let ping = Ping::new(
      seq,
      Node::new(local_id, local_addr),
      Node::new(target_id.cheap_clone(), target_addr.cheap_clone()),
    );
    // Build the Ping (+ buddy Suspect when the target is Suspect, so it
    // can refute on receipt). Co-sent as ONE compound datagram WHEN IT
    // FITS the MTU (the piggyback optimization).
    //
    // Node id `I` is unbounded, so although a Ping+Suspect pair is
    // ~100-300 B for any bounded id, a large id can make each message a
    // valid lone datagram (<= MTU) while their compound exceeds MTU. An
    // over-MTU compound has no split point and would be fragmented/dropped,
    // breaking the buddy-Suspect refutation for an id range that worked
    // when the probe emitted Ping and Suspect as two separate <=MTU
    // datagrams. So emit a Compound only when its conservative assembled
    // size (same u32-varint upper bounds as the gossip budget) fits the
    // configured `gossip_mtu`; otherwise split into two Packets in
    // Ping-then-Suspect order. Never emit an unsendable over-MTU compound.
    let mut probe_msgs: TinyVec<Message<I, A>> = TinyVec::new();
    probe_msgs.push(Message::Ping(ping));

    let target_state = self.members.get(&target_id).map(|m| m.state_ref().state());
    if matches!(target_state, Some(State::Suspect)) {
      let target_inc = self
        .members
        .get(&target_id)
        .map(|m| m.state_ref().incarnation())
        .unwrap_or(0);
      let local_id = self.cfg.local_id_ref().cheap_clone();
      let suspect = Suspect::new(target_inc, target_id, local_id);
      probe_msgs.push(Message::Suspect(suspect));
    }

    let to = target_addr.cheap_clone();
    let direct_dispatched = if probe_msgs.len() == 1 {
      // A lone probe Ping is subject to the SAME gossip_mtu ceiling as the
      // compound path below: node ids are unbounded, so a Ping carrying a
      // large already-admitted target id can exceed one datagram even though
      // the construction self-Ping floor covers only the local identity.
      let ping_msg = probe_msgs.pop().expect("len checked == 1");
      self.push_probe_packet_within_mtu(to, ping_msg)
    } else {
      // Conservative assembled-compound upper bound — identical idiom to
      // the gossip budget (COMPOUND_TAG_LEN + count varint + per-part
      // inner_len varint + each plain-frame len). .expect() per the
      // machine-built-message convention (Ping/Suspect always bridge).
      let assembled_upper = COMPOUND_TAG_LEN
        + COMPOUND_MAX_COUNT_PREFIX_LEN
        + probe_msgs
          .iter()
          .map(|m| {
            COMPOUND_MAX_PART_PREFIX_LEN
              + crate::wire::encode_message::<I, A>(m)
                .expect("outbound probe message must bridge to wire form")
                .len()
          })
          .sum::<usize>();
      if assembled_upper <= self.gossip_mtu() {
        self
          .pending_transmits
          .push_back(Transmit::Compound(CompoundTransmit::new(to, probe_msgs)));
        // The compound fits `gossip_mtu`, so the direct Ping rides inside it —
        // the probe HAS dispatched a datagram.
        true
      } else {
        // Over-MTU compound ⇒ split into two Packets, Ping then Suspect. Each
        // is delivered as a separate send under the same lone-datagram MTU
        // ceiling: a normal split (both parts <= MTU) emits both and preserves
        // the refutation path, while a part that is itself over-MTU (a large
        // peer id) is dropped rather than fragmented — never emit an
        // unsendable datagram from either branch.
        let mut it = probe_msgs.into_iter();
        let ping_msg = it.next().expect("probe_msgs[0] is the Ping");
        let suspect_msg = it.next().expect("probe_msgs[1] is the buddy Suspect");
        // Only the direct Ping decides whether the probe dispatched; the buddy
        // Suspect is a refutation optimization, not a probe of the target.
        let dispatched = self.push_probe_packet_within_mtu(to.cheap_clone(), ping_msg);
        self.push_probe_packet_within_mtu(to, suspect_msg);
        dispatched
      }
    };
    // Record whether the direct Ping actually queued. A probe that dispatches NO
    // datagram — an over-MTU direct Ping, and (after escalation) over-MTU
    // IndirectPings with no reliable fallback — reflects a local MTU limit, not
    // peer loss; `probe_terminate_failure` reads this to abort cleanly instead
    // of applying an awareness penalty and suspecting the target.
    if let Some(p) = self.probes.get_mut(&seq) {
      p.dispatched = direct_dispatched;
    }

    true
  }

  /// Whether `msg`'s wire encoding fits the configured `gossip_mtu` as a lone
  /// `Packet`. A pure predicate — a single encode, no queue — so an emitter can
  /// decide, without a wasted second encoding, whether to queue the datagram or
  /// reject/drop it.
  ///
  /// This is the single ceiling every unreliable-plane `Ping` emitter enforces:
  /// the periodic probe's direct `Ping` and buddy `Suspect`, and the indirect
  /// fan-out's `IndirectPing` (all three via `push_probe_packet_within_mtu`);
  /// the public `ping` API, which returns
  /// [`Error::PingExceedsMtu`](crate::error::Error::PingExceedsMtu) on an
  /// over-MTU target rather than register a doomed probe; and the indirect relay
  /// `handle_indirect_ping`, which — because a legitimate requester already
  /// counted this helper in its `expected_nacks` — Nacks that validated requester
  /// and bumps `indirect_forwards_oversized` rather than forward a doomed Ping.
  /// Node ids are unbounded, so any of these can
  /// carry a large already-admitted or attacker-supplied peer id that exceeds
  /// the single-datagram budget even though the construction-time self-`Ping`
  /// floor guarantees only the LOCAL identity fits. A single message has no
  /// compound split point, so an over-MTU one is never emitted (a fragmentable,
  /// undeliverable datagram) — the same ceiling the compound branch enforces,
  /// mirroring the lone-`Packet` gossip path (`len <= gossip_mtu`). The probe
  /// FSM is unaffected: a dropped direct `Ping` simply receives no Ack and fails
  /// on its deadline, exactly as a lost UDP datagram would; a dropped
  /// `IndirectPing` is excluded from the caller's outstanding-nack accounting so
  /// the FSM never waits on a Nack the unreached peer cannot send.
  fn probe_packet_within_mtu(&self, msg: &Message<I, A>) -> bool {
    let encoded_len = crate::wire::encode_message::<I, A>(msg)
      .expect("outbound probe message must bridge to wire form")
      .len();
    encoded_len <= self.gossip_mtu()
  }

  /// Queue `msg` as a lone `Packet` to `to` when it fits the configured
  /// `gossip_mtu` (via `probe_packet_within_mtu`), returning whether it was
  /// queued; an over-MTU message is dropped (returning `false`) rather than
  /// emitted. See `probe_packet_within_mtu` for the full rationale and the set
  /// of emitters that share this ceiling. Used by the periodic probe (direct
  /// `Ping` + buddy `Suspect`) and the indirect fan-out (`IndirectPing`); the
  /// public `ping` and indirect-relay paths call the pure predicate directly
  /// because each measures or registers differently on a miss (a typed error vs.
  /// a dropped forward), and reusing this queue-on-fit helper there would either
  /// re-encode for the size or emit a Packet they must not.
  fn push_probe_packet_within_mtu(&mut self, to: A, msg: Message<I, A>) -> bool {
    if self.probe_packet_within_mtu(&msg) {
      self
        .pending_transmits
        .push_back(Transmit::Packet(PacketTransmit::new(to, msg)));
      true
    } else {
      false
    }
  }

  /// Round-robin pick the next probe target. Returns `None` if no eligible
  /// peer exists. Round-robin state is maintained via `probe_index`.
  fn next_probe_target(&mut self) -> Option<I> {
    let n = self.members.len();
    if n == 0 {
      return None;
    }
    let local_id = self.cfg.local_id_ref().cheap_clone();
    for offset in 0..n {
      let idx = (self.probe_index + offset) % n;
      let candidate_id = match self.member_at(idx) {
        Some(id) => id,
        None => continue,
      };
      if candidate_id == local_id {
        continue;
      }
      if let Some(m) = self.members.get(&candidate_id) {
        if m.state_ref().dead_or_left() {
          continue;
        }
        self.probe_index = (idx + 1) % n;
        return Some(candidate_id);
      }
    }
    None
  }

  /// Internal helper: get the id of the member at vector position `idx`.
  fn member_at(&self, idx: usize) -> Option<I> {
    self
      .members
      .iter()
      .nth(idx)
      .map(|m| m.state_ref().id_ref().cheap_clone())
  }

  /// Driver feeds an incoming IndirectPing. We forward a Ping to the target
  /// on the requester's behalf. If the target acks within `cfg.probe_timeout`,
  /// we relay an Ack to the requester (see `handle_ack` Forward branch). If
  /// the deadline elapses without an ack, we send a Nack.
  pub(crate) fn handle_indirect_ping(&mut self, from: A, ind: IndirectPing<I, A>, now: Instant) {
    let target_id = ind.target_ref().id_ref().cheap_clone();
    let target_addr = ind.target_ref().addr_ref().cheap_clone();
    let requester_addr = ind.source_ref().addr_ref().cheap_clone();
    let requester_seq = ind.sequence_number();

    // The requester address/seq are taken from the packet *body*
    // (`ind.source()`), which is fully attacker-controlled. Trusting it
    // blind makes this node a relay oracle: an attacker sends us an
    // IndirectPing with `source = victim V`, `requester_seq = V's probe
    // seq`, and any responsive `target`; we forward a Ping, the target
    // Acks, and we relay an Ack to V carrying V's seq. If we happen to be
    // in V's `AwaitingIndirect` allowlist (likely in a small cluster), V's
    // `ack_source_is_valid` accepts our relayed Ack as a chosen-peer relay
    // and V's probe of its (possibly dead) real target is falsely completed
    // — plus we become a reflection/amplification vector. The transport
    // `from` (the real datagram source the driver observed via `recv_from`)
    // is NOT forgeable by packet content, so require it to match the
    // embedded source address before acting. This is the same
    // address-as-identity model enforced for relayed Acks and Nack dedup.
    // On mismatch drop entirely: no forwarded Ping, no AckRegistry/forward
    // registration, no Nack-on-expiry.
    if from != requester_addr {
      return;
    }

    // Flood backstop: bound the relay state a peer can induce. Drop a fresh
    // forward at the cap, and dedup an identical in-flight relay (same
    // requester, requester seq, and target) so a retransmitted IndirectPing
    // does not double the state. Both drop entirely (no forward, no Nack).
    if self.indirect_forwards.len() >= self.cfg.max_indirect_forwards() {
      self.metrics.indirect_forwards_dropped += 1;
      return;
    }
    if self.indirect_forwards.values().any(|f| {
      f.reply_to_addr == requester_addr
        && f.requester_seq == requester_seq
        && f.target_addr == target_addr
    }) {
      return;
    }

    let our_seq = self.allocate_seq();
    let deadline = now + self.cfg.probe_timeout();

    // Build the forwarded Ping and MTU-check it BEFORE registering any relay
    // state. A node id is unbounded and `target` is attacker-controlled, so a
    // large target id can push the forwarded Ping past the single-datagram
    // `gossip_mtu`; so too can differential MTU — this relay's longer local id
    // added to a `target` that the requester's shorter inbound IndirectPing
    // (`source + target`) still fit under the same budget. Unlike the
    // from-mismatch / flood-cap / dedup guards above — which correctly drop with
    // no Nack (a spoofed source, an overload backstop, or a duplicate the
    // original forward still answers: no legitimate requester waiting on THIS
    // reply) — an MTU-un-forwardable forward has a real requester that already
    // counted this helper in its `expected_nacks`. Dropping it silently would
    // make the requester read a responsive helper as unresponsive, inflating its
    // `expected_nacks - seen`, degrading its Lifeguard health and stretching its
    // failure deadlines. So send it an immediate Nack — at the same VALIDATED
    // requester address the relay-Ack / Nack-on-expiry paths use — and register
    // no relay/ack state (a doomed forward kept only to Nack on expiry would
    // instead waste the relay state the flood backstop protects). The miss bumps
    // its own counter, distinct from the flood cap.
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let local_addr = self.cfg.advertise_addr_ref().cheap_clone();
    let forwarded = Ping::new(
      our_seq,
      Node::new(local_id, local_addr),
      Node::new(target_id, target_addr.cheap_clone()),
    );
    let msg = Message::Ping(forwarded);
    if !self.probe_packet_within_mtu(&msg) {
      self.metrics.indirect_forwards_oversized += 1;
      // A Nack carries only a seq, so it always fits the datagram — no MTU gate
      // needed. It echoes the requester's own probe seq (`requester_seq`), not
      // our unused forward seq.
      let nack = Nack::new(requester_seq);
      self
        .pending_transmits
        .push_back(Transmit::Packet(PacketTransmit::new(
          requester_addr,
          Message::Nack(nack),
        )));
      return;
    }

    // Register the AckRegistry entry so handle_ack's Forward branch fires.
    self.ack_registry.register(
      our_seq,
      AckEntry::new(
        now,
        deadline,
        AckKind::Forward(ForwardAck::new(requester_addr.cheap_clone())),
      ),
    );

    // Track the forwarder bookkeeping. The Nack-on-timeout path uses the
    // SAME validated requester address as the relay-Ack path above —
    // no id→members lookup that could drop/misroute it.
    self.indirect_forwards.insert(
      our_seq,
      IndirectForward {
        reply_to_addr: requester_addr,
        target_addr: target_addr.cheap_clone(),
        requester_seq,
        deadline,
      },
    );

    // Emit the forwarded Ping (already MTU-validated above).
    self
      .pending_transmits
      .push_back(Transmit::Packet(PacketTransmit::new(target_addr, msg)));
  }

  /// Initiate a direct application-level ping to `node`. Returns a [`PingId`]
  /// correlation token that identifies this exchange in the terminal event.
  ///
  /// On success (peer Ack arrives within `cfg.probe_timeout`) the caller
  /// receives `Event::PingCompleted` carrying the same `PingId`. On timeout,
  /// `Event::PingFailed` is emitted, also carrying the `PingId`. Both events
  /// flow through `poll_event`; the caller does not block or await.
  ///
  /// Unlike a SWIM failure-detection probe, an application ping is
  /// direct-only: it does not fan out to indirect peers, request a reliable
  /// fallback, or mark the target as suspect on timeout.
  ///
  /// # Errors
  ///
  /// Returns [`Error::NotRunning`](crate::error::Error::NotRunning) if the
  /// endpoint has left or shut down. Returns
  /// [`Error::PingExceedsMtu`](crate::error::Error::PingExceedsMtu) if the
  /// framed `Ping` to `node` would exceed the single-datagram `gossip_mtu`
  /// budget — a node id is unbounded, so a large target id can push it past one
  /// datagram, and such a Ping is rejected up front (registering no probe or ack
  /// state) rather than begun as a probe doomed to time out. Returns
  /// [`Error::UnencodablePingTarget`](crate::error::Error::UnencodablePingTarget)
  /// if `node`'s id or address cannot be encoded on the compact wire layout
  /// (e.g. a scoped or flow-labelled IPv6 `SocketAddr`), likewise rejected up
  /// front before any probe/ack state is registered.
  pub fn ping(&mut self, node: Node<I, A>, now: Instant) -> Result<PingId, crate::error::Error> {
    // Reject once leaving/left: a departing node starts no new probe.
    self.ensure_running()?;
    let target_id = node.id_ref().cheap_clone();
    let target_addr = node.addr_ref().cheap_clone();
    // Bind the probe target to the address the Ping is actually SENT to (the
    // caller-supplied address), so `ack_source_is_valid` accepts the Ack from
    // that address and `PingFailed` reports the pinged address. Reuse the
    // stored NodeState (preserving its meta/versions in the terminal event)
    // ONLY when the member is tracked at that same address; otherwise — an
    // untracked node, or a known id reachable at a different address than the
    // one stored — synthesize a minimal NodeState carrying the caller's
    // id+address rather than binding to the stale stored address.
    let target_arc = match self.members.get(&target_id) {
      Some(m) if m.state_ref().address_ref() == &target_addr => m.state_ref().server_arc(),
      _ => std::sync::Arc::new(NodeState::new(
        target_id.cheap_clone(),
        target_addr.cheap_clone(),
        State::Alive,
      )),
    };
    // A Ping never suspects, so its snapshot incarnation and generation are
    // unused for suspicion; carry the member's current values (or `0`/"unset"
    // when the target is untracked) to satisfy the shared constructor.
    let target_incarnation = self
      .members
      .get(&target_id)
      .map(|m| m.state_ref().incarnation())
      .unwrap_or(0);
    let target_generation = self
      .members
      .get(&target_id)
      .map(|m| m.generation())
      .unwrap_or(0);

    let seq = self.allocate_seq();

    // Build the outbound Ping and measure its framed size with the REAL
    // allocated seq (the seq varint contributes to the byte count at the MTU
    // boundary). A direct application ping rides one gossip datagram, so an
    // over-budget Ping is undeliverable: the peer would never Ack and the probe
    // could only fail on its deadline. Measure and reject BEFORE inserting the
    // probe or registering the ack, so an over-MTU target leaves no dangling
    // probe/ack state and queues no Packet. The now-unused seq is harmless —
    // `allocate_seq` skips live slots, exactly as the wrapping counter advancing
    // does.
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let local_addr = self.cfg.advertise_addr_ref().cheap_clone();
    let ping = Ping::new(
      seq,
      Node::new(local_id, local_addr),
      Node::new(target_id, target_addr.cheap_clone()),
    );
    let msg = Message::Ping(ping);
    let budget = self.gossip_mtu();
    // `node` is caller-supplied. Unlike a wire-decoded address (whose
    // SocketAddrV6 `scope_id` / `flowinfo` always decode to 0, so it always
    // re-encodes), a caller-supplied target can carry a nonzero `scope_id` /
    // `flowinfo` that the compact encoder rejects, making the Ping
    // un-encodable. Map that to a typed error — before any probe/ack state is
    // registered — rather than panicking on an ordinary bad target.
    let encoded_len = match crate::wire::encode_message::<I, A>(&msg) {
      Ok(bytes) => bytes.len(),
      Err(_) => return Err(crate::error::Error::UnencodablePingTarget),
    };
    if encoded_len > budget {
      return Err(crate::error::Error::PingExceedsMtu(
        crate::error::SizeExceeded::new(encoded_len, budget),
      ));
    }

    // Fits the datagram budget: register the direct probe + ack, then queue it.
    // An application ping is direct-only — it waits just `probe_timeout`, with
    // no indirect/fallback escalation and no awareness scaling. Failure deadline
    // == the direct deadline.
    let pt = self.cfg.probe_timeout();
    let mut probe = Probe::new_direct(
      target_arc,
      target_incarnation,
      target_generation,
      now,
      ProbeKind::Ping,
      pt,
      now + pt,
    );
    // The over-MTU case already returned above, so this direct Ping IS queued
    // below — the probe has dispatched a datagram. (A Ping never suspects, so
    // this only keeps the flag honest; the abort path is Detection-only.)
    probe.dispatched = true;
    let deadline = probe.direct_deadline(pt);
    self.probes.insert(seq, probe);
    self
      .ack_registry
      .register(seq, AckEntry::new(now, deadline, AckKind::Ping));
    self
      .pending_transmits
      .push_back(Transmit::Packet(PacketTransmit::new(target_addr, msg)));
    Ok(PingId::new(seq))
  }

  /// Initiate an outbound push/pull state exchange with `peer`.
  ///
  /// Encodes the local membership state into a PushPull message immediately.
  /// Returns the `StreamId` identifying this dial. The driver observes
  /// `Event::DialRequested { id, peer, deadline }` from `poll_event()` and
  /// dials accordingly.
  ///
  /// `kind` should be `PushPullKind::Join` for initial join, `PushPullKind::Refresh`
  /// for periodic anti-entropy.
  pub fn start_push_pull(&mut self, peer: A, kind: PushPullKind, now: Instant) -> StreamId {
    let id = self.allocate_stream_id();
    // A leaving/left node initiates no push/pull: it queues no dial intent and
    // emits no DialRequested, so it advertises none of its pre-leave Alive state
    // to a seed. The returned id is inert; the gated callers (join command
    // handlers and seed drains) never reach this once not Running.
    if !self.is_running() {
      return id;
    }
    let deadline = now + self.cfg.stream_timeout();

    // Encode: PushPull message with current member state.
    // Collect first to avoid holding an immutable borrow on `self.members`
    // while also accessing `self.local_state_snapshot` below.
    let join = kind.is_join();
    let states: Vec<PushNodeState<I, A>> = self
      .members
      .iter()
      .map(|m| {
        let ls = m.state_ref();
        let ns = ls.server_ref();
        // State must be the live liveness (`ls.state()`), NOT the embedded
        // server snapshot (`ns.state()`): `set_server` only runs on Alive,
        // so `ns.state()` is frozen at the last Alive while Suspect/Dead/Left
        // update only `LocalNodeState`. Advertising `ns.state()` here would
        // serialize failed members as Alive and resurrect them on the peer.
        PushNodeState::new(
          ls.incarnation(),
          ns.id_ref().cheap_clone(),
          ns.address_ref().cheap_clone(),
          ls.state(),
        )
        .with_meta(ns.meta_ref().cheap_clone())
        .with_protocol_version(ns.protocol_version())
        .with_delegate_version(ns.delegate_version())
      })
      .collect();
    let push_pull =
      PushPull::new(join, states.into_iter()).with_user_data(self.local_state_snapshot.clone());

    let msg = Message::PushPull(push_pull);
    let encoded = crate::wire::encode_message::<I, A>(&msg)
      .expect("PushPull encode cannot fail for well-formed data");

    self.pending_stream_intents.insert(
      id,
      PendingStreamIntent {
        peer: peer.cheap_clone(),
        kind: OutboundKind::PushPull(kind),
        deadline,
        encoded,
        _marker: PhantomData,
      },
    );

    // Signal the driver to dial.
    self
      .pending_events
      .push_back(Event::DialRequested(DialRequested::new(id, peer, deadline)));

    id
  }

  /// Initiate a reliable-stream fallback ping for probe sequence number
  /// `probe_seq`. Encodes a Ping message (source = local node, target = peer)
  /// and queues a dial intent. Returns `StreamId`; the driver observes
  /// `Event::DialRequested` from `poll_event()`. Opened concurrently with the
  /// indirect fan-out by `probe_fan_out_indirect`.
  ///
  /// `deadline` is the owning probe's single cumulative deadline, NOT an
  /// independent stream timeout — the reliable fallback must race exactly
  /// the same deadline as the indirect pings. This keeps a slow/skewed
  /// `stream_timeout` from either killing the fallback before, or letting
  /// it outlive, the probe.
  pub fn start_reliable_ping(
    &mut self,
    peer_id: I,
    peer_addr: A,
    probe_seq: u32,
    deadline: Instant,
  ) -> StreamId {
    let id = self.allocate_stream_id();
    // A leaving/left node opens no reliable-ping fallback. The returned id is
    // inert; the only caller is the probe FSM, gated by handle_timeout.
    if !self.is_running() {
      return id;
    }

    let local_id = self.cfg.local_id_ref().cheap_clone();
    let local_addr = self.cfg.advertise_addr_ref().cheap_clone();

    let ping = Ping::new(
      probe_seq,
      Node::new(local_id, local_addr),
      Node::new(peer_id.cheap_clone(), peer_addr.cheap_clone()),
    );
    let msg = Message::Ping(ping);
    let encoded = crate::wire::encode_message::<I, A>(&msg)
      .expect("Ping encode cannot fail for well-formed data");

    self.pending_stream_intents.insert(
      id,
      PendingStreamIntent {
        peer: peer_addr.cheap_clone(),
        kind: OutboundKind::ReliablePing(probe_seq),
        deadline,
        encoded,
        _marker: PhantomData,
      },
    );

    self
      .pending_events
      .push_back(Event::DialRequested(DialRequested::new(
        id, peer_addr, deadline,
      )));

    id
  }

  /// Enqueue a reliable-stream delivery of `payload` to `peer`. Returns a
  /// `StreamId`; the driver observes `Event::DialRequested` and dials. On
  /// success the driver drains `stream.poll_transmit()`. No reply is expected;
  /// the stream transitions to Done after bytes are drained.
  ///
  /// Wire format: `[USER_DATA_MESSAGE_TAG=9][VARINT_LEN][PAYLOAD]`.
  pub fn start_user_message(
    &mut self,
    peer: A,
    payload: bytes::Bytes,
    now: Instant,
  ) -> Result<StreamId, crate::error::Error> {
    // Reject once leaving/left: a departing node starts no new reliable dial.
    self.ensure_running()?;
    let id = self.allocate_stream_id();
    let deadline = now + self.cfg.stream_timeout();

    let msg = Message::<I, A>::UserData(payload);
    let encoded = crate::wire::encode_message::<I, A>(&msg).expect("UserData encode cannot fail");

    self.pending_stream_intents.insert(
      id,
      PendingStreamIntent {
        peer: peer.cheap_clone(),
        kind: OutboundKind::UserMessage,
        deadline,
        encoded,
        _marker: PhantomData,
      },
    );

    self
      .pending_events
      .push_back(Event::DialRequested(DialRequested::new(id, peer, deadline)));

    Ok(id)
  }

  /// The driver successfully dialed the peer for stream `id`. Endpoint
  /// promotes the pending intent into a live `Stream<I, A>` with the encoded
  /// request bytes already in `output_buf`. The driver should call
  /// `stream.poll_transmit(&mut buf)` immediately to drain the bytes.
  ///
  /// Returns `None` if `id` is unknown (intent was already cancelled).
  pub fn dial_succeeded(&mut self, id: StreamId, now: Instant) -> Option<Stream<I, A>> {
    let intent = self.pending_stream_intents.remove(&id)?;
    // Write-side deadline authority (symmetric to the read-side check in
    // `Stream::handle_data`): the WHOLE reliable exchange is bounded by
    // `deadline`, so a dial that only completes at/after the exchange
    // deadline must NOT emit a stale request (push/pull or reliable-ping
    // bytes) onto the wire. Drop it exactly like a dial failure. The
    // inbound-response write side is already bounded transitively:
    // `handle_data` fails the stream past the deadline, so no
    // `PushPullRequestReceived` is emitted and no response is ever loaded.
    if now >= intent.deadline {
      if let OutboundKind::ReliablePing(probe_seq) = intent.kind {
        self.retire_reliable_fallback(probe_seq);
      }
      return None;
    }
    // A leaving/left node promotes no dial: a push/pull would hand the peer our
    // pre-leave Alive state (resurrecting the node on a seed that never sees the
    // dead-self notice), a reliable-ping is a detection fallback, and a user
    // message is new outbound I/O the post-leave contract forbids.
    if self.lifecycle != Lifecycle::Running {
      if let OutboundKind::ReliablePing(probe_seq) = intent.kind {
        self.retire_reliable_fallback(probe_seq);
      }
      return None;
    }
    let output_buf: std::collections::VecDeque<u8> = VecDeque::from(intent.encoded);
    let phase = StreamPhase::OutboundSendingRequest(intent.kind);
    Some(Stream {
      id,
      peer: intent.peer,
      local_id: self.cfg.local_id_ref().cheap_clone(),
      max_frame_size: self.cfg.max_stream_frame_size(),
      phase,
      input_buf: bytes::BytesMut::new(),
      output_buf,
      deadline: Some(intent.deadline),
      endpoint_events: std::collections::VecDeque::new(),
      stream_events: std::collections::VecDeque::new(),
    })
  }

  /// The driver accepted an inbound stream from `from`. Endpoint mints a
  /// fresh `StreamId` and returns an inbound-phase `Stream<I, A>`. The
  /// driver feeds bytes via `stream.handle_data(bytes, now)`.
  pub fn accept_stream(&mut self, from: A, now: Instant) -> Option<Stream<I, A>> {
    // Reliable-inbound lifecycle chokepoint, the stream-plane twin of
    // `handle_packet` refusing gossip-plane inbound once not Running. A
    // Leaving/Left node admits no new inbound reliable stream: it mints
    // nothing and returns `None`, and the caller drops the transport
    // connection rather than servicing an exchange the node is leaving.
    if !self.is_running() {
      return None;
    }
    let id = self.allocate_stream_id();
    let deadline = now + self.cfg.stream_timeout();
    Some(Stream {
      id,
      peer: from,
      local_id: self.cfg.local_id_ref().cheap_clone(),
      max_frame_size: self.cfg.max_stream_frame_size(),
      phase: StreamPhase::InboundAwaitingFirstMessage,
      input_buf: bytes::BytesMut::new(),
      output_buf: std::collections::VecDeque::new(),
      deadline: Some(deadline),
      endpoint_events: std::collections::VecDeque::new(),
      stream_events: std::collections::VecDeque::new(),
    })
  }

  /// Internal: route a reliable-ping (concurrent fallback) outcome into the
  /// probe FSM.
  ///
  /// `ReliablePingAcked` → `complete_probe_success` (ticks Awareness, emits
  /// `PingCompleted` for Ping probes): the fallback won the race, so the
  /// probe succeeds regardless of the still-pending indirect path.
  /// `ReliablePingFailed` does NOT fail the probe — the fallback is only
  /// one of two concurrent attempts; a failure is just a "did not make
  /// contact" signal. We retire the fallback stream; the indirect path
  /// keeps racing the single cumulative deadline, which alone decides
  /// suspicion.
  fn handle_reliable_ping_response(&mut self, ev: EndpointEvent<I, A>, now: Instant) {
    use EndpointEvent;
    // `now` is unused: the Acked path passes the event's `at` timestamp,
    // and the Failed path doesn't need a wall-clock anchor (it just
    // retires the fallback stream). Keep the parameter for symmetry with
    // the other `handle_*` dispatchers.
    let _ = now;
    match ev {
      EndpointEvent::ReliablePingAcked(p) => {
        // A reliable-fallback success is not a direct UDP target ack, so it does
        // not notify ping completion.
        self.complete_probe_success(p.seq(), false, bytes::Bytes::new(), p.at());
      }
      EndpointEvent::ReliablePingFailed(p) => {
        self.retire_reliable_fallback(p.seq());
      }
      _ => {}
    }
  }

  /// Initiate a graceful leave. Marks the local node `Left` synchronously
  /// (so `is_left()` is true on return) and queues the dead-self notice to
  /// every currently-live peer via the public `poll_transmit` path.
  /// Idempotent: a repeat once already Leaving/Left is `Ok(())`, never an
  /// error.
  ///
  /// `process_dead` only enqueues the dead-self into the gossip broadcast
  /// queue, but that queue stops being drained once `lifecycle == Left`
  /// (`poll_transmit` drains only `pending_transmits`; the broadcast→transmit
  /// hop runs solely in the gossip scheduler while `Running`, and `leave`
  /// clears `next_gossip`). So the queued copy alone would never reach peers
  /// and graceful leave would silently degrade to a failure-detection
  /// timeout. To make leave observable through the public `poll_transmit`
  /// path the drivers already drain, the dead-self is additionally fanned
  /// out as direct packets to every live peer.
  ///
  /// **Completion contract (Sans-I/O):** the machine cannot block, so
  /// `leave()` returns immediately. `Event::LeftCluster` is the
  /// leave-*completion* signal. It is emitted **after** the dead-self
  /// packets `leave()` queued have been drained via `poll_transmit`
  /// (handed to the I/O layer) — tracked by an explicit count of just the
  /// dead-self fan-out. `leave()` first drops every gossip-plane packet
  /// queued before it ran, so a departing node emits only its leave notice:
  /// no stale Ack/Ping/Alive/user packet that would re-advertise pre-leave
  /// state or pad the flush, and nothing a post-leave enqueue can use to
  /// delay or spuriously trigger completion. When there are no live peers it
  /// is emitted immediately. A driver's high-level `leave(timeout)` MUST wait for
  /// `LeftCluster` (racing its own timeout → `LeaveTimeout`) before
  /// reporting success or tearing down the socket; treating `leave()`'s
  /// `Ok(())` return or the `NodeLeft`/state transition as completion
  /// would drop the leave notice and peers would observe a failure
  /// instead of an intentional leave.
  ///
  /// **Queued user broadcasts:** payloads still queued via
  /// [`queue_user_broadcast`](Self::queue_user_broadcast) get a best-effort,
  /// at-most-one-MTU final flush — packed into the dead-self notice to every
  /// farewell recipient, ahead of the `Dead` part so a layered protocol
  /// processes them before the membership death carried by the same frame.
  /// Whatever does not fit that single-datagram budget is dropped, as it was
  /// before this final flush existed.
  pub fn leave(&mut self, now: Instant) -> Result<(), crate::error::Error> {
    self.leave_with(now, None)
  }

  /// The largest encoded user payload [`leave_with`](Self::leave_with) can
  /// reserve into the farewell compound beside this node's `Dead` part, or
  /// `None` when the `Dead` part alone exhausts the gossip MTU.
  ///
  /// A layered protocol whose departure payload MUST ride the farewell checks
  /// this BEFORE mutating any of its own leave state, so an oversized payload
  /// is refused up front instead of silently degrading to the bare `Dead`
  /// fan-out at leave time. Identity-aware: the bound depends on the encoded
  /// size of this node's own id.
  pub fn farewell_capacity(&self) -> Option<usize> {
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let dead_encoded_len = crate::wire::encode_message::<I, A>(&Message::Dead(Dead::new(
      self.incarnation,
      local_id.cheap_clone(),
      local_id,
    )))
    .map(|b| b.len())
    .ok()?;
    let budget = self
      .gossip_mtu()
      .saturating_sub(COMPOUND_TAG_LEN + COMPOUND_MAX_COUNT_PREFIX_LEN)
      .saturating_sub(dead_encoded_len.saturating_add(COMPOUND_MAX_PART_PREFIX_LEN));
    // `leave_with` charges the reserved payload its encoded `UserData` frame
    // plus a part prefix; `USER_PART_OVERHEAD` is exactly that worst-case
    // framing over the raw payload bytes, so subtracting it yields the largest
    // raw payload guaranteed to fit.
    let capacity = budget.saturating_sub(Self::USER_PART_OVERHEAD);
    (capacity > 0).then_some(capacity)
  }

  /// [`leave`](Self::leave) with an explicit farewell payload.
  ///
  /// `farewell` is a single already-encoded user frame that is RESERVED into
  /// every farewell compound ahead of the ordinary queue drain — first user
  /// part, before any drained payload and before the `Dead` part. The ordinary
  /// drain selects by the gossip ordering (fewest transmissions, then largest),
  /// so a payload queued immediately before leaving can lose the budget to an
  /// older, larger one; a layered protocol whose departure intent MUST reach
  /// every farewell recipient passes it here instead of (or in addition to)
  /// queueing it. A farewell too large for the budget left by the `Dead` part
  /// is dropped with a debug log, falling back to the ordinary fan-out.
  pub fn leave_with(
    &mut self,
    now: Instant,
    farewell: Option<Bytes>,
  ) -> Result<(), crate::error::Error> {
    // Idempotent: a repeated leave once already Leaving/Left is a harmless
    // no-op, NOT an error. Turning shutdown retries / partially-failed
    // orchestration into an error would be a behavior regression. Do not
    // re-enqueue or re-fan-out the dead-self.
    if self.lifecycle != Lifecycle::Running {
      return Ok(());
    }
    self.lifecycle = Lifecycle::Leaving;
    // Stop all periodic schedulers. poll_timeout will no longer wake the
    // driver for probe/gossip/push-pull after leave() is called.
    self.next_probe = None;
    self.next_gossip = None;
    self.next_pushpull = None;
    // Drop all pending outbound dial intents: a left node promotes none of them
    // (dial_succeeded refuses each kind once not Running). A push/pull would
    // advertise our pre-leave Alive, a reliable-ping is a detection fallback,
    // and a user message is new I/O the post-leave contract forbids.
    self.pending_stream_intents.clear();
    // Also drop any already-queued DialRequested events: a raw Endpoint driver
    // that polls events after leave must not still be told to dial. dial_succeeded
    // would refuse the promotion, but a driver opens the transport in response to
    // the event itself, which would be new post-leave I/O.
    self
      .pending_events
      .retain(|e| !matches!(e, Event::DialRequested(_)));
    let local_id = self.cfg.local_id_ref().cheap_clone();
    // The applied local incarnation is authoritative: an inbound self-Alive
    // is applied synchronously (no pending-decision gap), so there is no
    // un-applied higher-incarnation self-Alive to fold in here.
    let inc = self
      .members
      .get(&local_id)
      .map(|m| m.state_ref().incarnation())
      .unwrap_or(self.incarnation)
      .max(self.incarnation);
    // Self-marked-itself sentinel (target == from): peers treat node == from
    // as a definitive intentional leave.
    let dead = Dead::new(inc, local_id.cheap_clone(), local_id.cheap_clone());
    self.process_dead(dead, now);

    // Directly notify every currently-live peer so the leave is observable
    // through poll_transmit (see the doc comment above for why the gossip
    // broadcast queue alone is insufficient post-Left).
    let live_peers: Vec<A> = self
      .members
      .iter()
      .filter(|m| {
        m.state_ref().id_ref() != &local_id
          && matches!(m.state_ref().state(), State::Alive | State::Suspect)
      })
      .map(|m| m.state_ref().address_ref().cheap_clone())
      .collect();
    let dead_self_count = live_peers.len();
    // Drop every gossip-plane packet queued before leave(): a departing node
    // emits only its leave notice, never a stale Ack/Ping/Alive/user packet. A
    // buffered Alive would re-advertise our pre-leave state after the dead-self
    // (a resurrection vector), and an arbitrarily long backlog would pad the
    // flush and delay `LeftCluster`. Clearing here leaves the dead-self fan-out
    // below as the only content of `pending_transmits`. The user-broadcast drain
    // that follows repacks the fitting queued payloads INTO that fan-out, so they
    // ride the same leave notice instead of being silently dropped by the reset.
    self.pending_transmits.clear();

    // Give queued user broadcasts a final, measured ride on the dead-self
    // fan-out: a layered protocol that queues its departure intent immediately
    // before calling leave() needs that payload to reach peers ATOMICALLY with
    // the leave notice, so peers classify the departure as intentional rather
    // than a failure. Reserve the Dead part and the compound header up front,
    // then drain what fits using the gossip path's exact per-part charging
    // (`USER_PART_OVERHEAD`); the reserved Dead part keeps the assembled compound
    // within one gossip MTU. Empty when there are no live recipients, when the
    // Dead alone exhausts the MTU, or when nothing queued fits — each falls
    // through to the byte-identical bare-Dead fan-out below.
    let farewell_payloads: Vec<Bytes> = if dead_self_count > 0 {
      let gossip_mtu = self.gossip_mtu();
      let dead_encoded_len = crate::wire::encode_message::<I, A>(&Message::Dead(Dead::new(
        inc,
        local_id.cheap_clone(),
        local_id.cheap_clone(),
      )))
      .map(|b| b.len())
      .unwrap_or(usize::MAX);
      let mut farewell_budget = gossip_mtu
        .saturating_sub(COMPOUND_TAG_LEN + COMPOUND_MAX_COUNT_PREFIX_LEN)
        .saturating_sub(dead_encoded_len.saturating_add(COMPOUND_MAX_PART_PREFIX_LEN));
      // Reserve the explicit farewell FIRST: its ride must not depend on the
      // drain's fewest-transmissions-then-largest ordering, which an older,
      // larger tier-0 payload could otherwise win.
      let mut reserved: Option<Bytes> = None;
      if let Some(fw) = farewell {
        let fw_part_len = crate::wire::encode_message::<I, A>(&Message::UserData(fw.cheap_clone()))
          .map(|b| b.len())
          .unwrap_or(usize::MAX);
        let charge = fw_part_len.saturating_add(COMPOUND_MAX_PART_PREFIX_LEN);
        if charge <= farewell_budget {
          farewell_budget -= charge;
          reserved = Some(fw);
        } else {
          #[cfg(feature = "tracing")]
          tracing::debug!(
            gossip_mtu,
            fw_part_len,
            "explicit farewell payload exceeds the leave notice budget; dropped"
          );
        }
      }
      if farewell_budget == 0 && reserved.is_none() {
        // Degenerate: the Dead part alone consumes the whole gossip MTU (a
        // pathologically large node id, or a tiny configured MTU), leaving no
        // room for any user part. Fall through to the bare-Dead fan-out.
        #[cfg(feature = "tracing")]
        tracing::debug!(
          gossip_mtu,
          dead_encoded_len,
          "leave dead-self notice fills the gossip MTU; queued user broadcasts get no farewell ride"
        );
        Vec::new()
      } else {
        // `num_nodes` is the tracked member count (the retransmit basis), the
        // same argument the gossip scheduler passes to this drain.
        let num_nodes = self.num_members() as u32;
        let (drained, _) = self.drain_user_broadcasts(num_nodes, farewell_budget);
        // The reserved farewell leads the user parts so a layered protocol
        // processes the departure intent before anything else in the frame.
        let mut payloads: Vec<Bytes> = Vec::with_capacity(drained.len() + 1);
        if let Some(fw) = reserved {
          payloads.push(fw);
        }
        payloads.extend(drained);
        // The budget reserved the Dead part and compound header, and the measured
        // drain charged each user part at least its assembled size, so the
        // assembled compound is within one gossip MTU by construction.
        #[cfg(debug_assertions)]
        if !payloads.is_empty() {
          let mut assembled = COMPOUND_TAG_LEN
            + COMPOUND_MAX_COUNT_PREFIX_LEN
            + COMPOUND_MAX_PART_PREFIX_LEN
            + dead_encoded_len;
          for p in &payloads {
            let part_len = crate::wire::encode_message::<I, A>(&Message::UserData(p.cheap_clone()))
              .map(|b| b.len())
              .unwrap_or(usize::MAX);
            assembled =
              assembled.saturating_add(COMPOUND_MAX_PART_PREFIX_LEN.saturating_add(part_len));
          }
          debug_assert!(
            assembled <= gossip_mtu,
            "farewell compound must fit the gossip MTU"
          );
        }
        payloads
      }
    } else {
      Vec::new()
    };

    // Reset the user-broadcast queue so a left node stops gossiping stale user
    // data: the gossip scheduler is shut down above, so any payload still queued
    // via `queue_user_broadcast*` would never be drained again. The drain above
    // already gave the payloads that fit a final ride, so this drops only the
    // remainder that did not.
    self.user_broadcasts.reset();

    for to in live_peers {
      let dead = Message::Dead(Dead::new(
        inc,
        local_id.cheap_clone(),
        local_id.cheap_clone(),
      ));
      if farewell_payloads.is_empty() {
        // Byte-identical to the pre-farewell fan-out: one lone Dead plain frame
        // per live peer.
        self
          .pending_transmits
          .push_back(Transmit::Packet(PacketTransmit::new(to, dead)));
      } else {
        // ORDERING CONTRACT: the user parts MUST precede the Dead part so a
        // layered protocol processes its queued payloads (e.g. a departure
        // intent) BEFORE the membership death carried by the same frame. This
        // deliberately inverts the gossip scheduler's membership-first packing;
        // do not "unify" the two orderings.
        let mut parts: TinyVec<Message<I, A>> = farewell_payloads
          .iter()
          .map(|p| Message::UserData(p.cheap_clone()))
          .collect();
        parts.push(dead);
        self
          .pending_transmits
          .push_back(Transmit::Compound(CompoundTransmit::new(to, parts)));
      }
    }
    if dead_self_count == 0 {
      // No live peers ⇒ nothing to flush ⇒ the leave is complete now.
      self.emit_event(Event::LeftCluster);
      self.leave_flush_remaining = None;
    } else {
      // `pending_transmits` now holds exactly the dead-self fan-out. After
      // `dead_self_count` pops the last leave notice has reached the I/O layer;
      // FIFO keeps anything enqueued after leave() behind that boundary, so
      // neither a stale prefix nor post-leave traffic can delay or spuriously
      // trigger `LeftCluster`.
      self.leave_flush_remaining = Some(dead_self_count);
    }
    Ok(())
  }
}

// Probe / gossip / push-pull scheduling, peer selection, and the inbound
// handlers that trigger them — the methods that draw from the gossip RNG.
impl<I, A, R> Endpoint<I, A, R>
where
  R: Rng,
  I: Id,
  // `Eq + Hash` (strengthening the `PartialEq` the rest of this block relies on)
  // lets `probe_fan_out_indirect` deduplicate candidate helper ADDRESSES through
  // an `FxHashSet<A>` in O(n), so indirect helpers are sampled uniformly over
  // DISTINCT addresses rather than biased by alias multiplicity. The real
  // address types satisfy it (the std drivers pin `A = SocketAddr`).
  A: CheapClone + Data + PartialEq + Eq + core::hash::Hash + 'static,
{
  /// Process an incoming Alive announcement.
  ///
  /// The optional [`AliveDelegate`] admission filter is invoked **inline**;
  /// a `false` result drops the alive. On admit, the transition is applied
  /// immediately via [`process_alive_decided`](Self::process_alive_decided).
  ///
  /// There is deliberately NO deferral / pending-decision event: the
  /// filter is a pure synchronous predicate, so an inbound Alive is fully
  /// applied-or-dropped before the next message is processed. This
  /// in-order, atomic application is what makes Alive→Suspect/Dead
  /// ordering and timer stamping correct by construction.
  pub(crate) fn process_alive(&mut self, alive: Alive<I, A>, bootstrap: bool, now: Instant) {
    let alive_id = alive.node_ref().id_ref().cheap_clone();

    // A node that is Leaving or Left admits no Alive — neither a self-Alive (a
    // peer echoing our pre-leave state would resurrect the just-left local
    // node) nor a remote one (the graceful-leave drain must not re-establish or
    // grow the membership the node is in the middle of leaving). Inbound gossip
    // and push/pull merges both route Alives through here, so this single gate
    // keeps membership admission inert for the entire drain while the dead-self
    // flush and in-flight stream closes proceed.
    if !self.lifecycle.is_running() {
      return;
    }

    // Admission filter, inline. `None` ⇒ admit all.
    if let Some(d) = &self.alive_delegate {
      let server_view = NodeState::new(
        alive_id.cheap_clone(),
        alive.node_ref().addr_ref().cheap_clone(),
        State::Alive,
      )
      .with_meta(alive.meta_ref().cheap_clone())
      .with_protocol_version(alive.protocol_version())
      .with_delegate_version(alive.delegate_version());
      if !d.notify_alive(&server_view) {
        // Rejected — node is not considered a peer.
        return;
      }
    }

    // Admitted: apply immediately, synchronously, in-order.
    self.process_alive_decided(alive, bootstrap, now);
  }

  /// Apply an admitted Alive message — the core transition logic that runs
  /// after the `NotifyAlive` filter. Called directly and synchronously by
  /// [`process_alive`](Self::process_alive).
  pub(crate) fn process_alive_decided(
    &mut self,
    alive: Alive<I, A>,
    bootstrap: bool,
    now: Instant,
  ) {
    let node_ref = alive.node_ref();
    let alive_id = node_ref.id_ref().cheap_clone();
    let alive_addr = node_ref.addr_ref().cheap_clone();
    let alive_incarnation = alive.incarnation();
    let alive_meta = alive.meta_ref().cheap_clone();
    let alive_protocol = alive.protocol_version();
    let alive_delegate = alive.delegate_version();

    let is_local = &alive_id == self.cfg.local_id_ref();

    let mut updates_address = false;
    // Whether this Alive inserts a brand-new member below. A just-inserted Dead
    // placeholder carries incarnation 0, so the staleness guard must NOT apply to
    // it — an `Alive(incarnation = 0)` (a peer that starts at incarnation 0, or a
    // forged id) would otherwise be rejected against the placeholder's own 0 and
    // leave a Dead, snapshot-invisible entry that still consumes `max_members`.
    let mut is_new = false;
    if let Some(existing) = self.members.get(&alive_id) {
      let existing_addr = existing.state_ref().address_ref().cheap_clone();
      if existing_addr != alive_addr {
        let can_reclaim = self.cfg.dead_node_reclaim_time() > Duration::ZERO
          && now.saturating_duration_since(existing.state_ref().state_change())
            > self.cfg.dead_node_reclaim_time();
        let st = existing.state_ref().state();
        if st.is_left() || (st.is_dead() && can_reclaim) {
          // Adopt the new address.
          updates_address = true;
        } else {
          // Conflict. Build the conflicting NodeState lazily here — the common
          // (non-conflict) path never needs it, so it does not pay the clones.
          let other = Arc::new(
            NodeState::new(
              alive_id.cheap_clone(),
              alive_addr.cheap_clone(),
              State::Alive,
            )
            .with_meta(alive_meta.cheap_clone())
            .with_protocol_version(alive_protocol)
            .with_delegate_version(alive_delegate),
          );
          self.emit_event(Event::NodeConflict(NodeConflict::new(
            existing.state_ref().server_arc(),
            other,
          )));
          return;
        }
      }
    } else {
      // New peer. Admission-gate against the optional membership ceiling first:
      // at the cap, refuse to admit a new id so an open network cannot grow
      // membership (and every per-member structure) without bound. Known
      // members' state transitions are unaffected — this branch is new-ids only.
      if let Some(max) = self.cfg.max_members() {
        if self.members.len() >= max {
          self.metrics.members_rejected += 1;
          return;
        }
      }
      // Insert at random offset.
      let initial = NodeState::new(
        alive_id.cheap_clone(),
        alive_addr.cheap_clone(),
        State::Dead,
      )
      .with_meta(alive_meta.cheap_clone())
      .with_protocol_version(alive_protocol)
      .with_delegate_version(alive_delegate);
      let mut local_state = LocalNodeState::new(initial, now);
      local_state.set_incarnation(0);
      // A brand-new id is a fresh membership instance: stamp a new generation
      // token so a probe still in flight against a DEPARTED instance under this
      // id (removed by `reset_nodes`, then rejoined) cannot match this one, even
      // when the rejoin reuses the same address and incarnation.
      let new_member = Member::new(local_state).with_generation(self.next_member_generation());
      let n = self.members.len();
      let offset = if n == 0 {
        0
      } else {
        self.rng.random_range(0..=n)
      };
      self.members.insert_at_random_at(new_member, offset);
      is_new = true;
    }

    // Re-fetch (insert_at_random_at may have moved indices).
    let member = self
      .members
      .get_mut(&alive_id)
      .expect("inserted above or pre-existing");
    let local_incarnation = member.state_ref().incarnation();
    let old_state = member.state_ref().state();
    let old_meta = member.state_ref().server_ref().meta_ref().cheap_clone();

    if !is_new && !updates_address && !is_local && alive_incarnation <= local_incarnation {
      return;
    }
    // Strict-less-than for self (unlike peers, which use <=).
    if is_local && alive_incarnation < local_incarnation {
      return;
    }

    member.set_suspicion(None);

    if !bootstrap && is_local {
      // Same incarnation + same server → idempotent, no-op.
      let same_meta = old_meta == alive_meta;
      let same_pv = member.state_ref().server_ref().protocol_version() == alive_protocol;
      let same_dv = member.state_ref().server_ref().delegate_version() == alive_delegate;
      if alive_incarnation == local_incarnation && same_meta && same_pv && same_dv {
        return;
      }
      self.refute(alive_incarnation);
      return;
    }

    let new_server = Arc::new(
      NodeState::new(
        alive_id.cheap_clone(),
        alive_addr.cheap_clone(),
        State::Alive,
      )
      .with_meta(alive_meta.cheap_clone())
      .with_protocol_version(alive_protocol)
      .with_delegate_version(alive_delegate),
    );
    // Re-fetch the member (we relinquished it for the refute path above).
    // Apply the update, then drop the borrow before calling broadcast_message.
    let new_meta = new_server.meta_ref().cheap_clone();
    // A reclaim that ADOPTS A NEW ADDRESS (a Left/Dead id re-admitted at a
    // different address) is a new membership instance even though the record is
    // reused in place, so it draws a fresh generation. Drawn BEFORE re-borrowing
    // the member (the counter needs `&mut self`). A same-record update — a
    // refutation, meta change, or liveness transition at the same address —
    // keeps its generation and does NOT enter this branch.
    let reclaim_generation = updates_address.then(|| self.next_member_generation());
    {
      let member = self.members.get_mut(&alive_id).expect("present");
      if let Some(g) = reclaim_generation {
        member.set_generation(g);
      }
      let state_mut = member.state_mut();
      state_mut.set_incarnation(alive_incarnation);
      state_mut.set_server(new_server.clone());
      if state_mut.state() != State::Alive {
        state_mut.set_state(State::Alive, now);
      }
    }

    if !bootstrap || !is_local {
      self.broadcast_message(
        alive_id.cheap_clone(),
        Message::Alive(
          Alive::new(
            alive_incarnation,
            Node::new(alive_id.cheap_clone(), alive_addr.cheap_clone()),
          )
          .with_meta(alive_meta)
          .with_protocol_version(alive_protocol)
          .with_delegate_version(alive_delegate),
        ),
      );
    }

    if old_state.is_dead() || old_state.is_left() {
      self.emit_event(Event::NodeJoined(new_server));
    } else if old_meta != new_meta {
      self.emit_event(Event::NodeUpdated(new_server));
    } else {
      // The update applied a strictly-newer incarnation (the older/equal guards
      // returned above), changing the member's incarnation and possibly its
      // state (e.g. a Suspect -> Alive refutation), but NEITHER a resurrection
      // NOR a meta change fired an event. The published snapshot reflects that
      // new state/incarnation, so bump explicitly — otherwise a driver would
      // serve a stale `Suspect` for a node that is now `Alive`.
      self.bump_snapshot_version();
    }
  }

  /// Merge a list of remote `PushNodeState` entries into local state.
  ///
  /// For each remote entry: if Alive, treat as `process_alive`; if Left,
  /// treat as `process_dead`; if Dead or Suspect, treat as
  /// `process_suspect` (we prefer to suspect-then-confirm over jumping
  /// straight to Dead).
  ///
  /// Called from the inbound push/pull handler after the merge decision
  /// has been approved via `decide_merge`.
  pub fn merge_state(&mut self, remote: &[PushNodeState<I, A>], now: Instant) {
    for r in remote {
      let id = r.id_ref().cheap_clone();
      let addr = r.address_ref().cheap_clone();
      let inc = r.incarnation();
      let meta = r.meta_ref().cheap_clone();
      let pv = r.protocol_version();
      let dv = r.delegate_version();
      match r.state() {
        State::Alive => {
          let alive = Alive::new(inc, Node::new(id, addr))
            .with_meta(meta)
            .with_protocol_version(pv)
            .with_delegate_version(dv);
          self.process_alive(alive, false, now);
        }
        State::Left => {
          // `from` MUST be the local id, NOT `id`. `process_dead` records
          // `State::Left` only when `node == from` (the genuine self-leave
          // sentinel), otherwise `State::Dead`. `State::Left` is
          // *immediately* address-reclaimable, so forging `node == from`
          // here would let a stale or forged push/pull hijack a node id at
          // a new address with no reclaim wait. Use `from = local id ⇒ Dead`
          // (reclaim-protected by `dead_node_reclaim_time`).
          let dead = Dead::new(inc, id.cheap_clone(), self.cfg.local_id_ref().cheap_clone());
          self.process_dead(dead, now);
        }
        State::Dead | State::Suspect => {
          let from = self.cfg.local_id_ref().cheap_clone();
          let s = Suspect::new(inc, id, from);
          self.process_suspect(s, now);
        }
        State::Unknown(_) => {
          // Unknown peer state — skip silently.
        }
      }
    }
  }

  /// Driver feeds an incoming Alive message.
  pub(crate) fn handle_alive(&mut self, _from: A, alive: Alive<I, A>, at: Instant) {
    self.process_alive(alive, false, at);
  }

  /// Drive time forward. Fires expired suspicion timers, advances probe FSM
  /// transitions, emits Nacks for expired indirect-ping forwards, and drives
  /// the periodic probe / gossip / push-pull schedulers.
  pub fn handle_timeout(&mut self, now: Instant) {
    // A Leaving/Left node fires no SWIM timers: no suspicion expiry, no probe
    // escalation, no indirect-forward Nacks, no periodic schedulers. The drain
    // is driven by poll_transmit (the dead-self flush) and the driver's own
    // stream-close deadlines, not by the machine clock; poll_timeout likewise
    // reports no SWIM deadline once not Running, so the driver never spins.
    if self.lifecycle != Lifecycle::Running {
      return;
    }
    self.fire_expired_suspicions(now);
    self.advance_probe_fsm(now);
    self.fire_expired_forwards(now);
    self.fire_expired_stream_intents(now);
    self.fire_probe_scheduler(now);
    self.fire_gossip_scheduler(now);
    self.fire_pushpull_scheduler(now);
  }

  /// Advance the probe FSM. `AwaitingDirectAck` probes whose deadline
  /// elapsed transition to `AwaitingIndirect` — fanning out IndirectPings
  /// to k peers AND, concurrently, opening the reliable-ping fallback when
  /// enabled for the target (both race the single cumulative deadline).
  /// `AwaitingIndirect` probes whose deadline elapsed terminate as failure
  /// (no extra per-stream timeout is added). Detection → process_suspect
  /// for the target; Ping → emits `Event::PingFailed` carrying the ping's
  /// correlation token (no escalation, no awareness penalty).
  fn advance_probe_fsm(&mut self, now: Instant) {
    let pt = self.cfg.probe_timeout();
    let mut to_fan_out: Vec<u32> = Vec::new();
    let mut to_terminate_failure: Vec<u32> = Vec::new();
    for (seq, probe) in self.probes.iter() {
      match &probe.phase {
        ProbePhase::AwaitingDirectAck(_) => {
          // `failure_deadline` is the authoritative end of the probe in
          // EVERY phase. If it has elapsed there is no budget left to
          // escalate into — terminate now (Detection → suspect; Ping →
          // Event::PingFailed), do NOT spend a full direct sub-window
          // first. When `scale_timeout(probe_interval) < probe_timeout`
          // the authoritative deadline precedes the direct deadline; an
          // unconditional `sleep(probe_timeout)` here would ignore that
          // and let the probe outlive its own deadline.
          if probe.failure_deadline() <= now {
            to_terminate_failure.push(*seq);
          } else if probe.direct_deadline(pt) <= now {
            // Direct sub-window over (but budget remains). Only
            // failure-detection probes escalate to indirect + reliable
            // fallback. An application `ping` (ProbeKind::Ping) is
            // direct-only — it must NOT leak indirect traffic or emit a
            // late `PingCompleted` after the caller already saw the
            // timeout. For `ProbeKind::Ping`, `probe_terminate_failure`
            // emits `Event::PingFailed` (no escalation, no awareness
            // penalty) and drops the ack entry.
            match probe.kind {
              ProbeKind::Detection => to_fan_out.push(*seq),
              ProbeKind::Ping => to_terminate_failure.push(*seq),
            }
          }
        }
        // Failure boundary = the single-sourced authoritative deadline
        // (== this phase's stored deadline for Detection, by construction).
        ProbePhase::AwaitingIndirect(_) => {
          if probe.failure_deadline() <= now {
            to_terminate_failure.push(*seq);
          }
        }
      }
    }
    for seq in to_fan_out {
      self.probe_fan_out_indirect(seq, now);
    }
    for seq in to_terminate_failure {
      self.probe_terminate_failure(seq, now);
    }
  }

  /// AwaitingDirectAck → AwaitingIndirect: pick up to `indirect_checks` random
  /// Alive helpers by DISTINCT transport address (excluding the local advertise
  /// address and the target's address), send one IndirectPing to each, and
  /// update the FSM phase.
  fn probe_fan_out_indirect(&mut self, seq: u32, now: Instant) {
    let (target_id, target_addr) = match self.probes.get(&seq) {
      Some(p) => (
        p.target.id_ref().cheap_clone(),
        p.target.address_ref().cheap_clone(),
      ),
      None => return,
    };
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let local_addr = self.cfg.advertise_addr_ref().cheap_clone();

    // Candidate helper pool = the DISTINCT transport addresses of Alive members,
    // excluding the local advertise address and the target's address. Scanned
    // ONCE here under the immutable `members` borrow, fully owned before any
    // later `&mut self` call. Deduplicated AS IT IS BUILT via an `FxHashSet<A>`
    // (O(1) per insert), so the pass is O(n) in the membership size `n` and
    // `distinct` holds each address at most once.
    //
    // Sampling then draws uniformly over the DISTINCT addresses. An address
    // advertised by `m` alias ids must NOT get `m×` the selection probability:
    // an attacker who registers many aliases at one address would otherwise
    // reliably win a helper slot and, once in `indirect_peers`, could forge a
    // relay-Ack to falsely complete the probe and suppress a real suspicion.
    // Deduplicating FIRST, then sampling `k` uniformly from the distinct set,
    // gives every distinct address identical probability regardless of alias
    // count — biasing by entry multiplicity (shuffling a duplicate-laden pool)
    // is exactly the bug this avoids.
    //
    // memberlist-core selects helpers with `kRandomNodes` (util.go): it excludes
    // self, the target, and non-Alive members, then dedups the sample by node
    // Name — the unique node key. A Go node advertises a single address, so its
    // distinct nodes are already distinct addresses. This Sans-I/O port instead
    // admits DISTINCT ids at the SAME address, so the honest identity for helper
    // selection here is the ADDRESS, not the id: the IndirectPing body is
    // `local + target` (independent of the helper id); the relay
    // `handle_indirect_ping` dedups identical in-flight forwards to at most one
    // Nack; and both `handle_nack` and `ack_source_is_valid` key responders by
    // address. Selecting DISTINCT ADDRESSES is therefore what preserves the
    // configured fan-out width — two ids sharing one address must not let the
    // sample collapse a k-helper fan-out to fewer relays — and the address-keyed
    // Nack accounting. Excluding the local / target ADDRESS (not merely their
    // ids) keeps an id that only aliases either endpoint out of the sample: such
    // a "helper" would be a meaningless self-relay to our own or the probed
    // endpoint, and its address-keyed Nack would be miscounted against a peer
    // that by definition cannot answer.
    let mut seen: FxHashSet<A> = FxHashSet::default();
    let mut distinct: SmallVec<A> = SmallVec::new();
    for m in self.members.iter() {
      let st = m.state_ref();
      if st.state() != State::Alive {
        continue;
      }
      let addr = st.address_ref();
      if addr == &local_addr || addr == &target_addr {
        continue;
      }
      // `insert` returns `true` only the first time an address is seen, so each
      // distinct address is pushed once regardless of how many ids advertise it.
      if seen.insert(addr.cheap_clone()) {
        distinct.push(addr.cheap_clone());
      }
    }

    let k = self.cfg.indirect_checks() as usize;
    // Uniform over the distinct addresses: `pick_random` samples up to `k`
    // without replacement, so each distinct address has identical selection
    // probability (O(distinct) sampling atop the O(n) dedup above).
    let chosen_addrs = pick_random(&distinct, k, &mut self.rng);

    // The single cumulative deadline for the whole indirect+fallback
    // race is the probe's stored `failure_deadline` — snapshotted at
    // probe start as `sent + awareness.scale_timeout(probe_interval)`.
    // It is absolute (anchored at `sent`, not the possibly-late `now`),
    // so a very-late `handle_timeout` lands a `deadline <= now`
    // AwaitingIndirect that the next tick expires immediately rather
    // than getting a fresh window when suspicion should be MORE timely.
    // The reliable fallback is threaded this same absolute deadline.
    let cumulative_deadline = self
      .probes
      .get(&seq)
      .map(|p| p.failure_deadline())
      .expect("present");
    if cumulative_deadline <= now {
      // The anchored deadline already elapsed before this (late)
      // handle_timeout reached fan-out. Terminate NOW without emitting
      // any indirect pings or opening the reliable fallback — otherwise a
      // driver could process the queued dial/transmits and a late
      // relayed/fallback Ack would rescue the probe past its failure
      // deadline. For Detection this suspects the target; for Ping it is
      // a silent drop.
      self.probe_terminate_failure(seq, now);
      return;
    }
    // Open the reliable-ping fallback CONCURRENTLY with the indirect
    // fan-out (done UNCONDITIONALLY when enabled, even with zero
    // indirect peers — the fallback is still attempted with
    // `expected_nacks = 0`). The fallback is bounded by the same
    // cumulative deadline. `start_reliable_ping` borrows &mut self, so
    // call it before re-borrowing the probe entry.
    let reliable_stream_id = if self.is_reliable_ping_enabled(&target_id) {
      Some(self.start_reliable_ping(
        target_id.cheap_clone(),
        target_addr.cheap_clone(),
        seq,
        cumulative_deadline,
      ))
    } else {
      None
    };
    // Fan out one IndirectPing per sampled helper address, MTU-checking every
    // datagram. The pool is already address-distinct, so no send-side dedup is
    // needed — each sampled address is a single relay.
    //
    // A node id is unbounded, so an IndirectPing carrying a large already-admitted
    // target id can exceed one datagram even though it is a valid lone message;
    // such a datagram is dropped (the same ceiling the direct Ping enforces)
    // rather than emitted as a fragmentable, undeliverable packet. Both the Nack
    // allowlist (`indirect_peers`) and `expected_nacks` — the count the FSM waits
    // on and the source of the Lifeguard severity `expected_nacks -
    // nacked_by.len()` — count ONLY the addresses whose IndirectPing was actually
    // queued: a peer we never sent to relays no Ping and returns no Nack, so
    // counting it would make the FSM wait for a Nack that never comes and
    // penalize our own health for a probe we never dispatched.
    let mut indirect_peers: SmallVec<A> = SmallVec::new();
    for peer_addr in &chosen_addrs {
      let ind = IndirectPing::new(
        seq,
        Node::new(local_id.cheap_clone(), local_addr.cheap_clone()),
        Node::new(target_id.cheap_clone(), target_addr.cheap_clone()),
      );
      if self.push_probe_packet_within_mtu(peer_addr.cheap_clone(), Message::IndirectPing(ind)) {
        indirect_peers.push(peer_addr.cheap_clone());
      }
    }
    let expected_nacks = indirect_peers.len();

    if expected_nacks == 0 && reliable_stream_id.is_none() {
      // Nothing left to try: no IndirectPing was queued (no eligible peer, or
      // every candidate's datagram exceeded the MTU) AND reliable ping is
      // disabled for this target → suspect now. With reliable ping enabled we
      // instead race the deadline — a 2-node / zero-indirect topology, or one
      // whose indirect datagrams are all over-MTU, still gets the TCP fallback
      // (not bound by `gossip_mtu`) before the target is suspected.
      self.probe_terminate_failure(seq, now);
      return;
    }
    if let Some(probe) = self.probes.get_mut(&seq) {
      // Monotonic dispatch: reaching here means at least one escalation attempt
      // was dispatched — an IndirectPing queued (`expected_nacks > 0`) or the
      // reliable fallback opened (`reliable_stream_id.is_some()`); the
      // no-escalation case returned above. Record it now so a later fallback
      // retirement (which clears `reliable_stream_id`) cannot erase the fact
      // that an attempt was made, and a genuine failure still suspects.
      probe.dispatched = true;
      probe.phase = ProbePhase::AwaitingIndirect(AwaitingIndirect {
        expected_nacks,
        indirect_peers,
        nacked_by: SmallVec::new(),
        reliable_stream_id,
        deadline: cumulative_deadline,
      });
    }
  }

  /// Fire the probe scheduler if its deadline has elapsed.
  /// Calls `start_probe(now)` and reschedules `next_probe = now + probe_interval`.
  fn fire_probe_scheduler(&mut self, now: Instant) {
    let Some(deadline) = self.next_probe else {
      return;
    };
    if now < deadline {
      return;
    }
    // Once per full round-robin pass, prune long-dead members. Without
    // this, with the default `dead_node_reclaim_time == 0`, Dead/Left
    // entries are never collected and a returning id at a new address
    // keeps hitting the conflict path.
    self.probes_since_reset = self.probes_since_reset.saturating_add(1);
    let n = self.num_members();
    if n > 0 && self.probes_since_reset >= n {
      self.reset_nodes(now);
      self.probes_since_reset = 0;
    }
    self.start_probe(now);
    self.next_probe = Some(now + self.cfg.probe_interval());
  }

  /// Fire the gossip scheduler if its deadline has elapsed.
  ///
  /// Drains pending membership broadcasts (via [`BroadcastQueue::take_broadcasts`])
  /// and queued user payloads (via [`Self::drain_user_broadcasts`]), picks up to
  /// `gossip_nodes` random Alive/Suspect peers (excluding the local node), and
  /// emits one datagram per target — a [`Transmit::Packet`] for a single message
  /// or a [`Transmit::Compound`] when several messages share the datagram. The
  /// scheduler deadline is always advanced by `gossip_interval`, even when no
  /// broadcasts are queued.
  ///
  /// Datagrams are sized to the configured gossip MTU (`gossip_mtu`), keeping
  /// gossip packets UDP-safe.
  fn fire_gossip_scheduler(&mut self, now: Instant) {
    let Some(deadline) = self.next_gossip else {
      return;
    };
    if now < deadline {
      return;
    }

    let gossip_interval = self.cfg.gossip_interval();
    let gossip_nodes = self.cfg.gossip_nodes();
    let num_nodes = self.num_members() as u32;
    let dead_window = self.cfg.gossip_to_the_dead_time();

    // Select targets BEFORE draining any queue. Candidates are
    // Alive/Suspect peers AND recently-Dead peers still within
    // `gossip_to_the_dead_time` — the latter is the SWIM "gossip to the
    // dead" path that lets a falsely-dead node hear the accusation and
    // refute before it is garbage-collected. Excluding it (and draining
    // the broadcast queue regardless of whether a target exists) would
    // let false failures stick and silently age out membership broadcasts.
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let candidates: Vec<A> = self
      .members
      .iter()
      .filter(|m| {
        if m.state_ref().id_ref() == &local_id {
          return false;
        }
        match m.state_ref().state() {
          State::Alive | State::Suspect => true,
          State::Dead => now.saturating_duration_since(m.state_ref().state_change()) <= dead_window,
          // `State::Left` is deliberately excluded (the window covers only
          // `Dead`): a node that intentionally left does not need to hear and
          // refute a dead accusation, so unlike the falsely-`Dead` case there is
          // nothing to gossip to it for.
          _ => false,
        }
      })
      .map(|m| m.state_ref().address_ref().cheap_clone())
      .collect();

    let targets = pick_random(&candidates, gossip_nodes, &mut self.rng);

    // No eligible target: leave the broadcast/user queues untouched (do NOT
    // advance retransmit counters with zero packets emitted) and just
    // reschedule.
    if targets.is_empty() {
      self.next_gossip = Some(now + gossip_interval);
      return;
    }

    // The selected membership set is emitted as ONE compound datagram when
    // >= 2, so reserve the compound header (tag + count varint) from the
    // sub-MTU ceiling and charge each message the per-part inner-length
    // varint (conservative u32 upper bounds — never an over-MTU datagram).
    // `bytesAvail = gossip_mtu - compoundHeader` is the compound budget.
    let gossip_mtu = self.gossip_mtu();
    let compound_budget =
      gossip_mtu.saturating_sub(COMPOUND_TAG_LEN + COMPOUND_MAX_COUNT_PREFIX_LEN);

    // Membership top-item preemption (runs BEFORE the compound drain).
    //
    // The membership queue is id-deduplicated only PER node, so a near-MTU
    // `Alive`/`Suspect`/`Dead` for one node coexists with smaller updates for
    // others. The compound drain selects by `(transmits asc, msg_len desc, id
    // desc)` but SKIPS any item too large for a compound part, so under cross-node
    // churn it could keep shipping the smaller items while the near-MTU item is
    // never selected — zero sends, never disseminated. A node's own state MUST
    // disseminate (membership broadcasts have no enqueue gate to reject them, the
    // way oversized user payloads do), so when the first SENDABLE item (the first
    // the drain would pick, skipping any over-MTU head) does NOT fit a compound
    // part (`len + per-part overhead > compound_budget`) yet fits a lone datagram
    // (`len <= gossip_mtu`), lone-send exactly that one as a byte-identical
    // `Packet` and defer user data this tick (SWIM priority — best-effort user
    // gossip cannot starve a membership update; deferred user broadcasts wait, no
    // loss). This test is the exact inverse of "the compound drain can select this
    // top item", capturing precisely the case the compound drain strands.
    //
    // It does NOT guarantee the near-MTU item its full retransmit ceiling: once
    // lone-sent it is at a higher transmit count and correctly yields to fresher
    // (lower-transmit) updates on later ticks (Go's `Less` ordering) — pinning it
    // would invert that freshness invariant and starve fresh updates. Push-pull
    // anti-entropy is the backstop for any gossip-deferred membership state; this
    // only guarantees the item is never PERMANENTLY skipped (zero sends) by
    // smaller coexisting items. `peek_first_sendable_encoded_len` skips over-MTU
    // heads (those disseminate only via push-pull), and `take_one_broadcast(..,
    // gossip_mtu)` below selects that same first sendable item.
    let membership_top_is_near_mtu = self
      .broadcast
      .peek_first_sendable_encoded_len(gossip_mtu)
      .is_some_and(|len| len + COMPOUND_MAX_PART_PREFIX_LEN > compound_budget);

    // SWIM-priority assembly of the gossip datagram:
    //
    // - near-MTU top membership item: lone byte-identical `Packet`, user data
    //   deferred this tick.
    //
    // - otherwise the compound-budget membership drain runs. When it selects
    //   >= 1, user data fills the residual of the SAME compound_budget (membership
    //   and user broadcasts share one bytes_avail) so the compound stays within
    //   ONE MTU; `membership_used` is exactly what `take_broadcasts` charged, so
    //   the selected messages are NOT re-encoded. When it selects nothing, user
    //   data owns the full compound_budget. Every queued user payload passed the
    //   compound-part enqueue gate, so each selected payload fits; >= 2 assemble
    //   as a compound, exactly 1 a plain frame.
    let all_broadcasts: Vec<Message<I, A>> = if membership_top_is_near_mtu {
      match self.broadcast.take_one_broadcast(num_nodes, gossip_mtu) {
        // Lone byte-identical Packet; user broadcasts are intentionally NOT
        // drained this tick (SWIM priority).
        Some(m) => vec![m],
        // The peek reported a sendable near-MTU item, so this re-selection under
        // the same gossip_mtu limit always succeeds; the None arm is unreachable,
        // falling back to an empty datagram rather than panicking.
        None => Vec::new(),
      }
    } else {
      let (membership_broadcasts, membership_used) = self.broadcast.take_broadcasts_measured(
        num_nodes,
        COMPOUND_MAX_PART_PREFIX_LEN,
        compound_budget,
      );
      if !membership_broadcasts.is_empty() {
        // take_broadcasts guarantees membership_used <= compound_budget;
        // saturating_sub defends a contract break into an empty user budget
        // rather than a wrapped (usize::MAX) one.
        let user_budget = compound_budget.saturating_sub(membership_used);
        let mut v = membership_broadcasts;
        // The user drain charges and reports its own bytes; only the messages
        // are needed here (this tick's datagram is already sized by the budget).
        let (user_msgs, _user_used) = self.drain_user_broadcasts(num_nodes, user_budget);
        v.extend(user_msgs.into_iter().map(Message::UserData));
        v
      } else {
        let (user_msgs, _used) = self.drain_user_broadcasts(num_nodes, compound_budget);
        user_msgs.into_iter().map(Message::UserData).collect()
      }
    };

    // One datagram per target by the >= 2 rule: a single message stays a
    // byte-identical plain frame, >= 2 ride ONE compound datagram (the
    // budget above guarantees it fits the MTU).
    for to in targets {
      match all_broadcasts.len() {
        0 => {}
        1 => self
          .pending_transmits
          .push_back(Transmit::Packet(PacketTransmit::new(
            to.cheap_clone(),
            all_broadcasts[0].clone(),
          ))),
        _ => self
          .pending_transmits
          .push_back(Transmit::Compound(CompoundTransmit::new(
            to.cheap_clone(),
            all_broadcasts.iter().cloned().collect::<TinyVec<_>>(),
          ))),
      }
    }

    // Always reschedule, even when nothing was emitted.
    self.next_gossip = Some(now + gossip_interval);
  }

  /// Fire the push/pull scheduler if its deadline has elapsed.
  ///
  /// Picks one random Alive/Suspect peer (excluding the local node) and
  /// initiates a push/pull exchange via `start_push_pull`, which emits
  /// `Event::DialRequested` for the driver. Reschedules `next_pushpull`
  /// using `push_pull_scale` to account for cluster growth.
  fn fire_pushpull_scheduler(&mut self, now: Instant) {
    let Some(deadline) = self.next_pushpull else {
      return;
    };
    if now < deadline {
      return;
    }

    let pp_interval = self.cfg.push_pull_interval();

    // Count live members (excluding self) for scale factor.
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let candidates: Vec<A> = self
      .members
      .iter()
      .filter(|m| {
        let state = m.state_ref().state();
        let is_live = state == State::Alive || state == State::Suspect;
        let is_remote = m.state_ref().id_ref() != &local_id;
        is_live && is_remote
      })
      .map(|m| m.state_ref().address_ref().cheap_clone())
      .collect();

    let num_live = candidates.len();

    if !candidates.is_empty() {
      // Pick exactly one random peer.
      let chosen = pick_random(&candidates, 1, &mut self.rng);
      if let Some(peer_addr) = chosen.into_iter().next() {
        self.start_push_pull(peer_addr, PushPullKind::Refresh, now);
      }
    }

    // Scale next interval by cluster size. Always reschedule even when no
    // peer was picked so that a growing cluster will start syncing as soon
    // as a peer appears.
    let scaled = push_pull_scale(pp_interval, num_live);
    self.next_pushpull = Some(now + scaled);
  }

  /// Dispatch a decoded incoming datagram to the appropriate typed handler.
  ///
  /// Drivers call this from their UDP RX loop after the codec has unwrapped
  /// the outer label/encryption/checksum/compression layers and parsed a
  /// single `Message<I, A>`. Avoids each driver hand-matching every variant.
  ///
  /// `PushPull` and `ErrorResponse` only arrive over the reliable stream
  /// transport, not over datagrams; this method drops them silently if a
  /// driver hands one in by accident — the driver should route those through
  /// `accept_stream` + `Stream::handle_data` instead.
  pub fn handle_packet(&mut self, from: A, msg: Message<I, A>, now: Instant) {
    // A Leaving/Left node processes no gossip-plane inbound: it does not reply to
    // a Ping (which would advertise liveness against its own leave), forward an
    // IndirectPing, or admit, merge, or suspect from gossip. The graceful-leave
    // drain runs entirely on the dead-self flush and in-flight stream closes; the
    // reliable plane is gated separately in handle_stream_event.
    if self.lifecycle != Lifecycle::Running {
      return;
    }
    match msg {
      Message::Ping(p) => self.handle_ping(from, p, now),
      Message::IndirectPing(ip) => self.handle_indirect_ping(from, ip, now),
      Message::Ack(a) => self.handle_ack(from, a, now),
      Message::Nack(n) => self.handle_nack(from, n, now),
      Message::Suspect(s) => self.handle_suspect(from, s, now),
      Message::Alive(a) => self.handle_alive(from, a, now),
      Message::Dead(d) => self.handle_dead(from, d, now),
      Message::UserData(data) => self.handle_user_data(from, data, Reliability::Unreliable),
      // Stream-layer messages — drivers should route via accept_stream.
      Message::PushPull(_) | Message::ErrorResponse(_) => {}
    }
  }

  /// Remove `Dead` and `Left` members whose `state_change` is older than
  /// `gossip_to_the_dead_time` (legacy `Memberlist::reset_nodes`).
  ///
  /// In the legacy implementation `gossip_to_the_dead_time` acts as the
  /// *dead-node reclaim* window — nodes that have been Dead/Left for longer
  /// than this interval are pruned from the member list.  Calling this after
  /// advancing simulated time is sufficient to trigger the GC without waiting
  /// for a real scheduler.
  ///
  /// The local node itself is never removed.
  pub fn reset_nodes(&mut self, now: Instant) {
    let local_id = self.cfg.local_id_ref().cheap_clone();
    let window = self.cfg.gossip_to_the_dead_time();
    let ids_to_remove: Vec<I> = self
      .members
      .iter()
      .filter(|m| {
        let id = m.state_ref().id_ref();
        if id == &local_id {
          return false;
        }
        let state = m.state_ref().state();
        if state != State::Dead && state != State::Left {
          return false;
        }
        // Saturating: an out-of-order `now` (earlier than the member's
        // state_change — a stale timer tick, a virtual-clock domain, or a
        // direct `reset_nodes` call) yields zero elapsed and simply does not
        // reclaim yet. The machine is correct under any input ordering and
        // must not panic here.
        now.saturating_duration_since(m.state_ref().state_change()) > window
      })
      .map(|m| m.state_ref().id_ref().cheap_clone())
      .collect();
    if !ids_to_remove.is_empty() {
      // Removing reclaimed Dead/Left members shrinks the membership list (the
      // snapshot) but emits no event, so bump the version explicitly.
      self.bump_snapshot_version();
    }
    for id in ids_to_remove {
      self.members.remove(&id);
    }
    // Decorrelate probe order across rounds: without a reshuffle the round-robin
    // cursor walks the members in a fixed insertion-derived order every pass, so
    // probe patterns are predictable and correlated. Reshuffle the post-prune
    // list and restart the cursor (mirrors upstream `resetNodes`).
    self.members.shuffle(&mut self.rng);
    self.probe_index = 0;
  }

  /// Route an [`EndpointEvent`] produced by a
  /// [`Stream`] back into the Endpoint.
  ///
  /// - `PushPullReplyReceived`: applies the inbound merge inline (synchronous
  ///   `MergeDelegate` filter on every push/pull). A rejected merge returns
  ///   `Some(StreamCommand::Close)` to fail the exchange; otherwise returns
  ///   `None` (we sent our state before the peer replied).
  /// - `PushPullRequestReceived`: applies the same inline merge filter. A
  ///   rejected merge returns `Some(StreamCommand::Close)`; otherwise
  ///   returns `Some(StreamCommand::SendPushPullResponse)` so the driver can
  ///   encode and load the inbound stream's response payload.
  /// - `ReliablePingAcked` / `ReliablePingFailed`: drives the probe FSM via
  ///   [`handle_reliable_ping_response`].
  /// - `UserDataReceived`: emits [`Event::UserPacket`] with `Reliable` reliability.
  /// - `StreamClosed` / `StreamErrored`: silently ignored at this layer (the
  ///   driver manages stream lifetime).
  ///
  /// [`handle_reliable_ping_response`]: Endpoint::handle_reliable_ping_response
  pub fn handle_stream_event(
    &mut self,
    ev: EndpointEvent<I, A>,
    now: Instant,
  ) -> Option<StreamCommand<I, A>> {
    match ev {
      EndpointEvent::PushPullReplyReceived(p) => {
        // Outbound: we initiated; peer replied. Consult the MergeDelegate
        // inline on every push/pull (synchronous filter). A rejected merge
        // terminalizes this exchange via `StreamCommand::Close`: the bridge
        // fails with `AdmissionClosed`, so the synchronous join counts the
        // seed as NOT contacted. The membership merge needs only `states`;
        // `peer` and `user_data` are forwarded to the application via
        // `Event::RemoteStateReceived` once the merge is admitted.
        // The producing stream's id is the token a driver correlates this merge
        // with the `start_push_pull` it initiated.
        let originating_stream_id = p.stream_id();
        let (peer, states, user_data, kind) = p.into_parts();
        // A Leaving/Left node completes the in-flight exchange — the stream
        // closes — but merges no remote membership or application state: the
        // drain must not re-establish the membership the node is leaving, and a
        // departing application should not observe a peer's state snapshot. We
        // sent our own state when we initiated, so nothing remains to do here.
        if self.lifecycle != Lifecycle::Running {
          return None;
        }
        if !self.merge_admitted(&states) {
          return Some(StreamCommand::Close);
        }
        self.merge_state(&states, now);
        // Forward the peer's application-state snapshot only after the merge
        // is admitted — a rejected filter must not leak the peer's state.
        if !user_data.is_empty() {
          self.emit_event(Event::RemoteStateReceived(RemoteStateReceived::new(
            peer,
            user_data,
            kind.is_join(),
            originating_stream_id,
          )));
        }
        None
      }
      EndpointEvent::PushPullRequestReceived(p) => {
        // Inbound: peer initiated. Consult the MergeDelegate inline on every
        // push/pull. A rejected merge closes the stream — a rejected
        // `NotifyMerge` aborts the push/pull connection. Otherwise apply
        // the merge and reply with our state. The membership merge does not
        // consult `peer` or `user_data` at the FSM layer; once the merge is
        // admitted, a non-empty `user_data` is forwarded to the application
        // via `Event::RemoteStateReceived`, stamped with the accepted stream's id.
        let originating_stream_id = p.stream_id();
        let (peer, states, user_data, kind) = p.into_parts();
        // A Leaving/Left node sends no push/pull response: replying would start
        // new reliable I/O and leak our local membership/state snapshot during
        // the drain. Close the stream instead — the peer learns of the leave via
        // the dead-self gossip, and the drain merges nothing.
        if self.lifecycle != Lifecycle::Running {
          return Some(StreamCommand::Close);
        }
        if !self.merge_admitted(&states) {
          return Some(StreamCommand::Close);
        }
        self.merge_state(&states, now);
        if !user_data.is_empty() {
          self.emit_event(Event::RemoteStateReceived(RemoteStateReceived::new(
            peer,
            user_data,
            kind.is_join(),
            originating_stream_id,
          )));
        }
        let local_states: Vec<PushNodeState<I, A>> = self
          .members
          .iter()
          .map(|m| {
            let ls = m.state_ref();
            let ns = ls.server_ref();
            // Live liveness (`ls.state()`), not the frozen server snapshot
            // (`ns.state()`) — see start_push_pull for the rationale.
            PushNodeState::new(
              ls.incarnation(),
              ns.id_ref().cheap_clone(),
              ns.address_ref().cheap_clone(),
              ls.state(),
            )
            .with_meta(ns.meta_ref().cheap_clone())
            .with_protocol_version(ns.protocol_version())
            .with_delegate_version(ns.delegate_version())
          })
          .collect();
        Some(StreamCommand::SendPushPullResponse(
          SendPushPullResponse::new(local_states, self.local_state_snapshot.clone()),
        ))
      }
      // A Leaving/Left node processes no reliable-plane inbound: it completes no
      // reliable-ping probe (no awareness mutation) and delivers no user data to
      // a departing application. The stream still closes via its own lifecycle
      // (StreamClosed/StreamErrored below), so the drain is unaffected.
      EndpointEvent::ReliablePingAcked(p) => {
        if self.lifecycle == Lifecycle::Running {
          self.handle_reliable_ping_response(EndpointEvent::ReliablePingAcked(p), now);
        }
        None
      }
      EndpointEvent::ReliablePingFailed(p) => {
        if self.lifecycle == Lifecycle::Running {
          self.handle_reliable_ping_response(EndpointEvent::ReliablePingFailed(p), now);
        }
        None
      }
      EndpointEvent::UserDataReceived(p) => {
        if self.lifecycle == Lifecycle::Running {
          let (peer, data) = p.into_parts();
          self.emit_event(Event::UserPacket(UserPacket::new(
            peer,
            data,
            Reliability::Reliable,
          )));
        }
        None
      }
      EndpointEvent::StreamClosed(_) | EndpointEvent::StreamErrored(_) => None,
    }
  }

  /// Activate the periodic schedulers. Call this once after the node has
  /// completed its initial join. The first probe, gossip, and push/pull
  /// fires will occur after a random stagger within their respective
  /// intervals, preventing thundering-herd when many nodes start together.
  ///
  /// Subsequent ticks use the configured interval directly (no jitter).
  ///
  /// No-op if called when `lifecycle != Running` or if the relevant
  /// interval is `Duration::ZERO` (scheduler remains disabled).
  ///
  /// Idempotent: calling a second time resets the deadlines.
  pub fn start_scheduling(&mut self, now: Instant) {
    if self.lifecycle != Lifecycle::Running {
      return;
    }
    let probe_interval = self.cfg.probe_interval();
    if probe_interval > Duration::ZERO {
      let stagger = random_stagger(probe_interval, &mut self.rng);
      self.next_probe = Some(now + stagger);
    }
    let gossip_interval = self.cfg.gossip_interval();
    if gossip_interval > Duration::ZERO && self.cfg.gossip_nodes() > 0 {
      let stagger = random_stagger(gossip_interval, &mut self.rng);
      self.next_gossip = Some(now + stagger);
    }
    let pp_interval = self.cfg.push_pull_interval();
    if pp_interval > Duration::ZERO {
      let stagger = random_stagger(pp_interval, &mut self.rng);
      self.next_pushpull = Some(now + stagger);
    }
  }
}

/// Pick up to `k` items uniformly at random from `pool` using `rng`.
fn pick_random<T, R>(pool: &[T], k: usize, rng: &mut R) -> SmallVec<T>
where
  T: Clone,
  R: Rng,
{
  pool
    .iter()
    .sample(rng, k)
    .iter()
    .map(|val| (*val).clone())
    .collect()
}

/// Scale the push/pull interval as the cluster grows.
///
/// Below 32 nodes the interval is unchanged. Above 32 nodes the multiplier
/// is `ceil(log2(n) - log2(32)) + 1` so the 33rd node doubles the
/// interval, the 65th triples it, etc.
pub(crate) fn push_pull_scale(interval: Duration, n: usize) -> Duration {
  const THRESHOLD: usize = 32;
  if n <= THRESHOLD {
    return interval;
  }
  let multiplier =
    crate::mathf::ceil(crate::mathf::log2(n as f64) - crate::mathf::log2(THRESHOLD as f64)) as u32
      + 1;
  // `checked_mul` to degrade a pathologically-large configured product to the
  // unscaled `interval` instead of panicking on `Duration * u32` overflow.
  interval.checked_mul(multiplier).unwrap_or(interval)
}

/// Return a random duration uniformly in `[0, interval)`.
/// Used to stagger the first scheduler tick to avoid thundering-herd
/// at cluster formation time.
///
/// Uses the Endpoint's injected gossip RNG (passed in), so the result is
/// deterministic for a seeded RNG (useful in tests).
fn random_stagger<R>(interval: Duration, rng: &mut R) -> Duration
where
  R: Rng,
{
  use rand::RngExt;
  let nanos = interval.as_nanos() as u64;
  if nanos == 0 {
    return Duration::ZERO;
  }
  Duration::from_nanos(rng.random_range(0..nanos))
}

/// A fresh, deterministically-stepped gossip `SmallRng` for tests, so the test
/// suite can construct an endpoint without threading an RNG. Distinct per call
/// (a golden-ratio-stepped counter seed) so sibling endpoints in one test never
/// share a sequence.
#[cfg(test)]
fn test_seeded_rng() -> SmallRng {
  use core::sync::atomic::{AtomicU64, Ordering};

  use rand::SeedableRng;
  static COUNTER: AtomicU64 = AtomicU64::new(0xA5A5_A5A5_A5A5_A5A5);
  SmallRng::seed_from_u64(COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed))
}

/// Test-only seeded constructors mirroring the real ones but supplying
/// [`test_seeded_rng`], so the test suite need not thread a gossip RNG.
#[cfg(test)]
impl<I, A> Endpoint<I, A, SmallRng>
where
  I: Id,
  A: CheapClone + Data + PartialEq + 'static,
{
  #[cfg(feature = "std")]
  pub(crate) fn new_seeded(cfg: EndpointOptions<I, A>) -> Self {
    Self::new(cfg, test_seeded_rng())
  }

  pub(crate) fn new_at_seeded(cfg: EndpointOptions<I, A>, now: Instant) -> Self {
    Self::new_at(cfg, now, test_seeded_rng())
  }

  #[cfg(feature = "std")]
  pub(crate) fn try_new_seeded(cfg: EndpointOptions<I, A>) -> Result<Self, EndpointInitError> {
    Self::try_new(cfg, test_seeded_rng())
  }

  pub(crate) fn try_new_at_seeded(
    cfg: EndpointOptions<I, A>,
    now: Instant,
  ) -> Result<Self, EndpointInitError> {
    Self::try_new_at(cfg, now, test_seeded_rng())
  }
}
