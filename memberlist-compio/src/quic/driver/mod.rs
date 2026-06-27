//! QUIC driver task — single-owner of the [`QuicEndpoint`], the
//! UDP socket, and the synchronous-join `pending_joins` table.
//!
//! State is packed into [`QuicDriverState`] so individual fields can
//! be borrowed through the loop without fighting Rust's borrow
//! checker against a sprawling let-binding cluster.
//!
//! The loop runs a `select_biased` over three arms in priority order:
//! gossip UDP recv (kernel-buffered datagrams go to
//! [`QuicEndpoint::handle_udp`]), a coordinator-supplied wake timer
//! ([`QuicEndpoint::poll_timeout`] with an idle-wake fallback), and
//! the command channel. The cmd arm sits LAST so a continuous network
//! flood does not starve `Shutdown`; an iter-top `try_recv` drain
//! pulls up to `cmd_fairness_budget` commands before re-entering the
//! select, bounding shutdown latency under a pure network flood.

#![cfg(feature = "quic")]

use std::{
  cell::Cell,
  collections::{HashMap, HashSet},
  io,
  net::SocketAddr,
  rc::Rc,
  sync::Arc,
};

use crate::QuicEndpoint;
use bytes::Bytes;
use compio::{buf::BufResult, net::UdpSocket};
use flume::{Receiver, Sender};

use futures_util::{FutureExt, pin_mut, select_biased};
use lochan::mpsc;
use memberlist_proto::{
  DatagramSendStatus, Instant, Node, UnreliableTransport,
  codec::{
    DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
    parse_messages,
  },
  event::{Event, ExchangeKind, ExchangeStatus, PushPullKind, Transmit},
  typed::{NodeState, State},
};

#[cfg(checksum)]
use crate::command::SetChecksumOptionsCmd;
#[cfg(compression)]
use crate::command::SetCompressionOptionsCmd;
#[cfg(encryption)]
use crate::command::SetEncryptionOptionsCmd;
use crate::{
  command::{Command, JoinCmd, JoinKind, LeaveCmd, ShutdownCmd, UpdateNodeMetadataCmd},
  delegate::Delegate,
  driver::{
    options::RuntimeOptions,
    shared::{ExchangeId, cidr_blocks, dispatch_event_delegate},
  },
  error::{JoinAllFailed, MemberlistError, Result},
  snapshot::{MemberlistSnapshot, SnapshotCell},
  transport::runtime::CidrFilter,
};
use core::time::Duration;
use memberlist_proto::metrics::Metrics;

/// Hard ceiling on the per-recv UDP buffer. UDP's wire payload is
/// capped at 65507 bytes (the IP-layer maximum once the 8-byte UDP
/// header and 20-byte IPv4 header are deducted); a recv buffer larger
/// than that wastes an allocation per iteration. Mirrors the stream
/// driver's `GOSSIP_RECV_BUF_MAX`.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

/// The largest the encrypted wrapper can inflate a gossip datagram, or `0` when
/// no encryption backend is built in. The proto const exists only under an
/// encryption backend; with none the gossip frame is unencrypted, so the wrapper
/// adds nothing to the recv-buffer sizing.
#[cfg(encryption)]
const ENCRYPTED_WRAPPER_OVERHEAD: usize = memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD;
#[cfg(not(encryption))]
const ENCRYPTED_WRAPPER_OVERHEAD: usize = 0;

/// The largest the checksum wrapper can inflate a gossip datagram, or `0` when
/// no checksum backend is built in. The proto const exists only under a checksum
/// backend; with none the gossip frame carries no checksum, so the wrapper adds
/// nothing to the recv-buffer sizing.
#[cfg(checksum)]
const CHECKSUMED_WRAPPER_OVERHEAD: usize = memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD;
#[cfg(not(checksum))]
const CHECKSUMED_WRAPPER_OVERHEAD: usize = 0;

/// Driver-side state for one outstanding synchronous-join call.
///
/// Mirrors the stream driver's `PendingJoin` (`memberlist-compio/src/driver.rs`).
/// Tracks the set of dispatched outbound `ExchangeId`s waiting on a
/// terminal `Event::ExchangeCompleted` filtered to
/// `ExchangeKind::PushPull`. When the set empties (or the deadline
/// elapses), the driver replies on `reply` with either the success
/// count or `JoinAllFailed`.
struct PendingJoin {
  /// Set of outbound exchange IDs this waiter dispatched and is still
  /// waiting on. An `ExchangeId` is removed when its push/pull
  /// `ExchangeCompleted` arrives; when empty, the call has fully
  /// resolved.
  pending: HashSet<ExchangeId>,
  /// Number of dispatched outbound exchanges that terminated with
  /// `ExchangeStatus::Succeeded`. Duplicate seeds produce duplicate
  /// exchanges and each successful one counts independently.
  contacted: usize,
  /// Total outbound-exchange count this call dispatched, used to
  /// populate `JoinAllFailed { requested, contacted: 0 }` on a
  /// zero-contact resolution.
  requested: usize,
  /// Wall-clock instant past which the driver replies with whatever
  /// `contacted` it has accumulated even if `pending` is non-empty.
  deadline: Instant,
  /// One-shot reply channel back to the caller.
  reply: futures_channel::oneshot::Sender<Result<usize>>,
}

/// Driver-side state for the single in-flight graceful-leave operation.
///
/// Mirrors the stream driver's `PendingLeave`
/// (`memberlist-compio/src/driver.rs`). A [`Command::Leave`] that finds
/// the endpoint Running initiates the machine's `leave()`, which queues
/// the direct `Dead`-self notices to every live peer and withholds
/// [`Event::LeftCluster`] until they drain through `poll_transmit`. The
/// driver parks this here and replies only once that `LeftCluster`
/// arrives (success) or `deadline` elapses
/// ([`MemberlistError::LeaveTimeout`]) — so a returned `Ok(())` means
/// the leave actually reached the wire, never merely that it was queued.
///
/// Leave is a SHARED operation: a second `Command::Leave` racing an
/// in-flight one (cloned `Memberlist` handles can both call `leave()`)
/// does NOT re-invoke `endpoint.leave()` (a repeated leave once already
/// `Leaving`/`Left` is a terminal no-op that emits no completion event,
/// so a fresh parked waiter would hang waiting on a `LeftCluster` that
/// never re-fires); it joins this in-flight operation by pushing its
/// reply onto `repliers`. Every terminal path drains EVERY replier.
struct PendingLeave {
  /// Reply channels of every `leave()` caller that joined this in-flight
  /// leave — the initiator plus any racing clones. Drained together on
  /// the single terminal outcome (`Ok` on `LeftCluster`, `LeaveTimeout`
  /// on deadline, `Shutdown` on teardown).
  repliers: Vec<futures_channel::oneshot::Sender<Result<()>>>,
  /// Wall-clock instant past which the driver replies
  /// [`MemberlistError::LeaveTimeout`] to every replier even if
  /// `LeftCluster` has not yet fired.
  deadline: Instant,
}

impl PendingLeave {
  /// Reply to every joined `leave()` caller with a fresh `Result<()>`
  /// from `make_result`. The single terminal outcome fans out to the
  /// initiator and any racing clones. A constructor closure (rather than
  /// a single cloned value) sidesteps `MemberlistError` not being
  /// `Clone` — every terminal outcome here (`Ok(())`,
  /// [`MemberlistError::LeaveTimeout`], [`MemberlistError::Shutdown`])
  /// is a trivially reconstructible unit/variant value.
  async fn resolve_all(self, mut make_result: impl FnMut() -> Result<()>) {
    for replier in self.repliers {
      // Ignoring Err: a `leave()` caller dropped its reply receiver
      // (its user-facing future was cancelled); nothing to surface.
      let _ = replier.send(make_result());
    }
  }
}

/// Driver-side state for one outstanding application-ping call.
///
/// Parked by the `Command::Ping` dispatch arm when the endpoint is running.
/// Resolved on `Event::PingCompleted` (reply `Ok(rtt)`) or
/// `Event::PingFailed` (reply `Err(PingTimeout)`) — both carry the matching
/// `PingId` correlation token. On driver exit, drained with
/// `Err(MemberlistError::Shutdown)`.
struct PendingPing {
  /// Correlation token minted by `endpoint.ping(…)`.
  ping_id: memberlist_proto::PingId,
  /// One-shot reply channel back to the `ping()` caller.
  reply: futures_channel::oneshot::Sender<Result<Duration>>,
}

/// Driver-side state for one outstanding reliable directed-send call.
///
/// A single `Command::SendReliable` may dispatch multiple
/// `start_user_message` calls (one per payload). Each call returns a
/// `StreamId`; the driver parks one `PendingUserSend` whose `pending` set
/// tracks the `ExchangeId`s of the in-flight QUIC streams. Resolved on
/// `Event::ExchangeCompleted` where `kind() == ExchangeKind::UserMessage`:
/// each completed `ExchangeId` is removed from `pending`; when `pending`
/// empties the reply fires. On driver exit, drained with
/// `Err(MemberlistError::Shutdown)`.
struct PendingUserSend {
  /// Set of `ExchangeId`s this call dispatched and has not yet seen a
  /// terminal `ExchangeCompleted` for. An entry is removed on any terminal
  /// outcome (success or failure). When this set empties the call resolves.
  pending: HashSet<ExchangeId>,
  /// Running count of exchange failures for this call (outcome `Failed`).
  failed: usize,
  /// One-shot reply channel back to the `send_reliable` / `send_many_reliable`
  /// caller. `Ok(())` if every exchange succeeded; `Err(SendFailed)` if any
  /// failed.
  reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// All driver-owned state, packed so the loop body can borrow
/// individual fields without fighting Rust's borrow checker
/// against a sprawling let-binding cluster.
struct QuicDriverState<I, G = rand::rngs::StdRng> {
  endpoint: QuicEndpoint<I, G>,
  udp_socket: UdpSocket,
  commands: Receiver<Command<I>>,
  /// Cluster label threaded into the gossip `EncodeOptions` / `DecodeOptions` so
  /// outbound gossip is stamped and inbound gossip is verified against the same
  /// label.
  label: Option<bytes::Bytes>,
  /// Hand-off channel to the per-driver observation task (delegate
  /// dispatch + EventStream forward), sized per
  /// [`RuntimeOptions::observation_channel`]. The driver `try_send`s every
  /// surfaced event here, never blocking on user observation code. An
  /// `Unbounded` channel never drops; a `Bounded(n)` channel drops the
  /// newest event when full and counts it in `observation_dropped`.
  obs_tx: mpsc::Sender<Event<I, SocketAddr>>,
  /// Observation-channel drop counter: the driver loop increments it on a
  /// `Bounded` obs-channel drop (the surfacing `try_send` below) when the
  /// delegate falls behind — by count or the payload byte backstop. A drop here
  /// means the delegate (and EventStream) missed the event; for app-data it is
  /// unrecoverable. (The observation task's separate EventStream-forward drops
  /// increment `events_dropped`.)
  observation_dropped: Rc<Cell<u64>>,
  /// Bytes of payload-bearing events (`UserPacket` / `RemoteStateReceived`)
  /// currently queued in `obs_tx`: the driver adds on enqueue, the observation
  /// task subtracts on dequeue. Bounds the memory large reliable payloads
  /// occupy under a backed-up delegate (the count cap alone cannot).
  obs_payload_bytes: Rc<Cell<u64>>,
  /// Byte backstop budget for queued payload-bearing events: `Some(4 *
  /// max_stream_frame_size)` on a `Bounded` channel, `None` on `Unbounded`
  /// (which opts out of dropping).
  obs_payload_budget: Option<u64>,
  snapshot: SnapshotCell<I>,
  /// The endpoint snapshot version last stored into `snapshot`; the snapshot is
  /// rebuilt only when it differs (a rebuild clones every NodeState).
  last_snapshot_version: u64,
  metrics: Rc<Cell<Metrics>>,
  /// The load-shedding counters last stored into `metrics` (published on change).
  last_metrics: Metrics,
  shutdown_flag: Rc<Cell<bool>>,
  driver_opts: RuntimeOptions,
  /// Driver-level CIDR transport-source filter: a UDP packet (QUIC handshake or
  /// gossip datagram) from a blocked source IP is dropped before the QUIC machine
  /// sees it, so a blocked peer completes no handshake. `()` without the `cidr`
  /// feature. The advertised-address filter is the composed alive delegate.
  cidr_policy: CidrFilter,
  /// Outstanding synchronous-join waiters. Populated when a
  /// `Command::Join` with `JoinKind::WaitForCompletion` lands;
  /// reduced on `Event::ExchangeCompleted` filtered to
  /// `ExchangeKind::PushPull`; reaped on deadline, completion, or
  /// shutdown.
  pending_joins: HashMap<u64, PendingJoin>,
  /// Monotonic id source for `pending_joins` keys. Wraps via
  /// `wrapping_add` — collisions across the wrap point are not a
  /// concern (the map holds at most a few hundred entries at any
  /// time).
  next_pending_join_id: u64,
  /// Stash for the `Command::Shutdown` reply sender — drained in
  /// the post-loop cleanup so the ack lands AFTER the UDP socket
  /// drops and the bound port is free.
  shutdown_reply: Option<futures_channel::oneshot::Sender<Result<()>>>,
  /// Outstanding graceful-leave waiter. Parked by `dispatch_command`
  /// when a `Command::Leave` initiates the machine's `leave()` (the
  /// endpoint was Running); resolved `Ok` in `drain_actions`' events
  /// step on `Event::LeftCluster`; reaped `MemberlistError::LeaveTimeout`
  /// by `reap_pending_leave` once its deadline elapses. At most one is
  /// outstanding at a time.
  pending_leave: Option<PendingLeave>,
  /// Outstanding application-ping waiters. Parked by `dispatch_command`
  /// on `Command::Ping` when the endpoint is running; resolved in
  /// `drain_actions`' event step on `Event::PingCompleted` /
  /// `Event::PingFailed`; drained `Err(Shutdown)` on driver exit.
  pending_pings: Vec<PendingPing>,
  /// Outstanding reliable directed-send waiters. Parked by `dispatch_command`
  /// on `Command::SendReliable` when the endpoint is running; resolved in
  /// `drain_actions`' event step on `Event::ExchangeCompleted` filtered to
  /// `ExchangeKind::UserMessage`; drained `Err(Shutdown)` on driver exit.
  pending_user_sends: Vec<PendingUserSend>,
}

/// Compute the per-recv UDP buffer size from the coordinator's
/// configured `gossip_mtu`. A datagram on the wire is at most
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD`
/// bytes when checksum and encryption are enabled (the encryption wrapper
/// carries the algorithm tag + nonce + AEAD auth tag, and the checksum
/// wrapper its tag + digest); the driver sizes the recv buffer to that value so a
/// configured `with_gossip_mtu` above the historical 16 KiB default is
/// not silently truncated by the kernel. Mirrors the stream driver's
/// `gossip_recv_buf_len`.
fn gossip_recv_buf_len<I, G>(endpoint: &QuicEndpoint<I, G>) -> usize
where
  I: memberlist_proto::Id,
{
  endpoint
    .gossip_mtu()
    .saturating_add(ENCRYPTED_WRAPPER_OVERHEAD)
    .saturating_add(CHECKSUMED_WRAPPER_OVERHEAD)
    .min(GOSSIP_RECV_BUF_MAX)
}

/// Single-owner QUIC driver task.
///
/// Drives the [`QuicEndpoint`] until the command channel closes (all
/// `Memberlist` handles dropped) or a `Command::Shutdown` is received.
/// All mutations on the endpoint happen here; reads happen via the published
/// [`MemberlistSnapshot`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn quic_driver_loop<I, D, G>(
  endpoint: QuicEndpoint<I, G>,
  udp_socket: UdpSocket,
  commands: Receiver<Command<I>>,
  events_tx: Sender<Event<I, SocketAddr>>,
  events_dropped: Rc<Cell<u64>>,
  observation_dropped: Rc<Cell<u64>>,
  snapshot: SnapshotCell<I>,
  metrics: Rc<Cell<Metrics>>,
  shutdown_flag: Rc<Cell<bool>>,
  driver_opts: RuntimeOptions,
  delegate: D,
  label: Option<bytes::Bytes>,
  cidr_policy: CidrFilter,
) where
  D: Delegate<Id = I, Address = SocketAddr>,
  I: memberlist_proto::Id,
  G: rand::Rng + Unpin,
{
  // Spawn the per-driver observation task. It owns the user `Delegate`
  // and the `EventStream` sender and runs OFF this driver task: the
  // driver `try_send`s every surfaced event onto `obs_tx`, the task
  // dispatches the matching observation hook then forwards to
  // subscribers. Decoupling observation from the driver loop is what
  // keeps a slow `notify_*` / `merge_remote_state` from stalling
  // protocol advancement — and therefore from delaying a parked
  // join/leave reply that depends on a follow-up inbound datagram the
  // driver's recv arm must still service. The `obs_tx` queue is sized per
  // [`RuntimeOptions::observation_channel`] (default [`Channel::Bounded`]).
  // The `Delegate` carries the two application-data hooks (`notify_user_msg`
  // from `UserPacket`, `merge_remote_state` from `RemoteStateReceived`) whose
  // payloads are absent from `MemberlistSnapshot` and thus unrecoverable from
  // `members()`. `Unbounded` never drops — the delegate observes EVERY event
  // in order — but a persistently stuck handler grows memory (the handler's
  // contract is to keep up). `Bounded(n)` caps that growth: a full channel
  // drops the newest event (blocking would stall SWIM) and counts it in
  // `observation_dropped` so the drop is observable, not silent. Either way the
  // hand-off (`try_send`) is non-blocking, so a slow hook never stalls SWIM.
  // The EventStream (`events_tx`) stays bounded best-effort.
  let (obs_tx, obs_rx) = match driver_opts.observation_channel() {
    crate::Channel::Unbounded => mpsc::unbounded::<Event<I, SocketAddr>>(),
    crate::Channel::Bounded(n) => mpsc::bounded::<Event<I, SocketAddr>>(n),
  };
  // The two drop counters are kept distinct: the observation task increments
  // `events_dropped` on a bounded EventStream-forward drop (membership/control,
  // recoverable from the snapshot), and the driver loop increments
  // `observation_dropped` on a `Bounded` obs-channel drop (the delegate fell
  // behind; may have lost app-data). Clone `events_dropped` for the task; the
  // driver keeps `observation_dropped` in `QuicDriverState` for the surfacing
  // `try_send`.
  //
  // `obs_payload_bytes` tracks the bytes of payload-bearing events queued in
  // `obs_tx` (driver adds on enqueue, observation task subtracts on dequeue),
  // backing the byte backstop on the surfacing path; the byte budget caps
  // queued payload bytes at four frames' worth on a `Bounded` channel, while
  // `Unbounded` opts out of dropping (budget `None`).
  let obs_payload_bytes = Rc::new(Cell::new(0u64));
  let obs_payload_budget: Option<u64> = match driver_opts.observation_channel() {
    crate::Channel::Bounded(_) => Some((endpoint.max_stream_frame_size() as u64).saturating_mul(4)),
    crate::Channel::Unbounded => None,
  };
  compio::runtime::spawn(observation_task::<I, D>(
    obs_rx,
    delegate,
    events_tx,
    events_dropped.clone(),
    obs_payload_bytes.clone(),
  ))
  .detach();

  let mut state = QuicDriverState {
    endpoint,
    udp_socket,
    commands,
    label,
    obs_tx,
    observation_dropped,
    obs_payload_bytes,
    obs_payload_budget,
    snapshot,
    last_snapshot_version: 0,
    metrics,
    last_metrics: Metrics::default(),
    shutdown_flag,
    driver_opts,
    cidr_policy,
    pending_joins: HashMap::new(),
    next_pending_join_id: 0,
    shutdown_reply: None,
    pending_leave: None,
    pending_pings: Vec::new(),
    pending_user_sends: Vec::new(),
  };

  // Per-driver UDP recv buffer size derived once from the coordinator's
  // configured `gossip_mtu` + encrypted-wrapper overhead, capped at the
  // IP-layer UDP maximum. The coordinator's `gossip_mtu` is set at
  // construction time and never reconfigured at runtime, so a single
  // value computed up-front is correct.
  let recv_buf_len = gossip_recv_buf_len::<I, G>(&state.endpoint);

  // Arm the periodic probe / gossip / push-pull schedulers. Without this
  // the machine's `next_probe` / `next_gossip` / `next_pushpull` stay
  // `None`, so failure detection, broadcast dissemination, and anti-entropy
  // never run — the loop would only service explicit join push-pull and
  // passive inbound traffic.
  state.endpoint.start_scheduling(Instant::now());

  // Publish the initial snapshot at loop entry so the first observable
  // state is available before any input arrives (mirror-symmetric with
  // the stream driver's loop-entry `refresh_snapshot`).
  refresh_snapshot::<I, G>(&state.endpoint, &state.snapshot);
  // `Endpoint::new` bumped `snapshot_version` to 1 when it queued the
  // construction self-join, and the publish above already reflects it. Sync
  // `last_snapshot_version` to that post-self-join version (it was initialized
  // to 0) so the first drain below does not see the unchanged startup snapshot
  // as dirty and republish it — a spurious duplicate. Mirrors the stream
  // driver, which captures the post-construction version at loop entry.
  state.last_snapshot_version = state.endpoint.endpoint_ref().snapshot_version();

  let mut exit = false;
  // Tracks whether the previous iteration's inputs (cmd-fairness
  // drain, select arm) advanced state without the corresponding
  // four-surface drain having flushed yet. Hoisted across iterations
  // so a select arm that sets `dirty = true` is observed by the
  // NEXT iter-top dirty drain — placing the drain at iter top (not
  // after the select) avoids the borrow conflict between the drain's
  // `&mut state` and the recv/cmd futures' immutable borrows.
  //
  // Initialized `true` so the FIRST iteration's iter-top drain surfaces the
  // construction self-join (`NodeJoined(self)`, queued by `Endpoint::new` to
  // match Go's Create-time `NotifyJoin`) to `obs_tx` before the select can
  // park. Without it a no-peer node with the periodic schedulers disabled
  // would not surface its own member until the idle-wake interval. The version
  // sync above keeps that first drain from republishing the unchanged snapshot.
  let mut dirty = true;
  while !exit {
    // Iter-top command fairness drain. The `cmd` select arm sits at
    // LAST priority (recv > timer > cmd) so a continuous network
    // flood would otherwise starve `Shutdown`; the iter-top drain
    // pulls up to `cmd_fairness_budget` commands via `try_recv`
    // every loop pass so user calls always make bounded progress.
    // Capped so a cmd flood itself cannot starve the network arms
    // via this path either.
    let mut cmd_drained = 0;
    while cmd_drained < state.driver_opts.cmd_fairness_budget() {
      match state.commands.try_recv() {
        Ok(c) => {
          let now = Instant::now();
          let is_shutdown = matches!(c, Command::Shutdown(_));
          let leave_timeout = state.driver_opts.leave_timeout();
          dispatch_command::<I, G>(
            &mut state.endpoint,
            &mut state.shutdown_reply,
            &mut state.pending_joins,
            &mut state.next_pending_join_id,
            &mut state.pending_leave,
            &mut state.pending_pings,
            &mut state.pending_user_sends,
            leave_timeout,
            &state.cidr_policy,
            c,
            now,
          )
          .await;
          cmd_drained += 1;
          dirty = true;
          if is_shutdown {
            // Shutdown is terminal — stop draining further commands
            // on this iteration. Any commands queued behind shutdown
            // observe the driver loop already exited (the `commands`
            // receiver drops when `quic_driver_loop` returns, so a racing
            // `send_async` returns `Err(Disconnected)` and the caller
            // surfaces `MemberlistError::CommandSend`).
            exit = true;
            break;
          }
        }
        Err(_) => break,
      }
    }
    if exit {
      // Flush every queued surface before tearing down. A preceding
      // `leave()` directly enqueues `Dead`-self transmits to live peers (it
      // does not rely on the gossip queue), and those must reach the wire
      // before the post-loop cleanup drops the UDP socket — otherwise peers
      // observe a probe-timeout failure instead of an intentional leave.
      // `drain_actions` iterates to fixed point internally. Mirrors the
      // stream driver's exit drain (`memberlist-compio/src/driver.rs`).
      if drain_actions::<I, G>(&mut state).await {
        refresh_snapshot_if_changed::<I, G>(
          &state.endpoint,
          &state.snapshot,
          &mut state.last_snapshot_version,
        );
        refresh_metrics_if_changed::<I, G>(
          &state.endpoint,
          &state.metrics,
          &mut state.last_metrics,
        );
      }
      reap_pending_joins(&mut state.pending_joins, Instant::now()).await;
      reap_pending_leave(&mut state.pending_leave, Instant::now()).await;
      break;
    }

    // The iter-top cmd drain may have advanced state (a Join queued
    // outbound push/pulls, an UpdateNodeMetadata mutated the local
    // node) without any select arm firing. Drain the four
    // QuicEndpoint poll surfaces here so the snapshot / events /
    // UDP-out side-effects of those mutations are flushed before the
    // select suspends. Performed BEFORE building the recv/timer/cmd
    // futures so the drain's mutable borrow of `state` does not race
    // the immutable borrows those futures hold for the duration of
    // the select. Mirrors the stream driver's pre-select dirty drain
    // (`memberlist-compio/src/driver.rs:688`).
    if dirty {
      if drain_actions::<I, G>(&mut state).await {
        refresh_snapshot_if_changed::<I, G>(
          &state.endpoint,
          &state.snapshot,
          &mut state.last_snapshot_version,
        );
        refresh_metrics_if_changed::<I, G>(
          &state.endpoint,
          &state.metrics,
          &mut state.last_metrics,
        );
      }
      // Reap any pending-join waiter whose `pending` set was emptied
      // by the drain (a push/pull ExchangeCompleted reduction) or
      // whose deadline elapsed during the drain, and any graceful-leave
      // waiter past its deadline (the `LeftCluster` success path is
      // resolved inside the drain's events step). Mirrors the stream
      // driver's post-drain reap calls.
      reap_pending_joins(&mut state.pending_joins, Instant::now()).await;
      reap_pending_leave(&mut state.pending_leave, Instant::now()).await;
      dirty = false;
    }

    // Compute the next timer deadline. `poll_timeout` returns the
    // coordinator's nearest pending deadline; the idle-wake-interval
    // fallback bounds how stale a snapshot can get under a quiescent
    // endpoint.
    let setup_now = Instant::now();

    // Iter-top past-due preemption. Under a continuous UDP-recv flood
    // the main `select_biased!`'s `recv` arm always wins over `timer`
    // (recv arm 1, timer arm 2 in source order), so a deadline that
    // is already in the past at loop entry would never fire under
    // the select — `recv` keeps re-arming. The single one-packet peek
    // + re-polled-deadline `handle_timeout` sequence inside
    // `fire_timeout_with_drain` runs the past-due deadline protocol
    // BEFORE the select re-arms: a kernel-buffered datagram that
    // would resolve the deadline gets exactly one shot per past-due
    // iteration; if the deadline survives the peek `handle_timeout`
    // fires. Mirrors the stream driver's iter-top past-due branch
    // (`memberlist-compio/src/driver.rs:602`).
    //
    // The past-due check folds in the earliest pending-join AND
    // pending-leave deadline: under a continuous recv flood `recv`
    // would always win the main select over `timer`, so an expired
    // synchronous-join waiter or graceful-leave waiter would never have
    // its deadline reap fire either. Folding both into the past-due
    // trigger lets the iter-top branch run the reaps even when the
    // coordinator itself has no near-term work.
    let past_due_t = {
      let coord = state.endpoint.poll_timeout();
      let pj_min = min_pending_join_deadline(&state.pending_joins);
      let pl_min = min_pending_leave_deadline(&state.pending_leave);
      [coord, pj_min, pl_min].into_iter().flatten().min()
    };
    if let Some(t) = past_due_t
      && setup_now >= t
    {
      if fire_timeout_with_drain::<I, G>(&mut state, recv_buf_len).await {
        refresh_snapshot_if_changed::<I, G>(
          &state.endpoint,
          &state.snapshot,
          &mut state.last_snapshot_version,
        );
        refresh_metrics_if_changed::<I, G>(
          &state.endpoint,
          &state.metrics,
          &mut state.last_metrics,
        );
      }
      reap_pending_joins(&mut state.pending_joins, Instant::now()).await;
      reap_pending_leave(&mut state.pending_leave, Instant::now()).await;
      dirty = false;
      continue;
    }

    let timeout_deadline = {
      let coord = state.endpoint.poll_timeout();
      let pj_min = min_pending_join_deadline(&state.pending_joins);
      let pl_min = min_pending_leave_deadline(&state.pending_leave);
      let idle = setup_now + state.driver_opts.idle_wake_interval();
      // `coord`/`pj_min`/`pl_min` may be `None`; `idle` is always
      // populated. The earliest of the four drives the timer arm.
      [coord, pj_min, pl_min, Some(idle)]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(idle)
    };

    // Per-iteration fresh allocation: the recv future owns the buffer
    // by value while pending and the buffer is returned via
    // `BufResult`. On cancellation (any non-recv arm fires first) the
    // in-flight syscall is dropped and the buffer is freed; the next
    // iteration allocates fresh. Mirrors the stream driver's
    // `recv_buf` allocation discipline.
    //
    // `recv_fut` holds an immutable borrow on `state.udp_socket` and
    // `cmd_fut` holds one on `state.commands` for the duration of the
    // select; the select arms therefore access mutating state only
    // via `state.endpoint` / `state.shutdown_reply` (distinct fields,
    // so the disjoint-field borrow split holds). The narrow-borrow
    // `dispatch_command` signature mirrors the stream driver's
    // pattern (pass individual `&mut` field refs, not the whole
    // packed state) so the compiler can see the disjoint splits.
    let recv_buf = vec![0u8; recv_buf_len];
    let recv_fut = state.udp_socket.recv_from(recv_buf).fuse();
    let timer_fut = compio::time::sleep_until(timeout_deadline.into_std()).fuse();
    let cmd_fut = state.commands.recv_async().fuse();
    pin_mut!(recv_fut, timer_fut, cmd_fut);

    // Arm priority (top -> bottom; `select_biased!` resolves the
    // first ready arm in source order):
    //
    // 1. recv  — kernel-buffered UDP datagrams. MUST come before the
    //            timer so a buffered datagram that would resolve a
    //            probe deadline is applied BEFORE `handle_timeout`
    //            marks the peer suspect.
    // 2. timer — past-due coordinator deadline. Fires
    //            `handle_timeout` to advance suspicion / probe /
    //            forward reapers.
    // 3. cmd   — user commands. LAST priority so a cloned-handle
    //            command flood cannot starve recv / timer; the iter-
    //            top `cmd_fairness_budget` drain bounds shutdown
    //            latency under the symmetric concern (a continuous
    //            recv flood starving cmd).
    select_biased! {
      gossip = recv_fut => {
        let BufResult(res, buf) = gossip;
        match res {
          Ok((n, src)) => {
            let received_at = Instant::now();
            // Drop a UDP packet from a CIDR-blocked source before the QUIC machine
            // sees it: a blocked peer completes no handshake and forms no
            // connection (the advertised-address filter is the composed alive
            // delegate).
            if !cidr_blocks(&state.cidr_policy, src.ip()) {
              state.endpoint.handle_udp(src, &buf[..n], received_at);
            }
            // The next iter-top dirty drain flushes the
            // poll_event / poll_memberlist_ingress / poll_*_transmit
            // queues the input may have populated; the drain has to
            // sit at iter top (not here) so its `&mut state` borrow
            // does not race the `recv_fut` / `cmd_fut` immutable
            // borrows still live in this scope.
            dirty = true;
          }
          Err(_) => {
            // Best-effort logging point would go here; a transient
            // recv error (ICMP unreachable surfacing as a syscall
            // error on Linux, EAGAIN, etc.) is non-fatal — the next
            // iteration re-arms recv with a fresh buffer. Mirrors
            // the stream driver's silent-on-transient policy.
          }
        }
      }
      _ = timer_fut => {
        // `select_biased!` orders `recv_fut` ahead of `timer_fut`, so
        // a kernel-buffered datagram that would resolve the deadline
        // wins this iter via the recv arm; the timer arm only fires
        // when the kernel had nothing to deliver. Firing
        // `handle_timeout` inline + setting `dirty` defers the four-
        // surface flush to the next iter's iter-top dirty drain.
        //
        // The peek-based `fire_timeout_with_drain` helper is NOT
        // called here: it would build a SECOND `recv_from` SQE on
        // `state.udp_socket` while the outer `recv_fut` SQE is still
        // live in this iteration, and io_uring would deliver a
        // racing datagram to whichever SQE the kernel picked. The
        // outer `recv_fut` is dropped at the end of this loop
        // iteration without ever being polled (the select already
        // resolved on the timer arm) — a datagram routed to it
        // would be lost. The peek discipline therefore runs only at
        // the iter-top past-due site BEFORE `recv_fut` is built.
        // The next iter-top past-due check + iter-top dirty drain
        // jointly recover any deadline that was unresolved by this
        // tick's `handle_timeout`.
        state.endpoint.handle_timeout(Instant::now());
        dirty = true;
      }
      cmd = cmd_fut => {
        match cmd {
          Ok(c) => {
            // Refresh `now` at the moment of the actual state
            // mutation. Using the loop-top timestamp would feed a
            // stale `now` into the machine — any deadline computed
            // off it would be off by the time the arm sat in
            // `select!`.
            let now = Instant::now();
            let is_shutdown = matches!(c, Command::Shutdown(_));
            let leave_timeout = state.driver_opts.leave_timeout();
            dispatch_command::<I, G>(
              &mut state.endpoint,
              &mut state.shutdown_reply,
              &mut state.pending_joins,
              &mut state.next_pending_join_id,
              &mut state.pending_leave,
              &mut state.pending_pings,
              &mut state.pending_user_sends,
              leave_timeout,
              &state.cidr_policy,
              c,
              now,
            )
            .await;
            dirty = true;
            if is_shutdown {
              exit = true;
            }
          }
          // All `Memberlist` handles dropped -> channel is closed.
          // Treat the same as an explicit shutdown.
          Err(_) => {
            exit = true;
          }
        }
      }
    }
  }

  // Cleanup. Order matches the stream driver's post-loop sequence
  // (`memberlist-compio/src/driver.rs`):
  //   1. Set the shutdown flag so any racing clone's command method
  //      observes it on entry and returns Shutdown without sending.
  //   2. Drain pending commands and reply `Err(Shutdown)` on each —
  //      callers whose flag check passed BEFORE the flip and were
  //      already buffered get the documented error instead of
  //      hanging on a reply-channel whose Sender is buffered inside
  //      the dropped Receiver. A straggler `Shutdown` whose reply
  //      we have not stashed yet is captured here.
  //   3. Drop the `commands` Receiver. Any clone whose flag check
  //      passed before the flip and was still mid-method cannot
  //      land its command in the channel buffer after the Receiver
  //      is gone — `send_async` instead fails fast with
  //      `Disconnected` and the clone surfaces `CommandSend`.
  //   4. Drain `pending_joins` so a `join_with` caller in flight
  //      does not have to wait the full deadline.
  //   5. Drop the bound UDP socket BEFORE acking the observed
  //      shutdown caller so an immediate rebind on the same port
  //      after `shutdown.await` succeeds.
  state.shutdown_flag.set(true);
  while let Ok(c) = state.commands.try_recv() {
    let res = Err(MemberlistError::Shutdown);
    let reply: futures_channel::oneshot::Sender<Result<()>> = match c {
      Command::Shutdown(ShutdownCmd { reply }) => {
        // A straggler `Shutdown` whose racing clone slipped in
        // behind the first one we already stashed — prefer the
        // earliest stashed reply (the post-loop ack lands once),
        // and surface `Err(Shutdown)` to any later one.
        if state.shutdown_reply.is_none() {
          state.shutdown_reply = Some(reply);
          continue;
        }
        reply
      }
      Command::Leave(LeaveCmd { reply }) => reply,
      Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { reply, .. }) => reply,
      #[cfg(compression)]
      Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => reply,
      #[cfg(checksum)]
      Command::SetChecksumOptions(SetChecksumOptionsCmd { reply, .. }) => reply,
      #[cfg(encryption)]
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd { reply, .. }) => reply,
      Command::QueueUserBroadcast(cmd) => cmd.reply,
      Command::SetLocalState(cmd) => cmd.reply,
      Command::SetAckPayload(cmd) => cmd.reply,
      Command::SendUser(cmd) => cmd.reply,
      Command::SendReliable(cmd) => cmd.reply,
      Command::Join(JoinCmd { reply, .. }) => {
        // `Join`'s reply type is `Result<usize>`; surface the same
        // `Shutdown` error through a separate arm so the type
        // checker sees the right `Sender<Result<usize>>`.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = reply.send(Err(MemberlistError::Shutdown));
        continue;
      }
      Command::Ping(cmd) => {
        // `Ping`'s reply type is `Result<Duration>`; surface Shutdown
        // through a separate arm so the type checker sees the right sender.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::Shutdown));
        continue;
      }
    };
    // Ignoring Err: caller dropped the reply receiver.
    let _ = reply.send(res);
  }
  // Drop the `commands` Receiver immediately so any subsequent
  // clone send fails fast (see step 3 above). Move out of state so
  // the receiver is dropped at the end of the statement scope.
  drop(state.commands);

  // Reply `Err(Shutdown)` to every outstanding synchronous-join
  // waiter. Their reply Senders live in `pending_joins`; without
  // this drain the corresponding receivers on the caller side
  // would hang forever (the driver task is about to exit).
  for (_, pj) in state.pending_joins.drain() {
    // Ignoring Err: caller's reply receiver may have been dropped;
    // nothing to surface.
    let _ = pj.reply.send(Err(MemberlistError::Shutdown));
  }

  // Reply `Err(Shutdown)` to every parked application-ping waiter whose
  // `PingCompleted`/`PingFailed` will never arrive (driver is exiting).
  for pp in state.pending_pings.drain(..) {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = pp.reply.send(Err(MemberlistError::Shutdown));
  }
  // Reply `Err(Shutdown)` to every parked reliable directed-send waiter
  // whose `ExchangeCompleted(UserMessage)` will never arrive (driver is exiting).
  for ps in state.pending_user_sends.drain(..) {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = ps.reply.send(Err(MemberlistError::Shutdown));
  }

  // Reply `Err(Shutdown)` to every joined graceful-leave replier whose
  // `LeftCluster` never arrived before the loop exited (e.g. a shutdown
  // raced the leave flush). Without this their receivers would hang
  // forever — the driver task is about to exit.
  if let Some(pl) = state.pending_leave.take() {
    pl.resolve_all(|| Err(MemberlistError::Shutdown)).await;
  }

  // Close the bound UDP socket BEFORE acking the observed shutdown caller so an
  // immediate rebind on the same port after `shutdown.await` succeeds. A plain
  // drop releases the port synchronously on Linux/macOS, but Windows IOCP closes
  // the handle ASYNCHRONOUSLY — the port lingers past the drop and a same-port
  // rebind races into `AddrInUse`. Awaiting the explicit close (as the stream
  // driver does for its gossip socket) drains the close to completion, so the
  // kernel slot is released before the stashed reply fires below.
  //
  // Ignoring Err: a close error during teardown is unactionable.
  let _ = state.udp_socket.close().await;

  // Ack any stashed Shutdown command reply.
  if let Some(reply) = state.shutdown_reply.take() {
    // Ignoring Err: the caller dropped its reply receiver; nothing
    // to surface.
    let _ = reply.send(Ok(()));
  }
  // `state` (and with it `obs_tx`) drops here, closing the observation
  // channel; the observation task drains any buffered events and exits.
}

/// Per-driver observation task: dispatch each event's [`Delegate`] hook, then
/// fan membership / control events out to the `EventStream`, OFF the driver
/// loop.
///
/// Receives each event the driver hands off (in machine-emission order) on
/// `obs_rx` and fires the matching `notify_*` / `merge_remote_state` hook (so a
/// delegate observes the transition ahead of any EventStream consumer). The
/// obs-task recv side is lossless — it delivers every event that reached the
/// channel; a bounded `obs_tx` only drops at the driver-side hand-off when the
/// delegate cannot keep up (counted in `observation_dropped`).
///
/// Only membership / control events are then forwarded to `EventStream`
/// subscribers. App-data events (`UserPacket` / `RemoteStateReceived`) reach
/// the delegate hook above but are NOT fanned out — the EventStream cannot
/// reconstruct app-data from the membership snapshot, and forwarding the large
/// reliable payloads would let a slow subscriber pile them up in the bounded
/// `events_tx`. The forward is best-effort: a full queue (slow subscriber)
/// drops the event via `try_send` and counts it into `events_dropped` (gap
/// signal), never blocking. The task exits when `obs_rx` closes (the driver
/// dropped its `obs_tx` at teardown). Mirrors the stream driver's
/// `observation_task`.
async fn observation_task<I, D>(
  mut obs_rx: mpsc::Receiver<Event<I, SocketAddr>>,
  delegate: D,
  events_tx: Sender<Event<I, SocketAddr>>,
  events_dropped: Rc<Cell<u64>>,
  obs_payload_bytes: Rc<Cell<u64>>,
) where
  D: Delegate<Id = I, Address = SocketAddr>,
  I: memberlist_proto::Id,
{
  while let Some(ev) = obs_rx.recv().await {
    // App-data events carry the only large payloads. Measure this event for the
    // byte backstop and free the budget it occupied before the (possibly slow)
    // delegate hook, so the driver's enqueue side sees the reclaimed budget
    // promptly. Membership / control events have no weight.
    let payload = crate::driver::shared::observation_payload_bytes(&ev);
    if let Some(b) = payload {
      obs_payload_bytes.set(obs_payload_bytes.get() - b);
    }
    // Contain a panicking delegate hook so the task SURVIVES and keeps releasing
    // the byte-backstop reservations of still-queued events; a dead obs task
    // would strand them (wedging the byte budget) and silently stop all delegate
    // dispatch and EventStream fan-out. Ignoring the unwind result: the panic is
    // contained and this event is simply dropped.
    let _ = std::panic::AssertUnwindSafe(dispatch_event_delegate(&delegate, &ev))
      .catch_unwind()
      .await;
    // Fan out membership / control events only — app-data went to the delegate
    // (above), never the bounded best-effort `events_tx`, so a slow subscriber
    // cannot retain large payloads there. See the stream driver's
    // `observation_task` for the full rationale.
    if payload.is_some() {
      continue;
    }
    if events_tx
      .try_send(ev)
      .is_err_and(|e| matches!(e, flume::TrySendError::Full(_)))
    {
      events_dropped.set(events_dropped.get() + 1);
    }
  }
}

/// Dispatch one driver command.
///
/// Mirrors the stream driver's `dispatch_command` for the QUIC
/// endpoint surface. The `Join` arm fans out one
/// [`QuicEndpoint::start_push_pull`] per resolved seed; `Leave`,
/// `UpdateNodeMetadata`, `SetCompressionOptions`, `SetEncryptionOptions`,
/// and the application-data commands (`QueueUserBroadcast`,
/// `SetLocalState`, `SetAckPayload`) route to their `QuicEndpoint`
/// setters; `Shutdown` stashes the reply for the post-loop ack so the
/// bound UDP port is released before the caller's `shutdown().await`
/// resumes.
///
/// `JoinKind::WaitForCompletion` fans out one
/// [`QuicEndpoint::start_push_pull`] per seed and parks a
/// [`PendingJoin`] in `pending_joins`; the events drain reduces it on
/// [`Event::ExchangeCompleted`] filtered to [`ExchangeKind::PushPull`]
/// and the per-iteration `reap_pending_joins` replies on completion or
/// deadline expiry.
#[allow(clippy::too_many_arguments)]
async fn dispatch_command<I, G>(
  endpoint: &mut QuicEndpoint<I, G>,
  shutdown_reply: &mut Option<futures_channel::oneshot::Sender<Result<()>>>,
  pending_joins: &mut HashMap<u64, PendingJoin>,
  next_pending_join_id: &mut u64,
  pending_leave: &mut Option<PendingLeave>,
  pending_pings: &mut Vec<PendingPing>,
  pending_user_sends: &mut Vec<PendingUserSend>,
  leave_timeout: Duration,
  cidr_policy: &CidrFilter,
  cmd: Command<I>,
  now: Instant,
) where
  I: memberlist_proto::Id,
  G: rand::Rng,
{
  match cmd {
    Command::Join(JoinCmd { addrs, kind, reply }) => {
      // Gate on a running node FIRST: `leave()` is terminal — it stops
      // the periodic schedulers (probe / gossip / push-pull) and there
      // is no rejoin / re-arm path. A join enqueued after leave would
      // resolve `Ok(contacted)` from `ExchangeCompleted` yet leave the
      // node Left and non-participating, so orchestration would treat a
      // dead node as rejoined. Reject with `NotRunning` BEFORE any
      // `start_push_pull` so no push/pull is enqueued. The single-task
      // driver makes the check + dispatch atomic (no lifecycle race).
      if !endpoint.is_running() {
        // Ignoring Err: the caller may have dropped the reply receiver.
        let _ = reply.send(Err(MemberlistError::NotRunning));
        return;
      }
      match kind {
        JoinKind::Dispatch => {
          let mut count: usize = 0;
          for addr in &addrs {
            // Skip an outbound dial to a CIDR-blocked seed: starting the QUIC
            // push/pull would open a connection and emit handshake packets to a
            // peer our own policy excludes. The blocked seed is not contacted.
            if cidr_blocks(cidr_policy, addr.ip()) {
              continue;
            }
            // Ignoring StreamId return: per-seed completion / failure
            // surfaces through `poll_event` (the `NodeJoined` /
            // `DialFailed` events that the events_tx forwards to
            // subscribers). The dispatch arm tracks no per-exchange
            // waiter state.
            let _ = endpoint.start_push_pull(*addr, PushPullKind::Join, now);
            count += 1;
          }
          // Ignoring Err: the caller may have dropped the reply
          // receiver (e.g. the user-facing `dispatch_join_with`
          // future was cancelled).
          let _ = reply.send(Ok(count));
        }
        JoinKind::WaitForCompletion(crate::command::WaitForCompletionArgs { deadline }) => {
          // Dispatch one outbound push/pull per seed; each
          // `start_push_pull` returns a fresh machine `StreamId` that
          // coerces into the [`ExchangeId`] domain via the
          // [`From<StreamId> for ExchangeId`] impl in
          // `memberlist-proto/src/event.rs`. The QUIC backend keys
          // its bridges by `StreamId`, so the coerced value is the
          // same one the bridge-reap path stamps onto its
          // `Event::ExchangeCompleted` payload — the reduction in
          // `drain_actions` matches on this exact value.
          //
          // Mirrors the stream driver's `WaitForCompletion` arm: no
          // alive pre-count, dispatch every seed unconditionally so
          // duplicates produce duplicate exchanges (each counted
          // independently per the call-scoped `ExchangeId` ownership
          // discipline).
          let mut pending: HashSet<ExchangeId> = HashSet::with_capacity(addrs.len());
          for addr in &addrs {
            // Skip a CIDR-blocked seed (see the Dispatch arm): no QUIC handshake
            // is emitted to a peer our own policy excludes.
            if cidr_blocks(cidr_policy, addr.ip()) {
              continue;
            }
            let stream_id = endpoint.start_push_pull(*addr, PushPullKind::Join, now);
            pending.insert(ExchangeId::from(stream_id));
          }
          if pending.is_empty() {
            // `join_with` rejects an empty seed list before the command is sent,
            // so `addrs` is non-empty: an empty `pending` means every seed was
            // CIDR-blocked and the join contacted none of them. Reply now —
            // parking would hang with no terminal event incoming.
            // Ignoring Err: caller dropped the reply receiver.
            let _ = reply.send(Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
              addrs.len(),
              0,
            ))));
            return;
          }
          // `requested` is the number of outbound exchanges this call
          // dispatched (one per non-blocked address, including duplicates). The
          // public `join_with` guards against the empty-input case
          // before sending the command, so `addrs` is non-empty here.
          let requested = pending.len();
          let id = *next_pending_join_id;
          *next_pending_join_id = next_pending_join_id.wrapping_add(1);
          pending_joins.insert(
            id,
            PendingJoin {
              pending,
              contacted: 0,
              requested,
              deadline,
              reply,
            },
          );
        }
      }
    }
    Command::Leave(LeaveCmd { reply }) => {
      // Leave is a SHARED in-flight operation (cloned `Memberlist`
      // handles can race two `leave()` calls). The decision:
      //   * in-flight (`pending_leave` is `Some`) → JOIN it: push this
      //     reply onto the shared `repliers` and return. Do NOT
      //     re-invoke `endpoint.leave()` — a repeated leave once already
      //     `Leaving`/`Left` is a terminal no-op that emits no second
      //     `LeftCluster`, so a freshly-parked waiter would hang. Do NOT
      //     reply now; the single terminal outcome resolves every joined
      //     replier together.
      //   * not in-flight (`None`) → INITIATE: snapshot Running before
      //     the call (it decides whether a `LeftCluster` will fire),
      //     call `endpoint.leave()`, then either PARK (was Running — the
      //     machine queued the direct `Dead`-self notices and WILL emit
      //     `LeftCluster` once they drain) or reply IMMEDIATELY (a
      //     genuine no-op / error: nothing in flight, no completion
      //     event coming, so parking would hang).
      if let Some(pl) = pending_leave.as_mut() {
        pl.repliers.push(reply);
      } else {
        let was_running = endpoint.is_running();
        let res: Result<()> = endpoint
          .leave(now)
          .map_err(|e| MemberlistError::Io(io::Error::other(e.to_string())));
        match res {
          Ok(()) if was_running => {
            // Initiated → park. A returned `Ok(())` then means the leave
            // actually reached the wire (`LeftCluster`), never merely
            // that it was queued.
            *pending_leave = Some(PendingLeave {
              repliers: vec![reply],
              deadline: now + leave_timeout,
            });
          }
          // Idempotent no-op (not Running) ⇒ no `LeftCluster` will fire,
          // OR the call errored. Reply immediately; parking would hang.
          other => {
            // Ignoring Err: caller dropped the reply receiver.
            let _ = reply.send(other);
          }
        }
      }
    }
    Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { meta, reply }) => {
      // Gate on a running node: after `leave()` the endpoint clears its
      // periodic schedulers, so a metadata mutation could never be
      // gossiped. Reject with `NotRunning` rather than ack a change that
      // will never leave the local node. The single-task driver makes the
      // check + apply atomic (no lifecycle race).
      let res: Result<()> = if endpoint.is_running() {
        match memberlist_proto::typed::Meta::try_from(meta) {
          Ok(m) => endpoint
            .update_meta(m)
            .map_err(|e| MemberlistError::Io(io::Error::other(e.to_string()))),
          Err(e) => Err(MemberlistError::Io(io::Error::other(e.to_string()))),
        }
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send(res);
    }
    #[cfg(compression)]
    Command::SetCompressionOptions(SetCompressionOptionsCmd { opts, reply }) => {
      // Gate on a running node: after `leave()` the endpoint emits no
      // protocol traffic, so a new compression policy could never take
      // effect on the wire. Reject with `NotRunning` rather than ack a
      // change that will never be observed, and leave the endpoint
      // untouched. The single-task driver makes the check + apply atomic.
      let res: Result<()> = if endpoint.is_running() {
        endpoint.set_compression_options(opts);
        Ok(())
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send(res);
    }
    #[cfg(checksum)]
    Command::SetChecksumOptions(SetChecksumOptionsCmd { opts, reply }) => {
      // Gate on a running node FIRST: after `leave()` the endpoint emits no
      // gossip datagrams, so a new checksum policy could never take effect on
      // the wire. Reject with `NotRunning` without validating — there is no
      // point probing a policy that can never apply. When running, validate the
      // policy BEFORE applying it: a checksum algorithm whose backend feature is
      // not compiled into this build is accepted by the options builder, but
      // every later `checksum_gossip` would fail and the driver would drop the
      // datagram — so a "successful" change would silently disable ALL gossip
      // after a false `Ok`. Probe usability via a trial `apply`; only swap the
      // live policy when it is usable. Checksum is a gossip-plane concern only —
      // the reliable QUIC bridge carries no checksum (quinn provides its own
      // integrity) — so this updates just the coordinator's gossip policy. The
      // single-task driver makes the check + apply atomic.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .set_checksum_options(opts)
          .map_err(MemberlistError::Checksum)
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send(res);
    }
    #[cfg(encryption)]
    Command::SetEncryptionOptions(SetEncryptionOptionsCmd { opts, reply }) => {
      // Gate on a running node FIRST: after `leave()` the endpoint emits no
      // protocol traffic, so a new encryption policy could never take effect
      // on the wire. Reject with `NotRunning` without validating — there is
      // no point trial-encrypting a policy that can never apply. When
      // running, validate the policy BEFORE applying it: a keyring naming an
      // AEAD whose backend feature is not compiled into this build is
      // constructible, but every later `encrypt_gossip` would drop the
      // datagram and every reliable-stream encode would fail — the cluster
      // would silently break after a false `Ok`. Probe usability via a trial
      // encrypt through the existing wire API; only swap the live policy when
      // it is usable. The single-task driver makes the check + apply atomic.
      let res: Result<()> = if endpoint.is_running() {
        match crate::options::validate_encryption_options(&opts) {
          Ok(()) => {
            endpoint.set_encryption_options(opts);
            Ok(())
          }
          Err(e) => Err(MemberlistError::Encryption(e)),
        }
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send(res);
    }
    Command::QueueUserBroadcast(cmd) => {
      // Gate on a running node FIRST: after `leave()` the gossip scheduler
      // is stopped, so `user_broadcasts` would never drain. Then validate
      // the framed lone `UserData` packet against the gossip budget: an
      // over-budget payload is deterministically untransmittable, so the
      // machine setter rejects it without storing it — surface that as
      // `Proto` rather than a false `Ok`. The single-task driver
      // makes the check + apply atomic.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .queue_user_broadcast(cmd.data().clone())
          .map_err(MemberlistError::Proto)
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply.send(res);
    }
    Command::SetLocalState(cmd) => {
      // Gate on a running node FIRST: after `leave()` no push/pull
      // exchange will carry the snapshot, so reject with `NotRunning`. Then
      // validate the framed-PushPull size against the reliable-stream frame
      // budget: a snapshot whose framed PushPull exceeds it would be rejected
      // by every receiver's frame-length gate, so the application state would
      // never reach a peer. The machine setter rejects such a snapshot without
      // storing it; surface that as `Proto` rather than a false `Ok`.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .set_local_state_snapshot(cmd.state().clone())
          .map_err(MemberlistError::Proto)
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply.send(res);
    }
    Command::SetAckPayload(cmd) => {
      // Gate on a running node FIRST: after `leave()` no probe ack will
      // carry the payload, so reject with `NotRunning`. Then validate the
      // framed-ack size against the gossip packet budget: an over-budget ack
      // is emitted as a single UDP datagram that always fails to send, so a
      // probing peer would receive no ack and falsely suspect this node. The
      // machine setter rejects such a payload without storing it; surface
      // that as `Proto` rather than a false `Ok`.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .set_ack_payload(cmd.payload().clone())
          .map_err(MemberlistError::Proto)
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply.send(res);
    }
    Command::Shutdown(ShutdownCmd { reply }) => {
      // Stash the reply for the post-loop cleanup. Acking AFTER the
      // UDP socket drops ensures the bound port is free when the
      // caller resumes from `shutdown.await`, so an immediate rebind
      // on the same port succeeds.
      *shutdown_reply = Some(reply);
    }
    Command::Ping(cmd) => {
      // Gate on a running node: after `leave()` the probe scheduler is
      // stopped, so a new application ping's completion event would never
      // arrive and the caller would hang. Reject with `NotRunning` rather
      // than park an unresolving waiter.
      if endpoint.is_running() {
        let (id, addr) = cmd.node().clone().into_parts();
        let node = Node::new(id, addr);
        let ping_id = endpoint.ping(node, now).expect("issued while running");
        pending_pings.push(PendingPing {
          ping_id,
          reply: cmd.reply,
        });
      } else {
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::NotRunning));
      }
    }
    Command::SendUser(cmd) => {
      // Gate on a running node: after `leave()` the gossip scheduler is
      // stopped; reject immediately.
      let res: Result<()> = if !endpoint.is_running() {
        Err(MemberlistError::NotRunning)
      } else if cidr_blocks(cidr_policy, cmd.to().ip()) {
        // Our own policy excludes the destination: do not emit an unreliable user
        // datagram to a blocked peer.
        Err(MemberlistError::SendFailed)
      } else {
        endpoint
          .send_user_packets(*cmd.to(), cmd.payloads())
          .map_err(MemberlistError::Proto)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply.send(res);
    }
    Command::SendReliable(cmd) => {
      // Gate on a running node: after `leave()` the QUIC stream coordinator
      // is stopping; a new `start_user_message` would never produce a bridge
      // and the caller would hang. Reject with `NotRunning`.
      if !endpoint.is_running() {
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::NotRunning));
        return;
      }
      if cidr_blocks(cidr_policy, cmd.to().ip()) {
        // Our own policy excludes the destination: fail the reliable send at the
        // transport boundary without opening a QUIC stream (no handshake packets
        // are emitted to a blocked peer).
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::SendFailed));
        return;
      }
      let mut pending: HashSet<ExchangeId> = HashSet::with_capacity(cmd.payloads().len());
      for payload in cmd.payloads() {
        // `QuicEndpoint::start_user_message` calls `service_dials` +
        // `flush_outbound` in-band (no separate `poll_action` loop needed),
        // unlike the stream driver. The returned `StreamId` coerces to the
        // `ExchangeId` that the bridge-reap path stamps on
        // `Event::ExchangeCompleted(UserMessage)`.
        // Ignoring StreamId: the ExchangeId::from coercion below is the only
        // consumer; the raw StreamId is not needed after parking.
        let stream_id = endpoint
          .start_user_message(*cmd.to(), payload.clone(), now)
          .expect("issued while running");
        pending.insert(ExchangeId::from(stream_id));
      }
      if pending.is_empty() {
        // No exchanges dispatched (empty payloads). Reply Ok immediately;
        // parking would hang with no terminal event incoming.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Ok(()));
      } else {
        pending_user_sends.push(PendingUserSend {
          pending,
          failed: 0,
          reply: cmd.reply,
        });
      }
    }
  }
}

/// Past-due / timer-arm helper: a single one-packet peek off the UDP
/// socket, then re-poll the coordinator's deadline and fire
/// `handle_timeout` if it is still past, then drain to fixed point.
///
/// Under a continuous UDP-recv flood the main `select_biased!`'s
/// `recv` arm always wins over `timer` (recv arm 1, timer arm 2 in
/// source order), so `handle_timeout` would never fire and overdue
/// suspicion / probe deadlines would never be reaped. Under a clean
/// past-due event a kernel-buffered probe Ack would resolve the
/// deadline; applying that one buffered datagram before the deadline
/// re-poll keeps a just-arrived Ack from being decided as a timeout.
///
/// The bounded peek gives `recv` priority for the fire: a
/// `select_biased!` between the recv future and a sleep timer polls
/// recv first (so a buffered datagram wins) and falls through on the
/// timer otherwise. The peek's timer must be a real sleep (not a
/// zero-duration ready future) so io_uring has time to complete a
/// freshly-submitted recv SQE — on completion-based io_uring the recv
/// is ALWAYS Pending on first poll. The `peek_budget` default (1 ms;
/// see [`crate::DEFAULT_PEEK_BUDGET`]) is comfortably above io_uring's
/// completion latency for a kernel-buffered recv (~100 µs) while
/// keeping the per-fire cost negligible.
///
/// After the peek the deadline is re-polled: a consumed datagram may
/// have resolved it (a probe Ack landing the same tick the cumulative
/// deadline expires). `handle_timeout` fires only if the deadline is
/// STILL past. No socket drain-to-empty is needed: the probe FSM
/// anchors success on an absolute `failure_deadline`, so an Ack
/// decoded slightly later — even after `handle_timeout` — still
/// rescues the probe regardless of ordering (this mirrors the plain
/// stream driver's past-due protocol, which is correct over UDP).
/// Then `drain_actions` flushes every downstream consequence of the
/// consumed datagram and/or fired timeout (events, ingress, outbound
/// memberlist transmits, raw QUIC transmits) to fixed point.
///
/// Mirrors the stream driver's iter-top peek + `fire_timeout_with_drain`
/// pair (`memberlist-compio/src/driver.rs:602`); this is the only call
/// site that can safely build a peek `recv_from` SQE (the timer-arm
/// site already has the main loop's `recv_fut` SQE in flight on the
/// same socket — a second SQE there would race the kernel's datagram
/// delivery between two pending futures and lose the datagram if the
/// outer `recv_fut` is dropped without being polled).
///
/// Returns `true` iff any work was applied (the peek consumed a
/// datagram, `handle_timeout` fired, OR `drain_actions` made
/// progress), so the caller knows to republish the snapshot.
async fn fire_timeout_with_drain<I, G>(
  state: &mut QuicDriverState<I, G>,
  recv_buf_len: usize,
) -> bool
where
  I: memberlist_proto::Id,
  G: rand::Rng,
{
  let mut dirty = false;

  // Bounded peek: at most one buffered datagram, at most peek_budget wait.
  // The peek's select_biased puts recv first so a kernel-buffered datagram
  // is always consumed if present. Scoped so the borrows release before
  // drain_actions takes &mut state.
  {
    let peek_buf = vec![0u8; recv_buf_len];
    let peek_recv = state.udp_socket.recv_from(peek_buf).fuse();
    let peek_timer = compio::time::sleep(state.driver_opts.peek_budget()).fuse();
    pin_mut!(peek_recv, peek_timer);
    select_biased! {
      gossip = peek_recv => {
        let BufResult(res, buf) = gossip;
        // Best-effort: a transient recv error is non-fatal; the next iter's main
        // recv arm re-arms with a fresh buffer, so only the Ok case has work.
        if let Ok((n, src)) = res {
          let received_at = Instant::now();
          // Same CIDR source filter as the main recv arm: a blocked source's
          // packet is dropped before the QUIC machine sees it.
          if !cidr_blocks(&state.cidr_policy, src.ip()) {
            state.endpoint.handle_udp(src, &buf[..n], received_at);
          }
          dirty = true;
        }
      }
      _ = peek_timer => {
        // No buffered datagram surfaced within the peek budget; fall through
        // to the deadline-firing protocol below.
      }
    }
  }

  // Re-poll the deadline AFTER the peek — a consumed datagram may have
  // resolved it. Fire handle_timeout only if still past. The probe FSM's
  // absolute failure_deadline makes a slightly-later-decoded Ack still rescue
  // the probe, so no socket drain-to-empty is needed here (mirrors the plain
  // stream driver's past-due protocol).
  let now = Instant::now();
  let after_peek_deadline = state
    .endpoint
    .poll_timeout()
    .unwrap_or(now + state.driver_opts.idle_wake_interval());
  if now >= after_peek_deadline {
    state.endpoint.handle_timeout(now);
    dirty = true;
  }

  if drain_actions::<I, G>(state).await {
    dirty = true;
  }
  dirty
}

/// Drive the four [`QuicEndpoint`] poll surfaces to fixed point.
///
/// After any state-advancing input (recv, timer fire, dispatched
/// command) the endpoint may have queued application events, raw
/// inbound memberlist datagrams, outbound memberlist transmits, and
/// raw outbound QUIC datagrams (handshake / acks / etc.). One arm
/// firing can in turn enqueue work on the others (e.g. a `handle_udp`
/// that completes a push/pull enqueues a `NodeJoined` event AND a
/// follow-up outbound transmit). Iterating to fixed point — looping
/// until a full pass produces no work — drains every consequence
/// before the loop re-arms its select.
///
/// Returns `true` iff any of the four surfaces produced work, so the
/// caller knows to republish the snapshot.
///
/// ## Codec
///
/// Inbound: `poll_memberlist_ingress` surfaces RAW
/// (encrypted/labeled) datagrams. The driver runs
/// `decrypt_gossip` → `decode_incoming` (strip optional label) →
/// `parse_messages` (one plain frame OR a compound bundle) and feeds
/// each decoded message back via `handle_packet`. SWIM piggyback
/// naturally produces compound datagrams (probe + alive broadcast
/// bundled in one frame); `parse_messages` handles both plain and
/// compound transparently.
///
/// Outbound: `poll_memberlist_transmit` surfaces typed
/// `Transmit<I, SocketAddr>` values. The driver encodes via `encode_outgoing`
/// (single message → plain frame) or `encode_outgoing_compound` (a
/// SWIM piggyback batch of `>= 2` messages → one compound datagram),
/// then runs `compress_gossip` → `encrypt_gossip` and sends on the
/// shared UDP socket. `poll_transmit` carries raw QUIC datagrams
/// (handshake, acks, application stream data); those are sent on the
/// same socket without codec wrap.
async fn drain_actions<I, G>(state: &mut QuicDriverState<I, G>) -> bool
where
  I: memberlist_proto::Id,
  G: rand::Rng,
{
  let mut any_progress = false;
  let decode_opts = DecodeOptions::new(state.label.clone());
  let encode_opts = EncodeOptions::new(state.label.clone());
  loop {
    let mut iter_progress = false;

    // 1. Inbound memberlist datagrams — decrypt, strip optional
    //    label, decode the inner frame(s) (plain or compound), and
    //    feed each message back via `handle_packet`. Any decode failure
    //    (truncated, bad label, bad tag, trailing bytes,
    //    encryption-rejected) drops the datagram per the lossy-gossip
    //    discipline; the next probe round retransmits.
    let now = Instant::now();
    while let Some((from, raw)) = state.endpoint.poll_memberlist_ingress() {
      iter_progress = true;
      // Strip the wire transforms (encryption outermost, then checksum, then
      // compression). With no encryption backend built in there is no
      // `decrypt_gossip`, so the remaining transforms are stripped directly via
      // the wire `unwrap_transforms`, bounded by the configured gossip MTU.
      #[cfg(encryption)]
      let plain = match state.endpoint.decrypt_gossip(&raw) {
        Ok(p) => Bytes::from(p),
        Err(_) => continue,
      };
      #[cfg(not(encryption))]
      let plain =
        match memberlist_proto::framing::unwrap_transforms(&raw, state.endpoint.gossip_mtu()) {
          Ok(p) => Bytes::from(p.into_owned()),
          Err(_) => continue,
        };
      let inner = match decode_incoming(plain, &decode_opts) {
        Ok(b) => b,
        Err(_) => continue,
      };
      let msgs = match parse_messages::<I, SocketAddr>(inner) {
        Ok(m) => m,
        // Drop the datagram on a decode error. Gossip is lossy and
        // self-healing — the peer retransmits on the next gossip round.
        Err(_) => continue,
      };
      for msg in msgs {
        state.endpoint.handle_packet(from, msg, now);
      }
    }

    // 2. Outbound memberlist transmits — encode (plain or compound),
    //    then compress + encrypt and route onto the unreliable wire the
    //    endpoint is configured for: a QUIC datagram over the peer's pooled
    //    connection (`UnreliableTransport::Datagram`, the default) or the
    //    shared UDP socket (`UnreliableTransport::Udp`). `encode_outgoing*`
    //    only fails on a typed↔buffa bridge error that round-trips a
    //    locally-built message; matches the stream driver's drop-on-codec-fail
    //    policy. An empty encryption config makes `encrypt_gossip` a copy; a
    //    transient `send_to` error (ENOBUFS / ICMP unreachable surfacing as a
    //    syscall error) is non-fatal per the gossip drop discipline.
    let mut needs_flush = false;
    while let Some(transmit) = state.endpoint.poll_memberlist_transmit() {
      iter_progress = true;
      let (peer, plain) = match transmit {
        Transmit::Packet(pkt) => {
          let (to, msg) = pkt.into_parts();
          let bytes = match encode_outgoing(&msg, &encode_opts) {
            Ok(b) => b,
            Err(_) => continue,
          };
          (to, bytes.to_vec())
        }
        Transmit::Compound(cmp) => {
          let (to, msgs) = cmp.into_parts();
          let bytes = match encode_outgoing_compound(&msgs, &encode_opts) {
            Ok(b) => b,
            Err(_) => continue,
          };
          (to, bytes.to_vec())
        }
      };
      // Apply the cross-transport transforms to the encoded frame before it hits
      // the wire: compress, then checksum, then encrypt, so the on-wire byte
      // order is `[Encrypted[Checksumed[Compressed[frame]]]]`. Each is present
      // only when its backend is built in; with none, the encoded frame goes out
      // as-is.
      #[allow(unused_mut)]
      let mut staged: Vec<u8> = plain;
      #[cfg(compression)]
      {
        staged = state.endpoint.compress_gossip(&staged);
      }
      #[cfg(checksum)]
      {
        staged = match state.endpoint.checksum_gossip(&staged) {
          Ok(b) => b,
          // Checksum configured but its backend was not built in — drop rather
          // than emit an unverifiable datagram on a checksum-configured path.
          Err(_) => continue,
        };
      }
      #[cfg(encryption)]
      {
        staged = match state.endpoint.encrypt_gossip(&staged) {
          Ok(b) => b,
          // Encryption-configured + backend-rejected (e.g. unknown algorithm
          // baked in) — drop. Emitting plaintext on an encrypted-cluster path
          // would silently bypass auth.
          Err(_) => continue,
        };
      }
      // `Bytes` so the datagram-queue path and the UDP fallback can share the
      // same encoded payload without a second copy (`clone` is an O(1) refcount
      // bump).
      let on_wire = Bytes::from(staged);
      match state.endpoint.unreliable_transport() {
        UnreliableTransport::Udp => {
          let BufResult(res, _buf) = state.udp_socket.send_to(on_wire, peer).await;
          // Ignoring Err: a transient UDP send error (ENOBUFS / ICMP
          // unreachable surfacing as a syscall error) is non-fatal — gossip
          // is lossy and the next probe/gossip round recovers.
          let _ = res;
        }
        UnreliableTransport::Datagram => {
          match state
            .endpoint
            .queue_unreliable_datagram(peer, on_wire.clone(), now)
          {
            DatagramSendStatus::Queued => needs_flush = true,
            // NotReady may mean queue_unreliable_datagram just initiated a cold
            // dial; flush this tick so the connection's Initial is emitted now
            // (else the connection does not warm until the next driver wake). The
            // gossip itself still goes out immediately over the UDP fallback.
            DatagramSendStatus::NotReady => {
              needs_flush = true;
              let BufResult(res, _buf) = state.udp_socket.send_to(on_wire, peer).await;
              // Ignoring Err: a transient UDP send error is non-fatal — gossip is
              // lossy and the next probe/gossip round recovers.
              let _ = res;
            }
            // TooLarge: the connection is already Established (max_size was Some),
            // so there is no pending Initial to flush; just fall back to UDP.
            DatagramSendStatus::TooLarge => {
              let BufResult(res, _buf) = state.udp_socket.send_to(on_wire, peer).await;
              // Ignoring Err: a transient UDP send error is non-fatal — gossip is
              // lossy and the next probe/gossip round recovers.
              let _ = res;
            }
          }
        }
      }
    }

    // Flush any datagrams queued above into `out` THIS tick so Section 3 sends
    // them now — a datagram-borne probe whose timeout is armed this same tick
    // must not wait for the next driver wake (that wake can be the timeout).
    if needs_flush {
      state.endpoint.flush_outbound_transmits(now);
    }

    // 3. Raw QUIC datagrams (handshake, acks, application stream
    //    data) — already framed by quinn-proto, no codec wrap.
    while let Some((dest, bytes)) = state.endpoint.poll_transmit() {
      iter_progress = true;
      let BufResult(res, _buf) = state.udp_socket.send_to(bytes, dest).await;
      // Ignoring Err: same rationale as the memberlist transmit arm
      // above — a transient send failure is non-fatal because QUIC
      // retransmits unacked frames on its own timer.
      let _ = res;
    }

    // 4. Events — synchronous protocol accounting + hand-off to the
    //    observation task. Drains `poll_event` to empty doing ONLY the
    //    sync protocol accounting (join contact reduction + completion
    //    reply, leave-completion resolution), then hands each event to
    //    the per-driver observation task via the non-blocking `obs_tx`.
    //
    // The observation task (spawned in `quic_driver_loop`) owns the
    // user `Delegate` and the `EventStream` sender; it dispatches the
    // matching `notify_*` / `merge_remote_state` hook then forwards to
    // subscribers, OFF this driver task. The driver therefore never
    // `.await`s user observation code, so a slow hook cannot stall
    // protocol advancement. This is what decouples join completion from
    // observation latency on QUIC, where a join's `ExchangeCompleted`
    // arrives on a SEPARATE inbound datagram AFTER the one that surfaced
    // its `NodeJoined` — a driver that blocked on that `NodeJoined`'s
    // `notify_join` could not recv the completion datagram (the recv
    // arm is on this same task) and the parked join reply would wait
    // out the full hook. The `obs_tx` hand-off is a non-blocking `try_send`;
    // on a bounded channel (the default) a burst that outpaces the observation
    // task yields once to let it drain, then drops + counts the overflow only
    // if the task still cannot keep up — so a fast delegate never loses a
    // valid burst (including the application-data `UserPacket` /
    // `RemoteStateReceived` payloads that `members()` cannot reconstruct)
    // while memory stays bounded under a slow/stuck handler.

    // Synchronous accounting only — no `.await` on user code.
    while let Some(ev) = state.endpoint.poll_event() {
      iter_progress = true;
      // Per-exchange contact accounting. The machine's
      // `ExchangeCompleted` event carries the terminal outcome of every
      // outbound bridge — push/pull, reliable ping, and reliable
      // user-message. Sync-join consumes only `ExchangeKind::PushPull`
      // completions: a reliable-ping bridge resolving has no bearing on
      // a `join_with` waiter's contact count, and a user-message bridge
      // is one-way fire-and-forget. Filter on the payload's `kind()`
      // before reducing `pending_joins`. For the synchronous-join waiter
      // that dispatched this exact `ExchangeId`, remove the eid from its
      // `pending` set (always, regardless of outcome — the exchange has
      // terminated) and increment `contacted` iff the outcome is
      // `Succeeded`. Tracking by `ExchangeId` (not by `SocketAddr`)
      // gives correct duplicate-seed semantics. An eid belongs to at
      // most one waiter (the `WaitForCompletion` arm captures each
      // dispatched eid into the waiter that allocated it), so the search
      // stops on the first match.
      //
      // The COMPLETION reply (the waiter's `pending` is now empty) fires
      // RIGHT HERE on the driver task — the observation hooks
      // (`notify_join` / `merge_remote_state`) for the co-surfaced
      // `NodeJoined` / `RemoteStateReceived` run off-loop on the
      // observation task — so a slow hook cannot delay `join_with`'s
      // `Ok(contacted)`, mirroring the `LeftCluster` leave resolution
      // below. The DEADLINE path (a waiter still non-empty when its
      // `deadline` elapses) stays in `reap_pending_joins`; a waiter
      // replied + removed here is gone from `pending_joins`, so it can
      // never be double-replied by the reaper.
      if let Event::ExchangeCompleted(ref payload) = ev
        && payload.kind() == ExchangeKind::PushPull
      {
        let eid = payload.eid();
        let succeeded = matches!(payload.outcome(), ExchangeStatus::Succeeded);
        let completed_key = state
          .pending_joins
          .iter_mut()
          .find_map(|(key, pj)| {
            if pj.pending.remove(&eid) {
              if succeeded {
                pj.contacted += 1;
              }
              Some((*key, pj.pending.is_empty()))
            } else {
              None
            }
          })
          .and_then(|(key, empty)| empty.then_some(key));
        if let Some(key) = completed_key
          && let Some(pj) = state.pending_joins.remove(&key)
        {
          // Fully resolved — reply now and drop the waiter. `contacted`
          // is final (the FSM emits one `ExchangeCompleted` per
          // dispatched exchange, so an empty `pending` set means every
          // outbound exchange this call dispatched has terminated). A
          // zero-contact resolution is the same `JoinAllFailed` the
          // reaper would have produced.
          let reply_value = if pj.contacted == 0 {
            Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
              pj.requested,
              0,
            )))
          } else {
            Ok(pj.contacted)
          };
          // Ignoring Err: caller dropped the reply receiver (e.g. the
          // user-facing join_with future was cancelled).
          let _ = pj.reply.send(reply_value);
        }
      }
      // Ping completion: resolve the matching `PendingPing` waiter with the
      // observed RTT. The event still flows to the observation task below
      // (correlation is additive — `PingCompleted` also fires the delegate's
      // `notify_ping_complete`). An id not found in `pending_pings` is an
      // unsolicited or already-reaped event; safe to ignore.
      if let Event::PingCompleted(ref p) = ev {
        let pid = p.ping_id();
        if let Some(idx) = state.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = state.pending_pings.swap_remove(idx);
          // Ignoring Err: caller dropped the reply receiver (ping future was cancelled).
          let _ = pp.reply.send(Ok(p.rtt()));
        }
      }
      // Ping failure: resolve the matching `PendingPing` waiter with `PingTimeout`.
      // Same additive semantics as `PingCompleted` — event continues to obs task.
      if let Event::PingFailed(ref p) = ev {
        let pid = p.ping_id();
        if let Some(idx) = state.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = state.pending_pings.swap_remove(idx);
          // Ignoring Err: caller dropped the reply receiver (ping future was cancelled).
          let _ = pp.reply.send(Err(MemberlistError::PingTimeout));
        }
      }
      // Reliable user-send completion: each `ExchangeCompleted` with kind
      // `UserMessage` may reduce a `PendingUserSend`'s pending set. When the
      // set empties the call resolves. `Ok(())` if all exchanges succeeded;
      // `Err(SendFailed)` if any failed. The event continues to the observation
      // task below (additive — `ExchangeCompleted` feeds future monitoring).
      if let Event::ExchangeCompleted(ref payload) = ev
        && payload.kind() == ExchangeKind::UserMessage
      {
        let eid = payload.eid();
        let failed = matches!(payload.outcome(), ExchangeStatus::Failed);
        if let Some(idx) = state
          .pending_user_sends
          .iter()
          .position(|ps| ps.pending.contains(&eid))
        {
          let ps = &mut state.pending_user_sends[idx];
          ps.pending.remove(&eid);
          if failed {
            ps.failed += 1;
          }
          if ps.pending.is_empty() {
            let ps = state.pending_user_sends.swap_remove(idx);
            let result = if ps.failed > 0 {
              Err(MemberlistError::SendFailed)
            } else {
              Ok(())
            };
            // Ignoring Err: caller dropped the reply receiver.
            let _ = ps.reply.send(result);
          }
        }
      }
      // Leave-completion resolution. `LeftCluster` fires once the
      // direct `Dead`-self notices queued by `leave()` have drained to
      // the wire; resolving the parked waiter here — on this driver
      // task, ahead of the observation task's `notify_leave` — is what
      // makes `leave()` return promptly once the flush is done rather
      // than waiting on delegate latency.
      if matches!(ev, Event::LeftCluster)
        && let Some(pl) = state.pending_leave.take()
      {
        // Resolve EVERY joined replier (the initiator plus any racing
        // clones) with `Ok(())`.
        pl.resolve_all(|| Ok(())).await;
      }
      // Payload-bearing events (`UserPacket` / `RemoteStateReceived`) can each
      // own up to `max_stream_frame_size` bytes; size this one for the byte
      // backstop (`None` for small membership / control events).
      let payload_bytes = crate::driver::shared::observation_payload_bytes(&ev);

      // Byte backstop (bounded channels only): the count cap does not bound
      // memory when events carry large reliable payloads. If enqueueing this
      // payload event would push the queued payload bytes over budget, yield
      // once so the observation task can drain (it subtracts as it dequeues),
      // re-check, and drop + count if still over. Draining PAST the dropped
      // event (via `continue`) preserves the obs decoupling — the
      // `ExchangeCompleted` that fires a parked join/leave reply is never
      // delayed by a backed-up delegate.
      if let (Some(budget), Some(bytes)) = (state.obs_payload_budget, payload_bytes) {
        if state.obs_payload_bytes.get().saturating_add(bytes) > budget {
          crate::driver::shared::yield_once().await;
        }
        if state.obs_payload_bytes.get().saturating_add(bytes) > budget {
          state
            .observation_dropped
            .set(state.observation_dropped.get() + 1);
          continue;
        }
      }

      // Hand off to the observation task (delegate dispatch + EventStream
      // forward, off this driver task), non-blocking. `Disconnected` means the
      // task exited at teardown — drop the undeliverable event. `Full` means
      // the bounded channel is at its count capacity; yield once so the
      // observation task can drain (a fast delegate empties it in that
      // quantum), then retry; drop + count ONLY if still full (a slow/stuck
      // delegate) — bounding memory without an indefinite SWIM stall. On a
      // successful enqueue, add the payload bytes to the backstop counter.
      match state.obs_tx.try_send(ev) {
        Ok(()) => crate::driver::shared::add_obs_payload(&state.obs_payload_bytes, payload_bytes),
        Err(mpsc::TrySendError::Closed(_)) => {}
        Err(mpsc::TrySendError::Full(ev)) => {
          crate::driver::shared::yield_once().await;
          match state.obs_tx.try_send(ev) {
            Ok(()) => {
              crate::driver::shared::add_obs_payload(&state.obs_payload_bytes, payload_bytes)
            }
            Err(_) => {
              state
                .observation_dropped
                .set(state.observation_dropped.get() + 1);
            }
          }
        }
      }
    }

    if !iter_progress {
      break;
    }
    any_progress = true;
  }

  any_progress
}

/// Reap deadline-expired entries from `pending_joins`. Called after
/// every drain block. The normal COMPLETION path (a waiter whose
/// `pending` set drained to empty) now resolves inline in the
/// `drain_actions` events step — on the driver task, with observation
/// dispatch off-loop — so a fully-resolved waiter is replied + removed
/// there and is gone from `pending_joins` before this reaper runs (it
/// can never be double-replied here). This reaper handles DEADLINE
/// expiry (a still-non-empty waiter past its `deadline` replies with its
/// partial `contacted`) and the degenerate insert-time-empty case (a
/// `WaitForCompletion` that dispatched zero outbound exchanges never
/// sees an `ExchangeCompleted`, so the empty-`pending` branch resolves
/// it here). Mirrors the stream driver's `reap_pending_joins`
/// (`memberlist-compio/src/driver.rs`).
async fn reap_pending_joins(pending_joins: &mut HashMap<u64, PendingJoin>, now: Instant) {
  // Two-phase: collect ids to reap into a small vec, then drain. The
  // alternative (`retain` + async-in-closure) is not expressible
  // because `retain` is sync. Allocation is bounded by the number of
  // outstanding synchronous-join waiters (typically ≤ 1 per caller).
  let mut to_reap: Vec<u64> = Vec::new();
  for (id, pj) in pending_joins.iter() {
    if pj.pending.is_empty() || now >= pj.deadline {
      to_reap.push(*id);
    }
  }
  for id in to_reap {
    if let Some(pj) = pending_joins.remove(&id) {
      let reply_value = if pj.contacted == 0 {
        Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
          pj.requested,
          0,
        )))
      } else {
        Ok(pj.contacted)
      };
      // Ignoring Err: caller dropped the reply receiver (e.g.
      // the user-facing join_with future was cancelled).
      let _ = pj.reply.send(reply_value);
    }
  }
}

/// Earliest pending-join deadline, if any. Folded into the driver's
/// per-iteration `timeout_deadline` so the select's timer arm fires by
/// the deadline of the first expiring synchronous join even when the
/// coordinator itself has no nearer deadline. Mirrors the stream
/// driver's `min_pending_join_deadline`.
fn min_pending_join_deadline(pending_joins: &HashMap<u64, PendingJoin>) -> Option<Instant> {
  pending_joins.values().map(|pj| pj.deadline).min()
}

/// Reap a deadline-expired graceful-leave waiter.
///
/// Called alongside [`reap_pending_joins`] after every drain block. If
/// `pending_leave`'s `deadline` has elapsed without the machine's
/// `Event::LeftCluster` having resolved it (in `drain_actions`' events
/// step), reply [`MemberlistError::LeaveTimeout`] and clear the slot. A
/// successful `LeftCluster` resolution already cleared it, so this only
/// fires on the timeout path. Mirrors the stream driver's
/// `reap_pending_leave`.
async fn reap_pending_leave(pending_leave: &mut Option<PendingLeave>, now: Instant) {
  if let Some(pl) = pending_leave.as_ref()
    && now >= pl.deadline
  {
    let pl = pending_leave.take().expect("checked Some above");
    // Resolve EVERY joined replier (the initiator plus any racing
    // clones) with `LeaveTimeout`.
    pl.resolve_all(|| Err(MemberlistError::LeaveTimeout)).await;
  }
}

/// Earliest pending-leave deadline, if any. Folded into the driver's
/// per-iteration `timeout_deadline` and past-due trigger (alongside the
/// pending-join deadline) so the leave timeout fires even under a
/// continuous network flood that would otherwise keep the recv arm
/// winning the select. Mirrors the stream driver's
/// `min_pending_leave_deadline`.
fn min_pending_leave_deadline(pending_leave: &Option<PendingLeave>) -> Option<Instant> {
  pending_leave.as_ref().map(|pl| pl.deadline)
}

/// Publish a fresh snapshot of the coordinator's observable state into the
/// single-owner `SnapshotCell`. Readers on the same thread see the new
/// snapshot on their next read. Mirrors the
/// stream driver's `refresh_snapshot`
/// (`memberlist-compio/src/driver.rs`); reaches the inner membership
/// `Endpoint<I, SocketAddr>` via `QuicEndpoint::endpoint_ref()`.
///
/// Each `Arc<NodeState>` in `members_vec` is a freshly-allocated clone of
/// the membership FSM's wire-state entry with `member_liveness` stamped onto
/// the `state` field — cheap because `NodeState::clone` is O(1) (`meta` is
/// `Bytes`, which shares the underlying buffer). The local node entry is
/// taken directly from the membership map so it carries the real meta,
/// incarnation, and protocol versions.
fn refresh_snapshot<I, G>(endpoint: &QuicEndpoint<I, G>, snapshot: &SnapshotCell<I>)
where
  I: memberlist_proto::Id,
{
  let ep = endpoint.endpoint_ref();
  refresh_snapshot_inner::<I, _>(ep, snapshot);
}

/// As [`refresh_snapshot`], but rebuilds + stores only when the endpoint's
/// snapshot version changed since `*last_version` (the rebuild clones every
/// NodeState).
fn refresh_snapshot_if_changed<I, G>(
  endpoint: &QuicEndpoint<I, G>,
  snapshot: &SnapshotCell<I>,
  last_version: &mut u64,
) where
  I: memberlist_proto::Id,
{
  let ep = endpoint.endpoint_ref();
  let v = ep.snapshot_version();
  if v != *last_version {
    *last_version = v;
    refresh_snapshot_inner::<I, _>(ep, snapshot);
  }
}

/// Republish the machine's load-shedding counters if they changed (publish-on-
/// change; a cheap `Copy` compare, allocating only on a real change).
fn refresh_metrics_if_changed<I, G>(
  endpoint: &QuicEndpoint<I, G>,
  metrics: &Rc<Cell<Metrics>>,
  last: &mut Metrics,
) where
  I: memberlist_proto::Id,
{
  let m = endpoint.metrics();
  if m != *last {
    *last = m;
    metrics.set(m);
  }
}

fn refresh_snapshot_inner<I, R>(
  ep: &memberlist_proto::Endpoint<I, SocketAddr, R>,
  snapshot: &SnapshotCell<I>,
) where
  I: memberlist_proto::Id,
{
  // Build a snapshot-local NodeState for each member with the FSM-tracked
  // liveness state (`member_liveness`) rather than the wire-protocol state
  // (`ns.state()`, which is fixed at the last Alive broadcast and does not
  // reflect subsequent Suspect/Dead transitions).
  let mut alive_count: usize = 0;
  let members_vec: Vec<Arc<NodeState<I, SocketAddr>>> = ep
    .members()
    .map(|ns| {
      let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
      if matches!(fsm, State::Alive) {
        alive_count += 1;
      }
      Arc::new((*ns).clone().with_state(fsm))
    })
    .collect();
  let local = ep
    .member(ep.local_id_ref())
    .expect("local node is always in the membership map");
  let member_count = ep.num_members();
  let snap = MemberlistSnapshot::new(
    members_vec,
    local,
    alive_count,
    member_count,
    ep.health_score(),
  );
  *snapshot.borrow_mut() = Rc::new(snap);
}

#[cfg(test)]
mod tests;
