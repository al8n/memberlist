//! Driver task — single-owner of the `StreamEndpoint`, the UDP gossip
//! socket, the TCP reliable listener, and the per-bridge handle table.
//!
//! Runs a `select_biased` loop over six arms in priority order:
//! command-channel, bridge inbound channel, outbound-dial completion,
//! listener accept, gossip UDP recv, and a coordinator-supplied wake
//! timer. After each fired arm the driver drains every outbound surface
//! (`poll_action`, `poll_transport_transmit`, `poll_memberlist_transmit`,
//! `poll_event`) until no method makes progress, publishes a fresh
//! snapshot when state changed, and re-enters the select. The driver
//! owns the `StreamEndpoint` outright; user-facing handles communicate
//! exclusively via the command channel and read state through the
//! lock-free snapshot. The listener drops when the driver loop exits so
//! the bound port is released before `Memberlist::shutdown` returns.

use std::{
  collections::{HashMap, HashSet},
  io,
  net::SocketAddr,
  sync::Arc,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use compio::{
  buf::BufResult,
  net::{TcpListener, TcpStream, UdpSocket},
};
use core::task::{Context, Poll, Waker};
use flume::{Receiver, Sender};

use futures_util::{FutureExt, future::FusedFuture, pin_mut, select_biased};
use memberlist_proto::{
  Instant,
  codec::{
    DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
    parse_messages,
  },
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, StreamId, Transmit},
  streams::{StreamAction, StreamEndpoint, StreamTransport},
  typed::{NodeState, State},
};

use crate::{
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, SetChecksumOptionsCmd, SetCompressionOptionsCmd,
    SetEncryptionOptionsCmd, ShutdownCmd, UpdateNodeMetadataCmd,
  },
  delegate::Delegate,
  driver_options::{DriverOptions, StreamTransportOptions},
  driver_shared::{
    ExchangeId, add_obs_payload, cidr_blocks, dispatch_event_delegate, observation_payload_bytes,
    yield_once,
  },
  error::{JoinAllFailed, MemberlistError, Result},
  snapshot::MemberlistSnapshot,
};

/// Driver-side state for one outstanding synchronous-join call.
///
/// Each [`Memberlist::join_with`](crate::Memberlist::join_with) round
/// trip lands as a [`Command::Join`] carrying
/// [`JoinKind::WaitForCompletion`]. The driver dispatches a push/pull
/// for every seed (matching the fire-and-forget dispatch fan-out) and
/// parks the per-call state here.
///
/// Contact accounting is strictly **per-OUTBOUND-EXCHANGE**, observed
/// via the machine's `Event::ExchangeCompleted` filtered to
/// `ExchangeKind::PushPull` (the broadened event fires for every
/// outbound bridge kind; sync-join consumes only push/pull
/// completions). Each `start_push_pull` allocates a fresh
/// `ExchangeId`; the driver tracks every outbound `ExchangeId` it
/// dispatched and counts each one that terminates with
/// `ExchangeOutcome::Succeeded`. Tracking by `ExchangeId` (rather than
/// by `SocketAddr`) preserves duplicate-seed semantics: passing the
/// same address twice produces two exchanges and two independent
/// counts.
struct PendingJoin {
  /// Set of outbound exchange IDs this waiter dispatched and is still
  /// waiting on a terminal `ExchangeCompleted` for. An `ExchangeId`
  /// is removed when its `ExchangeCompleted` arrives (success or
  /// failure); when this set is empty (and `pending_eids` accounts
  /// for every dispatched exchange) the call has fully resolved.
  pending: HashSet<ExchangeId>,
  /// Number of dispatched outbound exchanges that terminated with
  /// `ExchangeOutcome::Succeeded`. Duplicate seeds produce duplicate
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
/// A [`Command::Leave`] that finds the endpoint Running initiates the
/// machine's `leave()`, which queues the direct `Dead`-self notices to
/// every live peer and withholds [`Event::LeftCluster`] until they have
/// drained through `poll_transmit`. The driver parks this here and
/// replies only once that `LeftCluster` arrives (success) or
/// `deadline` elapses ([`MemberlistError::LeaveTimeout`]) — so a
/// returned `Ok(())` means the leave actually reached the wire, never
/// merely that it was queued. Mirrors the [`PendingJoin`] /
/// `shutdown_reply` parking pattern.
///
/// Leave is a SHARED operation: a second `Command::Leave` racing an
/// in-flight one (cloned `Memberlist` handles can both call `leave()`)
/// does NOT re-invoke `endpoint.leave()` (a repeated leave once already
/// `Leaving`/`Left` is a terminal no-op that emits no completion event,
/// so a fresh parked waiter would hang waiting on a `LeftCluster` that
/// never re-fires). Instead it joins this in-flight operation by pushing
/// its reply onto `repliers`. Every terminal path — `LeftCluster`
/// success, `leave_timeout` reap, shutdown — drains EVERY replier.
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
  reply: futures_channel::oneshot::Sender<Result<core::time::Duration>>,
}

/// Driver-side state for one outstanding reliable directed-send call.
///
/// A single `Command::SendReliable` may dispatch multiple
/// `start_user_message` calls (one per payload). Each call returns a
/// `StreamId`; the driver parks one `PendingUserSend` whose `pending` set
/// tracks the `ExchangeId`s of the in-flight streams. Resolved on
/// `Event::ExchangeCompleted` where `kind() == ExchangeKind::UserMessage`:
/// each completed `ExchangeId` is removed from `pending`; when `pending`
/// empties the reply fires. On driver exit, drained with
/// `Err(MemberlistError::Shutdown)`.
struct PendingUserSend {
  /// Set of `ExchangeId`s this call dispatched and has not yet seen a
  /// terminal `ExchangeCompleted` for. An entry is removed on any terminal
  /// outcome (success or failure). When this set empties the call resolves.
  pending: HashSet<ExchangeId>,
  /// Running count of payload failures for this call. SEEDED at park time
  /// to the count of payloads that never produced a `StreamAction::Connect`
  /// (a pre-`Connect` retirement will never surface an `ExchangeCompleted`,
  /// so it cannot be observed via `pending`); incremented further for each
  /// tracked exchange whose terminal outcome is `Failed`.
  failed: usize,
  /// One-shot reply channel back to the `send_reliable` / `send_many_reliable`
  /// caller. `Ok(())` only if every payload was dispatched AND every exchange
  /// succeeded; `Err(SendFailed)` if any payload failed before `Connect` or
  /// any exchange failed.
  reply: futures_channel::oneshot::Sender<Result<()>>,
}

/// Driver-loop-local state tracking outstanding commands awaiting completion.
///
/// Passed by `&mut` to [`drain_events`] and [`dispatch_command`] so those
/// functions do not each carry four separate mutable parameters for the same
/// cohesive bookkeeping object.
struct PendingCommands {
  /// Outstanding synchronous-join waiters. See [`PendingJoin`].
  joins: Vec<PendingJoin>,
  /// Outstanding graceful-leave waiter (at most one at a time). See [`PendingLeave`].
  leave: Option<PendingLeave>,
  /// Outstanding application-ping waiters. See [`PendingPing`].
  pings: Vec<PendingPing>,
  /// Outstanding reliable directed-send waiters. See [`PendingUserSend`].
  user_sends: Vec<PendingUserSend>,
}

/// Payload for [`BridgeInbound::Bytes`]: a slice of plaintext bytes the
/// per-bridge task read from its stream half, addressed to one specific
/// exchange.
pub(crate) struct BridgeBytes {
  /// Exchange the bytes belong to.
  pub(crate) eid: ExchangeId,
  /// Owned heap-allocated copy of the bytes (the per-bridge task hands the
  /// buffer back to its read loop so it can be reused for the next read).
  pub(crate) bytes: Vec<u8>,
  /// Wall-clock instant at which the bridge observed these bytes on the
  /// socket. Passed to `endpoint.handle_transport_data` as the
  /// observation time so a successful response that arrived BEFORE the
  /// exchange deadline is not retroactively timed-out by the driver's
  /// post-deadline `Instant::now()` sample (the stream FSM's deadline
  /// gate compares the observation time against the per-exchange
  /// deadline at `handle_data_inner`).
  pub(crate) received_at: Instant,
}

/// Payload for [`BridgeInbound::Eof`]: the per-bridge task observed an
/// orderly close (read returned `Ok(0)` or the close-signal arm fired).
pub(crate) struct BridgeEof {
  /// Exchange that hit EOF.
  pub(crate) eid: ExchangeId,
  /// Wall-clock instant at which the bridge observed the peer's FIN.
  /// See [`BridgeBytes::received_at`] for the deadline-gate rationale.
  pub(crate) received_at: Instant,
}

/// Payload for [`BridgeInbound::Error`]: the per-bridge task hit an I/O
/// error on either the read or write half. The `err` is preserved on the
/// payload for the upcoming logger hook (the dead-code allow lets it
/// travel today even though no current consumer reads it).
#[allow(dead_code)]
pub(crate) struct BridgeError {
  /// Exchange that failed.
  pub(crate) eid: ExchangeId,
  /// The underlying I/O error from compio.
  pub(crate) err: io::Error,
  /// Wall-clock instant at which the bridge observed the error. See
  /// [`BridgeBytes::received_at`] for the deadline-gate rationale.
  pub(crate) received_at: Instant,
}

/// Payload for [`BridgeReady::OutboundOk`]: an outbound dial task
/// successfully connected to the peer.
///
/// Carries the `out_rx` allocated at Connect time alongside the stream so
/// the bridge spawned at receipt sees every byte the driver queued via
/// `drain_transport_transmits` between Connect and dial completion (the
/// machine surfaces the first push/pull request on the same tick the
/// Connect lands, well before the OS finishes the TCP handshake).
pub(crate) struct OutboundOkReady {
  /// The exchange the connection belongs to.
  pub(crate) eid: ExchangeId,
  /// The connected stream.
  pub(crate) stream: TcpStream,
  /// The receive half of the bridge's pre-allocated out-channel — the
  /// driver pushed pre-handshake bytes into the matching `out_tx` while
  /// the dial was in flight.
  pub(crate) out_rx: Receiver<BridgeOut>,
  /// The receive half of the bridge's pre-allocated cancel channel — a
  /// `StreamAction::Abort` for this exchange while the dial was in flight
  /// signals the matching `cancel_tx`, so the bridge breaks without
  /// draining `out_rx`.
  pub(crate) cancel_rx: futures_channel::oneshot::Receiver<()>,
}

/// Payload for [`BridgeReady::OutboundFail`]: an outbound dial task hit a
/// connect error. The `err` is preserved on the payload for the upcoming
/// logger hook (the dead-code allow lets it travel today even though no
/// current consumer reads it).
#[allow(dead_code)]
pub(crate) struct OutboundFailReady {
  /// The exchange whose dial failed.
  pub(crate) eid: ExchangeId,
  /// The connect error.
  pub(crate) err: io::Error,
  /// Wall-clock instant at which the dial task observed the failure
  /// (either the connect error or the dial-timeout fire). See
  /// [`BridgeBytes::received_at`] for the deadline-gate rationale —
  /// preserving this lets the FSM observe a pre-deadline dial failure
  /// as a clean EOF terminalization rather than reject as Timeout.
  pub(crate) received_at: Instant,
}

/// Messages an outbound dial task sends back to the driver.
///
/// The driver's response per variant:
/// - `OutboundOk` → if the exchange's [`BridgeHandle`] is still in the
///   driver's table, spawn a per-bridge byte-mover; otherwise drop the
///   stream (the exchange was retired while the dial was in flight).
/// - `OutboundFail` → terminalize the exchange as a dial failure
///   (`handle_dial_failed(eid, now)`); a never-connected dial must FAIL the
///   exchange — a benign EOF would let a one-way `UserMessage` send falsely
///   report success. A no-op if the exchange is already gone.
///
/// Inbound connections are NOT routed through this channel; the driver
/// owns the [`TcpListener`] directly and processes accept results in
/// its `accept` select arm.
pub(crate) enum BridgeReady {
  /// An outbound dial completed successfully.
  OutboundOk(OutboundOkReady),
  /// An outbound dial failed.
  OutboundFail(OutboundFailReady),
}

/// Messages a per-bridge task sends back to the driver.
///
/// The driver routes each variant into the appropriate `StreamEndpoint`
/// entry-point:
/// - `Bytes` → `handle_transport_data(eid, bytes, eof=false, now)`.
/// - `Eof` → `handle_transport_data(eid, &[], eof=true, now)`.
/// - `Error` → `handle_transport_error(eid, now)` — a transport error fails the
///   bridge rather than taking the benign-EOF path that would falsely complete a
///   one-way UserMessage as success.
pub(crate) enum BridgeInbound {
  /// Plaintext bytes read from the stream.
  Bytes(BridgeBytes),
  /// Orderly close.
  Eof(BridgeEof),
  /// Unrecoverable I/O error.
  Error(BridgeError),
}

/// Messages the driver sends to a per-bridge byte-mover task.
///
/// All variants share one channel so the bridge processes them in FIFO
/// order: every byte queued before a `ShutdownWrite` / `Close` is written
/// to the peer before the close signal fires. A dual-channel design
/// (separate transmit + control senders) would let `select!` resolve the
/// close arm first when both are ready and orphan in-flight bytes.
///
/// `ShutdownWrite` and `Close` are distinct because the push/pull
/// exchange requires a half-close: the requester writes its push, calls
/// `shutdown(write)` so the peer's read side sees FIN and knows the
/// request is complete, then the peer's response is read on the still-
/// open read half. A single full-close after writing would tear down the
/// read half before the response arrives and the exchange would time
/// out.
pub(crate) enum BridgeOut {
  /// Outbound bytes the coordinator surfaced via
  /// [`StreamEndpoint::poll_transport_transmit`].
  Bytes(Vec<u8>),
  /// Half-close the write side of the bridge's stream. The bridge calls
  /// `AsyncWrite::shutdown` on the write half, then continues reading
  /// from the peer. Driven by [`StreamAction::Shutdown`] from the
  /// coordinator (FIN-on-send-half anchor; `streams/mod.rs` docs).
  ShutdownWrite,
  /// Full close — the bridge sends `BridgeInbound::Eof` to the driver
  /// and exits its loop. Driven by [`StreamAction::Close`] from the
  /// coordinator (terminal teardown).
  Close,
}

/// Per-bridge handle the driver owns to communicate with the byte-mover.
///
/// Two channels: the FIFO `out_tx` for bytes + graceful control (see
/// [`BridgeOut`] for the flush-then-close ordering guarantee), and an
/// out-of-band `cancel_tx` priority signal for a hard abort. The bridge
/// task is detached at spawn time.
///
/// A graceful [`StreamAction::Close`] drives a flush-then-exit teardown:
/// the bridge processes its queued `Bytes` in FIFO order, pulls the
/// trailing `BridgeOut::Close`, and exits, so the last bytes of a normal
/// push/pull exchange reach the wire.
///
/// A failed [`StreamAction::Abort`] drives a hard cancel via `cancel_tx`:
/// the bridge's `select!` resolves the cancel arm with PRIORITY over both
/// reads and the `out_tx` FIFO, so it breaks IMMEDIATELY without draining
/// any `Bytes` still queued in `out_tx` and drops its write half. This is
/// the trust-boundary cutoff for a failed exchange (an elapsed deadline,
/// or a `set_encryption_options` mid-flight policy change that rejected the
/// bridge): stale bytes — including any encoded under a prior encryption
/// policy — are discarded rather than written, so a policy change cannot
/// leak post-enablement plaintext.
struct BridgeHandle {
  /// Bytes-or-graceful-control FIFO into the bridge task.
  out_tx: Sender<BridgeOut>,
  /// Out-of-band hard-abort signal. Sent by the `StreamAction::Abort` arm;
  /// the bridge selects on it with priority and breaks without draining
  /// `out_tx`. A one-shot: a single send is enough to cancel.
  cancel_tx: futures_channel::oneshot::Sender<()>,
}

/// Hard ceiling on the per-recv UDP buffer. UDP's wire payload is
/// capped at 65507 bytes (the IP-layer maximum once the 8-byte UDP
/// header and 20-byte IPv4 header — or 40 for IPv6 — are deducted),
/// so a recv buffer larger than that just wastes an allocation per
/// iteration. The driver clamps the
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`-derived size at this
/// ceiling.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

/// Compute the per-recv UDP buffer size from the coordinator's
/// configured `gossip_mtu`. A datagram on the wire is at most
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD` bytes when encryption
/// is enabled (the wrapper carries the algorithm tag + nonce + AEAD
/// auth tag); the driver sizes both the normal recv buffer and the
/// past-due peek recv buffer to that value so a configured
/// `with_gossip_mtu` above the historical 16 KiB default is not
/// silently truncated by the kernel.
fn gossip_recv_buf_len<I, A, R>(endpoint: &StreamEndpoint<I, A, R>) -> usize
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  endpoint
    .gossip_mtu()
    .saturating_add(memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD)
    .saturating_add(memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD)
    .min(GOSSIP_RECV_BUF_MAX)
}

/// Single-owner driver task.
///
/// Drives the `StreamEndpoint` until the command channel closes (all
/// `Memberlist` handles dropped) or a [`Command::Shutdown`] is received.
/// All mutations on the endpoint happen here; reads happen lock-free via
/// the published snapshot.
///
/// ## Connection lifecycle
///
/// The driver owns the [`TcpListener`] directly — inbound connections
/// surface in the loop's `accept` select arm without a separate task,
/// so the listener drops at the same instant the driver loop exits and
/// the port is released before [`Memberlist::shutdown`] returns.
///
/// Outbound dials run in spawned tasks per [`StreamAction::Connect`];
/// the dial task calls [`TcpStream::connect`] off-loop and sends a
/// [`BridgeReady::OutboundOk`] (or `OutboundFail`) back through the
/// `bridge_ready` channel.
///
/// Both transports (`R = RawRecords` for TCP, `R = Labeled<TlsRecords>`
/// for TLS) use a raw [`TcpStream`] as the wire — the TLS handshake bytes
/// flow through the same byte path as application bytes; the record-layer
/// codec inside [`StreamEndpoint::handle_transport_data`] internally
/// distinguishes handshake from application data.
///
/// ## Iter-top branching
///
/// Each loop iteration runs through up to four branches depending on
/// pending state — they are NOT redundant; each handles a distinct
/// condition that cannot be folded into the others without sacrificing
/// either correctness or fairness:
///
/// - **exit**: a `Command::Shutdown` was observed; flush every output
///   surface one last time and break.
/// - **past-due**: `setup_now >= timeout_deadline`. Under a
///   continuous UDP-recv flood the select's recv arm always wins over
///   the timer (recv is arm 1, timer is arm 2). The past-due branch
///   preempts the select so timer correctness is preserved even
///   under recv pressure; its UDP-peek (1ms budget) gives recv
///   exactly one shot per iteration before the deadline fires.
/// - **dirty**: drained inputs mutated state but no deadline fired;
///   flush every output surface so the snapshot republish reflects
///   the post-drain state before the select arms.
/// - **plain select**: the normal "wait for next event" path.
///
/// The same per-iteration drain block (`drain_actions` →
/// `drain_transport_transmits` → `drain_transmits` → `drain_events`,
/// looped to quiescence + `reap_pending_joins`) runs at the end of
/// each branch; the structure isn't accidental complexity — it's the
/// minimal expression of "service every I/O surface, then advance
/// the FSM, then publish".
///
/// `#[allow(clippy::too_many_arguments)]`: the parameter list is a
/// composition of the coordinator + every channel the per-transport
/// `run()` body owns. Packing into a struct would just shuffle the names
/// across the call site without reducing the actual coupling — the
/// per-transport `Transport::run` body (TCP / TLS) is the only caller and
/// reads cleaner with positional args.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stream_driver_loop<I, A, R, D>(
  mut endpoint: StreamEndpoint<I, A, R>,
  gossip_socket: UdpSocket,
  listener: TcpListener,
  commands: Receiver<Command<I>>,
  events_tx: Sender<Event<I, A>>,
  events_dropped: Arc<std::sync::atomic::AtomicU64>,
  observation_dropped: Arc<std::sync::atomic::AtomicU64>,
  snapshot: Arc<ArcSwap<MemberlistSnapshot<I, A>>>,
  metrics: Arc<ArcSwap<memberlist_proto::metrics::Metrics>>,
  bridge_ready_rx: Receiver<BridgeReady>,
  bridge_ready_tx: Sender<BridgeReady>,
  shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
  driver_opts: DriverOptions,
  stream_opts: StreamTransportOptions,
  delegate: D,
  // Cluster label applied to both gossip encode and decode: outbound gossip
  // is stamped with this label and inbound gossip must carry a matching label.
  // `None` disables per-cluster labeling, accepting datagrams from any cluster.
  label: Option<Bytes>,
  // Driver-level CIDR filter: drop gossip datagrams from a blocked source and
  // reject reliable streams from a blocked peer. `()` without the cidr feature.
  cidr_policy: crate::transport::runtime::CidrFilter,
) where
  D: Delegate<Id = I, Address = A>,
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + From<SocketAddr>
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let (bridge_inbound_tx, bridge_inbound_rx) =
    flume::bounded::<BridgeInbound>(stream_opts.bridge_inbound_cap());

  // Spawn the per-driver observation task. It owns the user `Delegate`
  // and the `EventStream` sender and runs OFF this driver task: the
  // driver `try_send`s every surfaced event onto `obs_tx`, the task
  // dispatches the matching observation hook then forwards to
  // subscribers. Decoupling observation from the driver loop keeps a
  // slow `notify_*` / `merge_remote_state` from stalling protocol
  // advancement — and therefore from delaying a parked join/leave reply
  // that depends on a follow-up input the driver's arms must still
  // service. The `obs_tx` queue is sized per
  // [`DriverOptions::observation_channel`] (default [`Channel::Bounded`]).
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
    crate::Channel::Unbounded => flume::unbounded::<Event<I, A>>(),
    crate::Channel::Bounded(n) => flume::bounded::<Event<I, A>>(n),
  };
  // The two drop counters are kept distinct so a consumer can tell recoverable
  // from potentially-unrecoverable loss: the observation task increments
  // `events_dropped` on a bounded EventStream-forward drop (membership/control,
  // recoverable from the snapshot), and the driver loop increments
  // `observation_dropped` on a `Bounded` obs-channel drop (the delegate fell
  // behind; may have lost app-data — see `drain_events`). Clone `events_dropped`
  // for the task; thread `observation_dropped` into `drain_events`.
  //
  // `obs_payload_bytes` tracks the bytes of payload-bearing events
  // (`UserPacket` / `RemoteStateReceived`) currently queued in `obs_tx`: the
  // driver adds on enqueue, the observation task subtracts on dequeue. The byte
  // backstop in `drain_events` uses it to bound the memory large reliable
  // payloads occupy while a delegate falls behind — a count cap alone cannot,
  // since one event can own a `max_stream_frame_size` payload.
  let obs_payload_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
  compio::runtime::spawn(observation_task::<I, A, D>(
    obs_rx,
    delegate,
    events_tx,
    events_dropped.clone(),
    obs_payload_bytes.clone(),
  ))
  .detach();

  // Stash for the [`Command::Shutdown`] reply sender — see
  // [`dispatch_command`]. Ack lands AFTER the post-loop cleanup drops
  // the listener and the gossip socket so the bound ports are free
  // when the caller resumes from `shutdown.await`.
  let mut shutdown_reply: Option<futures_channel::oneshot::Sender<Result<()>>> = None;
  // Outstanding commands awaiting completion. Bundled into one struct
  // and passed by `&mut` to [`dispatch_command`] and [`drain_events`].
  // See [`PendingCommands`] for per-field documentation.
  let mut pending = PendingCommands {
    joins: Vec::new(),
    leave: None,
    pings: Vec::new(),
    user_sends: Vec::new(),
  };

  // Per-driver UDP recv buffer size, derived from the coordinator's
  // configured `gossip_mtu` + encrypted wrapper overhead. Computed
  // once at loop entry — gossip_mtu is fixed for the endpoint
  // lifetime, so re-computing per iter would just allocate the same
  // value.
  let recv_buf_len = gossip_recv_buf_len::<I, A, R>(&endpoint);

  // Observation-channel payload byte backstop. The `Bounded(n)` count cap
  // bounds the NUMBER of queued events, but one `UserPacket` /
  // `RemoteStateReceived` can own up to `max_stream_frame_size` bytes, so `n`
  // large reliable payloads could still OOM. Cap the bytes of queued
  // payload-bearing events at four frames' worth: a small burst of large
  // payloads is absorbed, but a stuck delegate cannot accumulate without bound.
  // `Unbounded` opts out of dropping entirely, so it opts out of the byte
  // backstop too (budget `None`).
  let obs_payload_budget: Option<u64> = match driver_opts.observation_channel() {
    crate::Channel::Bounded(_) => Some((endpoint.max_stream_frame_size() as u64).saturating_mul(4)),
    crate::Channel::Unbounded => None,
  };

  refresh_snapshot::<I, A, R>(&endpoint, &snapshot);
  // Track the published snapshot version so the per-iteration republishes below
  // rebuild only on an actual membership/health change.
  let mut last_snapshot_version = endpoint.endpoint_ref().snapshot_version();
  // Same publish-on-change for the load-shedding counters.
  let mut last_metrics = endpoint.endpoint_ref().metrics();

  // Arm the periodic probe / gossip / push-pull schedulers. Without this
  // the machine's `next_probe` / `next_gossip` / `next_pushpull` stay
  // `None`, so failure detection, broadcast dissemination, and anti-entropy
  // never run — the loop would only service explicit join push-pull and
  // passive inbound traffic.
  endpoint.start_scheduling(Instant::now());

  // Hoist the listener-accept future ACROSS loop iterations. On a
  // completion-based backend (io_uring) `accept()` is an in-flight SQE; if it
  // were recreated each iteration like the other select arms, every wakeup on a
  // non-accept arm would DROP it, cancelling the accept — and a connection the
  // kernel already accepted into a fresh fd is then closed, surfacing to the
  // peer as a reset mid-handshake. Under concurrent inbound joins that loses a
  // large fraction of connections. Persisting the future means it is only ever
  // recreated when it RESOLVES (below), never dropped mid-flight, so no accepted
  // connection is silently discarded. A readiness-based backend (kqueue/epoll)
  // would not lose the connection either way, but this keeps both correct.
  let mut accept_fut = Box::pin(listener.accept().fuse());

  loop {
    let mut dirty = false;
    let mut exit = false;

    // Re-arm a resolved accept, then SERVICE any already-ready accept before the
    // select. `accept_fut` is hoisted above the loop so a competing select arm
    // never drops an in-flight accept (which on io_uring would cancel a kernel-
    // accepted connection) — but it sits at the THIRD biased select arm, so
    // without draining it here a continuous recv/timer-ready socket could hold a
    // kernel-accepted connection unbridged until the peer's handshake deadline.
    // Polling it here, off the select's borrow, gives accept bounded fairness
    // symmetric with the iter-top cmd drain below. The throwaway-waker poll is an
    // early check; the select re-registers the real waker if an accept is still
    // pending. Capped by `iter_drain_cap` so an accept flood cannot starve the
    // other arms in turn.
    if accept_fut.is_terminated() {
      accept_fut.set(listener.accept().fuse());
    }
    {
      let mut accept_cx = Context::from_waker(Waker::noop());
      let mut accepted_n = 0;
      // Floor the accept budget at 1. `iter_drain_cap == 0` is a supported config
      // (it makes the bridge drains rendezvous through their select arms), but the
      // accept arm has no equivalent fallback — leaving it at 0 would reopen the
      // recv/timer starvation this drain closes — so at least one ready accept is
      // always serviced per iteration regardless of the configured cap.
      while accepted_n < driver_opts.iter_drain_cap().max(1) {
        match accept_fut.as_mut().poll(&mut accept_cx) {
          Poll::Ready(accepted) => {
            if handle_accepted::<I, A, R>(
              accepted,
              &mut endpoint,
              &mut bridges,
              &bridge_inbound_tx,
              &cidr_policy,
              stream_opts,
            ) {
              dirty = true;
            }
            accept_fut.set(listener.accept().fuse());
            accepted_n += 1;
          }
          Poll::Pending => break,
        }
      }
    }

    // Iter-top command fairness drain. The `cmd` select arm sits at
    // MEDIUM priority so a network flood does not let a cmd flood
    // starve recv / timer / accept; but the symmetric concern is a
    // network flood starving `cmd` — under continuous recv pressure
    // the cmd arm could wait many iterations between fires, and
    // `shutdown` would not land promptly. The iter-top drain pulls
    // up to CMD_FAIRNESS_BUDGET commands via try_recv every loop
    // pass so user calls always make bounded progress. Capped so a
    // cmd flood itself cannot starve the network arms via this
    // path either.
    let mut cmd_drained = 0;
    while cmd_drained < driver_opts.cmd_fairness_budget() {
      match commands.try_recv() {
        Ok(c) => {
          let now = Instant::now();
          let is_shutdown = matches!(c, Command::Shutdown(_));
          if is_shutdown {
            exit = true;
          }
          dispatch_command::<I, A, R>(
            &mut endpoint,
            &mut bridges,
            &bridge_ready_tx,
            stream_opts,
            &cidr_policy,
            &mut shutdown_reply,
            &mut pending,
            driver_opts.leave_timeout(),
            c,
            now,
          )
          .await;
          cmd_drained += 1;
          dirty = true;
          if is_shutdown {
            // Shutdown is terminal — stop draining further commands on
            // this iteration. Any commands queued behind the shutdown
            // will observe the driver loop already exited (the
            // `commands_rx` drops when `driver_loop` returns, so a
            // racing `send_async` returns `Err(Disconnected)` and the
            // caller surfaces `MemberlistError::CommandSend`). Acking a
            // subsequent UpdateMeta / SetCompression / etc. after a
            // shutdown has been observed would let the caller believe
            // its mutation landed when the driver is about to drop
            // everything.
            break;
          }
        }
        Err(_) => break,
      }
    }

    // CRITICAL: drain every already-arrived bridge input BEFORE the
    // timeout decision. A peer's push/pull response (or a bridge's
    // EOF / error) that the per-bridge task already pushed into
    // `bridge_inbound_rx`, AND an outbound-dial completion already in
    // `bridge_ready_rx`, MUST be applied to the coordinator before
    // any `handle_timeout` call — otherwise the timeout sweep would
    // wrongly mark exchanges as overdue whose terminating bytes are
    // already in the channel.
    //
    // `try_recv` is non-blocking and the loop drains until the
    // channels are empty (or the per-channel cap is hit). UDP gossip
    // recv and TCP listener accept are kernel-buffered, so a few-
    // iteration lag for those is acceptable (gossip is lossy;
    // accept-on-next-tick is fine).
    let mut drained = 0;
    while drained < driver_opts.iter_drain_cap() {
      match bridge_inbound_rx.try_recv() {
        Ok(inbound) => {
          dispatch_bridge_inbound::<I, A, R>(&mut endpoint, inbound);
          drained += 1;
          dirty = true;
        }
        Err(_) => break,
      }
    }
    drained = 0;
    while drained < driver_opts.iter_drain_cap() {
      match bridge_ready_rx.try_recv() {
        Ok(ready) => {
          handle_bridge_ready::<I, A, R>(
            &mut endpoint,
            &mut bridges,
            &bridge_inbound_tx,
            ready,
            stream_opts.bridge_recv_buf_len(),
            stream_opts.close_timeout(),
          );
          drained += 1;
          dirty = true;
        }
        Err(_) => break,
      }
    }

    // Honor `exit` set by the iter-top cmd drain BEFORE entering the
    // past-due check or the select. A quiet shutdown (no network
    // activity) would otherwise block in the select for up to
    // IDLE_WAKE_INTERVAL (60s) before checking exit after the timer
    // arm fires. Run the drain phase + snapshot so the post-shutdown
    // state is flushed, then break.
    if exit {
      // Exit-path snapshot gate (mirror-symmetric with the QUIC
      // driver): publish a fresh snapshot iff the teardown drain
      // itself produced observable progress, NOT on the iteration
      // `dirty` flag. The drain-progress signal is the precise
      // "did teardown change published state" question; gating on it
      // (in both drivers) avoids a redundant publish when nothing
      // drained and keeps the two exit paths identical in shape.
      let mut drained_any = false;
      loop {
        let did_actions = drain_actions::<I, A, R>(
          &mut endpoint,
          &mut bridges,
          &bridge_ready_tx,
          stream_opts,
          &cidr_policy,
        );
        let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
        let did_transmits =
          drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket, label.clone()).await;
        let did_events = drain_events(
          &mut endpoint,
          &obs_tx,
          &observation_dropped,
          &obs_payload_bytes,
          obs_payload_budget,
          &mut pending,
        )
        .await;
        if !(did_actions || did_transports || did_transmits || did_events) {
          break;
        }
        drained_any = true;
      }
      reap_pending_joins(&mut pending.joins, Instant::now()).await;
      reap_pending_leave(&mut pending.leave, Instant::now()).await;
      if drained_any {
        refresh_snapshot_if_changed::<I, A, R>(&endpoint, &snapshot, &mut last_snapshot_version);
      }
      // Publish the counters unconditionally before exit: a load-shed (e.g. a
      // rejected accept at the max_inbound_streams cap) can bump them on a path
      // that produced no drainable work, so gating on `drained_any` would drop
      // the final increment from the metrics a caller inspects after shutdown.
      refresh_metrics_if_changed::<I, A, R>(&endpoint, &metrics, &mut last_metrics);
      break;
    }

    // Re-poll the deadline AFTER applying the drained inputs — the
    // applied bytes / completions may have advanced or cleared the
    // pending deadline. Fold in the earliest pending-join AND
    // pending-leave deadline so the timer arm fires by the first
    // expiring synchronous join / graceful-leave even when the
    // coordinator itself has no nearer deadline; the past-due path's
    // subsequent `handle_timeout` re-gate (after the peek) guards
    // against calling the coordinator with no real endpoint-side work.
    let setup_now = Instant::now();
    let endpoint_deadline = endpoint
      .poll_timeout()
      .unwrap_or(setup_now + driver_opts.idle_wake_interval());
    let timeout_deadline = [
      Some(endpoint_deadline),
      min_pending_join_deadline(&pending.joins),
      min_pending_leave_deadline(&pending.leave),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(endpoint_deadline);

    // BOUNDED past-due preemption. Under a continuous UDP-recv flood
    // the main `select_biased!`'s `recv` arm always wins over `timer`
    // (recv comes earlier in source order), so `handle_timeout` would
    // never fire and stale bridges / overdue probes would never be
    // reaped. Under a clean past-due event a kernel-buffered probe
    // Ack would resolve the deadline; firing `handle_timeout` without
    // applying it would wrongly suspect the peer.
    //
    // Bounded preemption gives recv exactly ONE shot per past-due
    // iteration: the peek-select polls `recv` first (so a buffered
    // datagram wins) and falls through immediately on `zero_now` if
    // recv is pending. After the peek the deadline is re-polled (the
    // buffered datagram may have resolved it); `handle_timeout` fires
    // only if the deadline is STILL past. The bound is the key
    // property — continuous recv readiness applies one datagram then
    // hands the timer its tick, so neither starves the other.
    if setup_now >= timeout_deadline {
      // The past-due peek pays at most `peek_budget` per iteration to
      // honor the "gossip-before-handle_timeout" invariant. Under a
      // continuous UDP-recv flood the main `select_biased!`'s `recv`
      // arm always wins over `timer` (recv comes earlier in source
      // order), so `handle_timeout` would never fire and stale
      // bridges / overdue probes would never be reaped. The peek
      // gives `recv` exactly ONE shot per past-due iteration: the
      // select polls `recv` first (so a buffered datagram wins) and
      // falls through immediately on `zero_now` if recv is pending.
      //
      // The peek timer must be a real sleep (not `future::ready(())`)
      // so io_uring has time to complete a freshly-submitted recv
      // SQE. On completion-based io_uring the recv is ALWAYS Pending
      // on first poll — the SQE has to be submitted, then the
      // runtime processes the completion. A zero-duration ready
      // future would always win the select on io_uring, the recv
      // would be dropped + cancelled, and a kernel-buffered Ack
      // would be lost. The 1ms default is comfortably above
      // io_uring's completion latency for a buffered recv (~100µs)
      // while keeping the per-iteration cost negligible; see
      // [`crate::DEFAULT_PEEK_BUDGET`].
      let peek_buf = vec![0u8; recv_buf_len];
      let peek_recv = gossip_socket.recv_from(peek_buf).fuse();
      let peek_timer = compio::time::sleep(driver_opts.peek_budget()).fuse();
      pin_mut!(peek_recv, peek_timer);
      select_biased! {
        gossip = peek_recv => {
          let BufResult(res, buf) = gossip;
          // CIDR: drop a datagram from a blocked source before any decode.
          if let Ok((n, src)) = res
            && !cidr_blocks(&cidr_policy, src.ip())
          {
            let now = Instant::now();
            dispatch_gossip::<I, A, R>(&mut endpoint, src, &buf[..n], now, label.clone());
            dirty = true;
          }
        }
        _ = peek_timer => {
          // No buffered datagram surfaced within the peek budget;
          // fall through to the deadline-firing protocol below.
        }
      }

      // Apply the deadline-firing protocol: drain bridge completions
      // (no cap), re-poll the deadline, fire handle_timeout iff still
      // past. See `fire_timeout_with_drain` for the full rationale.
      if fire_timeout_with_drain::<I, A, R>(
        &mut endpoint,
        &mut bridges,
        &bridge_inbound_tx,
        &bridge_inbound_rx,
        &bridge_ready_rx,
        driver_opts,
        stream_opts,
      ) {
        dirty = true;
      }

      loop {
        let did_actions = drain_actions::<I, A, R>(
          &mut endpoint,
          &mut bridges,
          &bridge_ready_tx,
          stream_opts,
          &cidr_policy,
        );
        let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
        let did_transmits =
          drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket, label.clone()).await;
        let did_events = drain_events(
          &mut endpoint,
          &obs_tx,
          &observation_dropped,
          &obs_payload_bytes,
          obs_payload_budget,
          &mut pending,
        )
        .await;
        if !(did_actions || did_transports || did_transmits || did_events) {
          break;
        }
      }
      reap_pending_joins(&mut pending.joins, Instant::now()).await;
      reap_pending_leave(&mut pending.leave, Instant::now()).await;
      if dirty {
        refresh_snapshot_if_changed::<I, A, R>(&endpoint, &snapshot, &mut last_snapshot_version);
        refresh_metrics_if_changed::<I, A, R>(&endpoint, &metrics, &mut last_metrics);
      }
      if exit {
        break;
      }
      continue;
    }

    // The drained inputs may have advanced state without past-due
    // timer pressure — run the drain phase to flush their outputs
    // before entering the select. Without this flush a snapshot
    // observer would not see the post-input state until some later
    // arm fires.
    if dirty {
      loop {
        let did_actions = drain_actions::<I, A, R>(
          &mut endpoint,
          &mut bridges,
          &bridge_ready_tx,
          stream_opts,
          &cidr_policy,
        );
        let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
        let did_transmits =
          drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket, label.clone()).await;
        let did_events = drain_events(
          &mut endpoint,
          &obs_tx,
          &observation_dropped,
          &obs_payload_bytes,
          obs_payload_budget,
          &mut pending,
        )
        .await;
        if !(did_actions || did_transports || did_transmits || did_events) {
          break;
        }
      }
      reap_pending_joins(&mut pending.joins, Instant::now()).await;
      reap_pending_leave(&mut pending.leave, Instant::now()).await;
      refresh_snapshot_if_changed::<I, A, R>(&endpoint, &snapshot, &mut last_snapshot_version);
      refresh_metrics_if_changed::<I, A, R>(&endpoint, &metrics, &mut last_metrics);
      dirty = false;
    }

    // Per-iteration fresh allocation: the recv future owns the buffer by
    // value while pending and the buffer is returned via `BufResult`. On
    // cancellation (any non-recv arm fires first) the in-flight syscall
    // is dropped and the buffer is freed; the next iteration allocates
    // fresh. Pool-recycling would require an unnameable-future slot for
    // marginal alloc savings.
    let recv_buf = vec![0u8; recv_buf_len];
    let recv_fut = gossip_socket.recv_from(recv_buf).fuse();
    let cmd_fut = commands.recv_async().fuse();
    let bridge_in_fut = bridge_inbound_rx.recv_async().fuse();
    let ready_fut = bridge_ready_rx.recv_async().fuse();
    let timer_fut = compio::time::sleep_until(timeout_deadline.into_std()).fuse();
    pin_mut!(recv_fut, cmd_fut, bridge_in_fut, ready_fut, timer_fut);

    // Arm priority (top → bottom; `select_biased!` resolves the first
    // ready arm in source order):
    //
    // 1. recv     — kernel-buffered UDP datagrams. MUST come before
    //               timer so an Ack already in the kernel buffer
    //               resolves a probe deadline before handle_timeout
    //               marks the peer suspect.
    // 2. timer    — past-due deadline. MUST come before accept so a
    //               saturated listener cannot starve the handshake /
    //               suspicion / probe reapers. In the main select the
    //               timer is usually pending (the iter-top past-due
    //               branch handles already-elapsed deadlines), so the
    //               position only matters under a same-tick race.
    // 3. accept   — inbound TCP connections. Front-door for new
    //               exchanges; higher priority than user commands so
    //               a peer's join handshake is never delayed by a
    //               user-side command flood.
    // 4. cmd      — user commands. Demoted below network arms so a
    //               cloned-handle command flood (many concurrent
    //               update_meta / set_*_options / etc.) cannot starve
    //               recv / timer / accept. The cmd path is always
    //               bounded (one command → one ack via one-shot
    //               reply), and the cmd channel is unbounded, so
    //               commands are still drained promptly when network
    //               arms are pending.
    // 5. ready    — outbound-dial completions (drained at iter top
    //               with cap ITER_DRAIN_CAP).
    // 6. bridge_in — per-bridge byte messages (drained at iter top
    //                with cap ITER_DRAIN_CAP). LOWEST priority so a
    //                continuous reliable-stream pressure cannot
    //                starve any of the above arms.
    select_biased! {
      gossip = recv_fut => {
        let BufResult(res, buf) = gossip;
        match res {
          // CIDR: drop a datagram from a blocked source before any decode
          // (this guard is always false without the cidr feature).
          Ok((_n, src)) if cidr_blocks(&cidr_policy, src.ip()) => {}
          Ok((n, src)) => {
            let now = Instant::now();
            dispatch_gossip::<I, A, R>(&mut endpoint, src, &buf[..n], now, label.clone());
            dirty = true;
          }
          Err(_) => {
            // Best-effort logging point would go here; a transient recv
            // error (ICMP unreachable surfacing as a syscall error on
            // Linux, EAGAIN, etc.) is non-fatal — the next iteration
            // re-arms recv with a fresh buffer.
          }
        }
      }
      _ = timer_fut => {
        // `select_biased!` orders `timer_fut` ahead of both
        // `ready_fut` and `bridge_in_fut`, so a completion that
        // became ready BEFORE the deadline (its `received_at`
        // already pre-deadline) but raced the timer-fire would
        // otherwise lose to `handle_timeout` and surface as
        // `ExchangeCompleted(Failed)` despite the success being
        // queued. The shared `fire_timeout_with_drain` helper drains
        // every already-arrived completion (no cap), re-polls the
        // deadline, and fires `handle_timeout` only if still past.
        if fire_timeout_with_drain::<I, A, R>(
          &mut endpoint,
          &mut bridges,
          &bridge_inbound_tx,
          &bridge_inbound_rx,
          &bridge_ready_rx,
          driver_opts,
          stream_opts,
        ) {
          dirty = true;
        }
      }
      accepted = accept_fut.as_mut() => {
        // A connection that landed while the select was parked. Ready accepts
        // are normally serviced by the iter-top drain; this arm wakes the loop
        // for one that arrives mid-await. The resolved future is re-armed at the
        // loop top.
        if handle_accepted::<I, A, R>(
          accepted,
          &mut endpoint,
          &mut bridges,
          &bridge_inbound_tx,
          &cidr_policy,
          stream_opts,
        ) {
          dirty = true;
        }
      }
      cmd = cmd_fut => {
        match cmd {
          Ok(c) => {
            exit = matches!(c, Command::Shutdown(_));
            // Refresh `now` at the moment of the actual state mutation.
            // Using the loop-top timestamp would feed a stale `now` into
            // the machine — any deadline computed off it would be off by
            // the time the arm sat in `select!`.
            let now = Instant::now();
            dispatch_command::<I, A, R>(
              &mut endpoint,
              &mut bridges,
              &bridge_ready_tx,
              stream_opts,
              &cidr_policy,
              &mut shutdown_reply,
              &mut pending,
              driver_opts.leave_timeout(),
              c,
              now,
            ).await;
            dirty = true;
          }
          // All `Memberlist` handles dropped → the channel is closed.
          // Treat the same as an explicit shutdown.
          Err(_) => {
            exit = true;
          }
        }
      }
      ready = ready_fut => {
        if let Ok(ready) = ready {
          handle_bridge_ready::<I, A, R>(
            &mut endpoint,
            &mut bridges,
            &bridge_inbound_tx,
            ready,
            stream_opts.bridge_recv_buf_len(),
            stream_opts.close_timeout(),
          );
          dirty = true;
        }
        // Ignoring Err: the bridge-ready channel's only other producer
        // is the outbound dial tasks. The driver holds its own
        // `bridge_ready_tx` clone, so the channel cannot disconnect
        // while the loop is alive — this catch-all preserves the arm
        // shape without a hot-path branch.
      }
      bi = bridge_in_fut => {
        if let Ok(inbound) = bi {
          dispatch_bridge_inbound::<I, A, R>(&mut endpoint, inbound);
          dirty = true;
        }
        // Ignoring Err: the bridge inbound channel's producers are the
        // per-bridge byte-mover tasks; an error here means every
        // bridge has dropped its sender. Subsequent iterations will
        // either receive more bridge events (if a fresh bridge is
        // spawned) or wake on other arms.
      }
    }

    // Drain every outbound surface in the documented order from
    // `streams/mod.rs` ("drain actions, drain transport-transmits, drain
    // memberlist transmits, drain events, sleep until `poll_timeout`,
    // repeat until no method makes progress"):
    //
    // 1. `poll_action` — `Connect` first, then per-exchange `Shutdown` /
    //    `Close` *withheld* while step 2 still has bytes tagged with the
    //    same exchange. Skip step 2 and a teardown that lands before its
    //    exchange's last bytes flush would orphan them on the wire.
    // 2. `poll_transport_transmit` — per-bridge byte stream. Must be
    //    drained before the action queue advances past pending Shutdowns.
    // 3. `poll_memberlist_transmit` — gossip / compound UDP outbound.
    // 4. `poll_event` — application-visible events.
    //
    // The four drains repeat until no method makes progress: pass 1's
    // `drain_actions` may queue transport-transmits which `pass 1`'s
    // `drain_transport_transmits` flushes; flushing the byte queue
    // releases per-exchange `Shutdown` / `Close` actions the coordinator
    // had withheld; pass 2's `drain_actions` then surfaces them. Without
    // the repeat the released teardowns would sit queued until the next
    // arm fire and the bridge state would linger in the machine.
    //
    // None of the drain functions need a timestamp — the Connect arm
    // spawns its dial off-loop (the dial-completion arm refreshes `now`
    // at the moment the result lands), and Shutdown / Close just signal
    // the per-bridge channel.
    loop {
      let did_actions = drain_actions::<I, A, R>(
        &mut endpoint,
        &mut bridges,
        &bridge_ready_tx,
        stream_opts,
        &cidr_policy,
      );
      let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
      let did_transmits =
        drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket, label.clone()).await;
      let did_events = drain_events(
        &mut endpoint,
        &obs_tx,
        &observation_dropped,
        &obs_payload_bytes,
        obs_payload_budget,
        &mut pending,
      )
      .await;
      if !(did_actions || did_transports || did_transmits || did_events) {
        break;
      }
    }
    reap_pending_joins(&mut pending.joins, Instant::now()).await;
    reap_pending_leave(&mut pending.leave, Instant::now()).await;

    if dirty {
      refresh_snapshot_if_changed::<I, A, R>(&endpoint, &snapshot, &mut last_snapshot_version);
      refresh_metrics_if_changed::<I, A, R>(&endpoint, &metrics, &mut last_metrics);
    }

    if exit {
      break;
    }
  }

  // Cleanup. Order matters:
  //   1. Set the shutdown flag so any racing clone's command method
  //      observes it on entry and returns Shutdown without sending.
  //   2. Drain pending commands and reply Err(Shutdown) on each —
  //      callers whose flag check passed BEFORE the flip and were
  //      already buffered get the documented error instead of
  //      hanging on a reply-channel whose Sender is buffered inside
  //      the dropped Receiver.
  //   3. Drop the `commands` Receiver IMMEDIATELY after the drain.
  //      Any clone whose flag check passed before the flip and was
  //      still mid-method (e.g. awaiting a slow `resolver.resolve`
  //      inside `join_with`) cannot land its command in the channel
  //      buffer after the Receiver is gone — `send_async` instead
  //      fails fast with `SendError::Disconnected` and the clone
  //      surfaces `MemberlistError::CommandSend`. Without this
  //      early drop the late send could buffer in the channel and
  //      its reply Sender would stay alive past driver exit; the
  //      clone's reply-receiver `await` would hang forever.
  //   4. Signal every live bridge to close.
  //   5. Drop the bound sockets BEFORE acking the observed shutdown
  //      caller so an immediate rebind on the same port after
  //      `shutdown.await` succeeds.
  shutdown_flag.store(true, std::sync::atomic::Ordering::Release);
  while let Ok(c) = commands.try_recv() {
    let res = Err(MemberlistError::Shutdown);
    let reply: futures_channel::oneshot::Sender<Result<()>> = match c {
      Command::Shutdown(ShutdownCmd { reply }) => reply,
      Command::Leave(LeaveCmd { reply }) => reply,
      Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { reply, .. }) => reply,
      Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => reply,
      Command::SetChecksumOptions(SetChecksumOptionsCmd { reply, .. }) => reply,
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd { reply, .. }) => reply,
      Command::QueueUserBroadcast(cmd) => cmd.reply,
      Command::SetLocalState(cmd) => cmd.reply,
      Command::SetAckPayload(cmd) => cmd.reply,
      Command::SendUser(cmd) => cmd.reply,
      Command::SendReliable(cmd) => cmd.reply,
      Command::Join(JoinCmd { reply, .. }) => {
        // Join's reply type is `Result<usize>`; surface the same
        // Shutdown error through a separate match arm so the type
        // checker sees the right `Sender<Result<usize>>`.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = reply.send(Err(MemberlistError::Shutdown));
        continue;
      }
      Command::Ping(cmd) => {
        // Ping's reply type is `Result<Duration>`; surface Shutdown
        // through a separate arm so the type checker sees the right sender.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::Shutdown));
        continue;
      }
    };
    // Ignoring Err: caller dropped the reply receiver.
    let _ = reply.send(res);
  }
  // Drop the Receiver immediately so any subsequent clone send
  // fails fast (see step 3 above). After this point flume's send
  // returns `Err(SendError::Disconnected)` for every clone.
  drop(commands);
  // Reply Err(Shutdown) to every outstanding synchronous-join waiter.
  // Their reply Senders live in `pending.joins`; without this drain
  // the corresponding receivers on the caller side would hang
  // forever (the loop body's reap path is gone, and the driver task
  // is about to exit).
  for pj in pending.joins.drain(..) {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = pj.reply.send(Err(MemberlistError::Shutdown));
  }
  // Reply Err(Shutdown) to every joined graceful-leave replier whose
  // `LeftCluster` never arrived before the loop exited (e.g. a shutdown
  // raced the leave flush). Without this their receivers would hang
  // forever — the loop body's reap path is gone and the task is exiting.
  if let Some(pl) = pending.leave.take() {
    pl.resolve_all(|| Err(MemberlistError::Shutdown)).await;
  }
  // Reply Err(Shutdown) to every parked application-ping waiter whose
  // `PingCompleted`/`PingFailed` will never arrive (driver is exiting).
  for pp in pending.pings.drain(..) {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = pp.reply.send(Err(MemberlistError::Shutdown));
  }
  // Reply Err(Shutdown) to every parked reliable-send waiter whose
  // `ExchangeCompleted(UserMessage)` will never arrive (driver is exiting).
  for ps in pending.user_sends.drain(..) {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = ps.reply.send(Err(MemberlistError::Shutdown));
  }
  for (_eid, handle) in bridges.drain() {
    // Ignoring Err: the bridge may have exited already; the close
    // notification is best-effort.
    let _ = handle.out_tx.try_send(BridgeOut::Close);
  }
  // Drop the persistent accept future before the listener: it holds an
  // in-flight accept borrowing `listener`, so the listener cannot be moved while
  // it is alive. Cancelling a pending accept during shutdown is correct — a
  // connection arriving as the driver tears down has nothing to be served.
  drop(accept_fut);
  // Drop the TCP reliable listener FIRST so the bound port is released
  // immediately. The listener does not have an explicit close API
  // distinct from drop — the local going out of scope here closes the
  // file descriptor.
  //
  // Windows caveat: unlike the gossip UDP socket below (which gets an awaited
  // `close()` because IOCP handle close is asynchronous), the listener has no
  // such API, so its close rides `drop`. If IOCP closes the listening handle
  // asynchronously, a same-port TCP rebind issued the instant `shutdown().await`
  // returns could in principle race `AddrInUse`. In practice the integration
  // suite rebinds on ephemeral ports (and retries `AddrInUse`), so this has not
  // surfaced; a fully synchronous fix needs a compio `TcpListener` async-close.
  drop(listener);
  // Ignoring Err: socket close on shutdown — the runtime tears down
  // file descriptors anyway and the error is unactionable.
  let _ = gossip_socket.close().await;

  // Now ack the shutdown caller — both bound ports have been released,
  // so the caller's `shutdown.await` returns to a state where an
  // immediate rebind on the same address succeeds.
  if let Some(reply) = shutdown_reply {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = reply.send(Ok(()));
  }
}

/// Dispatch one [`Command`] from the command channel onto the
/// coordinator. Replies are best-effort via the per-command reply channel
/// — a dropped reply receiver means the caller gave up.
///
/// The [`Command::Shutdown`] reply sender is NOT awaited inline; it is
/// stashed into `shutdown_reply` so the driver loop can ack the caller
/// only AFTER the listener + gossip socket drop in the post-loop
/// cleanup. Acking from inside the select arm would unblock the caller
/// before the bound TCP port releases, allowing an immediate-rebind
/// race window between `shutdown.await` returning and the listener
/// actually dropping.
// Nine arguments is past clippy's heuristic but each is a distinct
// piece of mutable state the function reads or threads through; the
// pending command bookkeeping is already bundled into `PendingCommands`.
#[allow(clippy::too_many_arguments)]
async fn dispatch_command<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_ready_tx: &Sender<BridgeReady>,
  stream_opts: StreamTransportOptions,
  cidr_policy: &crate::transport::runtime::CidrFilter,
  shutdown_reply: &mut Option<futures_channel::oneshot::Sender<Result<()>>>,
  pending: &mut PendingCommands,
  leave_timeout: core::time::Duration,
  cmd: Command<I>,
  now: Instant,
) where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + From<SocketAddr>
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
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
      // Both `JoinKind::Dispatch` and `JoinKind::WaitForCompletion`
      // share the same `start_push_pull` fan-out: each seed becomes
      // one queued Connect action that the inline drain below pops
      // and routes through `process_one_action`. The wait variant
      // captures each Connect's ExchangeId keyed on the `StreamId`
      // that THIS join's `start_push_pull` returned, so the captured
      // set is bound only to this join — never to a sibling waiter
      // also targeting the same address, and never to an unrelated
      // same-peer dial that the shared `service_dials` drain may flush
      // for another subsystem (a scheduled push/pull, a probe
      // reliable-ping, a gossip dial).
      match kind {
        JoinKind::Dispatch => {
          let mut count: usize = 0;
          for addr in addrs {
            // Ignoring StreamId return: the driver does not track
            // the per-exchange handle here — completion / failure
            // surfaces through `poll_event` (the `NodeJoined` /
            // `DialFailed` events that the events_tx forwards to
            // subscribers).
            let _ = endpoint.start_push_pull(addr.into(), PushPullKind::Join, now);
            // Drain queued actions so the Connect that `start_push_pull`
            // queued is routed to its bridge BEFORE the next
            // `start_push_pull` enqueues another. No capture: the
            // Dispatch arm tracks no per-exchange waiter state.
            while let Some(action) = endpoint.poll_action() {
              process_one_action(
                action,
                bridges,
                bridge_ready_tx,
                stream_opts,
                None,
                cidr_policy,
              );
            }
            count += 1;
          }
          // Ignoring Err: the caller may have dropped the reply
          // receiver (e.g. the user-facing `dispatch_join_with`
          // future was cancelled).
          let _ = reply.send(Ok(count));
        }
        JoinKind::WaitForCompletion(crate::command::WaitForCompletionArgs { deadline }) => {
          // The resolved seed count (one per address, duplicates included).
          // Capture it BEFORE the loop consumes `addrs`: this is the
          // denominator the caller sees in `JoinAllFailed`. A seed that
          // retires before producing a `Connect` (TLS `sni_provider`
          // returns `None`, dialer construction error, or an elapsed dial
          // deadline) never enters `exchange_ids`, so deriving `requested`
          // from the captured-exchange count would undercount the seeds
          // actually requested.
          let requested = addrs.len();
          let mut exchange_ids: HashSet<ExchangeId> = HashSet::with_capacity(requested);
          // The `StreamId`s this join's `start_push_pull` calls returned;
          // the Connect capture is keyed on this set (not the peer) so a
          // same-peer dial flushed by the shared `service_dials` for another
          // subsystem is never misattributed to this join.
          let mut started: HashSet<StreamId> = HashSet::with_capacity(requested);
          for addr in addrs {
            let sid = endpoint.start_push_pull(addr.into(), PushPullKind::Join, now);
            started.insert(sid);
            // Drain actions queued by THIS `start_push_pull`. The capture
            // binds the Connect's ExchangeId to this join only — duplicate
            // seeds produce duplicate exchanges, each with its own StreamId
            // and tracked independently through its own ExchangeId.
            while let Some(action) = endpoint.poll_action() {
              process_one_action(
                action,
                bridges,
                bridge_ready_tx,
                stream_opts,
                Some((&started, &mut exchange_ids)),
                cidr_policy,
              );
            }
          }
          if exchange_ids.is_empty() {
            // Every seed retired before a `Connect`, so no exchange will
            // ever surface a terminal `ExchangeCompleted`. Parking would
            // idle the waiter until `deadline`; resolve now with the
            // all-failed outcome carrying the full resolved seed count
            // (the same payload the deadline reaper would produce).
            // Ignoring Err: the caller dropped the reply receiver.
            let _ = reply.send(Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
              requested, 0,
            ))));
          } else {
            pending.joins.push(PendingJoin {
              pending: exchange_ids,
              contacted: 0,
              requested,
              deadline,
              reply,
            });
          }
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
      if let Some(pl) = pending.leave.as_mut() {
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
            pending.leave = Some(PendingLeave {
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
    Command::SetChecksumOptions(SetChecksumOptionsCmd { opts, reply }) => {
      // Gate on a running node FIRST: after `leave()` the endpoint emits no
      // gossip datagrams, so a new checksum policy could never take effect on
      // the wire. Reject with `NotRunning` without validating — there is no
      // point probing a policy that can never apply. When running,
      // `set_checksum_options` validates the algorithm's backend is built in
      // BEFORE storing it (an unusable policy would make every later
      // `checksum_gossip` fail and the driver drop the datagram, silently
      // disabling ALL gossip behind a false `Ok`). Checksum is a gossip-plane
      // concern only — reliable bridges carry none.
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
      // Drain every live bridge so the per-bridge byte movers observe
      // the close and exit. Do NOT ack the caller here — the listener
      // and gossip socket are still bound. Stash the reply and let the
      // post-loop cleanup ack AFTER both sockets drop, so an immediate
      // rebind on the same port after `shutdown.await` succeeds.
      for (_eid, handle) in bridges.drain() {
        // Ignoring Err: bridge may have already exited; the close
        // signal is best-effort.
        let _ = handle.out_tx.try_send(BridgeOut::Close);
      }
      *shutdown_reply = Some(reply);
    }
    Command::Ping(cmd) => {
      // Gate on a running node: after `leave()` the probe scheduler is
      // stopped, so a new application ping's completion event would never
      // arrive and the caller would hang forever on its reply channel.
      // Reject with `NotRunning` rather than park an unresolving waiter.
      if endpoint.is_running() {
        // `PingCmd` carries `Node<I, SocketAddr>`; the machine's
        // `StreamEndpoint::ping` expects `Node<I, A>`. Convert via
        // `A::from(SocketAddr)` (the `From<SocketAddr>` bound on `A` ensures
        // this is always defined).
        let (id, addr) = cmd.node().clone().into_parts();
        let node_a = memberlist_proto::Node::new(id, A::from(addr));
        let ping_id = endpoint.ping(node_a, now).expect("issued while running");
        pending.pings.push(PendingPing {
          ping_id,
          reply: cmd.reply,
        });
      } else {
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::NotRunning));
      }
    }
    Command::SendUser(cmd) => {
      // Gate on a running node: after `leave()` the gossip socket is
      // effectively closed to protocol traffic; reject immediately.
      let res: Result<()> = if !endpoint.is_running() {
        Err(MemberlistError::NotRunning)
      } else if cidr_blocks(cidr_policy, cmd.to().ip()) {
        // Our own policy excludes the destination: do not emit an unreliable user
        // datagram to a blocked peer (the reliable plane's outbound dial is gated
        // at the Connect handler; this is the unreliable counterpart).
        Err(MemberlistError::SendFailed)
      } else {
        // Convert `SocketAddr` to `A` via the `A: From<SocketAddr>` bound.
        endpoint
          .send_user_packets(A::from(*cmd.to()), cmd.payloads())
          .map_err(MemberlistError::Proto)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply.send(res);
    }
    Command::SendReliable(cmd) => {
      // Gate on a running node: after `leave()` the stream coordinator is
      // stopping, so a new `start_user_message` would never produce a
      // `DialRequested` action and the caller would hang.
      if !endpoint.is_running() {
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(Err(MemberlistError::NotRunning));
        return;
      }
      // Convert `SocketAddr` to `A` once; reused for each payload's dial.
      let peer_a = A::from(*cmd.to());
      let mut exchange_ids: HashSet<ExchangeId> = HashSet::with_capacity(cmd.payloads().len());
      // The `StreamId`s this command's `start_user_message` calls returned.
      // The Connect capture is bound to THIS set, not the peer: `service_dials`
      // drains a shared dial deque, so a same-peer dial flushed for another
      // subsystem (a scheduled push/pull, a probe reliable-ping) is never
      // misattributed to this send.
      let mut started: HashSet<StreamId> = HashSet::with_capacity(cmd.payloads().len());
      for payload in cmd.payloads() {
        // `start_user_message` emits a `DialRequested` event which
        // `service_dials` immediately converts into a `StreamAction::Connect`
        // carrying the per-bridge `ExchangeId` (allocated by `StreamConns::allocate`,
        // distinct from the machine-level `StreamId`) tagged with the originating
        // `StreamId`. The capture in `process_one_action` grabs that `ExchangeId`
        // for the Connect whose `stream_id()` is in `started`, so we can correlate
        // the terminal `Event::ExchangeCompleted(UserMessage)` with this waiter.
        let sid = endpoint
          .start_user_message(peer_a.cheap_clone(), payload.clone(), now)
          .expect("issued while running");
        started.insert(sid);
        while let Some(action) = endpoint.poll_action() {
          process_one_action(
            action,
            bridges,
            bridge_ready_tx,
            stream_opts,
            Some((&started, &mut exchange_ids)),
            cidr_policy,
          );
        }
      }
      // `n` = payloads dispatched; `m` = exchanges captured for this command.
      // Each `start_user_message` produces one `StreamId` in `started` and at
      // most one captured `Connect` (success) or zero (a pre-`Connect`
      // retirement: TLS SNI parse failure, record-layer fault, or an
      // already-elapsed exchange deadline — the API permits a `sni_provider`
      // that returns `Some` then `None`, so a partial failure mid-batch is
      // reachable). Because capture is keyed on `started`, an unrelated
      // same-peer dial cannot inflate `m`, so `m <= n` always holds and
      // `n - m` is the count of payloads that never produced an exchange and
      // will never surface a terminal `ExchangeCompleted`.
      let n = cmd.payloads().len();
      let m = exchange_ids.len();
      if m == 0 {
        // No exchange was created. Two distinct cases:
        // - empty payloads slice (`n == 0`): nothing to send, success is
        //   vacuous. Reply `Ok(())`; parking would hang with no terminal
        //   event.
        // - non-empty payloads (`n > 0`) but every dial was retired before a
        //   `Connect`: the send genuinely FAILED. Reply `Err(SendFailed)`.
        let res = if n == 0 {
          Ok(())
        } else {
          Err(MemberlistError::SendFailed)
        };
        // Ignoring Err: caller dropped the reply receiver.
        let _ = cmd.reply.send(res);
      } else {
        // Park with `failed` SEEDED to the `n - m` payloads that never
        // produced an exchange. The waiter resolves `Ok(())` only when
        // every dispatched exchange succeeds AND no payload was dropped
        // pre-`Connect`; any seeded or observed failure makes the final
        // reply `Err(SendFailed)`. Without the seed, a batch where some
        // payloads connect and a LATER one fails pre-`Connect` would
        // falsely report `Ok` once the connected exchanges succeed.
        //
        // Capture-by-`StreamId` makes `m <= n` structural, so `n - m` cannot
        // underflow. `checked_sub` is the fail-closed backstop: were a future
        // regression to make `m > n`, reply `Err(SendFailed)` rather than
        // panic on the underflow.
        match n.checked_sub(m) {
          Some(failed) => {
            pending.user_sends.push(PendingUserSend {
              pending: exchange_ids,
              failed,
              reply: cmd.reply,
            });
          }
          None => {
            // Ignoring Err: caller dropped the reply receiver.
            let _ = cmd.reply.send(Err(MemberlistError::SendFailed));
          }
        }
      }
    }
  }
}

/// Route one bridge inbound message into the coordinator. The driver-
/// side `bridges` table is kept in lockstep with the coordinator's
/// bridge set via the `StreamAction::Close` arm in [`drain_actions`] —
/// `Eof` and `Error` here only feed the coordinator's EOF anchor; the
/// bridge entry stays so the response (queued by the same `handle_
/// transport_data(eof=true)` call) can reach the still-alive bridge.
fn dispatch_bridge_inbound<I, A, R>(endpoint: &mut StreamEndpoint<I, A, R>, inbound: BridgeInbound)
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  // Each inbound carries its own `received_at` — the wall-clock
  // instant the bridge observed the bytes / EOF / error on the
  // socket. Forwarding THAT timestamp (not a fresh `Instant::now()`)
  // to `handle_transport_data` ensures the stream FSM's deadline gate
  // compares against the true arrival time. A successful response
  // queued before the exchange deadline therefore is not retroactively
  // marked Timeout by the driver's later processing (under
  // bridge-channel backlog or scheduling delay).
  match inbound {
    BridgeInbound::Bytes(BridgeBytes {
      eid,
      bytes,
      received_at,
    }) => {
      endpoint.handle_transport_data(eid, &bytes, false, received_at);
    }
    BridgeInbound::Eof(BridgeEof { eid, received_at }) => {
      // Feed the read-half EOF anchor to the coordinator. Do NOT remove
      // the [`BridgeHandle`] here — for an inbound (server-side)
      // push/pull bridge the read EOF arrives BEFORE the response is
      // generated; the machine queues the response into
      // [`StreamEndpoint::poll_transport_transmit`] inside this same
      // `handle_transport_data` call. Removing the bridge now would
      // cause `drain_transport_transmits` to find no entry for the
      // response bytes and drop them on the floor. The bridge entry
      // stays until the matching `StreamAction::Close` surfaces and the
      // `drain_actions` Close arm removes it (the bridge is in phase 2
      // — read side closed, write side still live — so it accepts the
      // response on `out_tx` and writes it before the eventual `Close`).
      endpoint.handle_transport_data(eid, &[], true, received_at);
    }
    BridgeInbound::Error(BridgeError {
      eid,
      err: _,
      received_at,
    }) => {
      // A transport ERROR is NOT a clean EOF: route it to handle_transport_error
      // so the bridge fails with ConnectionLost. Feeding it as the benign EOF
      // anchor (handle_transport_data with eof=true) would, for a one-way
      // UserMessage, falsely resolve send_reliable as success. The eventual
      // StreamAction::Close still cleans up the driver-side entry.
      endpoint.handle_transport_error(eid, received_at);
    }
  }
}

/// Decode and feed one inbound UDP gossip datagram into the coordinator,
/// then drain its memberlist ingress queue and feed every decoded message
/// back through `handle_packet`.
///
/// The coordinator's [`StreamEndpoint::handle_gossip`] only BUFFERS the
/// raw datagram into its [`StreamEndpoint::poll_memberlist_ingress`]
/// queue. The driver owns the codec hop (decrypt → decompress → decode
/// → optionally split compound) because `memberlist-proto` has no
/// codec dependency by design. After this function returns the gossip
/// is fully applied to the coordinator's membership FSM.
fn dispatch_gossip<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  src: SocketAddr,
  datagram: &[u8],
  now: Instant,
  label: Option<Bytes>,
) where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + From<SocketAddr>
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  endpoint.handle_gossip(src.into(), datagram, now);

  let decode_opts = DecodeOptions::new(label);
  // Drain the raw-buffer queue and decode each datagram. Each iteration
  // pops exactly the datagram we just fed (FIFO), but draining the loop
  // unconditionally handles a stale buffer left over from a prior
  // ingress that some earlier handle_gossip had no chance to drain.
  while let Some((from_addr, raw)) = endpoint.poll_memberlist_ingress() {
    let plain = match endpoint.decrypt_gossip(&raw) {
      Ok(p) => Bytes::from(p),
      // Drop the datagram on a decrypt / unknown-tag / oversize error.
      // Gossip is lossy and self-healing — the peer retransmits on the
      // next gossip round.
      Err(_) => continue,
    };
    // Strip the optional cluster label and verify it matches. A
    // mismatched or absent label on a labeled cluster (or a labeled
    // frame on an unlabeled cluster) is rejected here, isolating gossip
    // traffic between clusters sharing the same UDP port range.
    let inner = match decode_incoming(plain, &decode_opts) {
      Ok(b) => b,
      // LabelMismatch / DoubleLabel / truncated: drop the datagram per
      // the lossy-gossip discipline; the peer retransmits.
      Err(_) => continue,
    };
    // Demux plain vs compound and feed each decoded message to the
    // coordinator. A malformed frame drops the whole datagram.
    let msgs = match parse_messages::<I, A>(inner) {
      Ok(m) => m,
      Err(_) => continue,
    };
    for msg in msgs {
      endpoint.handle_packet(from_addr.cheap_clone(), msg, now);
    }
  }
}

/// Process one [`StreamAction`].
///
/// Connect: pre-allocate the bridge's out-channel and insert the
/// [`BridgeHandle`] BEFORE spawning the dial task. The machine surfaces
/// the first push/pull request on the same tick the Connect lands (via
/// the in-band `service_dials + flush_outbound` inside `start_push_pull`);
/// `drain_transport_transmits` pops those bytes on the same drain pass
/// and pushes them into the `out_tx` here. Without the pre-allocation
/// the bytes would be dropped because the bridge had not been spawned
/// yet (the matching `bridges.get(&eid)` would miss), and the peer would
/// observe an empty TCP stream that times out the exchange.
///
/// The `out_rx` half travels with the dial task and is handed to the
/// bridge in the `OutboundOk` arm so the bridge sees the pre-queued
/// bytes in producer order on its very first iteration.
///
/// Shutdown / Close just signal the per-bridge channel.
///
/// `capture` is `Some` only when this action is being processed inline
/// from `dispatch_command` (so the Connect's ExchangeId can be bound
/// to the specific synchronous command that just dispatched it). The
/// first element is the set of `StreamId`s that command's `start_*`
/// calls returned; a Connect's `ExchangeId` is captured into the second
/// set ONLY when its originating `stream_id()` is in that set. Matching
/// the StreamId — not the peer — is what call-scopes the capture:
/// `StreamEndpoint::service_dials` drains a SHARED dial deque, so one
/// command's drain can also surface an unrelated same-peer dial enqueued
/// by another subsystem (a scheduled push/pull, a probe reliable-ping, a
/// gossip dial). Each `StreamId` yields at most one Connect, so the
/// captured set never exceeds the count of dials this command started.
/// In the normal `drain_actions` path `capture` is `None` — actions
/// emitted by non-`dispatch_command` sources (probe-driven push/pull
/// fallback, teardown sweeps) have no synchronous-command ownership.
fn process_one_action(
  action: StreamAction,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_ready_tx: &Sender<BridgeReady>,
  stream_opts: StreamTransportOptions,
  capture: Option<(&HashSet<StreamId>, &mut HashSet<ExchangeId>)>,
  cidr_policy: &crate::transport::runtime::CidrFilter,
) {
  match action {
    StreamAction::Connect(info) => {
      let eid = info.id();
      let peer = info.peer();
      // Reject an outbound dial to a CIDR-blocked peer at the transport boundary:
      // report a dial failure (no connect, no bridge recorded) so the exchange
      // terminalizes without ever feeding the blocked peer's bytes to the machine,
      // matching the inbound accept guard and the QUIC source filter.
      if cidr_blocks(cidr_policy, peer.ip()) {
        // Ignoring Err: `bridge_ready` is unbounded so the send never blocks, and
        // a dropped failure (all receivers gone = driver exiting) is moot.
        let _ = bridge_ready_tx.send(BridgeReady::OutboundFail(OutboundFailReady {
          eid,
          err: io::Error::new(
            io::ErrorKind::PermissionDenied,
            "peer blocked by CIDR policy",
          ),
          received_at: Instant::now(),
        }));
        return;
      }
      if let Some((started, pending_exchanges)) = capture
        && started.contains(&info.stream_id())
      {
        pending_exchanges.insert(eid);
      }
      let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
      let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
      bridges.insert(eid, BridgeHandle { out_tx, cancel_tx });
      let ready_tx = bridge_ready_tx.clone();
      let dial_timeout = stream_opts.dial_timeout();
      compio::runtime::spawn(async move {
        // Bound the dial with the configured dial timeout (default
        // 5s) so a connect to an unreachable peer reports failure to
        // the driver promptly instead of hanging on the kernel's
        // ~3-minute default. Without the bound the dial task lives
        // until the kernel completes its connect attempt, leaking a
        // runtime task and the `bridge_ready_tx` clone for the full
        // kernel timeout.
        let dial = TcpStream::connect(peer).fuse();
        let timeout = compio::time::sleep(dial_timeout).fuse();
        futures_util::pin_mut!(dial, timeout);
        let msg = futures_util::select_biased! {
          res = dial => match res {
            Ok(stream) => BridgeReady::OutboundOk(OutboundOkReady {
              eid,
              stream,
              out_rx,
              cancel_rx,
            }),
            // Dropping `out_rx` (and `cancel_rx`) here disconnects the
            // channels; any bytes the driver pushed into `out_tx` during
            // the dial are dropped, which is correct — the exchange
            // never produced a wire to write them on.
            Err(err) => BridgeReady::OutboundFail(OutboundFailReady {
              eid,
              err,
              received_at: Instant::now(),
            }),
          },
          _ = timeout => {
            // The dial exceeded its bound. Treat as a connect
            // failure; the coordinator's per-exchange handshake
            // deadline (`ACCEPT_HANDSHAKE_DEADLINE = 10s`) will
            // observe the failure on the next pump and reap the
            // exchange.
            BridgeReady::OutboundFail(OutboundFailReady {
              eid,
              err: io::Error::new(io::ErrorKind::TimedOut, "dial timeout"),
              received_at: Instant::now(),
            })
          }
        };
        // Ignoring Err: driver has exited; the dial result is
        // unobservable.
        let _ = ready_tx.send_async(msg).await;
      })
      .detach();
    }
    StreamAction::Shutdown(eref) => {
      // Half-close the send side. The bridge writes every queued
      // `Bytes` ahead of `ShutdownWrite` (single-channel FIFO), then
      // calls `AsyncWrite::shutdown` on its write half and continues
      // reading. This is the push/pull half-close anchor: the peer's
      // read side sees FIN and knows the request/response on this
      // direction is complete, while the local read side stays open
      // to receive the peer's reply.
      if let Some(handle) = bridges.get(&eref.id()) {
        // Ignoring Err: the bridge may have already exited; the
        // shutdown signal is best-effort.
        let _ = handle.out_tx.try_send(BridgeOut::ShutdownWrite);
      }
    }
    StreamAction::Close(eref) => {
      // Graceful full teardown. The bridge writes any queued `Bytes`
      // (already ordered before this `Close` in the channel FIFO), then
      // sends a single EOF marker to the driver and exits. Removing the
      // `BridgeHandle` here is symmetric with the bridge's exit —
      // any later `Bytes` the machine surfaces for this exchange
      // miss the lookup in `drain_transport_transmits` and are
      // dropped.
      if let Some(handle) = bridges.remove(&eref.id()) {
        // Ignoring Err: see Shutdown arm.
        let _ = handle.out_tx.try_send(BridgeOut::Close);
      }
    }
    StreamAction::Abort(eref) => {
      // Hard teardown of a FAILED exchange. Signal the out-of-band
      // `cancel_tx`: the bridge's `select!` resolves the cancel arm
      // with priority over its `out_rx` FIFO, so it breaks IMMEDIATELY
      // and drops its write half WITHOUT writing any `Bytes` still
      // queued for this exchange — those bytes are stale (belonging to
      // an exchange the coordinator gave up on) and must be discarded,
      // not flushed. Removing the `BridgeHandle` drops `out_tx`, so any
      // later bytes the machine surfaces also miss the lookup and are
      // dropped.
      if let Some(handle) = bridges.remove(&eref.id()) {
        // Ignoring Err: the bridge may have already exited (its
        // cancel receiver gone); the abort signal is best-effort.
        let _ = handle.cancel_tx.send(());
      }
    }
  }
}

/// Drain every [`StreamAction`] the coordinator has queued, dispatching each on
/// the driver's per-bridge handle table. Returns `true` iff any action was
/// processed.
fn drain_actions<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_ready_tx: &Sender<BridgeReady>,
  stream_opts: StreamTransportOptions,
  cidr_policy: &crate::transport::runtime::CidrFilter,
) -> bool
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let mut progress = false;
  while let Some(action) = endpoint.poll_action() {
    progress = true;
    process_one_action(
      action,
      bridges,
      bridge_ready_tx,
      stream_opts,
      None,
      cidr_policy,
    );
  }
  progress
}

/// Drain every queued per-exchange transport-transmit and forward the
/// bytes to the matching bridge's write half via its transmit channel.
///
/// Per the `streams/mod.rs` ordering contract this MUST run before the
/// action queue advances past a pending `Shutdown` / `Close` — the
/// coordinator withholds the teardown action for an exchange until its
/// `poll_transport_transmit` queue is empty. Skipping this drain would
/// leave bytes queued on the machine indefinitely and prevent the bridge
/// from ever cleanly closing.
///
/// For an exchange with no live bridge handle (the dial failed and
/// `OutboundFail` already removed the entry, or a `Close` action
/// already retired the exchange) the bytes are dropped — the matching
/// `Shutdown` / `Close` then surfaces on the next `poll_action` call
/// and the no-op arm reaps the (already-absent) bridge entry.
fn drain_transport_transmits<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &HashMap<ExchangeId, BridgeHandle>,
) -> bool
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let mut progress = false;
  while let Some((eid, _peer, bytes)) = endpoint.poll_transport_transmit() {
    progress = true;
    let Some(handle) = bridges.get(&eid) else {
      // No live bridge — either the dial failed and OutboundFail
      // removed the entry, or a Close action already retired the
      // exchange. The documented contract is that draining this queue
      // unblocks the matching Shutdown/Close, which the next
      // `poll_action` returns.
      continue;
    };
    // Ignoring Err: the only failure mode on an unbounded channel is
    // `Disconnected` — the bridge has exited and is not reading any
    // more outbound bytes. Dropping is safe because a bridge that has
    // exited cannot deliver bytes regardless.
    let _ = handle.out_tx.try_send(BridgeOut::Bytes(bytes.to_vec()));
  }
  progress
}

/// Drain every queued unreliable (UDP gossip) [`Transmit`] and send the
/// resulting datagram on the gossip socket. Returns `true` iff any
/// transmit was processed.
///
/// Outbound gossip flows through the shared codec:
/// `encode_outgoing` / `encode_outgoing_compound` stamp the cluster label
/// (if any) before `compress_gossip` → `encrypt_gossip` → send, matching the
/// inbound path in `dispatch_gossip` and the reactor driver's gossip egress.
async fn drain_transmits<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  gossip_socket: &UdpSocket,
  label: Option<Bytes>,
) -> bool
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let encode_opts = EncodeOptions::new(label);
  let mut progress = false;
  while let Some(transmit) = endpoint.poll_memberlist_transmit() {
    progress = true;
    let (peer, plain) = match transmit {
      Transmit::Packet(pkt) => {
        let (to, msg) = pkt.into_parts();
        match encode_outgoing(&msg, &encode_opts) {
          Ok(b) => (to, b),
          // A locally-built message that fails to encode is dropped so a
          // single bad codec invocation cannot wedge the driver.
          Err(_) => continue,
        }
      }
      Transmit::Compound(cmp) => {
        let (to, msgs) = cmp.into_parts();
        match encode_outgoing_compound(&msgs, &encode_opts) {
          Ok(b) => (to, b),
          Err(_) => continue,
        }
      }
    };
    let compressed = endpoint.compress_gossip(&plain);
    let checksummed = match endpoint.checksum_gossip(&compressed) {
      Ok(bytes) => bytes,
      // Checksum configured but its backend was not built in — drop rather
      // than emit an unverifiable datagram on a checksum-configured path.
      Err(_) => continue,
    };
    let on_wire = match endpoint.encrypt_gossip(&checksummed) {
      Ok(bytes) => bytes,
      // Encryption-configured + backend-rejected (e.g. unknown
      // algorithm baked in) — drop the datagram. Emitting plaintext on
      // an encrypted-cluster path would silently bypass auth.
      Err(_) => continue,
    };
    let peer_socket = endpoint.resolve_peer_socket(&peer);
    let BufResult(res, _buf) = gossip_socket.send_to(on_wire, peer_socket).await;
    // Ignoring Err: a transient send error (ENOBUFS, network down on
    // an interface, ICMP unreachable surfacing as a syscall error) is
    // non-fatal — gossip is lossy and the next probe/gossip round
    // recovers. A logger hook would land here once wired.
    let _ = res;
  }
  progress
}

/// Drain every queued [`Event`]: synchronous protocol accounting, then
/// hand off to the observation task. NO `.await` on user delegate code.
///
/// Drains `poll_event` to empty doing only the sync protocol accounting
/// (join contact reduction + completed-join reply, leave-completion
/// resolution), then hands each event to the per-driver observation task
/// (delegate dispatch + EventStream forward) via `obs_tx`. The hand-off is
/// non-blocking; on a bounded `obs_tx` (the default) a burst that outpaces the
/// observation task [`yield_once`]s to let it drain, then drops + counts the
/// overflow only if the task still cannot keep up (a slow/stuck delegate) — so
/// a fast delegate never loses a valid burst while memory stays bounded.
/// Returns `true` iff any event was drained.
///
/// Observation dispatch is deliberately OFF this path: a slow `notify_*`
/// hook must not run on the driver task, because a join's
/// `ExchangeCompleted` can arrive on a follow-up input the driver must
/// still service — a hook blocking the loop would delay the parked
/// join/leave reply that depends on it. See [`observation_task`].
async fn drain_events<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  obs_tx: &Sender<Event<I, A>>,
  observation_dropped: &std::sync::atomic::AtomicU64,
  obs_payload_bytes: &std::sync::atomic::AtomicU64,
  obs_payload_budget: Option<u64>,
  pending: &mut PendingCommands,
) -> bool
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let mut drained = false;

  // Synchronous accounting only — no `.await` on user code.
  while let Some(ev) = endpoint.poll_event() {
    drained = true;
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
    // `Succeeded`. Tracking by `ExchangeId` (not by `SocketAddr`) gives
    // correct duplicate-seed semantics: passing the same address twice
    // produces two exchanges and each is counted independently. An eid
    // belongs to at most one waiter, so the search stops on the first
    // match.
    //
    // The COMPLETION reply (the waiter's `pending` is now empty) fires
    // RIGHT HERE on the driver task — the observation hooks
    // (`notify_join` / `merge_remote_state`) for the co-surfaced
    // `NodeJoined` / `RemoteStateReceived` run off-loop on the
    // observation task — so a slow hook cannot delay `join_with`'s
    // `Ok(contacted)`, mirroring the `LeftCluster` leave resolution
    // below. The DEADLINE path (a waiter whose `pending` is still
    // non-empty when its `deadline` elapses) stays in
    // `reap_pending_joins` with its partial `contacted`; a waiter
    // replied + removed here is gone from `pending_joins`, so it can
    // never be double-replied by the reaper.
    if let Event::ExchangeCompleted(ref payload) = ev
      && payload.kind() == ExchangeKind::PushPull
    {
      let eid = payload.eid();
      let succeeded = matches!(payload.outcome(), ExchangeOutcome::Succeeded);
      if let Some(idx) = pending
        .joins
        .iter()
        .position(|pj| pj.pending.contains(&eid))
      {
        let pj = &mut pending.joins[idx];
        pj.pending.remove(&eid);
        if succeeded {
          pj.contacted += 1;
        }
        if pj.pending.is_empty() {
          // Fully resolved — reply now and drop the waiter. `contacted`
          // is final (the FSM emits one `ExchangeCompleted` per
          // dispatched exchange, so an empty `pending` set means every
          // outbound exchange this call dispatched has terminated). A
          // zero-contact resolution is the same `JoinAllFailed` the
          // reaper would have produced.
          let pj = pending.joins.swap_remove(idx);
          let result = if pj.contacted == 0 {
            Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
              pj.requested,
              0,
            )))
          } else {
            Ok(pj.contacted)
          };
          // Ignoring Err: caller dropped the reply receiver (e.g. the
          // user-facing join_with future was cancelled).
          let _ = pj.reply.send(result);
        }
      }
    }
    // Leave-completion resolution. `LeftCluster` fires once the direct
    // `Dead`-self notices queued by `leave()` have drained to the wire;
    // resolving the parked waiter here — on this driver task, ahead of
    // the observation task's `notify_leave` — is what makes `leave()`
    // return promptly once the flush is done rather than waiting on
    // delegate latency.
    if matches!(ev, Event::LeftCluster)
      && let Some(pl) = pending.leave.take()
    {
      // Resolve EVERY joined replier (the initiator plus any racing
      // clones) with `Ok(())`.
      pl.resolve_all(|| Ok(())).await;
    }
    // Ping completion: resolve the matching `PendingPing` waiter with the
    // observed RTT. The event still flows to the observation task below
    // (correlation is additive — `PingCompleted` also fires the delegate's
    // `notify_ping_complete`). An eid not found in `pending_pings` is an
    // unsolicited or already-reaped event; safe to ignore.
    if let Event::PingCompleted(ref p) = ev {
      let pid = p.ping_id();
      if let Some(idx) = pending.pings.iter().position(|pp| pp.ping_id == pid) {
        let pp = pending.pings.swap_remove(idx);
        // Ignoring Err: caller dropped the reply receiver (ping future was cancelled).
        let _ = pp.reply.send(Ok(p.rtt()));
      }
    }
    // Ping failure: resolve the matching `PendingPing` waiter with `PingTimeout`.
    // Same additive semantics as `PingCompleted` — event continues to obs task.
    if let Event::PingFailed(ref p) = ev {
      let pid = p.ping_id();
      if let Some(idx) = pending.pings.iter().position(|pp| pp.ping_id == pid) {
        let pp = pending.pings.swap_remove(idx);
        // Ignoring Err: caller dropped the reply receiver (ping future was cancelled).
        let _ = pp.reply.send(Err(MemberlistError::PingTimeout));
      }
    }
    // Reliable user-send completion: each `ExchangeCompleted` with kind
    // `UserMessage` may reduce a `PendingUserSend`'s pending set. When the
    // set empties the call resolves. `Ok(())` if all exchanges succeeded;
    // `Err(SendFailed)` if any failed. The event continues to the observation
    // task below (additive — `ExchangeCompleted` can feed future monitoring).
    if let Event::ExchangeCompleted(ref payload) = ev
      && payload.kind() == ExchangeKind::UserMessage
    {
      let eid = payload.eid();
      let failed = matches!(payload.outcome(), ExchangeOutcome::Failed);
      if let Some(idx) = pending
        .user_sends
        .iter()
        .position(|ps| ps.pending.contains(&eid))
      {
        let ps = &mut pending.user_sends[idx];
        ps.pending.remove(&eid);
        if failed {
          ps.failed += 1;
        }
        if ps.pending.is_empty() {
          let ps = pending.user_sends.swap_remove(idx);
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
    // Payload-bearing events (`UserPacket` / `RemoteStateReceived`) can each
    // own up to `max_stream_frame_size` bytes; size this one for the byte
    // backstop (`None` for small membership / control events).
    let payload_bytes = observation_payload_bytes(&ev);

    // Byte backstop (bounded channels only): the count cap does not bound
    // memory when events carry large reliable payloads. If enqueueing this
    // payload event would push the queued payload bytes over budget, yield once
    // so the observation task can drain (it subtracts as it dequeues), re-check,
    // and drop + count if still over. Draining PAST the dropped event (via
    // `continue`) preserves the obs decoupling — the `ExchangeCompleted` that
    // fires a parked join/leave reply is never delayed by a backed-up delegate.
    if let (Some(budget), Some(bytes)) = (obs_payload_budget, payload_bytes) {
      if obs_payload_bytes
        .load(std::sync::atomic::Ordering::Relaxed)
        .saturating_add(bytes)
        > budget
      {
        yield_once().await;
      }
      if obs_payload_bytes
        .load(std::sync::atomic::Ordering::Relaxed)
        .saturating_add(bytes)
        > budget
      {
        observation_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        continue;
      }
    }

    // Hand off to the observation task (delegate dispatch + EventStream
    // forward, off this driver task), non-blocking. `Disconnected` means the
    // task exited at teardown — the event is undeliverable, drop it. `Full`
    // means the bounded channel is at its count capacity; yield once so the
    // observation task can drain (a fast delegate empties it in that quantum,
    // delivering a whole burst), then retry. Drop + count ONLY if still full —
    // a genuinely slow/stuck delegate — bounding memory without an indefinite
    // SWIM stall and without dropping a burst a fast delegate could absorb. On
    // a successful enqueue, add the payload bytes to the backstop counter.
    match obs_tx.try_send(ev) {
      Ok(()) => add_obs_payload(obs_payload_bytes, payload_bytes),
      Err(flume::TrySendError::Disconnected(_)) => {}
      Err(flume::TrySendError::Full(ev)) => {
        yield_once().await;
        match obs_tx.try_send(ev) {
          Ok(()) => add_obs_payload(obs_payload_bytes, payload_bytes),
          Err(_) => {
            observation_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          }
        }
      }
    }
  }

  drained
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
/// the delegate hook above but are NOT fanned out: the EventStream is a
/// best-effort channel whose documented recovery is reconciling from the
/// membership snapshot, which cannot reconstruct app-data, and forwarding the
/// large reliable payloads would let a slow subscriber pile them up in the
/// bounded `events_tx` (the byte backstop only bounds the `obs_rx` delegate
/// path). The forward is best-effort: a full queue (slow subscriber) drops the
/// event via `try_send` and counts it into `events_dropped` (gap signal), never
/// blocking. The task exits when `obs_rx` closes (the driver dropped its
/// `obs_tx` at teardown). Mirrors the QUIC driver's `observation_task`.
async fn observation_task<I, A, D>(
  obs_rx: Receiver<Event<I, A>>,
  delegate: D,
  events_tx: Sender<Event<I, A>>,
  events_dropped: Arc<std::sync::atomic::AtomicU64>,
  obs_payload_bytes: Arc<std::sync::atomic::AtomicU64>,
) where
  D: Delegate<Id = I, Address = A>,
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  while let Ok(ev) = obs_rx.recv_async().await {
    // App-data events (`UserPacket` / `RemoteStateReceived`) carry the only
    // large payloads. Measure this event for the byte backstop and free the
    // budget it occupied as soon as it leaves the channel — before the
    // (possibly slow) delegate hook — so the driver's enqueue side sees the
    // reclaimed budget promptly. Membership / control events have no weight.
    let payload = observation_payload_bytes(&ev);
    if let Some(b) = payload {
      obs_payload_bytes.fetch_sub(b, std::sync::atomic::Ordering::Relaxed);
    }
    // Contain a panicking delegate hook so the task SURVIVES and keeps releasing
    // the byte-backstop reservations of still-queued events. A dead obs task
    // would strand them — permanently wedging the byte budget against all future
    // app-data — and silently stop all delegate dispatch and EventStream
    // fan-out. Mirrors the reactor's obs task. Ignoring the unwind result: the
    // panic is contained and this event is simply dropped.
    let _ = std::panic::AssertUnwindSafe(dispatch_event_delegate(&delegate, &ev))
      .catch_unwind()
      .await;
    // Fan out membership / control events only. App-data was delivered to the
    // delegate above; the EventStream cannot reconstruct it from the snapshot,
    // and forwarding the large reliable payloads would let a slow subscriber
    // retain them in the bounded `events_tx`, reintroducing the OOM the obs_rx
    // byte backstop closes.
    if payload.is_some() {
      continue;
    }
    if events_tx
      .try_send(ev)
      .is_err_and(|e| matches!(e, flume::TrySendError::Full(_)))
    {
      events_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
  }
}

/// Reap deadline-expired entries from `pending_joins`.
///
/// Called after every drain block. The normal COMPLETION path (a
/// waiter whose `pending` set drained to empty) now resolves inline in
/// [`drain_events`] — ahead of the observation-delegate dispatch
/// — so a fully-resolved waiter is replied + removed there and is gone
/// from `pending_joins` before this reaper runs (it can never be
/// double-replied here). This reaper therefore handles two remaining
/// cases. First, DEADLINE expiry: a waiter whose `pending` is still
/// non-empty when its `deadline` elapses replies with its partial
/// `contacted`. Second, the degenerate insert-time-empty case: a
/// `WaitForCompletion` that dispatched zero outbound exchanges (its
/// `pending` set was empty on insert) never sees an `ExchangeCompleted`,
/// so the empty-`pending` branch resolves it here on the next sweep.
/// `swap_remove` is sound because `pending_joins` has no ordering
/// semantics.
async fn reap_pending_joins(pending_joins: &mut Vec<PendingJoin>, now: Instant) {
  let mut i = 0;
  while i < pending_joins.len() {
    let done = pending_joins[i].pending.is_empty();
    let expired = now >= pending_joins[i].deadline;
    if done || expired {
      let pj = pending_joins.swap_remove(i);
      let result = if pj.contacted == 0 {
        Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
          pj.requested,
          0,
        )))
      } else {
        Ok(pj.contacted)
      };
      // Ignoring Err: caller dropped the reply receiver (e.g.
      // the user-facing join_with future was cancelled).
      let _ = pj.reply.send(result);
    } else {
      i += 1;
    }
  }
}

/// Earliest pending-join deadline, if any. Folded into the driver's
/// per-iteration `timeout_deadline` so the select's timer arm fires by
/// the deadline of the first expiring join even when the coordinator
/// itself has no nearer deadline.
fn min_pending_join_deadline(pending_joins: &[PendingJoin]) -> Option<Instant> {
  pending_joins.iter().map(|pj| pj.deadline).min()
}

/// Reap a deadline-expired graceful-leave waiter.
///
/// Called alongside [`reap_pending_joins`] after every drain block. If
/// `pending_leave`'s `deadline` has elapsed without the machine's
/// `Event::LeftCluster` having resolved it (in [`drain_events`]),
/// reply [`MemberlistError::LeaveTimeout`] and clear the slot. A
/// successful `LeftCluster` resolution already cleared it, so this only
/// fires on the timeout path.
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
/// per-iteration `timeout_deadline` (alongside the pending-join
/// deadline) so the timeout fires even under a continuous network flood
/// that would otherwise keep the recv arm winning the select.
fn min_pending_leave_deadline(pending_leave: &Option<PendingLeave>) -> Option<Instant> {
  pending_leave.as_ref().map(|pl| pl.deadline)
}

/// Apply the deadline-firing protocol: drain every already-queued
/// bridge completion (no cap), re-poll the coordinator's deadline,
/// and call `handle_timeout` only if it is still past after the
/// drained completions are applied.
///
/// Every per-event timestamp (`received_at` on
/// `BridgeBytes`/`BridgeEof`/`BridgeError`/`OutboundFailReady`) is
/// honored by the FSM's deadline gate, so a successful exchange whose
/// completion arrived before the deadline is correctly counted even
/// when the driver processes it after the deadline.
///
/// Invoked from both the iter-top past-due branch (after its UDP
/// peek) and the main `select_biased!` timer arm. Centralised here
/// so a future timer-firing site can't accidentally regress to a
/// stale-state `handle_timeout` call.
///
/// # Tradeoff: unbounded drain under bridge backlog
///
/// The iter-top fairness drain is capped at
/// `driver_opts.iter_drain_cap()` (default 256) so a busy bridge
/// workload cannot starve `recv` / `accept` / `cmd`. This helper
/// deliberately **bypasses that cap** — the only way to keep
/// deadline accounting correct under bridge backlog is to apply
/// every already-arrived completion before firing the timer. The
/// per-iter cap remains on the iter-top drain, so the helper's
/// uncapped drain only runs once per deadline fire (rare in steady
/// state — deadlines are seconds apart). A pathological workload
/// that produces bridge events faster than the helper drains them
/// AND fires a deadline every iter would starve other arms; under
/// realistic SWIM cadences this trade is unconditionally correct.
///
/// `received_at` is sampled in user-space inside `bridge_task`
/// (right after the read syscall returns), not by the kernel. A
/// response sitting in the TCP recv buffer for milliseconds before
/// `read()` returns gets a timestamp that's "late" by that
/// kernel-buffer time. With JOIN_DEADLINE = 10s this is three
/// orders of magnitude inside the deadline budget; for a future
/// caller wanting tighter deadlines, kernel-level `SO_TIMESTAMPING`
/// would be the next step.
///
/// Returns `true` iff any work was applied (a queued completion was
/// dispatched OR `handle_timeout` fired).
#[allow(clippy::too_many_arguments)]
fn fire_timeout_with_drain<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_inbound_tx: &Sender<BridgeInbound>,
  bridge_inbound_rx: &Receiver<BridgeInbound>,
  bridge_ready_rx: &Receiver<BridgeReady>,
  driver_opts: DriverOptions,
  stream_opts: StreamTransportOptions,
) -> bool
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let mut dirty = false;
  while let Ok(inbound) = bridge_inbound_rx.try_recv() {
    dispatch_bridge_inbound::<I, A, R>(endpoint, inbound);
    dirty = true;
  }
  while let Ok(ready) = bridge_ready_rx.try_recv() {
    handle_bridge_ready::<I, A, R>(
      endpoint,
      bridges,
      bridge_inbound_tx,
      ready,
      stream_opts.bridge_recv_buf_len(),
      stream_opts.close_timeout(),
    );
    dirty = true;
  }
  let now = Instant::now();
  let after_drain_deadline = endpoint
    .poll_timeout()
    .unwrap_or(now + driver_opts.idle_wake_interval());
  if now >= after_drain_deadline {
    endpoint.handle_timeout(now);
    dirty = true;
  }
  dirty
}

/// Publish a fresh snapshot of the coordinator's observable state to
/// `arc-swap`. Readers see the new snapshot on their next
/// `MemberlistSnapshot::load` with no lock contention.
///
/// Each `Arc<NodeState>` in `members_vec` is a freshly-allocated clone of
/// the membership FSM's wire-state entry with `member_liveness` stamped onto
/// the `state` field — cheap because `NodeState::clone` is O(1) (`meta` is
/// `Bytes`, which shares the underlying buffer). The local node entry is
/// taken directly from the membership map so it carries the real meta,
/// incarnation, and protocol versions.
fn refresh_snapshot<I, A, R>(
  endpoint: &StreamEndpoint<I, A, R>,
  snapshot: &Arc<ArcSwap<MemberlistSnapshot<I, A>>>,
) where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let ep = endpoint.endpoint_ref();
  // Build a snapshot-local NodeState for each member with the FSM-tracked
  // liveness state (`member_liveness`) rather than the wire-protocol state
  // (`ns.state()`, which is fixed at the last Alive broadcast and does not
  // reflect subsequent Suspect/Dead transitions).
  let mut alive_count: usize = 0;
  let members_vec: Vec<Arc<NodeState<I, A>>> = ep
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
  snapshot.store(Arc::new(snap));
}

/// As [`refresh_snapshot`], but rebuilds + stores only when the endpoint's
/// snapshot version changed since `*last_version` — the rebuild clones every
/// `NodeState`, so skipping it on a dirty iteration that did not actually change
/// membership/health is the largest steady-state saving.
fn refresh_snapshot_if_changed<I, A, R>(
  endpoint: &StreamEndpoint<I, A, R>,
  snapshot: &Arc<ArcSwap<MemberlistSnapshot<I, A>>>,
  last_version: &mut u64,
) where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let v = endpoint.endpoint_ref().snapshot_version();
  if v != *last_version {
    *last_version = v;
    refresh_snapshot::<I, A, R>(endpoint, snapshot);
  }
}

/// Republish the machine's load-shedding counters into `metrics` if they changed
/// since `last` (a cheap `Copy` compare; the `ArcSwap` store allocates only on a
/// real change, which is rare).
fn refresh_metrics_if_changed<I, A, R>(
  endpoint: &StreamEndpoint<I, A, R>,
  metrics: &Arc<ArcSwap<memberlist_proto::metrics::Metrics>>,
  last: &mut memberlist_proto::metrics::Metrics,
) where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  let m = endpoint.endpoint_ref().metrics();
  if m != *last {
    *last = m;
    metrics.store(Arc::new(m));
  }
}

/// Route one [`BridgeReady`] message — either a freshly-accepted inbound
/// connection or the result of an outbound dial — into the coordinator,
/// spawning a per-bridge byte-mover on success.
fn handle_bridge_ready<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_inbound_tx: &Sender<BridgeInbound>,
  ready: BridgeReady,
  recv_buf_len: usize,
  close_timeout: core::time::Duration,
) where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  match ready {
    BridgeReady::OutboundOk(OutboundOkReady {
      eid,
      stream,
      out_rx,
      cancel_rx,
    }) => {
      // The `BridgeHandle` was pre-inserted at Connect time. If it is
      // still in the table the dial completed before any teardown for
      // this exchange — spawn the byte-mover with the pre-allocated
      // `out_rx` so it sees every byte the driver queued during the
      // dial. If the handle is GONE the coordinator already retired
      // the exchange (timeout, Close/Abort from the machine) while the
      // dial was in flight; dropping `stream` here closes the socket so
      // the peer is never asked to honor an exchange we no longer track.
      if !bridges.contains_key(&eid) {
        // The exchange was retired (timeout, Close/Abort from the
        // machine) while the dial was in flight. Drop the stream +
        // out_rx + cancel_rx so the peer is never asked to honor an
        // exchange we no longer track.
        drop(stream);
        drop(out_rx);
        drop(cancel_rx);
        return;
      }
      spawn_bridge(
        stream,
        eid,
        out_rx,
        cancel_rx,
        bridge_inbound_tx,
        recv_buf_len,
        close_timeout,
      );
    }
    BridgeReady::OutboundFail(OutboundFailReady {
      eid,
      err: _,
      received_at,
    }) => {
      // Remove the `BridgeHandle` (a same-tick Close may have already
      // taken it; `HashMap::remove` is a no-op for an absent key) so
      // the `out_tx` drops and any further bytes the machine surfaces
      // for the exchange route to a missing-bridge branch (silently
      // dropped). Then drive the exchange to a DIAL FAILURE (timestamped
      // with the dial-task's observation of the failure, NOT the
      // driver's later `Instant::now()`).
      //
      // NOT a benign EOF feed: a connect that never established has no
      // wire, and a one-way `UserMessage` maps a clean EOF to a
      // SUCCESSFUL completion (`Stream::handle_data`'s
      // `OutboundSendingRequest(UserMessage)` EOF arm) — which would
      // falsely report `send_reliable` success on an unreachable peer.
      // `handle_dial_failed` terminalizes the exchange as `Failed`
      // regardless of kind, so the parked reliable-send waiter resolves
      // with `Err(SendFailed)`.
      bridges.remove(&eid);
      endpoint.handle_dial_failed(eid, received_at);
    }
  }
}

/// Route one accepted inbound connection: reject a CIDR-blocked peer, else
/// allocate the exchange, register the bridge handle, and spawn the byte mover.
/// Returns `true` iff a bridge was spawned. Shared by the iter-top accept drain
/// and the select's accept arm so both paths stay byte-identical.
#[allow(clippy::too_many_arguments)]
fn handle_accepted<I, A, R>(
  accepted: io::Result<(TcpStream, SocketAddr)>,
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_inbound_tx: &Sender<BridgeInbound>,
  cidr_policy: &crate::transport::runtime::CidrFilter,
  stream_opts: StreamTransportOptions,
) -> bool
where
  I: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_proto::Data
    + memberlist_proto::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + From<SocketAddr>
    + Send
    + Sync
    + 'static,
  R: StreamTransport,
{
  match accepted {
    // CIDR: reject a reliable connection from a blocked peer at the boundary
    // (this guard is always false without the cidr feature).
    Ok((stream, peer)) if cidr_blocks(cidr_policy, peer.ip()) => {
      drop(stream);
      false
    }
    Ok((stream, peer)) => {
      // Inbound TCP/TLS connection from a peer. Allocate the exchange id, create
      // the bridge channels, spawn the byte mover. The record-layer codec inside
      // `StreamEndpoint::handle_transport_data` decrypts handshake bytes for the
      // TLS path; the bridge itself sees only raw socket bytes.
      let now = Instant::now();
      let Some(eid) = endpoint.accept_connection(peer.into(), now) else {
        // Not admitted (leaving, the inbound-stream cap is reached, or a
        // record-layer config error): no bridge exists, so drop the accepted
        // stream immediately rather than spawn a byte mover for a connection the
        // machine will never feed.
        drop(stream);
        return true;
      };
      let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
      let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
      bridges.insert(eid, BridgeHandle { out_tx, cancel_tx });
      spawn_bridge(
        stream,
        eid,
        out_rx,
        cancel_rx,
        bridge_inbound_tx,
        stream_opts.bridge_recv_buf_len(),
        stream_opts.close_timeout(),
      );
      true
    }
    // Transient accept error (file-descriptor pressure, peer reset during the
    // 3-way handshake). The kernel keeps the listening socket open across these
    // errors; the resolved accept is re-armed at the loop top and the next
    // connection succeeds.
    Err(_) => false,
  }
}

/// Spawn the [`crate::bridge::bridge_task`] byte-mover for `eid`.
/// The caller has already inserted the matching [`BridgeHandle`]
/// (out_tx) so any bytes queued before the bridge spawned reach the
/// wire via the `out_rx` handed in here. The task is detached — see
/// the [`BridgeHandle`] docstring for the rationale.
fn spawn_bridge(
  stream: TcpStream,
  eid: ExchangeId,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: futures_channel::oneshot::Receiver<()>,
  bridge_inbound_tx: &Sender<BridgeInbound>,
  recv_buf_len: usize,
  close_timeout: core::time::Duration,
) {
  let inbound_tx = bridge_inbound_tx.clone();
  compio::runtime::spawn(crate::bridge::bridge_task(
    stream,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    recv_buf_len,
    close_timeout,
  ))
  .detach();
}

#[cfg(all(test, feature = "tcp"))]
mod tests;
