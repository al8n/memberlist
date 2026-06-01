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
use compio::{
  buf::BufResult,
  net::{TcpListener, TcpStream, UdpSocket},
};
use flume::{Receiver, Sender};
use futures_util::{FutureExt, pin_mut, select_biased};
use memberlist_proto::{
  Instant, Node,
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, Transmit},
  framing::{MessageTag, decode_compound, decode_message, encode_message},
  message_from_any, message_to_any,
  streams::{StreamAction, StreamEndpoint, StreamTransport},
  typed::Message,
};

use crate::{
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, SetCompressionOptionsCmd, SetEncryptionOptionsCmd,
    ShutdownCmd, UpdateNodeMetadataCmd,
  },
  delegate::Delegate,
  driver_options::{DriverOptions, StreamTransportOptions},
  error::{JoinAllFailed, MemberlistError, Result},
  snapshot::MemberlistSnapshot,
};

/// Coordinator-allocated handle for one in-flight reliable exchange.
///
/// Re-exported from `memberlist-proto` so the driver and the per-bridge
/// task agree on the same opaque id without the driver having to expose
/// the machine's `streams` module to the rest of the crate.
pub(crate) type ExchangeId = memberlist_proto::streams::ExchangeId;

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
  reply: flume::Sender<Result<usize>>,
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
  repliers: Vec<flume::Sender<Result<()>>>,
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
      let _ = replier.send_async(make_result()).await;
    }
  }
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
  pub(crate) cancel_rx: Receiver<()>,
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
/// - `OutboundFail` → feed an EOF anchor
///   (`handle_transport_data(eid, &[], true, now)`) so the coordinator
///   retires the exchange. A no-op if the exchange is already gone.
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
/// - `Error` → `handle_transport_data(eid, &[], eof=true, now)` and the
///   bridge is dropped from the driver-side table.
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
  /// `out_tx`. Bounded(1): a single send is enough to cancel.
  cancel_tx: Sender<()>,
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
    .min(GOSSIP_RECV_BUF_MAX)
}

// The tuning knobs the driver loop reads (fallback idle sleep, dial
// timeout, bridge-inbound cap, iter-top drain cap, cmd fairness
// budget, past-due peek budget) all live on [`DriverOptions`] —
// constructed once per `Memberlist` and threaded into `stream_driver_loop`
// below. The historical const values are preserved as the
// `DEFAULT_*` constants in [`crate::driver_options`].

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
/// Both transports (`R = RawRecords` for TCP, `R = TlsRecords` for TLS)
/// use a raw [`TcpStream`] as the wire — the TLS handshake bytes flow
/// through the same byte path as application bytes; the record-layer
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
  commands: Receiver<Command>,
  events_tx: Sender<Event<I, A>>,
  events_dropped: Arc<std::sync::atomic::AtomicU64>,
  observation_dropped: Arc<std::sync::atomic::AtomicU64>,
  snapshot: Arc<ArcSwap<MemberlistSnapshot<I, A>>>,
  bridge_ready_rx: Receiver<BridgeReady>,
  bridge_ready_tx: Sender<BridgeReady>,
  shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
  driver_opts: DriverOptions,
  stream_opts: StreamTransportOptions,
  delegate: D,
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
  let mut shutdown_reply: Option<flume::Sender<Result<()>>> = None;
  // Outstanding synchronous-join waiters. Populated by
  // [`dispatch_command`] on [`JoinKind::WaitForCompletion`]; reduced
  // by [`drain_events`] on every push/pull `ExchangeCompleted`
  // (replied + removed there the moment a waiter's `pending` set
  // empties); the deadline path is reaped by [`reap_pending_joins`]
  // after each drain block.
  let mut pending_joins: Vec<PendingJoin> = Vec::new();
  // Outstanding graceful-leave waiter. Parked by [`dispatch_command`]
  // when a [`Command::Leave`] initiates the machine's `leave()` (the
  // endpoint was Running); resolved [`Ok`] in [`drain_events`]
  // on `Event::LeftCluster`; reaped [`MemberlistError::LeaveTimeout`]
  // by [`reap_pending_leave`] once its deadline elapses. At most one
  // is outstanding at a time (a second initiating Leave is replied
  // immediately rather than dropping a reply).
  let mut pending_leave: Option<PendingLeave> = None;

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

  // Arm the periodic probe / gossip / push-pull schedulers. Without this
  // the machine's `next_probe` / `next_gossip` / `next_pushpull` stay
  // `None`, so failure detection, broadcast dissemination, and anti-entropy
  // never run — the loop would only service explicit join push-pull and
  // passive inbound traffic.
  endpoint.start_scheduling(Instant::now());

  loop {
    let mut dirty = false;
    let mut exit = false;

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
            &mut shutdown_reply,
            &mut pending_joins,
            &mut pending_leave,
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
        let did_actions =
          drain_actions::<I, A, R>(&mut endpoint, &mut bridges, &bridge_ready_tx, stream_opts);
        let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
        let did_transmits = drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket).await;
        let did_events = drain_events(
          &mut endpoint,
          &obs_tx,
          &observation_dropped,
          &obs_payload_bytes,
          obs_payload_budget,
          &mut pending_joins,
          &mut pending_leave,
        )
        .await;
        if !(did_actions || did_transports || did_transmits || did_events) {
          break;
        }
        drained_any = true;
      }
      reap_pending_joins(&mut pending_joins, Instant::now()).await;
      reap_pending_leave(&mut pending_leave, Instant::now()).await;
      if drained_any {
        refresh_snapshot::<I, A, R>(&endpoint, &snapshot);
      }
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
      min_pending_join_deadline(&pending_joins),
      min_pending_leave_deadline(&pending_leave),
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
          if let Ok((n, src)) = res {
            let now = Instant::now();
            dispatch_gossip::<I, A, R>(&mut endpoint, src, &buf[..n], now);
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
        let did_actions =
          drain_actions::<I, A, R>(&mut endpoint, &mut bridges, &bridge_ready_tx, stream_opts);
        let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
        let did_transmits = drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket).await;
        let did_events = drain_events(
          &mut endpoint,
          &obs_tx,
          &observation_dropped,
          &obs_payload_bytes,
          obs_payload_budget,
          &mut pending_joins,
          &mut pending_leave,
        )
        .await;
        if !(did_actions || did_transports || did_transmits || did_events) {
          break;
        }
      }
      reap_pending_joins(&mut pending_joins, Instant::now()).await;
      reap_pending_leave(&mut pending_leave, Instant::now()).await;
      if dirty {
        refresh_snapshot::<I, A, R>(&endpoint, &snapshot);
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
        let did_actions =
          drain_actions::<I, A, R>(&mut endpoint, &mut bridges, &bridge_ready_tx, stream_opts);
        let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
        let did_transmits = drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket).await;
        let did_events = drain_events(
          &mut endpoint,
          &obs_tx,
          &observation_dropped,
          &obs_payload_bytes,
          obs_payload_budget,
          &mut pending_joins,
          &mut pending_leave,
        )
        .await;
        if !(did_actions || did_transports || did_transmits || did_events) {
          break;
        }
      }
      reap_pending_joins(&mut pending_joins, Instant::now()).await;
      reap_pending_leave(&mut pending_leave, Instant::now()).await;
      refresh_snapshot::<I, A, R>(&endpoint, &snapshot);
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
    let accept_fut = listener.accept().fuse();
    let timer_fut = compio::time::sleep_until(timeout_deadline.into_std()).fuse();
    pin_mut!(
      recv_fut,
      cmd_fut,
      bridge_in_fut,
      ready_fut,
      accept_fut,
      timer_fut
    );

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
          Ok((n, src)) => {
            let now = Instant::now();
            dispatch_gossip::<I, A, R>(&mut endpoint, src, &buf[..n], now);
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
      accepted = accept_fut => {
        match accepted {
          Ok((stream, peer)) => {
            // Inbound TCP/TLS connection from a peer. Allocate the
            // exchange id, create the bridge channels, spawn the byte
            // mover. The record-layer codec inside
            // `StreamEndpoint::handle_transport_data` decrypts handshake
            // bytes for the TLS path; the bridge itself sees only raw
            // socket bytes.
            let now = Instant::now();
            let eid = endpoint.accept_connection(peer.into(), now);
            let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
            let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
            bridges.insert(eid, BridgeHandle { out_tx, cancel_tx });
            spawn_bridge(
              stream,
              eid,
              out_rx,
              cancel_rx,
              &bridge_inbound_tx,
              stream_opts.bridge_recv_buf_len(),
              stream_opts.close_timeout(),
            );
            dirty = true;
          }
          Err(_) => {
            // Transient accept error (file-descriptor pressure, peer
            // reset during the 3-way handshake). The kernel keeps the
            // listening socket open across these errors; the next loop
            // iteration re-arms `listener.accept().fuse()` and the
            // next connection succeeds.
          }
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
              &mut shutdown_reply,
              &mut pending_joins,
              &mut pending_leave,
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
      let did_actions =
        drain_actions::<I, A, R>(&mut endpoint, &mut bridges, &bridge_ready_tx, stream_opts);
      let did_transports = drain_transport_transmits::<I, A, R>(&mut endpoint, &bridges);
      let did_transmits = drain_transmits::<I, A, R>(&mut endpoint, &gossip_socket).await;
      let did_events = drain_events(
        &mut endpoint,
        &obs_tx,
        &observation_dropped,
        &obs_payload_bytes,
        obs_payload_budget,
        &mut pending_joins,
        &mut pending_leave,
      )
      .await;
      if !(did_actions || did_transports || did_transmits || did_events) {
        break;
      }
    }
    reap_pending_joins(&mut pending_joins, Instant::now()).await;
    reap_pending_leave(&mut pending_leave, Instant::now()).await;

    if dirty {
      refresh_snapshot::<I, A, R>(&endpoint, &snapshot);
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
    let reply: flume::Sender<Result<()>> = match c {
      Command::Shutdown(ShutdownCmd { reply }) => reply,
      Command::Leave(LeaveCmd { reply }) => reply,
      Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { reply, .. }) => reply,
      Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => reply,
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd { reply, .. }) => reply,
      Command::QueueUserBroadcast(cmd) => cmd.reply().clone(),
      Command::SetLocalState(cmd) => cmd.reply().clone(),
      Command::SetAckPayload(cmd) => cmd.reply().clone(),
      Command::Join(JoinCmd { reply, .. }) => {
        // Join's reply type is `Result<usize>`; surface the same
        // Shutdown error through a separate match arm so the type
        // checker sees the right `Sender<Result<usize>>`.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = reply.send_async(Err(MemberlistError::Shutdown)).await;
        continue;
      }
    };
    // Ignoring Err: caller dropped the reply receiver.
    let _ = reply.send_async(res).await;
  }
  // Drop the Receiver immediately so any subsequent clone send
  // fails fast (see step 3 above). After this point flume's send
  // returns `Err(SendError::Disconnected)` for every clone.
  drop(commands);
  // Reply Err(Shutdown) to every outstanding synchronous-join waiter.
  // Their reply Senders live in `pending_joins`; without this drain
  // the corresponding receivers on the caller side would hang
  // forever (the loop body's reap path is gone, and the driver task
  // is about to exit).
  for pj in pending_joins.drain(..) {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = pj.reply.send_async(Err(MemberlistError::Shutdown)).await;
  }
  // Reply Err(Shutdown) to every joined graceful-leave replier whose
  // `LeftCluster` never arrived before the loop exited (e.g. a shutdown
  // raced the leave flush). Without this their receivers would hang
  // forever — the loop body's reap path is gone and the task is exiting.
  if let Some(pl) = pending_leave.take() {
    pl.resolve_all(|| Err(MemberlistError::Shutdown)).await;
  }
  for (_eid, handle) in bridges.drain() {
    // Ignoring Err: the bridge may have exited already; the close
    // notification is best-effort.
    let _ = handle.out_tx.try_send(BridgeOut::Close);
  }
  // Drop the TCP reliable listener FIRST so the bound port is released
  // immediately. The listener does not have an explicit close API
  // distinct from drop — the local going out of scope here closes the
  // file descriptor.
  drop(listener);
  // Ignoring Err: socket close on shutdown — the runtime tears down
  // file descriptors anyway and the error is unactionable.
  let _ = gossip_socket.close().await;

  // Now ack the shutdown caller — both bound ports have been released,
  // so the caller's `shutdown.await` returns to a state where an
  // immediate rebind on the same address succeeds.
  if let Some(reply) = shutdown_reply {
    // Ignoring Err: caller dropped the reply receiver.
    let _ = reply.send_async(Ok(())).await;
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
// Eight arguments is past clippy's heuristic but each is a distinct
// piece of mutable state the function reads or threads through; a
// container struct would just be a parameter pack with no internal
// invariant, costing readability for no gain.
#[allow(clippy::too_many_arguments)]
async fn dispatch_command<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_ready_tx: &Sender<BridgeReady>,
  stream_opts: StreamTransportOptions,
  shutdown_reply: &mut Option<flume::Sender<Result<()>>>,
  pending_joins: &mut Vec<PendingJoin>,
  pending_leave: &mut Option<PendingLeave>,
  leave_timeout: core::time::Duration,
  cmd: Command,
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
        let _ = reply.send_async(Err(MemberlistError::NotRunning)).await;
        return;
      }
      // Both `JoinKind::Dispatch` and `JoinKind::WaitForCompletion`
      // share the same `start_push_pull` fan-out: each seed becomes
      // one queued Connect action that the inline drain below pops
      // and routes through `process_one_action`. Inline draining
      // each seed's Connect immediately after `start_push_pull`
      // gives the Connect's ExchangeId CALL-SCOPED ownership: the
      // resulting eid is bound only to the synchronous-join waiter
      // (if any) that dispatched it, never to a sibling waiter
      // also targeting the same address. The driver's single-task
      // discipline guarantees no other source can interleave a
      // Connect action between this `start_push_pull` and the
      // immediately-following `poll_action`.
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
              process_one_action(action, bridges, bridge_ready_tx, stream_opts, None);
            }
            count += 1;
          }
          // Ignoring Err: the caller may have dropped the reply
          // receiver (e.g. the user-facing `dispatch_join_with`
          // future was cancelled).
          let _ = reply.send_async(Ok(count)).await;
        }
        JoinKind::WaitForCompletion(crate::command::WaitForCompletionArgs { deadline }) => {
          let mut pending: HashSet<ExchangeId> = HashSet::with_capacity(addrs.len());
          for addr in addrs {
            // Ignoring StreamId return: per the Dispatch arm's
            // rationale above.
            let _ = endpoint.start_push_pull(addr.into(), PushPullKind::Join, now);
            // Drain actions queued by THIS `start_push_pull`. The
            // capture binds the Connect's ExchangeId to the
            // dispatching waiter only — duplicate seeds produce
            // duplicate exchanges and each is tracked independently
            // through its own ExchangeId.
            while let Some(action) = endpoint.poll_action() {
              process_one_action(
                action,
                bridges,
                bridge_ready_tx,
                stream_opts,
                Some((addr, &mut pending)),
              );
            }
          }
          // `requested` is the number of outbound exchanges this call
          // actually dispatched (one per address, including duplicates).
          // For the call-into-empty case it would be 0, but the public
          // `join_with` already defends against that — by the time we
          // see a `WaitForCompletion` here `addrs` is non-empty.
          let requested = pending.len();
          pending_joins.push(PendingJoin {
            pending,
            contacted: 0,
            requested,
            deadline,
            reply,
          });
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
            let _ = reply.send_async(other).await;
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
      let _ = reply.send_async(res).await;
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
      let _ = reply.send_async(res).await;
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
      let _ = reply.send_async(res).await;
    }
    Command::QueueUserBroadcast(cmd) => {
      // Gate on a running node FIRST: after `leave()` the gossip scheduler
      // is stopped, so `user_broadcasts` would never drain. Then validate
      // the framed lone `UserData` packet against the gossip budget: an
      // over-budget payload is deterministically untransmittable, so the
      // machine setter rejects it without storing it — surface that as
      // `PayloadTooLarge` rather than a false `Ok`. The single-task driver
      // makes the check + apply atomic.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .queue_user_broadcast(cmd.data().clone())
          .map_err(|e| MemberlistError::PayloadTooLarge(e.to_string()))
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply().send_async(res).await;
    }
    Command::SetLocalState(cmd) => {
      // Gate on a running node FIRST: after `leave()` no push/pull
      // exchange will carry the snapshot, so reject with `NotRunning`. Then
      // validate the framed-PushPull size against the reliable-stream frame
      // budget: a snapshot whose framed PushPull exceeds it would be rejected
      // by every receiver's frame-length gate, so the application state would
      // never reach a peer. The machine setter rejects such a snapshot without
      // storing it; surface that as `PayloadTooLarge` rather than a false `Ok`.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .set_local_state_snapshot(cmd.state().clone())
          .map_err(|e| MemberlistError::PayloadTooLarge(e.to_string()))
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply().send_async(res).await;
    }
    Command::SetAckPayload(cmd) => {
      // Gate on a running node FIRST: after `leave()` no probe ack will
      // carry the payload, so reject with `NotRunning`. Then validate the
      // framed-ack size against the gossip packet budget: an over-budget ack
      // is emitted as a single UDP datagram that always fails to send, so a
      // probing peer would receive no ack and falsely suspect this node. The
      // machine setter rejects such a payload without storing it; surface
      // that as `PayloadTooLarge` rather than a false `Ok`.
      let res: Result<()> = if endpoint.is_running() {
        endpoint
          .set_ack_payload(cmd.payload().clone())
          .map_err(|e| MemberlistError::PayloadTooLarge(e.to_string()))
      } else {
        Err(MemberlistError::NotRunning)
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = cmd.reply().send_async(res).await;
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
      // A transport error and an orderly close both retire the recv
      // half from the coordinator's view; feed an EOF anchor and let
      // the machine reap the bridge through `pump_bridges`. Same
      // removal discipline as the Eof arm — let the eventual
      // `StreamAction::Close` clean up the driver-side entry.
      endpoint.handle_transport_data(eid, &[], true, received_at);
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

  // Drain the raw-buffer queue and decode each datagram. Each iteration
  // pops exactly the datagram we just fed (FIFO), but draining the loop
  // unconditionally handles a stale buffer left over from a prior
  // ingress that some earlier handle_gossip had no chance to drain.
  while let Some((from_addr, raw)) = endpoint.poll_memberlist_ingress() {
    let plain = match endpoint.decrypt_gossip(&raw) {
      Ok(p) => p,
      // Drop the datagram on a decrypt / unknown-tag / oversize error.
      // Gossip is lossy and self-healing — the peer retransmits on the
      // next gossip round.
      Err(_) => continue,
    };
    // Compound vs single-message demux: a compound frame carries N
    // plain-frame slices; everything else is a single plain frame.
    if !plain.is_empty() && plain[0] == MessageTag::Compound as u8 {
      let parts = match decode_compound(&plain) {
        Ok(parts) => parts,
        // A malformed compound (truncated, count < 2, trailing bytes)
        // drops the whole datagram per the codec contract.
        Err(_) => continue,
      };
      for part in parts {
        if let Some(msg) = decode_plain::<I, A>(part) {
          endpoint.handle_packet(from_addr.cheap_clone(), msg, now);
        }
        // Ignoring decode failures on individual parts: a malformed
        // inner is dropped silently, matching the gossip drop policy.
      }
    } else if let Some(msg) = decode_plain::<I, A>(&plain) {
      endpoint.handle_packet(from_addr, msg, now);
    }
  }
}

/// Decode a single plain frame into a typed [`Message<I, A>`]. Returns
/// `None` on any frame / bridge decoder error (the caller drops the
/// datagram, matching the lossy-gossip discipline).
///
/// Strict consumption: the decoded message MUST cover the entire input
/// slice. Trailing bytes after a valid message indicate a malformed
/// frame (truncated compound, codec misuse, hostile peer); dropping
/// closes the silent-acceptance window the partial-decode pattern
/// would otherwise open.
fn decode_plain<I, A>(buf: &[u8]) -> Option<Message<I, A>>
where
  I: memberlist_proto::Data,
  A: memberlist_proto::Data,
{
  let (consumed, any) = decode_message(buf).ok()?;
  if consumed != buf.len() {
    return None;
  }
  message_from_any::<I, A>(&any).ok()
}

/// Drain every [`StreamAction`] the coordinator has queued, dispatching
/// each on the driver's per-bridge handle table. Returns `true` iff any
/// action was processed.
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
/// to the specific synchronous-join waiter that just dispatched it).
/// In the normal `drain_actions` path it is `None` — actions emitted by
/// non-`dispatch_command` sources (probe-driven push/pull fallback,
/// teardown sweeps) have no synchronous-join ownership.
fn process_one_action(
  action: StreamAction,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_ready_tx: &Sender<BridgeReady>,
  stream_opts: StreamTransportOptions,
  capture: Option<(SocketAddr, &mut HashSet<ExchangeId>)>,
) {
  match action {
    StreamAction::Connect(info) => {
      let eid = info.id();
      let peer = info.peer();
      if let Some((target_addr, pending_exchanges)) = capture
        && peer == target_addr
      {
        pending_exchanges.insert(eid);
      }
      let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
      let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
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
        let _ = handle.cancel_tx.try_send(());
      }
    }
  }
}

fn drain_actions<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_ready_tx: &Sender<BridgeReady>,
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
  let mut progress = false;
  while let Some(action) = endpoint.poll_action() {
    progress = true;
    process_one_action(action, bridges, bridge_ready_tx, stream_opts, None);
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
async fn drain_transmits<I, A, R>(
  endpoint: &mut StreamEndpoint<I, A, R>,
  gossip_socket: &UdpSocket,
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
  while let Some(transmit) = endpoint.poll_memberlist_transmit() {
    progress = true;
    let (peer, datagram) = match encode_transmit::<I, A>(transmit) {
      Some(pair) => pair,
      // A locally-built message that bridges to AnyMessage and round-
      // trips through `encode_message` cannot fail in normal operation
      // (see `wire.rs`'s `.expect` convention); if it does we drop the
      // transmit so a single bad codec invocation cannot wedge the
      // driver.
      None => continue,
    };
    let compressed = endpoint.compress_gossip(&datagram);
    let on_wire = match endpoint.encrypt_gossip(&compressed) {
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

/// Encode one outbound [`Transmit`] into `(peer_addr, datagram_bytes)`.
/// A `Compound` carries 2+ messages packed into a single datagram via
/// [`memberlist_proto::framing::encode_compound`]; a `Packet` is one
/// plain frame via [`memberlist_proto::framing::encode_message`].
fn encode_transmit<I, A>(transmit: Transmit<I, A>) -> Option<(A, Vec<u8>)>
where
  I: memberlist_proto::Data,
  A: memberlist_proto::Data,
{
  use memberlist_proto::framing::encode_compound;
  match transmit {
    Transmit::Packet(pkt) => {
      let (to, msg) = pkt.into_parts();
      let any = message_to_any::<I, A>(&msg).ok()?;
      let bytes = encode_message(&any).ok()?;
      Some((to, bytes))
    }
    Transmit::Compound(cmp) => {
      let (to, msgs) = cmp.into_parts();
      let mut anys = Vec::with_capacity(msgs.len());
      for msg in msgs {
        anys.push(message_to_any::<I, A>(&msg).ok()?);
      }
      let bytes = encode_compound(&anys).ok()?;
      Some((to, bytes))
    }
  }
}

/// Fire the matching [`Delegate`] hook for one drained [`Event`].
///
/// The event-shaped hooks (`notify_join` / `notify_leave` / `notify_update`
/// / `notify_ping_complete`) run on the driver thread BEFORE the event is
/// forwarded to subscribers, so a delegate observes the transition before
/// any [`EventStream`](crate::EventStream) consumer does. The membership
/// FSM already carries the resolved `Arc<NodeState>` inside each variant,
/// so the hook borrows it (cheap `Arc` bump) with no re-projection.
///
/// Admission (`notify_alive` / `notify_merge`) is NOT fired here — those
/// are the machine's `AliveDelegate` / `MergeDelegate` predicates, supplied
/// via [`Options`](crate::Options) and run inline inside the FSM ahead of
/// the alive/merge transition. The observation [`Delegate`] is a distinct
/// concern: its hooks observe transitions the FSM has already applied.
pub(crate) async fn dispatch_event_delegate<I, A, D>(delegate: &D, ev: &Event<I, A>)
where
  D: Delegate<Id = I, Address = A>,
{
  match ev {
    Event::NodeJoined(node) => delegate.notify_join(node.clone()).await,
    Event::NodeLeft(node) => delegate.notify_leave(node.clone()).await,
    Event::NodeUpdated(node) => delegate.notify_update(node.clone()).await,
    Event::PingCompleted(payload) => {
      let node = payload.node_ref();
      delegate
        .notify_ping_complete(
          node.id_ref(),
          node.address_ref(),
          payload.rtt(),
          payload.payload_ref().clone(),
        )
        .await;
    }
    Event::NodeConflict(c) => {
      delegate
        .notify_conflict(c.existing_ref().clone(), c.other_ref().clone())
        .await;
    }
    Event::UserPacket(pkt) => {
      delegate
        .notify_user_msg(std::borrow::Cow::Borrowed(pkt.data_ref().as_ref()))
        .await;
    }
    Event::RemoteStateReceived(rs) => {
      delegate
        .merge_remote_state(rs.user_data_ref().as_ref(), rs.join())
        .await;
    }
    _ => {}
  }
}

/// Yield to the runtime exactly once.
///
/// The event drain below is synchronous — no `.await` fires for membership
/// events — so on a single-threaded runtime the observation task is not
/// scheduled mid-drain. A bounded `obs_tx` would therefore overflow on a
/// single large-but-valid burst (e.g. a join push/pull carrying many members)
/// before the task drains a single event. Yielding hands the scheduler to the
/// already-woken observation task so it can drain `obs_rx` before the drain
/// continues. Runtime-agnostic (no dependency on a specific `yield_now`):
/// re-arms the waker and returns `Pending` once, so the executor runs other
/// ready tasks before re-polling this one.
pub(crate) async fn yield_once() {
  let mut yielded = false;
  core::future::poll_fn(move |cx| {
    if yielded {
      core::task::Poll::Ready(())
    } else {
      yielded = true;
      cx.waker().wake_by_ref();
      core::task::Poll::Pending
    }
  })
  .await
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
  pending_joins: &mut Vec<PendingJoin>,
  pending_leave: &mut Option<PendingLeave>,
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
      if let Some(idx) = pending_joins
        .iter()
        .position(|pj| pj.pending.contains(&eid))
      {
        let pj = &mut pending_joins[idx];
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
          let pj = pending_joins.swap_remove(idx);
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
          let _ = pj.reply.send_async(result).await;
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
      && let Some(pl) = pending_leave.take()
    {
      // Resolve EVERY joined replier (the initiator plus any racing
      // clones) with `Ok(())`.
      pl.resolve_all(|| Ok(())).await;
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

/// The observation-channel byte-backstop weight of an event: `Some(len)` for
/// the payload-bearing variants (`UserPacket` / `RemoteStateReceived`, whose
/// `Bytes` ride up to `max_stream_frame_size`), `None` for the small membership
/// / control events the count cap already bounds.
pub(crate) fn observation_payload_bytes<I, A>(ev: &Event<I, A>) -> Option<u64> {
  match ev {
    Event::UserPacket(p) => Some(p.data_ref().len() as u64),
    Event::RemoteStateReceived(r) => Some(r.user_data_ref().len() as u64),
    _ => None,
  }
}

/// Add a just-enqueued event's payload weight (if any) to the byte-backstop
/// counter. Paired with the subtract in [`observation_task`] on dequeue.
pub(crate) fn add_obs_payload(counter: &std::sync::atomic::AtomicU64, bytes: Option<u64>) {
  if let Some(b) = bytes {
    counter.fetch_add(b, std::sync::atomic::Ordering::Relaxed);
  }
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
    dispatch_event_delegate(&delegate, &ev).await;
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
      let _ = pj.reply.send_async(result).await;
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
/// The snapshot's `<I, A>` parameters propagate straight through from
/// the [`StreamEndpoint`] — every `Node` is built by `cheap_clone`ing
/// the membership FSM's own id / address (an `Arc` bump for `SmolStr`,
/// scalar copy for `SocketAddr`).
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
  let mut members_vec: Vec<Node<I, A>> = Vec::new();
  let mut alive_count: usize = 0;
  for ns in ep.members() {
    members_vec.push(Node::new(
      ns.id_ref().cheap_clone(),
      ns.address_ref().cheap_clone(),
    ));
    if let Some(memberlist_proto::typed::State::Alive) = ep.member_liveness(ns.id_ref()) {
      alive_count += 1;
    }
  }
  let local = Node::new(
    ep.local_id_ref().cheap_clone(),
    ep.advertise_ref().cheap_clone(),
  );
  let member_count = ep.num_members();
  let snap = MemberlistSnapshot::new(members_vec, local, alive_count, member_count);
  snapshot.store(Arc::new(snap));
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
      // dropped). Then feed EOF (timestamped with the dial-task's
      // observation of the failure, NOT the driver's later
      // `Instant::now()`) so a pre-deadline dial failure terminalizes
      // the FSM cleanly rather than crossing the stream FSM's
      // deadline gate.
      bridges.remove(&eid);
      endpoint.handle_transport_data(eid, &[], true, received_at);
    }
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
  cancel_rx: Receiver<()>,
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
