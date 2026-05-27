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
  collections::{HashMap, HashSet},
  io,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Instant,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use compio::{buf::BufResult, net::UdpSocket};
use flume::{Receiver, Sender};
use futures_util::{FutureExt, pin_mut, select_biased};
use memberlist::codec::{
  DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
  parse_messages,
};
use memberlist_machine::{
  QuicEndpoint,
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, Transmit},
  streams::ExchangeId,
};
use memberlist_wire::{CheapClone, Node};

use crate::{
  QuicDriverOptions,
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, SetCompressionOptionsCmd, SetEncryptionOptionsCmd,
    ShutdownCmd, UpdateNodeMetadataCmd,
  },
  error::{JoinAllFailed, MemberlistError, Result},
  snapshot::MemberlistSnapshot,
};

/// Hard ceiling on the per-recv UDP buffer. UDP's wire payload is
/// capped at 65507 bytes (the IP-layer maximum once the 8-byte UDP
/// header and 20-byte IPv4 header are deducted); a recv buffer larger
/// than that wastes an allocation per iteration. Mirrors the stream
/// driver's `GOSSIP_RECV_BUF_MAX`.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

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

/// All driver-owned state, packed so the loop body can borrow
/// individual fields without fighting Rust's borrow checker
/// against a sprawling let-binding cluster.
struct QuicDriverState<I> {
  endpoint: QuicEndpoint<I>,
  udp_socket: UdpSocket,
  commands: Receiver<Command>,
  events_tx: Sender<Event<I, SocketAddr>>,
  events_dropped: Arc<AtomicU64>,
  snapshot: Arc<ArcSwap<MemberlistSnapshot<I, SocketAddr>>>,
  shutdown_flag: Arc<AtomicBool>,
  driver_opts: QuicDriverOptions,
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
  shutdown_reply: Option<flume::Sender<Result<()>>>,
}

/// Compute the per-recv UDP buffer size from the coordinator's
/// configured `gossip_mtu`. A datagram on the wire is at most
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD` bytes when encryption is
/// enabled (the wrapper carries the algorithm tag + nonce + AEAD auth
/// tag); the driver sizes the recv buffer to that value so a
/// configured `with_gossip_mtu` above the historical 16 KiB default is
/// not silently truncated by the kernel. Mirrors the stream driver's
/// `gossip_recv_buf_len`.
fn gossip_recv_buf_len<I>(endpoint: &QuicEndpoint<I>) -> usize
where
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  endpoint
    .gossip_mtu()
    .saturating_add(memberlist_wire::ENCRYPTED_WRAPPER_OVERHEAD)
    .min(GOSSIP_RECV_BUF_MAX)
}

/// Single-owner QUIC driver task.
///
/// Drives the [`QuicEndpoint`] until the command channel closes (all
/// `Memberlist` handles dropped) or a `Command::Shutdown` is received.
/// All mutations on the endpoint happen here; reads happen lock-free
/// via the published [`MemberlistSnapshot`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn driver_loop<I>(
  endpoint: QuicEndpoint<I>,
  udp_socket: UdpSocket,
  commands: Receiver<Command>,
  events_tx: Sender<Event<I, SocketAddr>>,
  events_dropped: Arc<AtomicU64>,
  snapshot: Arc<ArcSwap<MemberlistSnapshot<I, SocketAddr>>>,
  shutdown_flag: Arc<AtomicBool>,
  driver_opts: QuicDriverOptions,
) where
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  let mut state = QuicDriverState {
    endpoint,
    udp_socket,
    commands,
    events_tx,
    events_dropped,
    snapshot,
    shutdown_flag,
    driver_opts,
    pending_joins: HashMap::new(),
    next_pending_join_id: 0,
    shutdown_reply: None,
  };

  // Per-driver UDP recv buffer size derived once from the coordinator's
  // configured `gossip_mtu` + encrypted-wrapper overhead, capped at the
  // IP-layer UDP maximum. The coordinator's `gossip_mtu` is set at
  // construction time and never reconfigured at runtime, so a single
  // value computed up-front is correct.
  let recv_buf_len = gossip_recv_buf_len::<I>(&state.endpoint);

  let mut exit = false;
  // Tracks whether the previous iteration's inputs (cmd-fairness
  // drain, select arm) advanced state without the corresponding
  // four-surface drain having flushed yet. Hoisted across iterations
  // so a select arm that sets `dirty = true` is observed by the
  // NEXT iter-top dirty drain — placing the drain at iter top (not
  // after the select) avoids the borrow conflict between the drain's
  // `&mut state` and the recv/cmd futures' immutable borrows.
  let mut dirty = false;
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
          dispatch_command::<I>(
            &mut state.endpoint,
            &mut state.shutdown_reply,
            &mut state.pending_joins,
            &mut state.next_pending_join_id,
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
            // receiver drops when `driver_loop` returns, so a racing
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
      if drain_actions::<I>(&mut state).await {
        refresh_snapshot::<I>(&state.endpoint, &state.snapshot);
      }
      // Reap any pending-join waiter whose `pending` set was emptied
      // by the drain (a push/pull ExchangeCompleted reduction) or
      // whose deadline elapsed during the drain. Mirrors the stream
      // driver's post-drain `reap_pending_joins` call.
      reap_pending_joins(&mut state.pending_joins, Instant::now()).await;
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
    // the select — `recv` keeps re-arming. The bounded peek +
    // re-polled-deadline `handle_timeout` + drain sequence inside
    // `fire_timeout_with_drain` runs the past-due deadline protocol
    // BEFORE the select re-arms: a kernel-buffered datagram that
    // would resolve the deadline gets exactly one shot per past-due
    // iteration; if the deadline survives the peek `handle_timeout`
    // fires. Mirrors the stream driver's iter-top past-due branch
    // (`memberlist-compio/src/driver.rs:602`).
    //
    // The past-due check folds in the earliest pending-join
    // deadline: under a continuous recv flood `recv` would always win
    // the main select over `timer`, so an expired synchronous-join
    // waiter would never have its deadline reap fire either. Folding
    // the min pending-join deadline into the past-due trigger lets
    // the iter-top branch run `reap_pending_joins` even when the
    // coordinator itself has no near-term work.
    let past_due_t = {
      let coord = state.endpoint.poll_timeout();
      let pj_min = min_pending_join_deadline(&state.pending_joins);
      [coord, pj_min].into_iter().flatten().min()
    };
    if let Some(t) = past_due_t
      && setup_now >= t
    {
      if fire_timeout_with_drain::<I>(&mut state, recv_buf_len).await {
        refresh_snapshot::<I>(&state.endpoint, &state.snapshot);
      }
      reap_pending_joins(&mut state.pending_joins, Instant::now()).await;
      dirty = false;
      continue;
    }

    let timeout_deadline = {
      let coord = state.endpoint.poll_timeout();
      let pj_min = min_pending_join_deadline(&state.pending_joins);
      let idle = setup_now + state.driver_opts.idle_wake_interval();
      // `coord`/`pj_min` may be `None`; `idle` is always populated.
      // The earliest of the three drives the timer arm.
      [coord, pj_min, Some(idle)]
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
    let timer_fut = compio::time::sleep_until(timeout_deadline).fuse();
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
            state.endpoint.handle_udp(src, &buf[..n], received_at);
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
        // The bounded-peek `fire_timeout_with_drain` helper is NOT
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
            dispatch_command::<I>(
              &mut state.endpoint,
              &mut state.shutdown_reply,
              &mut state.pending_joins,
              &mut state.next_pending_join_id,
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
  state.shutdown_flag.store(true, Ordering::Release);
  while let Ok(c) = state.commands.try_recv() {
    let res = Err(MemberlistError::Shutdown);
    let reply: flume::Sender<Result<()>> = match c {
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
      Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => reply,
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd { reply, .. }) => reply,
      Command::Join(JoinCmd { reply, .. }) => {
        // `Join`'s reply type is `Result<usize>`; surface the same
        // `Shutdown` error through a separate arm so the type
        // checker sees the right `Sender<Result<usize>>`.
        // Ignoring Err: caller dropped the reply receiver.
        let _ = reply.send_async(Err(MemberlistError::Shutdown)).await;
        continue;
      }
    };
    // Ignoring Err: caller dropped the reply receiver.
    let _ = reply.send_async(res).await;
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
    let _ = pj.reply.send_async(Err(MemberlistError::Shutdown)).await;
  }

  // Drop the bound UDP socket BEFORE acking the observed shutdown
  // caller so an immediate rebind on the same port after
  // `shutdown.await` succeeds. The `Memberlist::shutdown` caller is
  // waiting on the stashed reply; once dropped here and the reply
  // fires below, the awaited future resolves and any subsequent
  // bind to the same port observes the released kernel slot.
  drop(state.udp_socket);

  // Ack any stashed Shutdown command reply.
  if let Some(reply) = state.shutdown_reply.take() {
    // Ignoring Err: the caller dropped its reply receiver; nothing
    // to surface.
    let _ = reply.send_async(Ok(())).await;
  }
}

/// Dispatch one driver command.
///
/// Mirrors the stream driver's `dispatch_command` for the QUIC
/// endpoint surface. The `Join` arm fans out one
/// [`QuicEndpoint::start_push_pull`] per resolved seed; `Leave` /
/// `SetEncryptionOptions` route to their machine setters; `Shutdown`
/// stashes the reply for the post-loop ack so the bound UDP port is
/// released before the caller's `shutdown().await` resumes.
///
/// `UpdateNodeMetadata` and `SetCompressionOptions` currently surface
/// `MemberlistError::Io` with a "not implemented" message. The
/// [`QuicEndpoint`] does not yet expose a public `update_meta` or
/// `set_compression_options`; both have machine-side equivalents on
/// [`memberlist_machine::streams::StreamEndpoint`] but are not on the
/// QUIC endpoint's public surface (the `endpoint_mut` accessor is
/// `#[cfg(test)]`-only). Wiring these requires an additive machine
/// extension; the driver replies clearly rather than silently
/// reporting success.
///
/// `JoinKind::WaitForCompletion` fans out one
/// [`QuicEndpoint::start_push_pull`] per seed and parks a
/// [`PendingJoin`] in `pending_joins`; the events drain reduces it on
/// [`Event::ExchangeCompleted`] filtered to [`ExchangeKind::PushPull`]
/// and the per-iteration `reap_pending_joins` replies on completion or
/// deadline expiry.
#[allow(clippy::too_many_arguments)]
async fn dispatch_command<I>(
  endpoint: &mut QuicEndpoint<I>,
  shutdown_reply: &mut Option<flume::Sender<Result<()>>>,
  pending_joins: &mut HashMap<u64, PendingJoin>,
  next_pending_join_id: &mut u64,
  cmd: Command,
  now: Instant,
) where
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  match cmd {
    Command::Join(JoinCmd { addrs, kind, reply }) => {
      match kind {
        JoinKind::Dispatch => {
          let mut count: usize = 0;
          for addr in &addrs {
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
          let _ = reply.send_async(Ok(count)).await;
        }
        JoinKind::WaitForCompletion(crate::command::WaitForCompletionArgs { deadline }) => {
          // Dispatch one outbound push/pull per seed; each
          // `start_push_pull` returns a fresh machine `StreamId` that
          // coerces into the [`ExchangeId`] domain via the
          // [`From<StreamId> for ExchangeId`] impl in
          // `memberlist-machine/src/event.rs`. The QUIC backend keys
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
            let stream_id = endpoint.start_push_pull(*addr, PushPullKind::Join, now);
            pending.insert(ExchangeId::from(stream_id));
          }
          // `requested` is the number of outbound exchanges this call
          // dispatched (one per address, including duplicates). The
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
      let res: Result<()> = endpoint
        .leave(now)
        .map_err(|e| MemberlistError::Io(io::Error::other(e.to_string())));
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send_async(res).await;
    }
    Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { meta, reply }) => {
      let res: Result<()> = match memberlist_wire::typed::Meta::try_from(meta) {
        Ok(m) => endpoint
          .update_meta(m)
          .map_err(|e| MemberlistError::Io(io::Error::other(e.to_string()))),
        Err(e) => Err(MemberlistError::Io(io::Error::other(e.to_string()))),
      };
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send_async(res).await;
    }
    Command::SetCompressionOptions(SetCompressionOptionsCmd { opts, reply }) => {
      endpoint.set_compression_options(opts);
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send_async(Ok(())).await;
    }
    Command::SetEncryptionOptions(SetEncryptionOptionsCmd { opts, reply }) => {
      endpoint.set_encryption_options(opts);
      // Ignoring Err: caller dropped the reply receiver.
      let _ = reply.send_async(Ok(())).await;
    }
    Command::Shutdown(ShutdownCmd { reply }) => {
      // Stash the reply for the post-loop cleanup. Acking AFTER the
      // UDP socket drops ensures the bound port is free when the
      // caller resumes from `shutdown.await`, so an immediate rebind
      // on the same port succeeds.
      *shutdown_reply = Some(reply);
    }
  }
}

/// Past-due / timer-arm helper: bounded peek on the UDP socket
/// (consume one buffered datagram if any arrived during the
/// deadline), re-poll the coordinator's deadline, fire
/// `handle_timeout` only if still past, then drain to fixed point.
///
/// Under a continuous UDP-recv flood the main `select_biased!`'s
/// `recv` arm always wins over `timer` (recv arm 1, timer arm 2 in
/// source order), so `handle_timeout` would never fire and overdue
/// suspicion / probe deadlines would never be reaped. Under a clean
/// past-due event a kernel-buffered probe Ack would resolve the
/// deadline; firing `handle_timeout` without applying it first would
/// wrongly suspect the peer.
///
/// The bounded peek gives `recv` exactly ONE shot per past-due fire:
/// a `select_biased!` between the recv future and a `peek_budget`
/// sleep timer polls recv first (so a buffered datagram wins) and
/// falls through on the timer otherwise. The peek timer must be a
/// real sleep (not a zero-duration ready future) so io_uring has
/// time to complete a freshly-submitted recv SQE — on completion-
/// based io_uring the recv is ALWAYS Pending on first poll. The
/// `peek_budget` default (1 ms; see [`crate::DEFAULT_PEEK_BUDGET`])
/// is comfortably above io_uring's completion latency for a
/// kernel-buffered recv (~100 µs) while keeping the per-fire cost
/// negligible.
///
/// After the peek the deadline is re-polled: a consumed datagram may
/// have resolved it (a probe Ack landing the same tick the
/// cumulative deadline expires). `handle_timeout` fires only if the
/// deadline is STILL past. Then `drain_actions` flushes every
/// downstream consequence of the consumed datagram and/or fired
/// timeout (events, ingress, outbound memberlist transmits, raw QUIC
/// transmits) to fixed point.
///
/// Mirrors the stream driver's iter-top peek + `fire_timeout_with_drain`
/// pair (`memberlist-compio/src/driver.rs:602`); the QUIC version
/// bundles all three phases into one helper because the iter-top
/// past-due site is the only call site that can safely build a peek
/// `recv_from` SQE (the timer-arm site already has the main loop's
/// `recv_fut` SQE in flight on the same socket — a second SQE there
/// would race the kernel's datagram delivery between two pending
/// futures and lose the datagram if the outer `recv_fut` is dropped
/// without being polled).
///
/// Returns `true` iff any work was applied (the peek consumed a
/// datagram, `handle_timeout` fired, OR `drain_actions` made
/// progress), so the caller knows to republish the snapshot.
async fn fire_timeout_with_drain<I>(state: &mut QuicDriverState<I>, recv_buf_len: usize) -> bool
where
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  let mut dirty = false;

  // Bounded peek: at most one buffered datagram, at most peek_budget
  // wait. The peek's select_biased puts recv first so a kernel-
  // buffered datagram is always consumed if present.
  //
  // Scoped in an inner block so `peek_recv` / `peek_timer` (pinned
  // via `pin_mut!`) drop at the closing brace — their immutable
  // borrows on `state.udp_socket` / `state.driver_opts` release
  // before `drain_actions(state)` below takes `&mut state`.
  {
    let peek_buf = vec![0u8; recv_buf_len];
    let peek_recv = state.udp_socket.recv_from(peek_buf).fuse();
    let peek_timer = compio::time::sleep(state.driver_opts.peek_budget()).fuse();
    pin_mut!(peek_recv, peek_timer);
    select_biased! {
      gossip = peek_recv => {
        let BufResult(res, buf) = gossip;
        match res {
          Ok((n, src)) => {
            let received_at = Instant::now();
            state.endpoint.handle_udp(src, &buf[..n], received_at);
            dirty = true;
          }
          Err(_) => {
            // Best-effort: a transient recv error (ICMP unreachable
            // surfacing as a syscall error on Linux, EAGAIN, etc.) is
            // non-fatal — the next iter's main recv arm re-arms with
            // a fresh buffer.
          }
        }
      }
      _ = peek_timer => {
        // No buffered datagram surfaced within the peek budget; fall
        // through to the deadline-firing protocol below.
      }
    }
  }

  // Re-poll the deadline AFTER the peek — a consumed datagram may
  // have resolved it. Fire `handle_timeout` only if still past.
  let now = Instant::now();
  let after_peek_deadline = state
    .endpoint
    .poll_timeout()
    .unwrap_or(now + state.driver_opts.idle_wake_interval());
  if now >= after_peek_deadline {
    state.endpoint.handle_timeout(now);
    dirty = true;
  }

  if drain_actions::<I>(state).await {
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
async fn drain_actions<I>(state: &mut QuicDriverState<I>) -> bool
where
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  let mut any_progress = false;
  let decode_opts = DecodeOptions::default();
  let encode_opts = EncodeOptions::default();
  loop {
    let mut iter_progress = false;

    // 1. Events — reduce `pending_joins` on push/pull
    //    `ExchangeCompleted`, then forward to subscribers. `Full` ⇒
    //    count the drop into `events_dropped`; `Disconnected` ⇒ no
    //    subscribers, silently absorb (not a gap).
    //
    // Per-exchange contact accounting. The machine's
    // `ExchangeCompleted` event carries the terminal outcome of every
    // outbound bridge — push/pull, reliable ping, and reliable
    // user-message. Sync-join consumes only `ExchangeKind::PushPull`
    // completions: a reliable-ping bridge resolving has no bearing on
    // a `join_with` waiter's contact count, and a user-message bridge
    // is one-way fire-and-forget. Filter on the payload's `kind()`
    // before reducing `pending_joins`. For every synchronous-join
    // waiter that dispatched this exact `ExchangeId`, remove the eid
    // from its `pending` set (always, regardless of outcome — the
    // exchange has terminated) and increment `contacted` iff the
    // outcome is `Succeeded`. Tracking by `ExchangeId` (not by
    // `SocketAddr`) gives correct duplicate-seed semantics. An eid
    // belongs to at most one waiter (the `WaitForCompletion` arm
    // captures each dispatched eid into the waiter that allocated
    // it), so the inner loop breaks on the first match.
    while let Some(ev) = state.endpoint.poll_event() {
      iter_progress = true;
      if let Event::ExchangeCompleted(ref payload) = ev
        && payload.kind() == ExchangeKind::PushPull
      {
        let eid = payload.eid();
        let succeeded = matches!(payload.outcome(), ExchangeOutcome::Succeeded);
        for pj in state.pending_joins.values_mut() {
          if pj.pending.remove(&eid) {
            if succeeded {
              pj.contacted += 1;
            }
            break;
          }
        }
      }
      match state.events_tx.try_send(ev) {
        Ok(()) => {}
        Err(flume::TrySendError::Full(_)) => {
          state.events_dropped.fetch_add(1, Ordering::Relaxed);
        }
        Err(flume::TrySendError::Disconnected(_)) => {
          // No subscribers — silently absorb (not a gap).
        }
      }
    }

    // 2. Inbound memberlist datagrams — decrypt, strip optional
    //    label, decode the inner frame(s) (plain or compound), and
    //    feed each message back via `handle_packet`. Any decode failure
    //    (truncated, bad label, bad tag, trailing bytes,
    //    encryption-rejected) drops the datagram per the lossy-gossip
    //    discipline; the next probe round retransmits.
    let now = Instant::now();
    while let Some((from, raw)) = state.endpoint.poll_memberlist_ingress() {
      iter_progress = true;
      let plain = match state.endpoint.decrypt_gossip(&raw) {
        Ok(p) => Bytes::from(p),
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

    // 3. Outbound memberlist transmits — encode (plain or compound),
    //    then compress + encrypt + send on the shared UDP socket.
    //    `encode_outgoing*` only fails on a typed↔buffa bridge error
    //    that round-trips a locally-built message; matches the
    //    stream driver's drop-on-codec-fail policy. An empty
    //    encryption config makes `encrypt_gossip` a copy; a
    //    transient `send_to` error (ENOBUFS / ICMP unreachable
    //    surfacing as a syscall error) is non-fatal per the gossip
    //    drop discipline.
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
      let compressed = state.endpoint.compress_gossip(&plain);
      let on_wire = match state.endpoint.encrypt_gossip(&compressed) {
        Ok(b) => b,
        Err(_) => continue,
      };
      let BufResult(res, _buf) = state.udp_socket.send_to(on_wire, peer).await;
      // Ignoring Err: a transient UDP send error (ENOBUFS, network
      // down on an interface, ICMP unreachable surfacing as a
      // syscall error) is non-fatal — gossip is lossy and the next
      // probe/gossip round recovers.
      let _ = res;
    }

    // 4. Raw QUIC datagrams (handshake, acks, application stream
    //    data) — already framed by quinn-proto, no codec wrap.
    while let Some((dest, bytes)) = state.endpoint.poll_transmit() {
      iter_progress = true;
      let BufResult(res, _buf) = state.udp_socket.send_to(bytes, dest).await;
      // Ignoring Err: same rationale as the memberlist transmit arm
      // above — a transient send failure is non-fatal because QUIC
      // retransmits unacked frames on its own timer.
      let _ = res;
    }

    if !iter_progress {
      break;
    }
    any_progress = true;
  }
  any_progress
}

/// Reap fully-resolved or deadline-expired entries from
/// `pending_joins`. Called after every drain block. Replies + removes
/// every entry whose `pending` set is now empty OR whose `deadline`
/// has elapsed. Mirrors the stream driver's `reap_pending_joins`
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
      let _ = pj.reply.send_async(reply_value).await;
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

/// Publish a fresh snapshot of the coordinator's observable state to
/// `arc-swap`. Readers see the new snapshot on their next
/// `MemberlistSnapshot::load` with no lock contention. Mirrors the
/// stream driver's `refresh_snapshot`
/// (`memberlist-compio/src/driver.rs`); the only structural
/// difference is that the QUIC version goes through `endpoint_ref()`
/// to reach the inner membership `Endpoint<I, SocketAddr>`.
fn refresh_snapshot<I>(
  endpoint: &QuicEndpoint<I>,
  snapshot: &Arc<ArcSwap<MemberlistSnapshot<I, SocketAddr>>>,
) where
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  let ep = endpoint.endpoint_ref();
  let mut members_vec: Vec<Node<I, SocketAddr>> = Vec::new();
  let mut alive_count: usize = 0;
  for ns in ep.members() {
    members_vec.push(Node::new(
      ns.id_ref().cheap_clone(),
      ns.address_ref().cheap_clone(),
    ));
    if let Some(memberlist_wire::typed::State::Alive) = ep.member_liveness(ns.id_ref()) {
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
