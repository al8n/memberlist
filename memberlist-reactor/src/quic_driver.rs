//! The QUIC backend driver: a quinn-style `Future::poll` pump that solely owns a
//! [`QuicEndpoint`], its UDP socket, and the periodic schedulers, advancing the
//! membership machine and republishing the [`MemberlistSnapshot`].

use std::{
  collections::{HashMap, HashSet, VecDeque},
  future::Future,
  net::SocketAddr,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
use bytes::Bytes;
use flume::Sender;
use memberlist::codec::{
  DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
  parse_messages,
};
use memberlist_proto::{
  Instant, PingId, QuicEndpoint,
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, Transmit},
  streams::ExchangeId,
};

use crate::{
  NodeId,
  command::{Command, JoinCmd, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, ShutdownCmd},
  error::Error,
  observation::observation_payload_bytes,
  shared::Shared,
  snapshot::snapshot_of,
};

/// IP-layer UDP payload maximum; caps the per-recv buffer.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

/// Cap on the count of application-data events retained after a full observation
/// channel (the payload byte budget bounds their bytes; this bounds their count).
const OBS_OVERFLOW_MAX: usize = 1024;

/// A `WaitForCompletion` join awaiting its dispatched push/pull exchanges. The
/// machine emits one `ExchangeCompleted` per dispatched exchange (success or
/// `stream_timeout` failure), so the set always drains — no deadline needed.
struct PendingJoin {
  pending: HashSet<ExchangeId>,
  contacted: usize,
  requested: usize,
  reply: Sender<Result<usize, Error>>,
}

/// An in-flight graceful leave; every joined caller's reply resolves together
/// when `LeftCluster` fires.
struct PendingLeave {
  repliers: Vec<Sender<Result<(), Error>>>,
}

/// An outstanding application-ping call; resolved on `PingCompleted` (reply
/// `Ok(rtt)`) or `PingFailed` (reply `Err(PingTimeout)`) via the matching
/// `PingId`. On driver exit, drained with `Err(Shutdown)`.
struct PendingPing {
  ping_id: PingId,
  reply: Sender<Result<Duration, Error>>,
}

/// An outstanding reliable directed-send call; resolved on
/// `ExchangeCompleted(UserMessage)` as each tracked `ExchangeId` surfaces.
/// For QUIC, `QuicEndpoint::start_user_message` returns a `StreamId` that
/// coerces directly to the `ExchangeId` stamped on the completion event —
/// no `poll_action` drain is needed. On driver exit, drained with
/// `Err(Shutdown)`.
struct PendingUserSend {
  pending: HashSet<ExchangeId>,
  failed: usize,
  reply: Sender<Result<(), Error>>,
}

/// The single-owner QUIC driver future. Runs until shutdown (the last handle
/// dropped, or a `Shutdown` command).
pub(crate) struct QuicDriver<I: NodeId, R: Runtime> {
  endpoint: QuicEndpoint<I>,
  socket: <R::Net as Net>::UdpSocket,
  shared: Arc<Shared<I>>,
  /// Hand-off to the observation task (delegate dispatch + event-stream fan-out).
  obs_tx: Sender<Event<I, SocketAddr>>,
  /// Outstanding synchronous joins, reduced as their exchanges complete.
  pending_joins: HashMap<u64, PendingJoin>,
  /// Monotonic key source for `pending_joins`.
  next_pending_join_id: u64,
  /// The in-flight graceful leave, resolved on `LeftCluster`.
  pending_leave: Option<PendingLeave>,
  /// Outstanding application-ping calls; resolved via `PingId` correlation.
  pending_pings: Vec<PendingPing>,
  /// Outstanding reliable-send calls; resolved when all tracked exchange ids
  /// surface a terminal `ExchangeCompleted(UserMessage)`.
  pending_user_sends: Vec<PendingUserSend>,
  /// Bytes of payload-bearing events queued in `obs_tx` (added on enqueue,
  /// subtracted by the obs task on dequeue) — the byte backstop's counter.
  obs_payload_bytes: Arc<AtomicU64>,
  /// Queued-payload byte budget: `Some(4 * max_stream_frame_size)` on a bounded
  /// obs channel, `None` (no byte cap) on an unbounded one.
  obs_payload_budget: Option<u64>,
  /// Application-data events retained after a full obs channel, retried on a
  /// later poll rather than dropped (bounded by the payload byte budget and
  /// `OBS_OVERFLOW_MAX`).
  obs_overflow: VecDeque<Event<I, SocketAddr>>,
  recv_buf: Vec<u8>,
  recv_batch: usize,
  /// Per-poll cap on each drained surface (bounds the work one poll performs;
  /// remaining work triggers a self-wake).
  transmit_batch: usize,
  timer: Option<Pin<Box<R::Sleep>>>,
  timer_deadline: Option<Instant>,
  idle_wake: Duration,
}

impl<I: NodeId, R: Runtime> QuicDriver<I, R> {
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    endpoint: QuicEndpoint<I>,
    socket: <R::Net as Net>::UdpSocket,
    shared: Arc<Shared<I>>,
    recv_batch: usize,
    transmit_batch: usize,
    obs_tx: Sender<Event<I, SocketAddr>>,
    obs_payload_bytes: Arc<AtomicU64>,
    obs_payload_budget: Option<u64>,
  ) -> Self {
    let buf_len = endpoint
      .gossip_mtu()
      .saturating_add(memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD)
      .min(GOSSIP_RECV_BUF_MAX);
    Self {
      endpoint,
      socket,
      shared,
      obs_tx,
      pending_joins: HashMap::new(),
      next_pending_join_id: 0,
      pending_leave: None,
      pending_pings: Vec::new(),
      pending_user_sends: Vec::new(),
      obs_payload_bytes,
      obs_payload_budget,
      obs_overflow: VecDeque::new(),
      recv_buf: vec![0u8; buf_len],
      recv_batch,
      transmit_batch,
      timer: None,
      timer_deadline: None,
      idle_wake: Duration::from_secs(1),
    }
  }

  /// Applies one command to the machine and replies on its oneshot.
  fn dispatch(&mut self, cmd: Command<I>, now: Instant) {
    match cmd {
      Command::Join(JoinCmd { addrs, wait, reply }) => {
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Err(Error::NotRunning));
          return;
        }
        if !wait {
          // Dispatch: fire-and-forget; reply the dispatched count.
          let mut count = 0usize;
          for addr in &addrs {
            // Ignoring StreamId: per-seed outcome surfaces via poll_event.
            let _ = self
              .endpoint
              .start_push_pull(*addr, PushPullKind::Join, now);
            count += 1;
          }
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Ok(count));
          return;
        }
        // WaitForCompletion: track each dispatched exchange; account_event
        // replies the contacted count once they all complete.
        let mut pending = HashSet::with_capacity(addrs.len());
        for addr in &addrs {
          let sid = self
            .endpoint
            .start_push_pull(*addr, PushPullKind::Join, now);
          pending.insert(ExchangeId::from(sid));
        }
        if pending.is_empty() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Ok(0));
          return;
        }
        let requested = pending.len();
        let id = self.next_pending_join_id;
        self.next_pending_join_id = self.next_pending_join_id.wrapping_add(1);
        self.pending_joins.insert(
          id,
          PendingJoin {
            pending,
            contacted: 0,
            requested,
            reply,
          },
        );
      }
      Command::Leave(LeaveCmd { reply }) => {
        // A second leave racing an in-flight one joins it (both replies resolve
        // together on the single LeftCluster); re-invoking leave once
        // Leaving/Left emits no second completion, so a fresh waiter would hang.
        if let Some(pl) = self.pending_leave.as_mut() {
          pl.repliers.push(reply);
          return;
        }
        let was_running = self.endpoint.is_running();
        let res = self
          .endpoint
          .leave(now)
          .map_err(|e| Error::Io(std::io::Error::other(e.to_string())));
        match res {
          // A running leave queues the Dead-self notices and WILL emit
          // LeftCluster once they drain; park until then, so Ok means the leave
          // reached the wire.
          Ok(()) if was_running => {
            self.pending_leave = Some(PendingLeave {
              repliers: vec![reply],
            });
          }
          // A no-op (not running) or an error fires no completion — reply now.
          other => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.try_send(other);
          }
        }
      }
      Command::Shutdown(ShutdownCmd { reply }) => {
        self.shared.begin_shutdown();
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.try_send(Ok(()));
      }
      Command::Ping(PingCmd { node, reply }) => {
        // Gate on a running node: after `leave()` the probe scheduler is
        // stopped, so a new application ping's completion event would never
        // arrive and the caller would hang forever. Reject with `NotRunning`.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Err(Error::NotRunning));
          return;
        }
        let ping_id = self.endpoint.ping(node, now);
        self.pending_pings.push(PendingPing { ping_id, reply });
      }
      Command::SendUser(SendUserCmd {
        to,
        payloads,
        reply,
      }) => {
        // Gate on a running node: after `leave()` the gossip scheduler is
        // stopped; reject immediately.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Err(Error::NotRunning));
          return;
        }
        let res = self
          .endpoint
          .send_user_packets(to, &payloads)
          .map_err(|e| Error::PayloadTooLarge(e.to_string()));
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.try_send(res);
      }
      Command::SendReliable(SendReliableCmd {
        to,
        payloads,
        reply,
      }) => {
        // Gate on a running node: after `leave()` the QUIC stream coordinator
        // is stopping; a new `start_user_message` would never produce a bridge
        // and the caller would hang. Reject with `NotRunning`.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Err(Error::NotRunning));
          return;
        }
        if payloads.is_empty() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Ok(()));
          return;
        }
        let mut pending = HashSet::with_capacity(payloads.len());
        for payload in payloads {
          // `QuicEndpoint::start_user_message` calls `service_dials` +
          // `flush_outbound` in-band (no separate `poll_action` loop needed,
          // unlike the stream driver). The returned `StreamId` coerces to the
          // `ExchangeId` that the bridge-reap path stamps on
          // `Event::ExchangeCompleted(UserMessage)`.
          // Ignoring StreamId: only ExchangeId::from is needed for correlation.
          let stream_id = self.endpoint.start_user_message(to, payload, now);
          pending.insert(ExchangeId::from(stream_id));
        }
        self.pending_user_sends.push(PendingUserSend {
          pending,
          failed: 0,
          reply,
        });
      }
    }
  }

  /// Synchronous-command accounting for a surfaced event: reduces the matching
  /// `WaitForCompletion` join on a push/pull `ExchangeCompleted` (replying when
  /// its set empties), and resolves a parked leave on `LeftCluster`.
  fn account_event(&mut self, ev: &Event<I, SocketAddr>) {
    match ev {
      Event::ExchangeCompleted(p) if p.kind() == ExchangeKind::PushPull => {
        let eid = p.eid();
        let succeeded = matches!(p.outcome(), ExchangeOutcome::Succeeded);
        let mut completed = None;
        for (key, pj) in self.pending_joins.iter_mut() {
          if pj.pending.remove(&eid) {
            if succeeded {
              pj.contacted += 1;
            }
            if pj.pending.is_empty() {
              completed = Some(*key);
            }
            break;
          }
        }
        if let Some(key) = completed
          && let Some(pj) = self.pending_joins.remove(&key)
        {
          let res = if pj.contacted == 0 {
            Err(Error::JoinFailed(pj.requested))
          } else {
            Ok(pj.contacted)
          };
          // Ignoring Err: the join caller dropped its reply receiver.
          let _ = pj.reply.try_send(res);
        }
      }
      Event::LeftCluster => {
        if let Some(pl) = self.pending_leave.take() {
          for replier in pl.repliers {
            // Ignoring Err: a leave caller dropped its reply receiver.
            let _ = replier.try_send(Ok(()));
          }
        }
      }
      // Ping completion: resolve the matching waiter with the observed RTT.
      // The event still flows to the observation task (additive correlation —
      // `PingCompleted` also fires the delegate's `notify_ping_complete`).
      Event::PingCompleted(p) => {
        let pid = p.ping_id();
        if let Some(idx) = self.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = self.pending_pings.swap_remove(idx);
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = pp.reply.try_send(Ok(p.rtt()));
        }
      }
      // Ping failure: resolve the matching waiter with `PingTimeout`. Same
      // additive semantics as `PingCompleted`.
      Event::PingFailed(p) => {
        let pid = p.ping_id();
        if let Some(idx) = self.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = self.pending_pings.swap_remove(idx);
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = pp.reply.try_send(Err(Error::PingTimeout));
        }
      }
      // Reliable user-send completion: reduce the pending set; reply when empty.
      Event::ExchangeCompleted(p) if p.kind() == ExchangeKind::UserMessage => {
        let eid = p.eid();
        let failed = matches!(p.outcome(), ExchangeOutcome::Failed);
        if let Some(idx) = self
          .pending_user_sends
          .iter()
          .position(|ps| ps.pending.contains(&eid))
        {
          let ps = &mut self.pending_user_sends[idx];
          ps.pending.remove(&eid);
          if failed {
            ps.failed += 1;
          }
          if ps.pending.is_empty() {
            let ps = self.pending_user_sends.swap_remove(idx);
            let res = if ps.failed > 0 {
              Err(Error::SendFailed)
            } else {
              Ok(())
            };
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = ps.reply.try_send(res);
          }
        }
      }
      _ => {}
    }
  }

  /// Drains each machine surface up to `transmit_batch` items in one pass.
  /// Returns `(worked, more)`: whether any surface produced work (republish the
  /// snapshot) and whether any surface hit its cap with work left (self-wake).
  fn drain_surfaces(&mut self, cx: &mut Context<'_>) -> (bool, bool) {
    let decode_opts = DecodeOptions::default();
    let encode_opts = EncodeOptions::default();
    let budget = self.transmit_batch.max(1);
    let now = Instant::now();
    let mut worked = false;
    let mut more = false;

    // Inbound gossip: decrypt, strip label, decode, feed each message back.
    let mut ingress = 0;
    while ingress < budget {
      let Some((from, raw)) = self.endpoint.poll_memberlist_ingress() else {
        break;
      };
      ingress += 1;
      let plain = match self.endpoint.decrypt_gossip(&raw) {
        Ok(p) => Bytes::from(p),
        Err(_) => continue,
      };
      let inner = match decode_incoming(plain, &decode_opts) {
        Ok(b) => b,
        Err(_) => continue,
      };
      let msgs = match parse_messages::<I, SocketAddr>(inner) {
        Ok(m) => m,
        Err(_) => continue,
      };
      for msg in msgs {
        self.endpoint.handle_packet(from, msg, now);
      }
    }
    worked |= ingress > 0;
    more |= ingress == budget;

    // Outbound gossip: encode (plain or compound), compress, encrypt, send.
    let mut sent = 0;
    while sent < budget {
      let Some(transmit) = self.endpoint.poll_memberlist_transmit() else {
        break;
      };
      sent += 1;
      let (peer, plain) = match transmit {
        Transmit::Packet(pkt) => {
          let (to, msg) = pkt.into_parts();
          match encode_outgoing(&msg, &encode_opts) {
            Ok(b) => (to, b.to_vec()),
            Err(_) => continue,
          }
        }
        Transmit::Compound(cmp) => {
          let (to, msgs) = cmp.into_parts();
          match encode_outgoing_compound(&msgs, &encode_opts) {
            Ok(b) => (to, b.to_vec()),
            Err(_) => continue,
          }
        }
      };
      let compressed = self.endpoint.compress_gossip(&plain);
      let on_wire = match self.endpoint.encrypt_gossip(&compressed) {
        Ok(b) => b,
        Err(_) => continue,
      };
      // Ignoring Poll: gossip is best-effort — a full or errored UDP send drops
      // the datagram and SWIM recovers on the next round.
      let _ = self.socket.poll_send_to(cx, &on_wire, peer);
    }
    worked |= sent > 0;
    more |= sent == budget;

    // Raw QUIC datagrams: already wire-framed by quinn-proto, no codec wrap.
    let mut raw_sent = 0;
    while raw_sent < budget {
      let Some((dest, bytes)) = self.endpoint.poll_transmit() else {
        break;
      };
      raw_sent += 1;
      // Ignoring Poll: a dropped QUIC datagram is retransmitted by quinn-proto.
      let _ = self.socket.poll_send_to(cx, &bytes, dest);
    }
    worked |= raw_sent > 0;
    more |= raw_sent == budget;

    // Observation events: retry the overflow first, then drain up to the budget.
    self.flush_obs_overflow();
    let mut events = 0;
    while events < budget {
      let Some(ev) = self.endpoint.poll_event() else {
        break;
      };
      events += 1;
      self.send_observation(ev);
    }
    worked |= events > 0;
    more |= events == budget;

    (worked, more)
  }

  /// Retries retained overflow events into the obs channel, stopping at the first
  /// `Full`. Sent events stay byte-accounted (the obs task subtracts on dequeue).
  fn flush_obs_overflow(&mut self) {
    while let Some(ev) = self.obs_overflow.pop_front() {
      match self.obs_tx.try_send(ev) {
        Ok(()) => {}
        Err(flume::TrySendError::Full(ev)) => {
          self.obs_overflow.push_front(ev);
          break;
        }
        Err(flume::TrySendError::Disconnected(ev)) => {
          // The obs task is gone: reclaim this event's reserved payload bytes.
          if let Some(bytes) = observation_payload_bytes(&ev) {
            self.obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
          }
        }
      }
    }
  }

  /// Hands one event to the obs task. A full channel retains application data for
  /// retry (bounded by the payload byte budget and `OBS_OVERFLOW_MAX`) and drops
  /// recoverable membership/control events, counting them.
  fn send_observation(&mut self, ev: Event<I, SocketAddr>) {
    self.account_event(&ev);
    let payload = observation_payload_bytes(&ev);
    // Byte backstop: refuse a payload event if enqueuing it would push the queued
    // payload bytes over budget — the count cap alone does not bound memory when
    // events carry large reliable payloads.
    if let (Some(budget), Some(bytes)) = (self.obs_payload_budget, payload)
      && self
        .obs_payload_bytes
        .load(Ordering::Relaxed)
        .saturating_add(bytes)
        > budget
    {
      self.shared.add_observation_dropped(1);
      return;
    }
    // Reserve the payload bytes before the event becomes visible to the obs task,
    // so its release (subtract on receive) can never run ahead of the reservation
    // and wrap the counter on a multi-thread runtime.
    if let Some(bytes) = payload {
      self.obs_payload_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    match self.obs_tx.try_send(ev) {
      // Reserved above; the obs task releases it on receive.
      Ok(()) => {}
      Err(flume::TrySendError::Full(ev)) => match payload {
        // Application data the event stream cannot reconstruct: retain (still
        // reserved) for a retry.
        Some(_) if self.obs_overflow.len() < OBS_OVERFLOW_MAX => {
          self.obs_overflow.push_back(ev);
        }
        // Recoverable membership/control, or the overflow is full: drop, count,
        // and roll back any reservation.
        _ => {
          if let Some(bytes) = payload {
            self.obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
          }
          self.shared.add_observation_dropped(1);
        }
      },
      // The obs task is gone: roll back the reservation.
      Err(flume::TrySendError::Disconnected(_)) => {
        if let Some(bytes) = payload {
          self.obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
      }
    }
  }

  /// (Re)arms the wakeup timer for `target` if it is not already armed for it.
  fn arm_timer(&mut self, target: Instant, now: Instant) {
    if self.timer_deadline != Some(target) {
      self.timer = Some(Box::pin(R::sleep(target.saturating_duration_since(now))));
      self.timer_deadline = Some(target);
    }
  }
}

impl<I: NodeId, R: Runtime> Future for QuicDriver<I, R> {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let this = self.get_mut();
    let now = Instant::now();
    let mut progress = false;
    let mut more = false;

    // Drain queued commands (parks the waker for the next push).
    for cmd in this.shared.drain_commands(cx.waker()) {
      this.dispatch(cmd, now);
      progress = true;
    }

    // Shutdown: best-effort leave, flush once, fail any parked waiters, stop.
    if this.shared.is_shutdown() {
      // Ignoring Err: best-effort leave during shutdown.
      let _ = this.endpoint.leave(Instant::now());
      this.drain_surfaces(cx);
      // Close the command queue and fail any still-queued commands, so a handle
      // that raced the shutdown gets a reply instead of hanging.
      for cmd in this.shared.close_and_drain() {
        match cmd {
          // Ignoring Err: the caller dropped its reply receiver.
          Command::Join(JoinCmd { reply, .. }) => {
            let _ = reply.try_send(Err(Error::Shutdown));
          }
          Command::Leave(LeaveCmd { reply }) => {
            let _ = reply.try_send(Err(Error::Shutdown));
          }
          Command::Shutdown(ShutdownCmd { reply }) => {
            let _ = reply.try_send(Ok(()));
          }
          // Ignoring Err: the caller dropped its reply receiver.
          Command::Ping(PingCmd { reply, .. }) => {
            let _ = reply.try_send(Err(Error::Shutdown));
          }
          Command::SendUser(SendUserCmd { reply, .. }) => {
            let _ = reply.try_send(Err(Error::Shutdown));
          }
          Command::SendReliable(SendReliableCmd { reply, .. }) => {
            let _ = reply.try_send(Err(Error::Shutdown));
          }
        }
      }
      for (_, pj) in this.pending_joins.drain() {
        // Ignoring Err: the join caller dropped its reply receiver.
        let _ = pj.reply.try_send(Err(Error::Shutdown));
      }
      if let Some(pl) = this.pending_leave.take() {
        for replier in pl.repliers {
          // Ignoring Err: the leave caller dropped its reply receiver.
          let _ = replier.try_send(Err(Error::Shutdown));
        }
      }
      for pp in this.pending_pings.drain(..) {
        // Ignoring Err: the ping caller dropped its reply receiver.
        let _ = pp.reply.try_send(Err(Error::Shutdown));
      }
      for ps in this.pending_user_sends.drain(..) {
        // Ignoring Err: the send_reliable caller dropped its reply receiver.
        let _ = ps.reply.try_send(Err(Error::Shutdown));
      }
      return Poll::Ready(());
    }

    // Receive gossip (bounded; a full batch means more may be waiting).
    let mut recv_n = 0;
    while recv_n < this.recv_batch {
      match this.socket.poll_recv_from(cx, &mut this.recv_buf) {
        Poll::Ready(Ok((n, src))) => {
          this.endpoint.handle_udp(src, &this.recv_buf[..n], now);
          recv_n += 1;
        }
        // Ignoring Err: a transient recv error is non-fatal; re-armed next poll.
        Poll::Ready(Err(_)) => break,
        Poll::Pending => break,
      }
    }
    if recv_n > 0 {
      progress = true;
    }
    if recv_n == this.recv_batch {
      more = true;
    }

    // Drain machine surfaces (bounded per surface).
    let (drained, drain_more) = this.drain_surfaces(cx);
    progress |= drained;
    more |= drain_more;

    // Timer: fire an overdue deadline inline, else arm + poll the sleep. A fired
    // deadline may queue transmits/events this pass did not drain.
    let target = match this.endpoint.poll_timeout() {
      Some(d) => d.min(now + this.idle_wake),
      None => now + this.idle_wake,
    };
    if target <= now {
      this.endpoint.handle_timeout(now);
      progress = true;
      more = true;
    } else {
      this.arm_timer(target, now);
      if let Some(timer) = this.timer.as_mut()
        && timer.as_mut().poll(cx).is_ready()
      {
        this.endpoint.handle_timeout(Instant::now());
        this.timer = None;
        this.timer_deadline = None;
        progress = true;
        more = true;
      }
    }

    // Republish the snapshot if anything advanced.
    if progress {
      this
        .shared
        .publish(snapshot_of(this.endpoint.endpoint_ref()));
    }

    // Yield to other tasks, but re-poll promptly while work remains.
    if more {
      cx.waker().wake_by_ref();
    }
    Poll::Pending
  }
}
