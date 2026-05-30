//! The TCP backend driver: a quinn-style `Future::poll` pump that owns a
//! [`StreamEndpoint`] over plain-TCP records, its gossip UDP socket, and the
//! periodic schedulers. Per-exchange reliable TCP I/O runs in spawned bridge
//! tasks (one per exchange) wired to the pump by channels; inbound connections
//! are accepted by a dedicated task (the listener's `accept` is async-only).

use std::{
  collections::{HashMap, HashSet, VecDeque},
  future::Future,
  net::{Shutdown, SocketAddr},
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  task::{Context, Poll},
  time::{Duration, Instant},
};

use agnostic::{
  Runtime,
  net::{Net, TcpListener, TcpStream, UdpSocket},
};
use bytes::Bytes;
use flume::{Receiver, Sender};
use futures_util::{AsyncReadExt, AsyncWriteExt, FutureExt, select};
use memberlist::codec::{
  DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
  parse_messages,
};
use memberlist_machine::{
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, Transmit},
  streams::{ExchangeId, StreamAction, StreamEndpoint, StreamTransport},
};

use crate::{
  NodeId,
  command::{Command, JoinCmd, LeaveCmd, ShutdownCmd},
  error::Error,
  observation::observation_payload_bytes,
  shared::Shared,
  snapshot::snapshot_of,
};

/// IP-layer UDP payload maximum; caps the per-recv gossip buffer.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

/// Per-read buffer for a bridge's TCP socket. The machine reassembles frames
/// across reads, so this only bounds one read's chunk, not a frame.
const BRIDGE_READ_BUF: usize = 16 * 1024;

/// Capacity of the shared bridge-inbound channel. Bounds the bytes a fast peer
/// can buffer ahead of the pump: bridges block (TCP backpressure) once it fills.
const BRIDGE_INBOUND_CAP: usize = 256;

/// Capacity of the accepted-connection channel; the accept task backpressures
/// once the pump is this many connections behind.
pub(crate) const ACCEPT_CAP: usize = 256;

/// Caps an outbound dial's OS connect so a dead peer cannot leak the dial task
/// indefinitely; the machine's own dial deadline is the protocol-level bound.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Cap on the count of application-data events retained after a full observation
/// channel (the payload byte budget bounds their bytes; this bounds their count).
const OBS_OVERFLOW_MAX: usize = 1024;

/// A message from the pump to a bridge's TCP write side. Teardown is signalled
/// out of band by dropping the [`BridgeHandle`], not by a variant here, so it can
/// preempt even a write stalled on an unresponsive peer.
enum BridgeOut {
  /// Plaintext transport bytes to write to the peer.
  Data(Bytes),
  /// Half-close the write side (FIN) after the send half retired.
  ShutdownWrite,
}

/// The pump's end of a live bridge: the write channel plus a cancel channel held
/// only to be dropped. Dropping the handle (on the machine's Close action or
/// driver shutdown) disconnects both, so the bridge exits from any state — even a
/// write blocked on a peer that stopped reading.
struct BridgeHandle {
  out_tx: Sender<BridgeOut>,
  _cancel_tx: Sender<()>,
}

/// Inbound transport bytes (or EOF) from a bridge's TCP read side to the pump.
/// Each carries the instant the read completed, so an item processed after
/// backpressure or scheduler delay is timed at socket arrival, not at drain — the
/// stream FSM would otherwise read the delay as a deadline timeout and fail a
/// response that actually arrived in time.
enum BridgeInbound {
  /// Bytes read from the peer for an exchange.
  Data(BridgeData),
  /// The peer closed its write side (or the read errored) for an exchange.
  Eof(BridgeEof),
}

/// Payload of [`BridgeInbound::Data`].
struct BridgeData {
  eid: ExchangeId,
  bytes: Vec<u8>,
  at: Instant,
}

/// Payload of [`BridgeInbound::Eof`].
struct BridgeEof {
  eid: ExchangeId,
  at: Instant,
}

/// The result a dial task reports back to the pump.
enum DialOutcome<R: Runtime> {
  /// The connection succeeded; hand the stream and its write channel back.
  Connected(DialConnected<R>),
  /// The connection failed or timed out; the pump fails the exchange.
  Failed(ExchangeId),
}

/// Payload of [`DialOutcome::Connected`].
struct DialConnected<R: Runtime> {
  eid: ExchangeId,
  stream: <R::Net as Net>::TcpStream,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: Receiver<()>,
}

/// A `WaitForCompletion` join awaiting its dispatched push/pull exchanges. The
/// machine emits one `ExchangeCompleted` per dispatched exchange (success or
/// `stream_timeout` failure), so the set always drains — no deadline needed.
///
/// `start_push_pull` returns a `StreamId` that is not the exchange's
/// `ExchangeId`, and there is no public reverse index, so a join cannot key on
/// the id it started. Instead `dispatch` drains the action(s) each
/// `start_push_pull` queues and captures the resulting `Connect`'s `ExchangeId`
/// here — synchronously, so the capture is unambiguous across concurrent or
/// duplicate-seed joins, and a dial that fails before any `Connect` (e.g. a TLS
/// peer with no SNI) simply contributes no id rather than wedging the join.
struct PendingJoin {
  /// Exchanges this join is awaiting, captured from their `Connect` at dispatch.
  pending_eids: HashSet<ExchangeId>,
  contacted: usize,
  requested: usize,
  reply: Sender<Result<usize, Error>>,
}

/// An in-flight graceful leave; every joined caller's reply resolves together
/// when `LeftCluster` fires.
struct PendingLeave {
  repliers: Vec<Sender<Result<(), Error>>>,
}

/// The single-owner TCP driver future. Runs until shutdown (the last handle
/// dropped, or a `Shutdown` command).
pub(crate) struct StreamDriver<I: NodeId, R: Runtime, T: StreamTransport> {
  endpoint: StreamEndpoint<I, SocketAddr, T>,
  /// Unreliable gossip datagrams (the reliable exchanges run over TCP bridges).
  socket: <R::Net as Net>::UdpSocket,
  shared: Arc<Shared<I>>,
  /// Hand-off to the observation task (delegate dispatch + event-stream fan-out).
  obs_tx: Sender<Event<I, SocketAddr>>,
  /// Bytes of payload-bearing events queued in `obs_tx` (added on enqueue,
  /// subtracted by the obs task on dequeue) — the byte backstop's counter.
  obs_payload_bytes: Arc<AtomicU64>,
  /// Queued-payload byte budget on a bounded obs channel, `None` if unbounded.
  obs_payload_budget: Option<u64>,
  /// Application-data events retained after a full obs channel, retried on a
  /// later poll rather than dropped.
  obs_overflow: VecDeque<Event<I, SocketAddr>>,
  /// Outstanding synchronous joins, keyed by a monotonic id. `dispatch` captures
  /// each join's exchange ids synchronously and completions are matched by that
  /// unique id, so map iteration order does not matter.
  pending_joins: HashMap<u64, PendingJoin>,
  /// Monotonic key source for `pending_joins`.
  next_pending_join_id: u64,
  /// The in-flight graceful leave, resolved on `LeftCluster`.
  pending_leave: Option<PendingLeave>,
  /// Each live exchange's bridge: its write channel and teardown handle.
  bridges: HashMap<ExchangeId, BridgeHandle>,
  /// Held only to be dropped on driver exit; closing the accept task's shutdown
  /// channel cancels its pending accept() so the listener is released at once.
  _accept_shutdown_tx: Sender<()>,
  /// Inbound connections from the accept task.
  accepted_rx: Receiver<(<R::Net as Net>::TcpStream, SocketAddr)>,
  /// Inbound transport bytes/EOF from the bridge read tasks.
  inbound_rx: Receiver<BridgeInbound>,
  /// Cloned into each spawned bridge task to report inbound bytes/EOF.
  inbound_tx: Sender<BridgeInbound>,
  /// Dial completions from the dial tasks.
  dial_rx: Receiver<DialOutcome<R>>,
  /// Cloned into each spawned dial task to report its outcome.
  dial_tx: Sender<DialOutcome<R>>,
  recv_buf: Vec<u8>,
  recv_batch: usize,
  /// Per-poll cap on each drained surface (bounds the work one poll performs;
  /// remaining work triggers a self-wake).
  transmit_batch: usize,
  timer: Option<Pin<Box<R::Sleep>>>,
  timer_deadline: Option<Instant>,
  idle_wake: Duration,
}

impl<I: NodeId, R: Runtime, T: StreamTransport> StreamDriver<I, R, T> {
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    endpoint: StreamEndpoint<I, SocketAddr, T>,
    socket: <R::Net as Net>::UdpSocket,
    shared: Arc<Shared<I>>,
    recv_batch: usize,
    transmit_batch: usize,
    obs_tx: Sender<Event<I, SocketAddr>>,
    obs_payload_bytes: Arc<AtomicU64>,
    obs_payload_budget: Option<u64>,
    accepted_rx: Receiver<(<R::Net as Net>::TcpStream, SocketAddr)>,
    accept_shutdown_tx: Sender<()>,
  ) -> Self {
    let buf_len = endpoint
      .gossip_mtu()
      .saturating_add(memberlist_wire::ENCRYPTED_WRAPPER_OVERHEAD)
      .min(GOSSIP_RECV_BUF_MAX);
    let (inbound_tx, inbound_rx) = flume::bounded(BRIDGE_INBOUND_CAP);
    let (dial_tx, dial_rx) = flume::unbounded();
    Self {
      endpoint,
      socket,
      shared,
      obs_tx,
      obs_payload_bytes,
      obs_payload_budget,
      obs_overflow: VecDeque::new(),
      pending_joins: HashMap::new(),
      next_pending_join_id: 0,
      pending_leave: None,
      bridges: HashMap::new(),
      _accept_shutdown_tx: accept_shutdown_tx,
      accepted_rx,
      inbound_rx,
      inbound_tx,
      dial_rx,
      dial_tx,
      recv_buf: vec![0u8; buf_len],
      recv_batch,
      transmit_batch,
      timer: None,
      timer_deadline: None,
      idle_wake: Duration::from_secs(1),
    }
  }

  /// Spawns the per-exchange bridge task that moves bytes between `stream` and
  /// the pump for `eid`.
  fn spawn_bridge(
    &self,
    eid: ExchangeId,
    stream: <R::Net as Net>::TcpStream,
    out_rx: Receiver<BridgeOut>,
    cancel_rx: Receiver<()>,
  ) {
    R::spawn_detach(bridge_task::<I, <R::Net as Net>::TcpStream>(
      stream,
      eid,
      out_rx,
      cancel_rx,
      self.inbound_tx.clone(),
      self.shared.clone(),
    ));
  }

  /// Spawns the dial task that connects to `peer` for outbound exchange `eid`.
  fn spawn_dial(
    &self,
    eid: ExchangeId,
    peer: SocketAddr,
    out_rx: Receiver<BridgeOut>,
    cancel_rx: Receiver<()>,
  ) {
    R::spawn_detach(dial_task::<I, R>(
      eid,
      peer,
      out_rx,
      cancel_rx,
      self.dial_tx.clone(),
      self.shared.clone(),
    ));
  }

  /// Applies one handle command to the machine.
  fn dispatch(&mut self, cmd: Command, now: Instant) {
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
        // WaitForCompletion: account_event replies the contacted count once every
        // exchange this join started completes. An empty seed list is a no-op
        // success, not a failure.
        if addrs.is_empty() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Ok(0));
          return;
        }
        // Drain already-queued actions first, so a same-peer Connect from an
        // earlier call (join_detached, a gossip ping) is handled and not captured
        // by this join.
        while let Some(action) = self.endpoint.poll_action() {
          self.handle_stream_action(action);
        }
        // Start each seed's exchange and capture the Connect it produces
        // synchronously. A dial that fails before any Connect (e.g. a TLS peer
        // with no SNI) yields none, so it cannot leave the join awaiting forever.
        let mut pending_eids = HashSet::with_capacity(addrs.len());
        for addr in &addrs {
          // Ignoring StreamId: the exchange's ExchangeId is captured from Connect.
          self
            .endpoint
            .start_push_pull(*addr, PushPullKind::Join, now);
          while let Some(action) = self.endpoint.poll_action() {
            if let StreamAction::Connect(info) = &action
              && info.peer() == *addr
            {
              pending_eids.insert(info.id());
            }
            self.handle_stream_action(action);
          }
        }
        if pending_eids.is_empty() {
          // Every seed's dial failed before producing an exchange: a bounded
          // failure rather than an indefinite wait.
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.try_send(Err(Error::JoinFailed(addrs.len())));
          return;
        }
        let id = self.next_pending_join_id;
        self.next_pending_join_id = self.next_pending_join_id.wrapping_add(1);
        self.pending_joins.insert(
          id,
          PendingJoin {
            pending_eids,
            contacted: 0,
            requested: addrs.len(),
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
          if pj.pending_eids.remove(&eid) {
            if succeeded {
              pj.contacted += 1;
            }
            if pj.pending_eids.is_empty() {
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
      _ => {}
    }
  }

  /// Applies one stream action: open a dial, half-close a bridge's write, or tear
  /// a bridge down. A `dispatch` wait join captures its own exchanges' `Connect`
  /// ids synchronously, so this only sets up the bridge.
  fn handle_stream_action(&mut self, action: StreamAction) {
    match action {
      StreamAction::Connect(info) => {
        let eid = info.id();
        let peer = info.peer();
        let (out_tx, out_rx) = flume::unbounded();
        let (cancel_tx, cancel_rx) = flume::bounded(1);
        self.bridges.insert(
          eid,
          BridgeHandle {
            out_tx,
            _cancel_tx: cancel_tx,
          },
        );
        self.spawn_dial(eid, peer, out_rx, cancel_rx);
      }
      StreamAction::Shutdown(eref) => {
        if let Some(handle) = self.bridges.get(&eref.id()) {
          // Ignoring Err: the bridge task exited; its socket is already gone.
          let _ = handle.out_tx.try_send(BridgeOut::ShutdownWrite);
        }
      }
      StreamAction::Close(eref) => {
        // Dropping the handle disconnects the bridge's write and cancel channels,
        // tearing it down even if a write is stalled mid-send.
        self.bridges.remove(&eref.id());
      }
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

    // Stream actions: open dials, half-close, or tear down reliable exchanges.
    // Drained before transport bytes so a fresh Connect installs the bridge
    // handle before this pass routes that exchange's first bytes.
    let mut actions = 0;
    while actions < budget {
      let Some(action) = self.endpoint.poll_action() else {
        break;
      };
      actions += 1;
      self.handle_stream_action(action);
    }
    worked |= actions > 0;
    more |= actions == budget;

    // Outbound transport bytes: route each exchange's plaintext to its bridge.
    let mut tx = 0;
    while tx < budget {
      let Some((eid, _peer, bytes)) = self.endpoint.poll_transport_transmit() else {
        break;
      };
      tx += 1;
      if let Some(handle) = self.bridges.get(&eid) {
        // Ignoring Err: the bridge task exited; the exchange will time out.
        let _ = handle.out_tx.try_send(BridgeOut::Data(bytes));
      }
    }
    worked |= tx > 0;
    more |= tx == budget;

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

impl<I: NodeId, R: Runtime, T: StreamTransport> Future for StreamDriver<I, R, T>
where
  T: Unpin,
  T::Options: Unpin,
{
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
    // Dropping `this` afterwards drops the bridge/dial/accept channel ends, so
    // those tasks observe their send/recv errors and exit.
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
      return Poll::Ready(());
    }

    // Receive gossip (bounded; a full batch means more may be waiting).
    let mut recv_n = 0;
    while recv_n < this.recv_batch {
      match this.socket.poll_recv_from(cx, &mut this.recv_buf) {
        Poll::Ready(Ok((n, src))) => {
          this.endpoint.handle_gossip(src, &this.recv_buf[..n], now);
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

    // Accept inbound connections: register each with the machine and bridge it.
    // Aux tasks wake the driver after enqueueing, so try_recv (no waker
    // registration) is sufficient and does not drop a waker each poll.
    while let Ok((stream, peer)) = this.accepted_rx.try_recv() {
      let eid = this.endpoint.accept_connection(peer, now);
      let (out_tx, out_rx) = flume::unbounded();
      let (cancel_tx, cancel_rx) = flume::bounded(1);
      this.bridges.insert(
        eid,
        BridgeHandle {
          out_tx,
          _cancel_tx: cancel_tx,
        },
      );
      this.spawn_bridge(eid, stream, out_rx, cancel_rx);
      progress = true;
    }

    // Dial outcomes: bridge a connected stream, or fail the exchange via EOF.
    while let Ok(outcome) = this.dial_rx.try_recv() {
      match outcome {
        DialOutcome::Connected(DialConnected {
          eid,
          stream,
          out_rx,
          cancel_rx,
        }) => {
          // If the exchange was reaped while dialing, its handle is gone; drop
          // the stream and channels rather than bridge a dead exchange.
          if this.bridges.contains_key(&eid) {
            this.spawn_bridge(eid, stream, out_rx, cancel_rx);
          }
        }
        DialOutcome::Failed(eid) => {
          this.bridges.remove(&eid);
          this.endpoint.handle_transport_data(eid, &[], true, now);
        }
      }
      progress = true;
    }

    // Inbound transport bytes/EOF from the bridge read tasks (bounded per poll
    // for fairness; a full batch self-wakes via `more`).
    let mut inbound_n = 0;
    while inbound_n < this.recv_batch {
      let Ok(msg) = this.inbound_rx.try_recv() else {
        break;
      };
      inbound_n += 1;
      match msg {
        BridgeInbound::Data(BridgeData { eid, bytes, at }) => {
          this.endpoint.handle_transport_data(eid, &bytes, false, at);
        }
        BridgeInbound::Eof(BridgeEof { eid, at }) => {
          this.endpoint.handle_transport_data(eid, &[], true, at);
        }
      }
    }
    if inbound_n > 0 {
      progress = true;
    }
    if inbound_n == this.recv_batch {
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

/// Accepts inbound TCP connections and forwards each to the pump, waking it after
/// each enqueue. `accept` is async-only, so this cannot fold into the pump's
/// poll; it stops promptly when the driver drops `shutdown_rx`'s sender, which
/// cancels the pending `accept()` so the listener is released at once.
pub(crate) async fn accept_task<I: NodeId, L: TcpListener>(
  listener: L,
  accepted_tx: Sender<(L::Stream, SocketAddr)>,
  shutdown_rx: Receiver<()>,
  shared: Arc<Shared<I>>,
) {
  loop {
    select! {
      conn = listener.accept().fuse() => match conn {
        Ok((stream, peer)) => {
          // Bounded channel: await space (accept backpressure), then wake the pump.
          if accepted_tx.send_async((stream, peer)).await.is_err() {
            break;
          }
          shared.wake_driver();
        }
        // Ignoring Err: a transient accept error (e.g. a reset mid-handshake) is
        // non-fatal; keep listening for the next connection.
        Err(_) => continue,
      },
      // The driver dropped its shutdown sender: stop and release the listener.
      _ = shutdown_rx.recv_async().fuse() => break,
    }
  }
}

/// Dials `peer` for outbound exchange `eid` and reports the outcome to the pump,
/// waking it afterwards.
async fn dial_task<I: NodeId, R: Runtime>(
  eid: ExchangeId,
  peer: SocketAddr,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: Receiver<()>,
  dial_tx: Sender<DialOutcome<R>>,
  shared: Arc<Shared<I>>,
) {
  let outcome = match <R::Net as Net>::TcpStream::connect_timeout(&peer, DIAL_TIMEOUT).await {
    Ok(stream) => DialOutcome::Connected(DialConnected {
      eid,
      stream,
      out_rx,
      cancel_rx,
    }),
    Err(_) => DialOutcome::Failed(eid),
  };
  // Ignoring Err: the pump dropped its receiver (driver shut down).
  let _ = dial_tx.send(outcome);
  shared.wake_driver();
}

/// Moves bytes between one exchange's TCP stream and the pump, waking it after
/// each inbound enqueue. Reads forward to `inbound_tx`; `out_rx` drives writes
/// and the write half-close. The bridge tears down when the pump drops its handle
/// (disconnecting `out_rx` and `cancel_rx`), which preempts even a stalled write.
async fn bridge_task<I: NodeId, S: TcpStream>(
  stream: S,
  eid: ExchangeId,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: Receiver<()>,
  inbound_tx: Sender<BridgeInbound>,
  shared: Arc<Shared<I>>,
) {
  let (mut read_half, mut write_half) = stream.into_split();
  let mut buf = vec![0u8; BRIDGE_READ_BUF];
  let mut read_eof = false;
  let mut write_closed = false;
  loop {
    // A push/pull peer half-closes (FINs) after sending its half, then reads the
    // reply. So a read EOF retires only the read side; the bridge stays alive to
    // write the reply over the half-open connection and tears down only when the
    // pump drops the handle (disconnecting out_rx and cancel_rx).
    if read_eof {
      match out_rx.recv_async().await {
        Ok(BridgeOut::Data(bytes)) => {
          if !write_closed && write_cancellable(&mut write_half, &bytes, &cancel_rx).await {
            break;
          }
        }
        Ok(BridgeOut::ShutdownWrite) => {
          // Ignoring Err: half-closing a gone peer is moot.
          let _ = write_half.close().await;
          write_closed = true;
        }
        Err(_) => break,
      }
      continue;
    }
    select! {
      read = read_half.read(&mut buf).fuse() => match read {
        Ok(0) | Err(_) => {
          // Timestamp at read completion, before any send backpressure.
          let msg = BridgeInbound::Eof(BridgeEof { eid, at: Instant::now() });
          // Bounded channel: await space (backpressure), then wake the pump.
          if inbound_tx.send_async(msg).await.is_err() {
            break;
          }
          shared.wake_driver();
          read_eof = true;
        }
        Ok(n) => {
          let msg = BridgeInbound::Data(BridgeData {
            eid,
            bytes: buf[..n].to_vec(),
            at: Instant::now(),
          });
          if inbound_tx.send_async(msg).await.is_err() {
            break;
          }
          shared.wake_driver();
        }
      },
      out = out_rx.recv_async().fuse() => match out {
        Ok(BridgeOut::Data(bytes)) => {
          if !write_closed && write_cancellable(&mut write_half, &bytes, &cancel_rx).await {
            break;
          }
        }
        Ok(BridgeOut::ShutdownWrite) => {
          // Ignoring Err: half-closing a gone peer is moot.
          let _ = write_half.close().await;
          write_closed = true;
        }
        // The pump dropped the handle (Close / shutdown): tear down.
        Err(_) => break,
      },
    }
  }
  // Best-effort: ensure the OS socket is fully closed once the bridge exits.
  let _ = S::reunite(read_half, write_half).map(|s| s.shutdown(Shutdown::Both));
}

/// Writes `bytes` fully, racing the write against the bridge's cancel channel so
/// a teardown (handle drop) preempts a write stalled on an unresponsive peer.
/// Returns true if cancelled — the bridge should then tear down.
async fn write_cancellable<W: futures_util::io::AsyncWrite + Unpin>(
  write_half: &mut W,
  bytes: &[u8],
  cancel_rx: &Receiver<()>,
) -> bool {
  select! {
    res = write_half.write_all(bytes).fuse() => {
      // Ignoring Err: a write failure ends the exchange via its stream timeout.
      let _ = res;
      false
    }
    _ = cancel_rx.recv_async().fuse() => true,
  }
}
