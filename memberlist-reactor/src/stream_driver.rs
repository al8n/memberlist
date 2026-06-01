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
  time::Duration,
};

use agnostic::{
  Runtime,
  net::{Net, TcpListener, TcpStream, UdpSocket},
};
use bytes::Bytes;
use flume::{Receiver, Sender};
use futures_util::{
  AsyncReadExt, AsyncWriteExt, FutureExt,
  future::{FusedFuture, pending},
  pin_mut, select, select_biased,
};
use memberlist::codec::{
  DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
  parse_messages,
};
use memberlist_proto::{
  Instant,
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

/// The pump's end of a live bridge: the write channel plus an explicit-abort
/// channel. A graceful `StreamAction::Close` (or driver shutdown) drops the whole
/// handle: `out_tx` disconnects, and the bridge tears down after draining the
/// `BridgeOut::Data` it already queued. A failed `StreamAction::Abort` instead
/// sends `()` on `cancel_tx` first, which preempts the bridge — even a write
/// stalled on a peer that stopped reading — and discards the queued bytes.
struct BridgeHandle {
  out_tx: Sender<BridgeOut>,
  cancel_tx: Sender<()>,
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
  /// No-progress (idle) bound on a bridge's post-Close graceful drain. A
  /// graceful Close has no remaining cancel path, so a non-reading peer would
  /// otherwise wedge the drain forever; if a single partial write makes NO
  /// progress for this long the drain is abandoned and the bridge torn down
  /// (RST). A peer that keeps reading — even slowly — resets the deadline on
  /// every chunk and never trips it. Threaded into each spawned `bridge_task`.
  close_timeout: Duration,
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
    close_timeout: Duration,
  ) -> Self {
    let buf_len = endpoint
      .gossip_mtu()
      .saturating_add(memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD)
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
      close_timeout,
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
    R::spawn_detach(bridge_task::<I, R, <R::Net as Net>::TcpStream>(
      stream,
      eid,
      out_rx,
      cancel_rx,
      self.inbound_tx.clone(),
      self.shared.clone(),
      self.close_timeout,
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
        self.bridges.insert(eid, BridgeHandle { out_tx, cancel_tx });
        self.spawn_dial(eid, peer, out_rx, cancel_rx);
      }
      StreamAction::Shutdown(eref) => {
        if let Some(handle) = self.bridges.get(&eref.id()) {
          // Ignoring Err: the bridge task exited; its socket is already gone.
          let _ = handle.out_tx.try_send(BridgeOut::ShutdownWrite);
        }
      }
      StreamAction::Close(eref) => {
        // Graceful close. Dropping the handle disconnects `out_tx` and
        // `cancel_tx`: the bridge first drains the `BridgeOut::Data` it already
        // queued (writing the exchange's final response), then exits on the
        // `out_rx` disconnect. The `cancel_tx` disconnect is mapped to a
        // never-resolving future in the bridge, so it does NOT preempt that
        // drain — unlike Abort below, Close delivers the queued bytes.
        self.bridges.remove(&eref.id());
      }
      StreamAction::Abort(eref) => {
        // A FAILED exchange. Send the explicit cancel BEFORE dropping the handle:
        // it resolves the bridge's cancel future with `Ok(())`, preempting the
        // bridge — even a write stalled on an unresponsive peer — and discarding
        // any queued bytes (possibly encoded under a superseded policy). Abort and
        // Close no longer coincide: Abort preempts and discards, Close drains.
        if let Some(handle) = self.bridges.get(&eref.id()) {
          // Ignoring Err: the bridge already exited, or the bounded(1) channel is
          // full from a prior signal — the handle drop below still disconnects it;
          // the explicit signal is only the abort fast-path.
          let _ = handle.cancel_tx.try_send(());
        }
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
      this.bridges.insert(eid, BridgeHandle { out_tx, cancel_tx });
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
/// and the write half-close.
///
/// Teardown distinguishes a graceful close from an explicit abort:
/// - A graceful `StreamAction::Close` drops the handle, disconnecting `out_rx`
///   and `cancel_rx`. The bridge first drains the `BridgeOut::Data` already
///   queued in `out_rx` (flushing the exchange's final response), then exits on
///   the `out_rx` disconnect. The `cancel_rx` disconnect does NOT preempt that
///   drain — it is mapped to a never-resolving future below.
/// - An explicit `StreamAction::Abort` sends `()` on `cancel_rx` before dropping
///   the handle. That resolves `cancel_fut`, which preempts the bridge — even a
///   write stalled on an unresponsive peer — and discards the queued bytes.
///
/// A graceful Close has NO remaining cancel path (the handle is gone), so a peer
/// that sent its request+FIN and then STOPPED reading would wedge the post-Close
/// drain forever — leaking this detached task and its socket. The drain is
/// therefore a chunked write loop, and EACH partial write is bounded by a fresh
/// `close_timeout`. Because progress resets the deadline, this is a NO-PROGRESS
/// (idle) timeout, not a cap on total drain duration: a peer that keeps reading
/// — even slowly, so a large frame outlasts `close_timeout` overall — advances
/// on every chunk and never trips it. It fires only when a single partial write
/// makes NO progress for the full `close_timeout`; the bridge is then torn down,
/// dropping the write half so the OS RSTs the stuck stream.
async fn bridge_task<I: NodeId, R: Runtime, S: TcpStream>(
  stream: S,
  eid: ExchangeId,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: Receiver<()>,
  inbound_tx: Sender<BridgeInbound>,
  shared: Arc<Shared<I>>,
  close_timeout: Duration,
) {
  let (mut read_half, mut write_half) = stream.into_split();
  let mut buf = vec![0u8; BRIDGE_READ_BUF];
  let mut read_eof = false;
  let mut write_closed = false;
  // Hoist a single cancel future that resolves ONLY on an explicit abort. A
  // graceful Close drops `cancel_tx` (Err(Disconnected)); that is NOT an abort,
  // so map it to a future that never resolves — queued writes then complete and
  // the bridge tears down via the `out_rx` disconnect after draining. Because the
  // disconnect maps to `pending()`, `cancel_fut` never wins the write race on a
  // graceful close, so a write is never dropped mid-flight (no partial-write /
  // duplication hazard); an explicit abort still resolves and preempts.
  let cancel_fut = async {
    match cancel_rx.recv_async().await {
      Ok(()) => (),
      Err(_) => pending::<()>().await,
    }
  }
  .fuse();
  pin_mut!(cancel_fut);
  loop {
    // A push/pull peer half-closes (FINs) after sending its half, then reads the
    // reply. So a read EOF retires only the read side; the bridge stays alive to
    // write the reply over the half-open connection and tears down only when the
    // pump drops the handle (disconnecting out_rx and cancel_rx).
    if read_eof {
      match out_rx.recv_async().await {
        Ok(BridgeOut::Data(bytes)) => {
          // Tear down on either teardown signal: an explicit abort, OR a drain
          // that makes NO progress for `close_timeout` on a non-reading peer (a
          // graceful Close has no cancel path, so the idle timeout is its only
          // backstop). A slow-but-reading peer resets the deadline each chunk.
          if !write_closed
            && write_cancellable::<_, R, _>(&mut write_half, &bytes, &mut cancel_fut, close_timeout)
              .await
          {
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
          // Tear down on an explicit abort OR a drain that makes NO progress for
          // `close_timeout` on a non-reading peer (the graceful-Close backstop).
          // A slow-but-reading peer resets the deadline each chunk and is not
          // timed out.
          if !write_closed
            && write_cancellable::<_, R, _>(
              &mut write_half,
              &bytes,
              &mut cancel_fut,
              close_timeout,
            )
            .await
          {
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

/// Writes `bytes` fully via a chunked loop, racing EACH partial write against
/// TWO backstops.
///
/// 1. The bridge's hoisted `cancel_fut` — resolves ONLY on an explicit
///    `StreamAction::Abort` (a graceful Close maps its handle-drop disconnect to
///    a never-resolving future). Listed FIRST in every iteration so it preempts
///    immediately, even a write stalled mid-frame on an unresponsive peer; on a
///    graceful close the write always wins this arm and the queued bytes flush.
/// 2. An `R::sleep(close_timeout)` re-armed FRESH on every iteration — the
///    backstop for a post-Close drain that has NO remaining cancel path. After a
///    graceful Close the pump removed the handle, so a non-reading peer could
///    otherwise wedge the drain forever. Because the deadline resets on each
///    partial write, it is a NO-PROGRESS (idle) timeout, not a cap on total write
///    duration: a peer that keeps reading — even slowly, so the whole frame takes
///    longer than `close_timeout` to drain — advances on every chunk and never
///    trips it. It fires only when a single partial write makes NO progress for
///    the full `close_timeout` (a genuinely stalled peer).
///
/// Returns true if the bridge should tear down (drop the write half → RST):
/// aborted, the drain made no progress for `close_timeout`, or a write error /
/// write-zero. Returns false only on a fully-written frame.
async fn write_cancellable<W, R, C>(
  write_half: &mut W,
  bytes: &[u8],
  mut cancel_fut: &mut C,
  close_timeout: Duration,
) -> bool
where
  W: futures_util::io::AsyncWrite + Unpin,
  R: Runtime,
  C: Future<Output = ()> + FusedFuture + Unpin,
{
  let mut written = 0;
  while written < bytes.len() {
    // Re-arm the deadline FRESH each iteration: progress (a non-empty partial
    // write) resets the clock, so this is an idle timeout, not a total-duration
    // cap. A slow-but-reading peer advances every chunk and never trips it.
    let timeout_fut = R::sleep(close_timeout).fuse();
    pin_mut!(timeout_fut);
    let res = select_biased! {
      // Explicit abort only (a disconnect was mapped to `pending()`). Listed
      // first so it can preempt even a write blocked mid-frame on an
      // unresponsive peer, ahead of the timeout backstop.
      _ = cancel_fut => return true,
      // Backstop: no progress on this partial write for the full `close_timeout`
      // (a non-reading peer) → tear down (RST on teardown).
      _ = timeout_fut => return true,
      // Write the unwritten tail; a partial write returns its byte count.
      res = write_half.write(&bytes[written..]).fuse() => res,
    };
    match res {
      // Progress: advance and loop with a fresh deadline.
      Ok(n) if n > 0 => written += n,
      // A zero-byte write makes no progress, and a write error ends the
      // exchange: tear down rather than spin or write further.
      // Ignoring Err: the inbound side observes the same broken socket on its
      // next read; surfacing it here would race the pump's teardown.
      _ => return true,
    }
  }
  false
}

#[cfg(test)]
mod tests {
  use agnostic::{
    Runtime,
    net::{Net, TcpListener, TcpStream},
    tokio::TokioRuntime,
  };
  use memberlist_proto::{
    Instant, RawRecords, TcpOptions, config::EndpointConfig, endpoint::Endpoint,
    streams::StreamEndpoint,
  };
  use smol_str::SmolStr;

  use super::*;

  type TokioNet = <TokioRuntime as Runtime>::Net;
  type TokioTcpStream = <TokioNet as Net>::TcpStream;

  /// Mints a real [`ExchangeId`] via a minimal in-memory `StreamEndpoint`.
  ///
  /// `accept_connection` allocates only in-memory bridge state (no I/O), and the
  /// `ExchangeId` it returns is opaque to the bridge — it merely stamps it onto
  /// `BridgeInbound` events. These tests assert on the bytes the bridge writes to
  /// the wire, not on the stamped id, so any valid id suffices.
  fn fresh_eid() -> ExchangeId {
    let cfg = EndpointConfig::new(
      SmolStr::new("bridge-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    );
    let ep = Endpoint::new(cfg);
    let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      ep,
      TcpOptions::new(None),
      Box::new(|_| None),
      Box::new(|addr| *addr),
    );
    endpoint.accept_connection("127.0.0.1:1".parse().unwrap(), Instant::now())
  }

  /// Connects a loopback TCP pair, returning `(server, client)`. The bridge owns
  /// `server`; the test plays the peer through `client`, reading what the bridge
  /// writes.
  async fn loopback_pair() -> (TokioTcpStream, TokioTcpStream) {
    let listener = <TokioNet as Net>::TcpListener::bind("127.0.0.1:0")
      .await
      .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener local_addr");
    let client = TokioTcpStream::connect(addr).await.expect("connect client");
    let (server, _peer_addr) = listener.accept().await.expect("accept server");
    (server, client)
  }

  /// A `Shared` whose only role here is to absorb the bridge's `wake_driver`
  /// calls — these tests assert on the wire, not on driver wakeups.
  fn test_shared() -> Arc<Shared<SmolStr>> {
    let ep = Endpoint::new(EndpointConfig::new(
      SmolStr::new("bridge-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ));
    Arc::new(Shared::new(snapshot_of(&ep)))
  }

  /// A graceful close (handle drop) must flush every byte already queued in
  /// `out_rx` before the bridge exits.
  ///
  /// Reproduces the push/pull final-response shape: the peer half-closes its
  /// write side (request EOF), so the bridge enters its read-EOF mode and stays
  /// alive to write the reply. The reply is then queued and the whole handle
  /// dropped — `out_tx` AND `cancel_tx` disconnect together. The peer must
  /// receive the full reply, then EOF. If the cancel disconnect were treated as
  /// an abort (the bug), `write_cancellable` would preempt the reply and the peer
  /// would read `UnexpectedEof` / a truncated body.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn graceful_close_drains_queued_bytes_before_exit() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    let response = b"the-final-response-bytes".to_vec();

    // A long `close_timeout` so this graceful-drain test exercises the FIFO
    // drain, never the timeout backstop (the peer reads promptly here).
    let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      shared,
      Duration::from_secs(60),
    ));

    // The peer sends its request, then half-closes its write side. The bridge
    // reads the request, forwards it, then observes the FIN and enters read-EOF
    // mode (the half-open state where it writes the reply).
    client.write_all(b"request").await.expect("write request");
    client
      .shutdown(Shutdown::Write)
      .expect("half-close client write side");

    // Wait until the bridge has reported the request EOF: this proves it reached
    // read-EOF mode, the exact clean state the graceful close must drain from.
    loop {
      match inbound_rx
        .recv_async()
        .await
        .expect("bridge reports inbound")
      {
        BridgeInbound::Eof(_) => break,
        BridgeInbound::Data(_) => continue,
      }
    }

    // Queue the reply, then drop the whole handle: the graceful-Close shape —
    // `out_tx` and `cancel_tx` both disconnect with the reply already queued.
    out_tx
      .send(BridgeOut::Data(Bytes::from(response.clone())))
      .expect("queue response bytes");
    drop((out_tx, cancel_tx));

    // The peer must read the full reply before EOF.
    let mut got = vec![0u8; response.len()];
    client
      .read_exact(&mut got)
      .await
      .expect("peer reads the full response before EOF");
    assert_eq!(
      got, response,
      "graceful close must flush the queued response; a cancel disconnect must \
       not truncate the drain"
    );

    // After the reply the bridge exits and shuts the socket: the peer sees EOF.
    let tail = client
      .read_to_end(&mut Vec::new())
      .await
      .expect("read tail after response");
    assert_eq!(tail, 0, "no bytes after the response, only EOF");

    bridge.await.expect("bridge task exits cleanly");
  }

  /// An explicit abort (`cancel_tx.send(())`, a FAILED exchange's
  /// `StreamAction::Abort`) must preempt and DISCARD the bytes still queued in
  /// `out_rx` — stale bytes (possibly encoded under a superseded policy) never
  /// reach the wire. Keeping `out_tx` alive proves the ONLY teardown signal is
  /// the explicit abort, not a disconnect.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn explicit_abort_preempts_and_discards() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    let stale = b"stale-bytes-that-must-not-reach-the-wire".to_vec();

    // Queue stale bytes and pre-arm the explicit abort BEFORE the task runs, so
    // the bridge's first write race sees a ready cancel and breaks without
    // writing. Keep `out_tx` alive: the abort send is the sole teardown signal.
    out_tx
      .send(BridgeOut::Data(Bytes::from(stale)))
      .expect("queue stale bytes");
    cancel_tx.try_send(()).expect("signal explicit abort");
    let _out_tx_kept = out_tx;

    // A long `close_timeout`: the abort, not the backstop, must drive teardown.
    let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      shared,
      Duration::from_secs(60),
    ));

    // The peer must see EOF with NO bytes — the abort dropped the write side
    // without flushing the stale queue.
    let n = client
      .read_to_end(&mut Vec::new())
      .await
      .expect("read peer side to EOF");
    assert_eq!(n, 0, "explicit abort must discard queued bytes, got {n}");

    bridge.await.expect("bridge task exits cleanly");
  }

  /// A post-Close graceful drain whose peer STOPPED reading must be reclaimed by
  /// `close_timeout` — it has NO remaining cancel path, so without the timeout
  /// backstop the bridge's drain blocks FOREVER, leaking the detached task and
  /// its socket. A fully stalled peer makes NO progress, so the idle
  /// `close_timeout` fires and reclaims it.
  ///
  /// Shape: the peer sends its request then half-closes (the bridge enters
  /// read-EOF mode), an oversized reply is queued, and the whole handle is
  /// dropped — the exact `StreamAction::Close` ordering. The peer is then held
  /// but NEVER read, so its receive window collapses to zero and the drain
  /// stalls once the kernel buffers fill. With no cancel path the ONLY teardown
  /// is the `close_timeout` backstop.
  ///
  /// The outer `close_timeout * 5` bound fails fast: without the backstop the
  /// drain would block forever and trip it; with it the bridge reclaims within
  /// ~`close_timeout`. A SHORT `close_timeout` keeps the test fast.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn graceful_close_drain_bounded_by_close_timeout_when_peer_stalls() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    let close_timeout = Duration::from_millis(300);

    // A reply far larger than any kernel socket buffer: with the peer never
    // reading, its window collapses to zero and the drain blocks once the
    // send/recv buffers fill — it cannot complete unless the peer reads.
    let response = vec![0xCDu8; 16 * 1024 * 1024];

    let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      shared,
      close_timeout,
    ));

    // The peer sends its request then half-closes its write side, driving the
    // bridge into read-EOF mode (the half-open state from which a graceful Close
    // drains the queued reply).
    client.write_all(b"request").await.expect("write request");
    client
      .shutdown(Shutdown::Write)
      .expect("half-close client write side");

    // Wait until the bridge reports the request EOF: it is now in read-EOF mode,
    // the exact clean state from which a graceful Close drains.
    loop {
      match inbound_rx
        .recv_async()
        .await
        .expect("bridge reports inbound")
      {
        BridgeInbound::Eof(_) => break,
        BridgeInbound::Data(_) => continue,
      }
    }

    // Queue the oversized reply and drop the whole handle: the graceful-Close
    // shape (`out_tx` and `cancel_tx` both disconnect, the reply queued). No
    // cancel can ever be sent now — only `close_timeout` can reclaim the bridge.
    out_tx
      .send(BridgeOut::Data(Bytes::from(response)))
      .expect("queue oversized response");
    drop((out_tx, cancel_tx));

    // The peer is NEVER read: the drain stalls on a zero window. The bridge must
    // reclaim within ~`close_timeout`; the generous `close_timeout * 5` outer
    // bound fails fast — without the backstop the drain blocks forever and trips it.
    tokio::time::timeout(close_timeout * 5, bridge)
      .await
      .expect("bridge reclaims within ~close_timeout, not blocking forever on a stalled drain")
      .expect("bridge task exits cleanly");
  }

  /// A peer that reads SLOWLY but CONTINUOUSLY must receive the WHOLE reply, even
  /// when the total drain outlasts `close_timeout`.
  ///
  /// `close_timeout` is a NO-PROGRESS (idle) bound, not a cap on total write
  /// duration. The peer here reads the oversized reply in chunks with a small
  /// delay between each, so the writer is gated by the reader: the total drain
  /// greatly EXCEEDS `close_timeout`, yet each chunk is read well within it, so no
  /// single partial write ever goes idle for the full `close_timeout`.
  ///
  /// A total-duration cap would instead race the ENTIRE write against a single
  /// `R::sleep(close_timeout)`; a drain that takes longer than `close_timeout`
  /// overall (this slow reader) would lose that race mid-transfer, the bridge
  /// would tear down, and the socket would be RST — the peer's `read_exact` then
  /// failing with `UnexpectedEof` on the truncated body. The idle bound instead
  /// re-arms on each partial write, so progress keeps resetting it and the full
  /// reply is delivered.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn slow_but_progressing_reader_is_not_timed_out() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    // SHORT idle bound: the slow reader's TOTAL drain (below) runs several times
    // longer, while no single partial write ever goes idle this long.
    let close_timeout = Duration::from_millis(250);

    // A reply far larger than the kernel socket buffers, so the writer cannot
    // dump it all and finish instantly — it stays gated by the slow reader, so
    // the TOTAL drain wall-clock greatly outlasts `close_timeout`.
    let len = 16 * 1024 * 1024;
    let response: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();

    let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      shared,
      close_timeout,
    ));

    // The peer sends its request then half-closes its write side, driving the
    // bridge into read-EOF mode (the half-open state from which a graceful Close
    // drains the queued reply).
    client.write_all(b"request").await.expect("write request");
    client
      .shutdown(Shutdown::Write)
      .expect("half-close client write side");

    // Wait until the bridge reports the request EOF: it is now in read-EOF mode,
    // the exact clean state from which a graceful Close drains.
    loop {
      match inbound_rx
        .recv_async()
        .await
        .expect("bridge reports inbound")
      {
        BridgeInbound::Eof(_) => break,
        BridgeInbound::Data(_) => continue,
      }
    }

    // Queue the reply and drop the whole handle: the graceful-Close shape
    // (`out_tx` and `cancel_tx` both disconnect, the reply queued). No cancel can
    // ever be sent now — only the idle `close_timeout` could reclaim the bridge,
    // and it must NOT while the peer keeps reading.
    out_tx
      .send(BridgeOut::Data(Bytes::from(response.clone())))
      .expect("queue response bytes");
    drop((out_tx, cancel_tx));

    // Read the whole reply SLOWLY but CONTINUOUSLY: 2 MiB chunks with a 60ms gap
    // between reads. The chunk is large enough (~ the kernel buffer) that the
    // writer never gets more than one sleep ahead, so no single partial write
    // goes idle for `close_timeout`; yet with eight chunks the TOTAL drain runs
    // ~0.5s, several times the 250ms idle bound. A total-duration cap would lose
    // its race against the ~0.5s drain mid-transfer and RST, so this `read_exact`
    // would see `UnexpectedEof` on a truncated body; the idle bound re-arms on
    // each chunk, so the full body arrives.
    let chunk = 2 * 1024 * 1024;
    let mut got = vec![0u8; len];
    let mut off = 0;
    while off < len {
      let end = (off + chunk).min(len);
      client
        .read_exact(&mut got[off..end])
        .await
        .expect("slow reader receives the full reply, never a truncated body");
      off = end;
      tokio::time::sleep(Duration::from_millis(60)).await;
    }

    assert_eq!(
      got, response,
      "a slow-but-progressing reader must receive the exact reply, not a \
       truncated or corrupted body"
    );

    // After the full reply the bridge exits and shuts the socket: the peer sees
    // EOF.
    let tail = client
      .read_to_end(&mut Vec::new())
      .await
      .expect("read tail after reply");
    assert_eq!(tail, 0, "no bytes after the reply, only EOF");

    bridge.await.expect("bridge task exits cleanly");
  }
}
