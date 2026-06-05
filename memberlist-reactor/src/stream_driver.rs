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
use futures_channel::oneshot;
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
  Instant, PingId,
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, StreamId, Transmit},
  streams::{ExchangeId, StreamAction, StreamEndpoint, StreamTransport},
};

use crate::{
  NodeId,
  command::{
    Command, JoinCmd, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, SetChecksumOptionsCmd,
    SetCompressionOptionsCmd, SetEncryptionOptionsCmd, ShutdownCmd,
  },
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
  cancel_tx: oneshot::Sender<()>,
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
  cancel_rx: oneshot::Receiver<()>,
}

/// A `WaitForCompletion` join awaiting its dispatched push/pull exchanges. The
/// machine emits one `ExchangeCompleted` per dispatched exchange (success or
/// `stream_timeout` failure), so the set always drains — no deadline needed.
///
/// `start_push_pull` returns a `StreamId`, not the exchange's `ExchangeId`, and
/// there is no public reverse index, so `dispatch` drains the action(s) each
/// `start_push_pull` queues and captures the resulting `Connect`'s `ExchangeId`
/// here, keyed on the originating `StreamId`. Matching the StreamId — not the
/// peer — is what call-scopes the capture: `service_dials` drains a shared dial
/// deque, so one join's drain can also surface an unrelated same-peer dial
/// enqueued by another subsystem. A dial that fails before any `Connect` (e.g. a
/// TLS peer with no SNI) simply contributes no id rather than wedging the join.
struct PendingJoin {
  /// Exchanges this join is awaiting, captured from their `Connect` at dispatch.
  pending_eids: HashSet<ExchangeId>,
  contacted: usize,
  requested: usize,
  reply: oneshot::Sender<Result<usize, Error>>,
}

/// An in-flight graceful leave; every joined caller's reply resolves together
/// when `LeftCluster` fires.
struct PendingLeave {
  repliers: Vec<oneshot::Sender<Result<(), Error>>>,
}

/// An outstanding application-ping call; resolved on `PingCompleted` (reply
/// `Ok(rtt)`) or `PingFailed` (reply `Err(PingTimeout)`) via the matching
/// `PingId`. On driver exit, drained with `Err(Shutdown)`.
struct PendingPing {
  ping_id: PingId,
  reply: oneshot::Sender<Result<Duration, Error>>,
}

/// An outstanding reliable directed-send call; a single `Command::SendReliable`
/// may dispatch multiple `start_user_message` calls (one per payload). Each
/// call returns a `StreamId`; the driver captures the resulting `Connect`'s
/// `ExchangeId` in `dispatch` keyed on that `StreamId` (mirror of the join
/// capture) and parks one `PendingUserSend` whose `pending` set tracks the
/// in-flight `ExchangeId`s. Resolved on `ExchangeCompleted(UserMessage)` as each
/// id empties from `pending`. On driver exit, drained with `Err(Shutdown)`.
struct PendingUserSend {
  /// `ExchangeId`s this call dispatched and has not yet seen a terminal
  /// `ExchangeCompleted` for; an entry is removed on any terminal outcome.
  pending: HashSet<ExchangeId>,
  /// Running count of payload failures. SEEDED at park time to the count of
  /// payloads that never produced a `Connect` (a pre-`Connect` retirement
  /// will never surface an `ExchangeCompleted`); incremented further for each
  /// tracked exchange whose terminal outcome is `Failed`. A non-zero value
  /// makes the final reply `Err(SendFailed)`.
  failed: usize,
  reply: oneshot::Sender<Result<(), Error>>,
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
  /// Cluster label threaded into the gossip `EncodeOptions` / `DecodeOptions` so
  /// outbound gossip is stamped and inbound gossip is verified against the same
  /// label. Stored here so `drain_surfaces` can read it each poll without a
  /// per-call allocation.
  pub(crate) label: Option<bytes::Bytes>,
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
  /// Outstanding application-ping calls; resolved via `PingId` correlation.
  pending_pings: Vec<PendingPing>,
  /// Outstanding reliable-send calls; resolved when all tracked exchange ids
  /// surface a terminal `ExchangeCompleted(UserMessage)`.
  pending_user_sends: Vec<PendingUserSend>,
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
    label: Option<bytes::Bytes>,
  ) -> Self {
    let buf_len = endpoint
      .gossip_mtu()
      .saturating_add(memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD)
      .saturating_add(memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD)
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
      label,
      obs_overflow: VecDeque::new(),
      pending_joins: HashMap::new(),
      next_pending_join_id: 0,
      pending_leave: None,
      pending_pings: Vec::new(),
      pending_user_sends: Vec::new(),
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
    cancel_rx: oneshot::Receiver<()>,
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
    cancel_rx: oneshot::Receiver<()>,
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
  fn dispatch(&mut self, cmd: Command<I>, now: Instant) {
    match cmd {
      Command::Join(JoinCmd { addrs, wait, reply }) => {
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
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
          let _ = reply.send(Ok(count));
          return;
        }
        // WaitForCompletion: account_event replies the contacted count once every
        // exchange this join started completes. An empty seed list is a no-op
        // success, not a failure.
        if addrs.is_empty() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Ok(0));
          return;
        }
        // Drain already-queued actions first, so a same-peer Connect from an
        // earlier call (join_detached, a gossip ping) is handled and not captured
        // by this join.
        while let Some(action) = self.endpoint.poll_action() {
          self.handle_stream_action(action);
        }
        // Start each seed's exchange and capture the Connect it produces
        // synchronously, keyed on the `StreamId` each `start_push_pull` returned
        // (not the peer): `service_dials` drains a shared dial deque, so a
        // same-peer dial flushed for another subsystem (a scheduled push/pull, a
        // probe reliable-ping, a gossip dial) must not be misattributed to this
        // join. A dial that fails before any Connect (e.g. a TLS peer with no
        // SNI) yields none, so it cannot leave the join awaiting forever.
        let mut pending_eids = HashSet::with_capacity(addrs.len());
        let mut started: HashSet<StreamId> = HashSet::with_capacity(addrs.len());
        for addr in &addrs {
          let sid = self
            .endpoint
            .start_push_pull(*addr, PushPullKind::Join, now);
          started.insert(sid);
          while let Some(action) = self.endpoint.poll_action() {
            if let StreamAction::Connect(info) = &action
              && started.contains(&info.stream_id())
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
          let _ = reply.send(Err(Error::JoinFailed(addrs.len())));
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
            let _ = reply.send(other);
          }
        }
      }
      Command::Shutdown(ShutdownCmd { reply }) => {
        self.shared.begin_shutdown();
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(Ok(()));
      }
      Command::Ping(PingCmd { node, reply }) => {
        // Gate on a running node: after `leave()` the probe scheduler is
        // stopped, so a new application ping's completion event would never
        // arrive and the caller would hang forever. Reject with `NotRunning`.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
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
        // Gate on a running node: after `leave()` the gossip socket is
        // effectively closed to protocol traffic; reject immediately.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
          return;
        }
        let res = self
          .endpoint
          .send_user_packets(to, &payloads)
          .map_err(|e| Error::PayloadTooLarge(e.to_string()));
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SendReliable(SendReliableCmd {
        to,
        payloads,
        reply,
      }) => {
        // Gate on a running node: after `leave()` the stream coordinator is
        // stopping; a new `start_user_message` would never produce a `Connect`
        // and the caller would hang. Reject with `NotRunning`.
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::NotRunning));
          return;
        }
        if payloads.is_empty() {
          // No payloads: nothing to track; reply Ok immediately.
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Ok(()));
          return;
        }
        // Drain any already-queued actions before starting new user messages
        // so a pre-existing Connect from an earlier call is not captured here.
        while let Some(action) = self.endpoint.poll_action() {
          self.handle_stream_action(action);
        }
        // Start each payload's stream and capture the resulting Connect's
        // ExchangeId synchronously, keyed on the `StreamId` each
        // `start_user_message` returned (mirror of the join capture logic).
        // `n` = payloads dispatched; `m` = exchanges captured for this command.
        // Each `start_user_message` produces one `StreamId` in `started` and at
        // most one captured `Connect` (success) or zero (a pre-`Connect`
        // retirement: TLS SNI parse failure, record-layer fault, or an
        // already-elapsed exchange deadline — the API permits a `sni_provider`
        // that returns `Some` then `None`, so a partial failure mid-batch is
        // reachable). Keying on `started` (not the peer) is what prevents a
        // same-peer dial flushed by the shared `service_dials` for another
        // subsystem from being misattributed to this send, so `m <= n` always
        // holds.
        let n = payloads.len();
        let mut pending = HashSet::with_capacity(n);
        let mut started: HashSet<StreamId> = HashSet::with_capacity(n);
        for payload in payloads {
          let sid = self.endpoint.start_user_message(to, payload, now);
          started.insert(sid);
          while let Some(action) = self.endpoint.poll_action() {
            if let StreamAction::Connect(ref info) = action
              && started.contains(&info.stream_id())
            {
              pending.insert(info.id());
            }
            self.handle_stream_action(action);
          }
        }
        let m = pending.len();
        if m == 0 {
          // The empty-payloads case already returned `Ok(())` above, so
          // reaching here means there WERE payloads to send but every dial
          // was retired BEFORE a `StreamAction::Connect`. No exchange was
          // created and no terminal `ExchangeCompleted` will arrive, so the
          // send genuinely FAILED. Reply `Err(SendFailed)` rather than
          // falsely reporting success.
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::SendFailed));
          return;
        }
        // Park with `failed` SEEDED to the `n - m` payloads that never
        // produced an exchange (a partial pre-`Connect` failure). The
        // waiter resolves `Ok(())` only when every dispatched exchange
        // succeeds AND no payload was dropped pre-`Connect`; any seeded or
        // observed failure makes the final reply `Err(SendFailed)`. Without
        // the seed, a batch where some payloads connect and a LATER one
        // fails pre-`Connect` would falsely report `Ok` once the connected
        // exchanges succeed.
        //
        // Capture-by-`StreamId` makes `m <= n` structural, so `n - m` cannot
        // underflow. `checked_sub` is the fail-closed backstop: were a future
        // regression to make `m > n`, reply `Err(SendFailed)` rather than
        // panic on the underflow.
        match n.checked_sub(m) {
          Some(failed) => {
            self.pending_user_sends.push(PendingUserSend {
              pending,
              failed,
              reply,
            });
          }
          None => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::SendFailed));
          }
        }
      }
      Command::SetCompressionOptions(SetCompressionOptionsCmd { opts, reply }) => {
        // Gate on a running node: after `leave()` the endpoint emits no
        // protocol traffic, so a new compression policy could never take
        // effect on the wire. Reject with `NotRunning` rather than ack a
        // change that will never be observed.
        let res = if self.endpoint.is_running() {
          self.endpoint.set_compression_options(opts);
          Ok(())
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetChecksumOptions(SetChecksumOptionsCmd { opts, reply }) => {
        // Gate on a running node FIRST: after `leave()` the endpoint emits no
        // gossip datagrams, so a new checksum policy (a gossip-plane concern)
        // could never take effect on the wire. When running, validate the
        // policy before applying it: an algorithm whose backend feature is
        // absent is accepted by the options builder, but every later
        // `checksum_gossip` would fail and the driver would drop the datagram —
        // so a "successful" change would silently disable ALL gossip after a
        // false `Ok`.
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .set_checksum_options(opts)
            .map_err(Error::Checksum)
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd { opts, reply }) => {
        // Gate on a running node FIRST: after `leave()` the endpoint emits
        // no protocol traffic, so a new encryption policy could never take
        // effect on the wire. When running, validate the policy before
        // applying it: a keyring naming an unsupported AEAD would silently
        // break the cluster after a false `Ok`.
        let res = if self.endpoint.is_running() {
          match crate::transform::validate_encryption(&opts) {
            Ok(()) => {
              self.endpoint.set_encryption_options(opts);
              Ok(())
            }
            Err(e) => Err(e),
          }
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
    }
  }

  /// Synchronous-command accounting for a surfaced event: reduces the matching
  /// `WaitForCompletion` join on a push/pull `ExchangeCompleted`, resolves a
  /// parked leave on `LeftCluster`, resolves ping waiters on
  /// `PingCompleted`/`PingFailed`, and resolves reliable-send waiters on
  /// `ExchangeCompleted(UserMessage)`.
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
          let _ = pj.reply.send(res);
        }
      }
      Event::LeftCluster => {
        if let Some(pl) = self.pending_leave.take() {
          for replier in pl.repliers {
            // Ignoring Err: a leave caller dropped its reply receiver.
            let _ = replier.send(Ok(()));
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
          let _ = pp.reply.send(Ok(p.rtt()));
        }
      }
      // Ping failure: resolve the matching waiter with `PingTimeout`. Same
      // additive semantics as `PingCompleted`.
      Event::PingFailed(p) => {
        let pid = p.ping_id();
        if let Some(idx) = self.pending_pings.iter().position(|pp| pp.ping_id == pid) {
          let pp = self.pending_pings.swap_remove(idx);
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = pp.reply.send(Err(Error::PingTimeout));
        }
      }
      // Reliable user-send completion: reduce the pending set; reply when empty.
      // The event continues to the observation task below (additive).
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
            let _ = ps.reply.send(res);
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
        let (cancel_tx, cancel_rx) = oneshot::channel();
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
        if let Some(handle) = self.bridges.remove(&eref.id()) {
          // Ignoring Err: the bridge already exited (cancel receiver gone);
          // the abort signal is best-effort.
          let _ = handle.cancel_tx.send(());
        }
      }
    }
  }

  /// Drains each machine surface up to `transmit_batch` items in one pass.
  /// Returns `(worked, more)`: whether any surface produced work (republish the
  /// snapshot) and whether any surface hit its cap with work left (self-wake).
  fn drain_surfaces(&mut self, cx: &mut Context<'_>) -> (bool, bool) {
    let decode_opts = DecodeOptions::new(self.label.clone());
    let encode_opts = EncodeOptions::new(self.label.clone());
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
      let checksummed = match self.endpoint.checksum_gossip(&compressed) {
        Ok(b) => b,
        // Checksum configured but its backend was not built in — drop rather
        // than emit an unverifiable datagram on a checksum-configured path.
        Err(_) => continue,
      };
      let on_wire = match self.endpoint.encrypt_gossip(&checksummed) {
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
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::Leave(LeaveCmd { reply }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::Shutdown(ShutdownCmd { reply }) => {
            let _ = reply.send(Ok(()));
          }
          // Ignoring Err: the caller dropped its reply receiver.
          Command::Ping(PingCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SendUser(SendUserCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SendReliable(SendReliableCmd { reply, .. }) => {
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetChecksumOptions(SetChecksumOptionsCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
          Command::SetEncryptionOptions(SetEncryptionOptionsCmd { reply, .. }) => {
            // Ignoring Err: the caller dropped its reply receiver.
            let _ = reply.send(Err(Error::Shutdown));
          }
        }
      }
      for (_, pj) in this.pending_joins.drain() {
        // Ignoring Err: the join caller dropped its reply receiver.
        let _ = pj.reply.send(Err(Error::Shutdown));
      }
      if let Some(pl) = this.pending_leave.take() {
        for replier in pl.repliers {
          // Ignoring Err: the leave caller dropped its reply receiver.
          let _ = replier.send(Err(Error::Shutdown));
        }
      }
      for pp in this.pending_pings.drain(..) {
        // Ignoring Err: the ping caller dropped its reply receiver.
        let _ = pp.reply.send(Err(Error::Shutdown));
      }
      for ps in this.pending_user_sends.drain(..) {
        // Ignoring Err: the send_reliable caller dropped its reply receiver.
        let _ = ps.reply.send(Err(Error::Shutdown));
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
      let (cancel_tx, cancel_rx) = oneshot::channel();
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
          // Drive the exchange to a DIAL FAILURE — NOT a benign EOF feed.
          // A connect that never established has no wire, and a one-way
          // `UserMessage` maps a clean EOF to a SUCCESSFUL completion
          // (`Stream::handle_data`'s `OutboundSendingRequest(UserMessage)`
          // EOF arm), which would falsely report `send_reliable` success on
          // an unreachable peer. `handle_dial_failed` terminalizes the
          // exchange as `Failed` regardless of kind, so the parked
          // reliable-send waiter resolves with `Err(SendFailed)`.
          this.bridges.remove(&eid);
          this.endpoint.handle_dial_failed(eid, now);
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
  cancel_rx: oneshot::Receiver<()>,
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
  cancel_rx: oneshot::Receiver<()>,
  inbound_tx: Sender<BridgeInbound>,
  shared: Arc<Shared<I>>,
  close_timeout: Duration,
) {
  let (mut read_half, mut write_half) = stream.into_split();
  let mut buf = vec![0u8; BRIDGE_READ_BUF];
  let mut read_eof = false;
  let mut write_closed = false;
  // Hoist a single cancel future that resolves ONLY on an explicit abort. A
  // graceful Close drops `cancel_tx` (Err(Canceled)); that is NOT an abort,
  // so map it to a future that never resolves — queued writes then complete and
  // the bridge tears down via the `out_rx` disconnect after draining. Because the
  // cancellation maps to `pending()`, `cancel_fut` never wins the write race on a
  // graceful close, so a write is never dropped mid-flight (no partial-write /
  // duplication hazard); an explicit abort still resolves and preempts.
  let cancel_fut = async {
    match cancel_rx.await {
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

#[cfg(all(test, feature = "tcp"))]
mod tests {
  use agnostic::{
    Runtime,
    net::{Net, TcpListener, TcpStream},
    tokio::TokioRuntime,
  };
  use memberlist_proto::{
    Instant, RawRecords,
    config::EndpointOptions,
    endpoint::Endpoint,
    streams::{LabelOptions, StreamEndpoint},
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
    let cfg = EndpointOptions::new(
      SmolStr::new("bridge-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    );
    let ep = Endpoint::new(cfg);
    let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      ep,
      LabelOptions::new_in(None, ()),
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
    let ep = Endpoint::new(EndpointOptions::new(
      SmolStr::new("bridge-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ));
    Arc::new(Shared::new(snapshot_of(&ep)))
  }

  fn capture_test_endpoint() -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
    let ep = Endpoint::new(EndpointOptions::new(
      SmolStr::new("capture-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ));
    StreamEndpoint::new(
      ep,
      LabelOptions::new_in(Some(b"capture-test".to_vec()), ()),
      Box::new(|_| None),
      Box::new(|a: &SocketAddr| *a),
    )
  }

  use std::{
    sync::atomic::AtomicU64,
    task::{Context, Wake, Waker},
  };

  use memberlist_proto::{
    event::{Reliability, UserPacket},
    typed::{NodeState, State},
  };

  /// A waker that records, via a shared flag, whether it was woken. Built on the
  /// safe `std::task::Wake` trait (the crate forbids `unsafe`). The driver-poll
  /// tests pass one through a `Context` and only need it to be a valid, harmless
  /// waker — its wake is a no-op flag flip.
  struct FlagWaker(Arc<std::sync::atomic::AtomicBool>);

  impl Wake for FlagWaker {
    fn wake(self: Arc<Self>) {
      self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
      self.0.store(true, Ordering::SeqCst);
    }
  }

  fn flag_waker() -> Waker {
    Waker::from(Arc::new(FlagWaker(Arc::new(
      std::sync::atomic::AtomicBool::new(false),
    ))))
  }

  /// An application-data `Event::UserPacket` of exactly `len` payload bytes —
  /// `observation_payload_bytes` reports `Some(len)`, so it drives the obs-channel
  /// byte backstop. `account_event` treats it as a no-op (no pending state), so it
  /// can be fed to `send_observation` without any join/leave/ping bookkeeping.
  fn user_packet(len: usize) -> Event<SmolStr, SocketAddr> {
    Event::UserPacket(UserPacket::new(
      "127.0.0.1:2".parse::<SocketAddr>().unwrap(),
      Bytes::from(vec![0xABu8; len]),
      Reliability::Reliable,
    ))
  }

  /// A control event carrying no app-data (`observation_payload_bytes` is `None`)
  /// and, with no parked leave/join/ping, a no-op for `account_event`. Used to
  /// drive the obs-channel `Full`-and-recoverable drop arm.
  fn control_event() -> Event<SmolStr, SocketAddr> {
    Event::NodeJoined(Arc::new(NodeState::new(
      SmolStr::new("ctl"),
      "127.0.0.1:3".parse::<SocketAddr>().unwrap(),
      State::Alive,
    )))
  }

  /// Builds a real `StreamDriver` with a bound gossip socket and a caller-supplied
  /// observation channel, so the obs-backstop and shutdown branches can be driven
  /// directly. The accept channel is wired but never fed.
  ///
  /// `obs_cap` sizes the bounded obs channel (fill it to hit the `Full` arms);
  /// `obs_budget` is the payload byte budget (small to hit the byte backstop).
  /// Returns the driver, the obs receiver (drop it to hit the `Disconnected`
  /// arms), and the shared payload-byte counter.
  async fn build_driver(
    obs_cap: usize,
    obs_budget: Option<u64>,
  ) -> (
    StreamDriver<SmolStr, TokioRuntime, RawRecords>,
    Receiver<Event<SmolStr, SocketAddr>>,
    Arc<Shared<SmolStr>>,
    Arc<AtomicU64>,
  ) {
    let socket = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
      .await
      .expect("bind gossip socket");
    let ep = Endpoint::new(EndpointOptions::new(
      SmolStr::new("drv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ));
    let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      ep,
      LabelOptions::new_in(None, ()),
      Box::new(|_| None),
      Box::new(|a: &SocketAddr| *a),
    );
    endpoint.start_scheduling(Instant::now());
    let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));
    let obs_payload_bytes = Arc::new(AtomicU64::new(0));
    let (obs_tx, obs_rx) = flume::bounded(obs_cap);
    let (_accepted_tx, accepted_rx) = flume::bounded(ACCEPT_CAP);
    let (accept_shutdown_tx, _accept_shutdown_rx) = flume::bounded(1);
    let driver = StreamDriver::<SmolStr, TokioRuntime, RawRecords>::new(
      endpoint,
      socket,
      shared.clone(),
      8,
      8,
      obs_tx,
      obs_payload_bytes.clone(),
      obs_budget,
      accepted_rx,
      accept_shutdown_tx,
      Duration::from_secs(60),
      None,
    );
    (driver, obs_rx, shared, obs_payload_bytes)
  }

  /// `send_observation`'s byte backstop: when enqueuing a payload event would push
  /// the queued payload bytes over budget, the event is DROPPED and counted —
  /// never retained — because the count cap alone does not bound memory.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_byte_backstop_drops_oversized_payload() {
    // Budget of 4 bytes; an 8-byte payload alone exceeds it on the first event.
    let (mut driver, _obs_rx, shared, bytes) = build_driver(16, Some(4)).await;

    driver.send_observation(user_packet(8));

    assert_eq!(
      shared.observation_dropped(),
      1,
      "an over-budget payload event is dropped and counted"
    );
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      0,
      "a dropped payload reserves no bytes"
    );
    assert!(
      driver.obs_overflow.is_empty(),
      "a byte-backstop drop never retains the event for retry"
    );
  }

  /// `send_observation` on a FULL obs channel RETAINS application data (still
  /// byte-reserved) for a later retry, rather than dropping it.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_full_channel_retains_app_data() {
    // Capacity-1 channel, ample byte budget. Fill the channel, then a second
    // payload event finds it full and is retained in the overflow.
    let (mut driver, _obs_rx, shared, bytes) = build_driver(1, Some(1 << 20)).await;

    driver.send_observation(user_packet(4)); // fills the capacity-1 channel
    assert!(
      driver.obs_overflow.is_empty(),
      "first event went to the channel"
    );
    driver.send_observation(user_packet(7)); // channel full → retained

    assert_eq!(
      driver.obs_overflow.len(),
      1,
      "app-data is retained for retry on a full channel, not dropped"
    );
    assert_eq!(
      shared.observation_dropped(),
      0,
      "a retained event is not counted as dropped"
    );
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      4 + 7,
      "both the queued and the retained payload stay byte-reserved"
    );
  }

  /// `send_observation` on a FULL obs channel DROPS a recoverable control event
  /// (no app-data) and counts it — only application data is worth retaining.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_full_channel_drops_recoverable_control() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(1, Some(1 << 20)).await;

    driver.send_observation(control_event()); // fills the capacity-1 channel
    driver.send_observation(control_event()); // channel full → dropped + counted

    assert!(
      driver.obs_overflow.is_empty(),
      "a recoverable control event is never retained"
    );
    assert_eq!(
      shared.observation_dropped(),
      1,
      "the second control event found the channel full and was counted"
    );
  }

  /// `send_observation` with the obs task GONE (receiver dropped) rolls back the
  /// reservation it made before the `try_send`, so the byte counter never leaks.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn obs_disconnected_rolls_back_reservation() {
    let (mut driver, obs_rx, shared, bytes) = build_driver(16, Some(1 << 20)).await;
    drop(obs_rx); // the obs task is gone → try_send returns Disconnected

    driver.send_observation(user_packet(9));

    assert_eq!(
      bytes.load(Ordering::Relaxed),
      0,
      "a Disconnected send rolls back the payload reservation"
    );
    assert!(
      driver.obs_overflow.is_empty(),
      "a Disconnected send retains nothing"
    );
    assert_eq!(
      shared.observation_dropped(),
      0,
      "a Disconnected (obs task gone) send is not a recoverable drop"
    );
  }

  /// `flush_obs_overflow` stops at the first `Full`, pushing the un-sendable event
  /// back to the FRONT so retry order is preserved.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn flush_overflow_stops_and_repushes_on_full() {
    // `_obs_rx` is held (never drained) so the channel stays connected-but-full.
    let (mut driver, _obs_rx, _shared, _bytes) = build_driver(1, Some(1 << 20)).await;
    // Fill the capacity-1 channel so the flush below cannot make progress.
    driver
      .obs_tx
      .try_send(control_event())
      .expect("seed the channel full");
    // Two events queued for retry; neither can be sent while the channel is full.
    driver.obs_overflow.push_back(control_event());
    driver.obs_overflow.push_back(control_event());

    driver.flush_obs_overflow();

    assert_eq!(
      driver.obs_overflow.len(),
      2,
      "flush stops at the first Full and re-pushes the event to the front"
    );
  }

  /// `flush_obs_overflow` with the obs task GONE reclaims each retained event's
  /// reserved payload bytes (the obs task can no longer release them).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn flush_overflow_disconnected_reclaims_bytes() {
    let (mut driver, obs_rx, _shared, bytes) = build_driver(16, Some(1 << 20)).await;
    // Stage a retained payload event AS IF previously reserved, then drop the
    // receiver so the flush sees Disconnected and reclaims the reservation.
    bytes.store(6, Ordering::Relaxed);
    driver.obs_overflow.push_back(user_packet(6));
    drop(obs_rx);

    driver.flush_obs_overflow();

    assert!(
      driver.obs_overflow.is_empty(),
      "a Disconnected flush drains the overflow"
    );
    assert_eq!(
      bytes.load(Ordering::Relaxed),
      0,
      "a Disconnected flush reclaims the retained payload's reserved bytes"
    );
  }

  /// Drives the driver through exactly one `Future::poll` with a harmless waker.
  fn poll_once(driver: &mut StreamDriver<SmolStr, TokioRuntime, RawRecords>) -> Poll<()> {
    let waker = flag_waker();
    let mut cx = Context::from_waker(&waker);
    Pin::new(driver).poll(&mut cx)
  }

  /// On shutdown, a parked synchronous `WaitForCompletion` join is failed with
  /// `Err(Shutdown)` (the `pending_joins.drain()` arm). Dispatching the wait-join
  /// while still running parks it (its dial produces a `Connect`); the same poll's
  /// shutdown branch then drains it.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_join() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (tx, rx) = oneshot::channel::<Result<usize, Error>>();
    shared.push_command(Command::Join(JoinCmd {
      addrs: vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()],
      wait: true,
      reply: tx,
    }));
    shared.begin_shutdown();
    assert!(
      poll_once(&mut driver).is_ready(),
      "shutdown poll returns Ready"
    );
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked wait-join is failed with Shutdown on driver exit"
    );
  }

  /// On shutdown, a parked application-ping is failed with `Err(Shutdown)` (the
  /// `pending_pings.drain()` arm).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_ping() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (tx, rx) = oneshot::channel::<Result<Duration, Error>>();
    let node = memberlist_proto::Node::new(
      SmolStr::new("peer"),
      "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
    );
    shared.push_command(Command::Ping(PingCmd { node, reply: tx }));
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked ping is failed with Shutdown on driver exit"
    );
  }

  /// On shutdown, a parked reliable directed send is failed with `Err(Shutdown)`
  /// (the `pending_user_sends.drain()` arm).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_reliable_send() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    let (tx, rx) = oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::SendReliable(SendReliableCmd {
      to: "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
      payloads: vec![Bytes::from_static(b"reliable")],
      reply: tx,
    }));
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked reliable send is failed with Shutdown on driver exit"
    );
  }

  /// The shutdown branch's `close_and_drain` loop fails EVERY queued command
  /// variant: a handle that pushed a command in the race window between the
  /// poll's top-of-poll command drain and the shutdown `close_and_drain` gets a
  /// reply (`Err(Shutdown)`, or `Ok(())` for `Shutdown`) instead of hanging.
  ///
  /// That window — between the poll's top-of-poll dispatch and `close_and_drain`
  /// — only exists mid-poll, so the commands must be enqueued concurrently. A
  /// pusher thread bursts all variants the instant a barrier releases, while the
  /// main task polls the already-`begin_shutdown()` driver. A variant whose
  /// `close_and_drain` reply (`Shutdown`) differs from its dispatch reply proves
  /// it reached `close_and_drain`. The window is racy, so each variant is
  /// accumulated across attempts (rotating the push order so none is starved);
  /// the bound fails loudly if any variant is never observed.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn shutdown_close_and_drain_fails_every_queued_command() {
    use std::{sync::Barrier, thread};

    // The five distinguishable variants: dispatched-while-running they reply
    // Ok / a non-Shutdown error, so a Shutdown reply on these proves the command
    // reached `close_and_drain` rather than the top-of-poll dispatch.
    const MAX_ATTEMPTS: usize = 20000;
    // The window is racy, so accumulate per-variant rather than demanding all
    // five in one attempt; the push order is rotated each attempt so no variant
    // is starved by always racing from the same position. The loop breaks as
    // soon as every variant has been observed replying Shutdown.
    let mut seen_join = false;
    let mut seen_user = false;
    let mut seen_comp = false;
    let mut seen_chk = false;
    let mut seen_enc = false;
    for attempt in 0..MAX_ATTEMPTS {
      let (mut driver, _obs_rx, shared, _bytes) = build_driver(64, Some(1 << 20)).await;
      shared.begin_shutdown();

      // Distinguishable replies.
      let (join_tx, join_rx) = oneshot::channel::<Result<usize, Error>>();
      let (user_tx, user_rx) = oneshot::channel::<Result<(), Error>>();
      let (comp_tx, comp_rx) = oneshot::channel::<Result<(), Error>>();
      let (chk_tx, chk_rx) = oneshot::channel::<Result<(), Error>>();
      let (enc_tx, enc_rx) = oneshot::channel::<Result<(), Error>>();
      // The remaining four arms (covered when the window is hit, but their
      // Shutdown / Ok reply is not uniquely attributable to this arm).
      let (leave_tx, _leave_rx) = oneshot::channel::<Result<(), Error>>();
      let (shutdown_tx, _shutdown_rx) = oneshot::channel::<Result<(), Error>>();
      let (ping_tx, _ping_rx) = oneshot::channel::<Result<Duration, Error>>();
      let (rel_tx, _rel_rx) = oneshot::channel::<Result<(), Error>>();

      let to = "127.0.0.1:9".parse::<SocketAddr>().unwrap();
      let node = memberlist_proto::Node::new(SmolStr::new("peer"), to);
      let mut cmds: Vec<Command<SmolStr>> = vec![
        Command::Join(JoinCmd {
          addrs: vec![to],
          wait: false,
          reply: join_tx,
        }),
        Command::SendUser(SendUserCmd {
          to,
          payloads: vec![Bytes::from_static(b"u")],
          reply: user_tx,
        }),
        Command::SetCompressionOptions(SetCompressionOptionsCmd {
          opts: memberlist_proto::CompressionOptions::new(),
          reply: comp_tx,
        }),
        Command::SetChecksumOptions(SetChecksumOptionsCmd {
          opts: memberlist_proto::ChecksumOptions::new(),
          reply: chk_tx,
        }),
        Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
          opts: memberlist_proto::EncryptionOptions::new(),
          reply: enc_tx,
        }),
        Command::Leave(LeaveCmd { reply: leave_tx }),
        Command::Shutdown(ShutdownCmd { reply: shutdown_tx }),
        Command::Ping(PingCmd {
          node,
          reply: ping_tx,
        }),
        Command::SendReliable(SendReliableCmd {
          to,
          payloads: vec![Bytes::from_static(b"r")],
          reply: rel_tx,
        }),
      ];
      // Rotate the push order so each variant races from a different position
      // across attempts rather than always last (which would starve it).
      let rotation = attempt % cmds.len();
      cmds.rotate_left(rotation);

      let barrier = Arc::new(Barrier::new(2));
      let pusher_barrier = barrier.clone();
      let pusher_shared = shared.clone();
      let pusher = thread::spawn(move || {
        pusher_barrier.wait();
        for cmd in cmds {
          // Ignoring bool: a push rejected after the driver closed the queue just
          // means this attempt missed the window; the outer loop retries.
          let _ = pusher_shared.push_command(cmd);
        }
      });

      barrier.wait();
      assert!(
        poll_once(&mut driver).is_ready(),
        "a shutdown poll returns Ready"
      );
      pusher.join().expect("pusher thread joins");

      // Record any variant that `close_and_drain` failed with Shutdown this
      // attempt (a Shutdown reply on these proves the command reached
      // `close_and_drain` rather than the top-of-poll dispatch).
      seen_join |= matches!(join_rx.await, Ok(Err(Error::Shutdown)));
      seen_user |= matches!(user_rx.await, Ok(Err(Error::Shutdown)));
      seen_comp |= matches!(comp_rx.await, Ok(Err(Error::Shutdown)));
      seen_chk |= matches!(chk_rx.await, Ok(Err(Error::Shutdown)));
      seen_enc |= matches!(enc_rx.await, Ok(Err(Error::Shutdown)));
      if seen_join && seen_user && seen_comp && seen_chk && seen_enc {
        break;
      }
    }
    assert!(
      seen_join && seen_user && seen_comp && seen_chk && seen_enc,
      "every queued command variant must reply Shutdown via close_and_drain within \
       {MAX_ATTEMPTS} attempts (join={seen_join} user={seen_user} comp={seen_comp} \
       chk={seen_chk} enc={seen_enc})"
    );
  }

  /// On shutdown, an in-flight graceful leave's waiter(s) resolve with
  /// `Err(Shutdown)` (the `pending_leave.take()` arm).
  ///
  /// The shutdown branch itself calls `endpoint.leave()` then `drain_surfaces`,
  /// and a no-peer leave emits `LeftCluster` within that same drain — which would
  /// resolve a parked leave with `Ok(())` before the shutdown arm runs. To isolate
  /// the shutdown arm, the endpoint is first driven to fully `Left` (a prior poll
  /// consumes its `LeftCluster`), so the shutdown's own `leave()` is an idempotent
  /// no-op emitting nothing; the freshly seeded `pending_leave` then survives the
  /// drain and is failed with `Shutdown`.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn shutdown_fails_parked_leave() {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
    // Drive the endpoint to Left first: a real leave whose LeftCluster is consumed
    // by this poll (resolving the dispatched waiter with Ok, then removed).
    let (warm_tx, _warm_rx) = oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::Leave(LeaveCmd { reply: warm_tx }));
    assert!(
      poll_once(&mut driver).is_pending(),
      "warm-up poll keeps running"
    );
    assert!(
      driver.pending_leave.is_none(),
      "the no-peer leave completed within the warm-up poll"
    );
    assert!(
      !driver.endpoint.is_running(),
      "the endpoint is now Left, so the shutdown leave() is a no-op"
    );

    // Now seed a fresh parked leave and shut down: the no-op leave() emits no
    // LeftCluster, so this waiter survives the drain and hits the shutdown arm.
    let (tx, rx) = oneshot::channel::<Result<(), Error>>();
    driver.pending_leave = Some(PendingLeave { repliers: vec![tx] });
    shared.begin_shutdown();
    assert!(poll_once(&mut driver).is_ready());
    assert!(
      matches!(rx.await, Ok(Err(Error::Shutdown))),
      "a parked leave waiter is failed with Shutdown on driver exit"
    );
  }

  /// The reliable-send / join capture keys on the originating `StreamId`, NOT
  /// the peer. Regression guard for the cross-subsystem misattribution: a
  /// same-peer dial flushed by the shared `service_dials` for another subsystem
  /// (here a `start_push_pull`) is left pending while a `send_reliable`-style
  /// `start_user_message` runs; the command must capture ONLY its own exchange.
  /// If capture matched by peer (the bug), the foreign push/pull's `ExchangeId`
  /// would be captured too — `m > n` (`checked_sub` underflow) or a waiter parked
  /// on a PushPull completion the `UserMessage` resolver ignores (leaked waiter).
  #[test]
  fn capture_binds_to_started_stream_id_not_peer() {
    let mut endpoint = capture_test_endpoint();
    let now = Instant::now();
    let peer = "127.0.0.1:7000".parse::<SocketAddr>().unwrap();

    // An UNRELATED same-peer dial enqueued by another subsystem; its Connect is
    // already queued by the in-band `service_dials` and deliberately left
    // undrained — the exact shared-deque hazard `send_many_reliable` faces.
    let foreign_sid = endpoint.start_push_pull(peer, PushPullKind::Join, now);

    // This command's own dial to the SAME peer; capture is keyed on `started`.
    let mut started: HashSet<StreamId> = HashSet::new();
    let user_sid = endpoint.start_user_message(peer, Bytes::from_static(b"hi"), now);
    started.insert(user_sid);
    assert_ne!(
      foreign_sid, user_sid,
      "distinct dials get distinct StreamIds"
    );

    // Drain every queued action, replicating the dispatch loop's inline capture
    // predicate exactly. Both Connects surface; only the one in `started` is
    // captured.
    let mut captured: HashSet<ExchangeId> = HashSet::new();
    let mut connect_count = 0usize;
    let mut foreign_eid = None;
    let mut user_eid = None;
    while let Some(action) = endpoint.poll_action() {
      if let StreamAction::Connect(ref info) = action {
        connect_count += 1;
        if info.stream_id() == foreign_sid {
          foreign_eid = Some(info.id());
        }
        if info.stream_id() == user_sid {
          user_eid = Some(info.id());
        }
        if started.contains(&info.stream_id()) {
          captured.insert(info.id());
        }
      }
    }

    assert_eq!(
      connect_count, 2,
      "both same-peer dials surfaced a Connect in this drain",
    );
    let foreign_eid = foreign_eid.expect("foreign push/pull surfaced a Connect");
    let user_eid = user_eid.expect("user-message surfaced a Connect");
    assert_eq!(
      captured.len(),
      1,
      "exactly this command's one exchange was captured (m <= n holds)",
    );
    assert!(
      captured.contains(&user_eid),
      "the user-message exchange was captured",
    );
    assert!(
      !captured.contains(&foreign_eid),
      "the foreign same-peer push/pull exchange was NOT captured",
    );
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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    let stale = b"stale-bytes-that-must-not-reach-the-wire".to_vec();

    // Queue stale bytes and pre-arm the explicit abort BEFORE the task runs, so
    // the bridge's first write race sees a ready cancel and breaks without
    // writing. Keep `out_tx` alive: the abort send is the sole teardown signal.
    out_tx
      .send(BridgeOut::Data(Bytes::from(stale)))
      .expect("queue stale bytes");
    cancel_tx.send(()).expect("signal explicit abort");
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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
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

  /// `BridgeOut::ShutdownWrite` (the machine's `StreamAction::Shutdown`,
  /// half-closing the write side after the send half retires) closes the bridge's
  /// write half — the peer reads EOF on its read side — while the bridge stays
  /// alive for the still-open read direction.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn bridge_shutdown_write_half_closes_write_side() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      shared,
      Duration::from_secs(60),
    ));

    // Half-close the bridge's write side (FIN). The peer's read side sees EOF.
    out_tx
      .send(BridgeOut::ShutdownWrite)
      .expect("queue write half-close");
    let tail = client
      .read_to_end(&mut Vec::new())
      .await
      .expect("peer reads its read side to EOF after the bridge FIN");
    assert_eq!(tail, 0, "the write half-close delivers EOF with no bytes");

    // The bridge is still alive (read side open); dropping the handle tears it
    // down cleanly.
    drop((out_tx, cancel_tx));
    bridge.await.expect("bridge task exits cleanly");
  }

  /// A bridge write that fails (the peer dropped its whole socket, so the write
  /// gets a broken pipe / connection reset) tears the bridge down rather than
  /// spinning — `write_cancellable` returns the tear-down signal on a write error.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn bridge_write_error_tears_down() {
    let (server, client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
    let shared = test_shared();

    // Drop the peer entirely: its socket is gone (RST on subsequent writes).
    drop(client);

    let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      shared,
      Duration::from_secs(60),
    ));

    // Keep the handle alive and keep queuing writes: the bridge's read side first
    // sees EOF (peer gone) and enters read-EOF mode, then a queued write to the
    // dead peer eventually errors, returning the tear-down signal. The bridge must
    // exit on its own (broken socket), NOT hang, even though the handle is held.
    let _out_tx_kept = out_tx.clone();
    let _cancel_kept = cancel_tx;
    for _ in 0..64 {
      // Ignoring Err: once the bridge tears down, out_rx disconnects; the test's
      // assertion is that the bridge EXITS, which the timeout below enforces.
      if out_tx
        .send(BridgeOut::Data(Bytes::from(vec![0u8; 64 * 1024])))
        .is_err()
      {
        break;
      }
    }

    tokio::time::timeout(Duration::from_secs(10), bridge)
      .await
      .expect("bridge tears down on a write error to a dropped peer, not hang")
      .expect("bridge task exits cleanly");
  }
}
