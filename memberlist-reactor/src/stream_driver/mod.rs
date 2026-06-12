//! The TCP backend driver: a quinn-style `Future::poll` pump that owns a
//! [`StreamEndpoint`] over plain-TCP records, its gossip UDP socket, and the
//! periodic schedulers. Per-exchange reliable TCP I/O runs in spawned bridge
//! tasks (one per exchange) wired to the pump by channels; inbound connections
//! are accepted by a dedicated task (the listener's `accept` is async-only).

use std::{
  borrow::Cow,
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
  AsyncSpawner, Runtime,
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
use memberlist_proto::{
  ChecksumOptions, CompressionOptions, EncryptionOptions, Instant, PingId,
  codec::{
    DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
    parse_messages,
  },
  event::{Event, ExchangeKind, ExchangeOutcome, PushPullKind, StreamId, Transmit},
  streams::{
    ExchangeId, StreamAction, StreamEndpoint, StreamTransport, checksum_gossip_datagram,
    compress_gossip_datagram, encrypt_gossip_datagram,
  },
  typed::Message,
  unwrap_transforms_with_encryption,
};

use crate::{
  NodeId,
  cidr::{CidrFilter, cidr_blocks},
  command::{
    Command, JoinCmd, LeaveCmd, PingCmd, QueueUserBroadcastCmd, SendReliableCmd, SendUserCmd,
    SetAckPayloadCmd, SetChecksumOptionsCmd, SetCompressionOptionsCmd, SetEncryptionOptionsCmd,
    SetLocalStateCmd, ShutdownCmd, UpdateNodeMetadataCmd,
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

/// Max raw inbound datagrams buffered in the endpoint's `mem_ingress` before the
/// recv loop stops reading the socket (a memory-DoS backstop).
const MAX_BUFFERED_INGRESS: usize = 1024;

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
/// channel. A graceful `StreamAction::Close` drops the whole handle: `out_tx`
/// disconnects, and the bridge tears down after draining the `BridgeOut::Data` it
/// already queued. A failed `StreamAction::Abort` instead sends `()` on
/// `cancel_tx` first, which preempts the bridge — even a write stalled on a peer
/// that stopped reading — and discards the queued bytes.
///
/// Driver shutdown sends `cancel_tx.send(())` on every live handle (preempting
/// even a stalled write) and then drops the handle; it does NOT await the bridge
/// task. The preempted bridge tears down promptly on its own, so shutdown is not
/// blocked on it.
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
  /// The peer cleanly closed its write side (transport `read == 0`).
  Eof(BridgeEof),
  /// A transport READ ERROR (not a clean EOF). Routed to
  /// `handle_transport_error` so a one-way UserMessage is NOT falsely completed
  /// as success by the benign-EOF path.
  Error(BridgeEof),
}

/// Payload of [`BridgeInbound::Data`].
struct BridgeData {
  eid: ExchangeId,
  bytes: Vec<u8>,
  at: Instant,
}

/// Payload of [`BridgeInbound::Eof`] and [`BridgeInbound::Error`].
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

/// The gossip transform context: the wire-transform options used to unwrap
/// inbound and wrap outbound gossip datagrams inline on the pump. Rebuilt
/// whenever the runtime options change ([`StreamDriver::rebuild_transform`]).
struct TransformCtx {
  compression: CompressionOptions,
  checksum: ChecksumOptions,
  encryption: EncryptionOptions,
  gossip_mtu: usize,
  label: Option<Bytes>,
}

/// Transform one inbound gossip datagram: decrypt + decompress (one pass) →
/// strip label → parse. Returns the source and the parsed messages — empty on
/// any malformed/undecryptable datagram (gossip is lossy; the driver drops it).
/// Runs inline on the pump (the transform is microsecond-scale per datagram).
fn transform_ingress<I: NodeId>(
  ctx: &TransformCtx,
  from: SocketAddr,
  raw: Bytes,
) -> (SocketAddr, Vec<Message<I, SocketAddr>>) {
  // The only `Cow::Borrowed` result is the no-transform passthrough (the whole
  // input unchanged), so reuse the input `Bytes` instead of copying it — this is
  // the common plain-gossip path. A real transform yields `Owned`.
  let transformed = match unwrap_transforms_with_encryption(&raw, ctx.gossip_mtu, &ctx.encryption) {
    Ok(Cow::Borrowed(_)) => None,
    Ok(Cow::Owned(v)) => Some(Bytes::from(v)),
    Err(_) => return (from, Vec::new()),
  };
  let plain = transformed.unwrap_or(raw);
  let inner = match decode_incoming(plain, &DecodeOptions::new(ctx.label.clone())) {
    Ok(b) => b,
    Err(_) => return (from, Vec::new()),
  };
  match parse_messages::<I, SocketAddr>(inner) {
    Ok(msgs) => (from, msgs),
    Err(_) => (from, Vec::new()),
  }
}

/// Transform one outbound gossip [`Transmit`]: encode (plain or compound) →
/// compress → checksum → encrypt. Returns the peer and on-wire bytes, or `None`
/// when encoding fails or a configured checksum/encryption backend is missing
/// (the driver drops the datagram). Pure and `Send`.
fn transform_egress<I: NodeId>(
  ctx: &TransformCtx,
  transmit: Transmit<I, SocketAddr>,
) -> Option<(SocketAddr, Vec<u8>)> {
  let encode_opts = EncodeOptions::new(ctx.label.clone());
  let (peer, plain) = match transmit {
    Transmit::Packet(pkt) => {
      let (to, msg) = pkt.into_parts();
      (to, encode_outgoing(&msg, &encode_opts).ok()?)
    }
    Transmit::Compound(cmp) => {
      let (to, msgs) = cmp.into_parts();
      (to, encode_outgoing_compound(&msgs, &encode_opts).ok()?)
    }
  };
  let compressed = compress_gossip_datagram(&ctx.compression, &plain);
  let checksummed = checksum_gossip_datagram(&ctx.checksum, &compressed).ok()?;
  let on_wire = encrypt_gossip_datagram(&ctx.encryption, &checksummed).ok()?;
  Some((peer, on_wire))
}

/// Build the transform context from the endpoint's current options.
fn build_transform<I: NodeId, T: StreamTransport>(
  endpoint: &StreamEndpoint<I, SocketAddr, T>,
  label: &Option<Bytes>,
) -> Arc<TransformCtx> {
  Arc::new(TransformCtx {
    compression: endpoint.compression(),
    checksum: endpoint.checksum(),
    encryption: endpoint.encryption_options().clone(),
    gossip_mtu: endpoint.gossip_mtu(),
    label: label.clone(),
  })
}

/// The single-owner TCP driver future. Runs until shutdown (the last handle
/// dropped, or a `Shutdown` command).
pub(crate) struct StreamDriver<I: NodeId, R: Runtime, T: StreamTransport> {
  endpoint: StreamEndpoint<I, SocketAddr, T>,
  /// Unreliable gossip datagrams (the reliable exchanges run over TCP bridges).
  /// Wrapped in `Option` so the shutdown branch can drop it (releasing the bound
  /// UDP port) BEFORE acking the shutdown caller; it is `Some` for the whole
  /// running lifetime and only taken during teardown.
  socket: Option<<R::Net as Net>::UdpSocket>,
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
  /// The parked replies of `Shutdown` commands. A reply is NOT sent inline at
  /// dispatch: every caller is parked here and acked only after the shutdown
  /// branch drops the gossip socket and the accept task's listener, so the bound
  /// ports are free when each caller resumes from `shutdown().await` and an
  /// immediate rebind on the same address succeeds. A `Vec` because several
  /// callers can race `shutdown()` concurrently — each must get its own ack, and
  /// none before teardown.
  shutdown_reply: Vec<oneshot::Sender<Result<(), Error>>>,
  /// Held only to be dropped on driver exit; closing the accept task's shutdown
  /// channel cancels its pending accept() so the listener is released at once.
  /// Wrapped in `Option` so the shutdown branch can drop it (cancelling the
  /// accept task and releasing the TCP listener) BEFORE acking the caller.
  accept_shutdown_tx: Option<Sender<()>>,
  /// Join handle of the accept task. The shutdown branch signals that task (by
  /// dropping `accept_shutdown_tx`) and then polls this handle to completion
  /// before acking the caller: the listener FD lives in the accept task's
  /// `listener` local and is released only when that task actually exits, so the
  /// driver must AWAIT the exit — not merely signal it — or a same-address rebind
  /// after `shutdown().await` can race the still-open listener (`AddrInUse`).
  /// Taken on the first shutdown poll and polled across subsequent polls.
  accept_join: Option<<R::Spawner as AsyncSpawner>::JoinHandle<()>>,
  /// Inbound connections from the accept task.
  accepted_rx: Receiver<(<R::Net as Net>::TcpStream, SocketAddr)>,
  /// Inbound transport bytes/EOF from the bridge read tasks. Wrapped in `Option`
  /// so the shutdown branch can drop it: that disconnects every live bridge's
  /// shared `inbound_tx`, so a bridge parked on the bounded inbound hand-off send
  /// wakes with a disconnect error and exits promptly. `Some` for the whole
  /// running lifetime; taken once during teardown.
  inbound_rx: Option<Receiver<BridgeInbound>>,
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
  /// CIDR transport filter: a gossip datagram from a blocked source IP (recv) or
  /// a reliable connection from a blocked peer IP (accept) is dropped before the
  /// machine sees it. `()` when the `cidr` feature is off. The same policy also
  /// gates membership admission via the composed alive delegate.
  cidr_policy: CidrFilter,
  /// The gossip transform context used to unwrap inbound (and wrap outbound)
  /// gossip datagrams inline on the pump. Rebuilt on a runtime options change.
  transform: Arc<TransformCtx>,
  /// The endpoint snapshot version last published to `shared`. The snapshot is
  /// rebuilt + republished only when `endpoint.snapshot_version()` differs from
  /// this, not on every productive poll (rebuilding clones every `NodeState`).
  last_snapshot_version: u64,
  /// The load-shedding counters last published to `shared`; republished only when
  /// `endpoint.metrics()` differs from this.
  last_metrics: memberlist_proto::metrics::Metrics,
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
    accept_join: <R::Spawner as AsyncSpawner>::JoinHandle<()>,
    close_timeout: Duration,
    label: Option<bytes::Bytes>,
    cidr_policy: CidrFilter,
  ) -> Self {
    let buf_len = endpoint
      .gossip_mtu()
      .saturating_add(memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD)
      .saturating_add(memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD)
      .min(GOSSIP_RECV_BUF_MAX);
    let (inbound_tx, inbound_rx) = flume::bounded(BRIDGE_INBOUND_CAP);
    let (dial_tx, dial_rx) = flume::unbounded();
    let transform = build_transform(&endpoint, &label);
    // `shared` already holds the initial snapshot (published at its construction
    // with the endpoint at this version), so start in sync — the first poll
    // republishes only after a real membership/health change.
    let last_snapshot_version = endpoint.endpoint_ref().snapshot_version();
    Self {
      endpoint,
      socket: Some(socket),
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
      shutdown_reply: Vec::new(),
      accept_shutdown_tx: Some(accept_shutdown_tx),
      accept_join: Some(accept_join),
      accepted_rx,
      inbound_rx: Some(inbound_rx),
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
      cidr_policy,
      transform,
      last_snapshot_version,
      last_metrics: memberlist_proto::metrics::Metrics::default(),
    }
  }

  /// Rebuild the cached transform context after a runtime options change
  /// (`Set{Compression,Checksum,Encryption}Options`), so the inline transform
  /// path observes the new options.
  fn rebuild_transform(&mut self) {
    self.transform = build_transform(&self.endpoint, &self.label);
  }

  /// Spawns the per-exchange bridge task that moves bytes between `stream` and
  /// the pump for `eid`. The task is detached: shutdown preempts a live bridge by
  /// sending its `cancel_tx` (see [`BridgeHandle`]) rather than awaiting its exit.
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
        // Do NOT ack inline: the gossip socket and the TCP listener are still
        // bound here. Flag shutdown and park the reply; the shutdown branch acks
        // every parked caller only AFTER it drops both, so an immediate rebind on
        // the same address after `shutdown().await` succeeds.
        self.shared.begin_shutdown();
        self.shutdown_reply.push(reply);
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
        let ping_id = self.endpoint.ping(node, now).expect("issued while running");
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
        if cidr_blocks(&self.cidr_policy, to.ip()) {
          // Our own policy excludes the destination: do not emit an unreliable
          // user datagram to a blocked peer (the reliable plane's outbound dial
          // is gated at the Connect handler; this is the unreliable counterpart).
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err(Error::SendFailed));
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
          let sid = self
            .endpoint
            .start_user_message(to, payload, now)
            .expect("issued while running");
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
          self.rebuild_transform();
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
          match self.endpoint.set_checksum_options(opts) {
            Ok(()) => {
              self.rebuild_transform();
              Ok(())
            }
            Err(e) => Err(Error::Checksum(e)),
          }
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
              // The endpoint clears its synchronous `mem_ingress` on a key change,
              // and every inbound transform runs inline on the pump with the
              // current key, so a rotation cannot leave a datagram to be decrypted
              // under the old key after the switch.
              self.endpoint.set_encryption_options(opts);
              self.rebuild_transform();
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
      Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { meta, reply }) => {
        // Gate on a running node: after `leave()` the schedulers are stopped, so
        // a metadata change could never be gossiped. Build the validated `Meta`
        // (rejecting an over-cap value) before applying.
        let res = if self.endpoint.is_running() {
          match memberlist_proto::typed::Meta::try_from(meta) {
            Ok(m) => self
              .endpoint
              .update_meta(m)
              .map_err(|e| Error::PayloadTooLarge(e.to_string())),
            Err(e) => Err(Error::PayloadTooLarge(e.to_string())),
          }
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::QueueUserBroadcast(QueueUserBroadcastCmd { data, reply }) => {
        // Gate on a running node FIRST: after `leave()` the gossip scheduler is
        // stopped, so the broadcast would never drain. The machine setter rejects
        // an over-MTU lone datagram without storing it.
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .queue_user_broadcast(data)
            .map_err(|e| Error::PayloadTooLarge(e.to_string()))
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetLocalState(SetLocalStateCmd { state, reply }) => {
        // Gate on a running node FIRST: after `leave()` no push/pull exchange
        // will carry the snapshot. The machine setter rejects a snapshot whose
        // framed PushPull exceeds the stream frame budget.
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .set_local_state_snapshot(state)
            .map_err(|e| Error::PayloadTooLarge(e.to_string()))
        } else {
          Err(Error::NotRunning)
        };
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(res);
      }
      Command::SetAckPayload(SetAckPayloadCmd { payload, reply }) => {
        // Gate on a running node FIRST: after `leave()` no probe ack will carry
        // the payload. The machine setter rejects an over-budget ack without
        // storing it (an over-budget ack always fails to send, so a probing peer
        // would otherwise falsely suspect this node).
        let res = if self.endpoint.is_running() {
          self
            .endpoint
            .set_ack_payload(payload)
            .map_err(|e| Error::PayloadTooLarge(e.to_string()))
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
        // Reject an outbound dial to a CIDR-blocked peer at the transport boundary:
        // terminalize as a dial failure (no bridge, no dial spawned) so a join
        // toward a blocked seed fails rather than completing the push/pull —
        // matching the inbound accept guard and the QUIC source filter.
        if cidr_blocks(&self.cidr_policy, peer.ip()) {
          self.endpoint.handle_dial_failed(eid, Instant::now());
          return;
        }
        let (out_tx, out_rx) = flume::unbounded();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        // No bridge task yet — the dial owns the connecting FD until it completes,
        // at which point `DialOutcome::Connected` spawns the bridge.
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
    let now = Instant::now();
    let budget = self.transmit_batch.max(1);
    let mut worked = false;
    let mut more = false;

    // Inbound gossip: decrypt + decompress + strip-label + parse, inline on the
    // pump. The transform is microsecond-scale per datagram, so even a full batch
    // of near-MTU encrypted datagrams is a sub-millisecond pass — negligible
    // against the probe/suspicion deadlines — and the latency-critical Ping/Ack
    // path is a small standalone datagram. The parsed messages reach the
    // single-owner machine HERE, on the pump, with their arrival-time `now`.
    let mut ingress = 0;
    while ingress < budget {
      let Some((from, raw)) = self.endpoint.poll_memberlist_ingress() else {
        break;
      };
      ingress += 1;
      let (from, msgs) = transform_ingress::<I>(&self.transform, from, raw);
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

    // Outbound gossip: encode (plain or compound) + compress + checksum +
    // encrypt, then send. Popping the last transmit is the endpoint's
    // leave-completion fence (it emits `LeftCluster`, which resolves `leave()`),
    // so the leave/shutdown datagrams must reach the socket synchronously, before
    // that fence fires. Egress is bounded fanout, so the transform is not a
    // throughput concern.
    let mut sent = 0;
    while sent < budget {
      let Some(transmit) = self.endpoint.poll_memberlist_transmit() else {
        break;
      };
      sent += 1;
      if let Some((peer, on_wire)) = transform_egress::<I>(&self.transform, transmit)
        && let Some(socket) = self.socket.as_ref()
      {
        // Ignoring Poll: gossip is best-effort — a full or errored UDP send drops
        // the datagram and SWIM recovers on the next round.
        let _ = socket.poll_send_to(cx, &on_wire, peer);
      }
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

    // Shutdown: best-effort leave, flush once, fail any parked waiters, preempt
    // every in-flight reliable exchange, then await ONLY the accept task's exit
    // before acking the caller. The completion latch promises the bind address is
    // free — the UDP gossip socket and the TCP listener — not that every connected
    // stream FD has closed. Dropping `this` at the very end drops the dial channel
    // ends, so any dial task observes its send/recv error and exits.
    //
    // The teardown is re-entrant: the one-time work (leave/flush/fail-waiters,
    // dropping the gossip socket, signalling the accept task, and preempting the
    // bridges) runs exactly once, guarded on `accept_shutdown_tx` still being
    // `Some`; every shutdown poll then polls the retained `accept_join`. The accept
    // task owns the TCP listener (its `listener` local), so the FD is released only
    // when that task actually EXITS — not when it is merely signalled. Polling the
    // join handle here registers the waker, so the driver re-polls when the accept
    // task finishes; the stashed reply fires only after that, so a same-address
    // rebind resuming from `shutdown().await` cannot race a still-open listener.
    // The bridge tasks are preempted (each `cancel_tx` fired) but NOT awaited, so
    // an exchange stalled on the bounded inbound hand-off cannot wedge shutdown.
    if this.shared.is_shutdown() {
      if this.accept_shutdown_tx.is_some() {
        // One-time teardown: runs on the first shutdown poll only (the
        // `accept_shutdown_tx.take()` below clears this guard).
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
              // A straggler `Shutdown` racing the first one: park it too, so it is
              // acked after the socket/listener drop like every other caller. Never
              // ack inline here — the listener is still bound, so an inline ack
              // would let that caller rebind into a still-open port.
              this.shutdown_reply.push(reply);
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
            Command::UpdateNodeMetadata(UpdateNodeMetadataCmd { reply, .. }) => {
              // Ignoring Err: the caller dropped its reply receiver.
              let _ = reply.send(Err(Error::Shutdown));
            }
            Command::QueueUserBroadcast(QueueUserBroadcastCmd { reply, .. }) => {
              // Ignoring Err: the caller dropped its reply receiver.
              let _ = reply.send(Err(Error::Shutdown));
            }
            Command::SetLocalState(SetLocalStateCmd { reply, .. }) => {
              // Ignoring Err: the caller dropped its reply receiver.
              let _ = reply.send(Err(Error::Shutdown));
            }
            Command::SetAckPayload(SetAckPayloadCmd { reply, .. }) => {
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
        // Signal the accept task and release the gossip socket. Dropping
        // `accept_shutdown_tx` cancels the accept task's pending `accept()` so it
        // breaks its loop and drops the `listener` local — but that drop happens
        // when the task is next scheduled, NOT here, which is why the join handle
        // is polled below before the ack. Dropping the gossip socket closes its
        // UDP FD synchronously (the agnostic `UdpSocket` has no async `close`, so
        // the local going out of scope here closes the FD). Taking
        // `accept_shutdown_tx` also clears the one-time guard.
        drop(this.accept_shutdown_tx.take());
        drop(this.socket.take());
        // Preempt every in-flight reliable exchange WITHOUT awaiting it. The
        // completion latch promises only the bind address is free, not that every
        // connected stream FD has closed, so shutdown does not block on the bridge
        // tasks — a bridge parked on the bounded inbound hand-off send is not
        // cancellable, and awaiting its join would hang shutdown under inbound
        // backpressure. Sending `cancel_tx` preempts each ACTIVE bridge even
        // mid-write on a stalled peer, so its established stream closes at once; an
        // exchange already past `StreamAction::Close` (handle removed, draining its
        // tail) is not in `bridges` — its connected stream FD closes on its own,
        // with a non-reading peer dropped after `close_timeout` of no write
        // progress (an idle bound, not a total one). Dropping the handle also
        // disconnects `out_tx`. A dialing
        // exchange has no bridge task yet (its connecting FD lives in the dial
        // task, bounded by `DIAL_TIMEOUT`). Dropping `inbound_rx` then disconnects
        // every bridge's shared `inbound_tx`, so a bridge parked on a full inbound
        // hand-off send wakes with a disconnect error and exits promptly instead of
        // lingering until this driver future is finally dropped.
        for (_, handle) in this.bridges.drain() {
          // Ignoring Err: the bridge may have already exited (cancel receiver
          // gone); the preemption is best-effort.
          let _ = handle.cancel_tx.send(());
        }
        drop(this.inbound_rx.take());
      }

      // Await the accept task's exit before acking, so the TCP listener FD is
      // actually released (not merely signalled). Polling the handle registers the
      // waker; the driver re-polls once the accept task finishes. The bridge tasks
      // were preempted in the one-time block above and are NOT awaited here.
      if let Some(join) = this.accept_join.as_mut() {
        // Drive the handle to completion; its `Result<(), JoinError>` carries no
        // value the driver needs (the accept task returns `()` and a join error
        // only means the task was cancelled/panicked, which is still a terminal
        // exit that has dropped the listener).
        // Ignoring Ok/Err: only the readiness matters — the listener is released
        // once the task has exited, regardless of how.
        if join.poll_unpin(cx).is_pending() {
          return Poll::Pending;
        }
        this.accept_join = None;
      }
      // The accept task has exited, releasing the listener; the gossip socket was
      // dropped above. The bind address is now free, so a caller resuming from
      // `shutdown().await` can immediately rebind on the same address. Ack the
      // stashed reply and stop.
      for reply in this.shutdown_reply.drain(..) {
        // Ignoring Err: the caller dropped its reply receiver.
        let _ = reply.send(Ok(()));
      }
      // The bind address is now free; release any late `shutdown()` caller that
      // found the command queue already closed and parked on the completion latch.
      this.shared.mark_shutdown_complete();
      return Poll::Ready(());
    }

    // Receive gossip (bounded by recv_batch; a full batch means more may be
    // waiting). The socket is always `Some` here — the shutdown branch above
    // (which takes it) returned `Poll::Ready` before reaching this point.
    //
    // The gate is the RAW BUFFER depth (`mem_ingress`): recv stops reading the
    // socket once the buffer is full, so it only grows if recv truly outruns the
    // pump, which this cap bounds (a memory-DoS backstop). The kernel buffer
    // absorbs and lossily drops the overflow; gossip self-heals.
    // Operators expecting sustained high gossip rates (large clusters, fast
    // gossip_interval) should raise the OS receive buffer (SO_RCVBUF) on the
    // gossip socket so the kernel absorbs a deeper burst before dropping.
    let mut recv_n = 0;
    let mut recv_gated = false;
    while recv_n < this.recv_batch {
      if this.endpoint.pending_memberlist_ingress() >= MAX_BUFFERED_INGRESS {
        // Gated WITHOUT polling the socket, so NO socket waker is registered
        // this pass. The flag forces a self-wake after the ingress drain below
        // frees buffer space (the drain always makes inline progress), so the
        // next pass re-polls the socket instead of parking until the idle
        // timer with readable packets queued in the kernel.
        recv_gated = true;
        break;
      }
      let Some(socket) = this.socket.as_ref() else {
        break;
      };
      match socket.poll_recv_from(cx, &mut this.recv_buf) {
        Poll::Ready(Ok((n, src))) => {
          // Drop a gossip datagram from a CIDR-blocked source before the machine
          // sees it (the transport-source filter; the advertised-address filter
          // is the composed alive delegate). The recv still counts toward the batch.
          if !cidr_blocks(&this.cidr_policy, src.ip()) {
            this.endpoint.handle_gossip(src, &this.recv_buf[..n], now);
          }
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
    more |= recv_gated;
    if recv_n == this.recv_batch {
      more = true;
    }

    // Accept inbound connections: register each with the machine and bridge it.
    // Aux tasks wake the driver after enqueueing, so try_recv (no waker
    // registration) is sufficient and does not drop a waker each poll.
    while let Ok((stream, peer)) = this.accepted_rx.try_recv() {
      // Reject a reliable connection from a CIDR-blocked peer: drop the stream
      // without registering it, so the push/pull exchange never starts (the
      // advertised-address filter is the composed alive delegate).
      if cidr_blocks(&this.cidr_policy, peer.ip()) {
        drop(stream);
        progress = true;
        continue;
      }
      let Some(eid) = this.endpoint.accept_connection(peer, now) else {
        // Not admitted (leaving, the inbound-stream cap is reached, or a
        // record-layer config error): no bridge exists, so drop the accepted
        // stream immediately rather than spawn a servicing task for a connection
        // the machine will never feed.
        drop(stream);
        progress = true;
        continue;
      };
      let (out_tx, out_rx) = flume::unbounded();
      let (cancel_tx, cancel_rx) = oneshot::channel();
      this.spawn_bridge(eid, stream, out_rx, cancel_rx);
      this.bridges.insert(eid, BridgeHandle { out_tx, cancel_tx });
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
          // the stream and channels rather than bridge a dead exchange. Otherwise
          // spawn the bridge for the existing handle.
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
    // for fairness; a full batch self-wakes via `more`). Always `Some` here — the
    // shutdown branch above (which takes it) returned before reaching this point.
    let mut inbound_n = 0;
    while inbound_n < this.recv_batch {
      let Some(inbound_rx) = this.inbound_rx.as_ref() else {
        break;
      };
      let Ok(msg) = inbound_rx.try_recv() else {
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
        BridgeInbound::Error(BridgeEof { eid, at }) => {
          this.endpoint.handle_transport_error(eid, at);
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

    // Republish the snapshot only when membership/health actually changed —
    // rebuilding clones every NodeState, so doing it on every productive poll is
    // wasted under steady traffic.
    let snap_v = this.endpoint.endpoint_ref().snapshot_version();
    if progress && snap_v != this.last_snapshot_version {
      this.last_snapshot_version = snap_v;
      this
        .shared
        .publish(snapshot_of(this.endpoint.endpoint_ref()));
    }
    // Republish the load-shedding counters only when they change (a cheap u64
    // compare; the publish allocates only on a real change, which is rare).
    let metrics = this.endpoint.endpoint_ref().metrics();
    if metrics != this.last_metrics {
      this.last_metrics = metrics;
      this.shared.publish_metrics(metrics);
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
          // Bounded channel: await space (accept backpressure). Race the send
          // against shutdown so a full queue at shutdown cannot wedge the task —
          // the driver stops draining `accepted_rx` once teardown begins and then
          // awaits this task's exit, so an un-cancellable send would deadlock.
          select! {
            res = accepted_tx.send_async((stream, peer)).fuse() => {
              if res.is_err() {
                break;
              }
              shared.wake_driver();
            }
            _ = shutdown_rx.recv_async().fuse() => break,
          }
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
        // A clean `read == 0` (peer half-closed) is a benign EOF anchor; a read
        // ERROR is a transport failure and must NOT take the benign-EOF path (it
        // would falsely complete a one-way UserMessage as success). Both stop
        // this task's reads.
        Ok(0) | Err(_) => {
          // Timestamp at read completion, before any send backpressure.
          let payload = BridgeEof { eid, at: Instant::now() };
          let msg = if read.is_err() {
            BridgeInbound::Error(payload)
          } else {
            BridgeInbound::Eof(payload)
          };
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
mod tests;
