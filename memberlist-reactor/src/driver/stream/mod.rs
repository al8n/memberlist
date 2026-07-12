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
#[cfg(checksum)]
use memberlist_proto::ChecksumOptions;
#[cfg(compression)]
use memberlist_proto::CompressionOptions;
#[cfg(encryption)]
use memberlist_proto::EncryptionOptions;
#[cfg(checksum)]
use memberlist_proto::streams::checksum_gossip_datagram;
#[cfg(compression)]
use memberlist_proto::streams::compress_gossip_datagram;
#[cfg(encryption)]
use memberlist_proto::streams::encrypt_gossip_datagram;
// `unwrap_transforms_with_encryption` strips the outer `Encrypted` wrapper via
// the configured keyring, so it exists only under an encryption backend; with
// none the base `unwrap_transforms` strips the remaining (checksum/compression)
// wrappers.
#[cfg(not(encryption))]
use memberlist_proto::unwrap_transforms;
#[cfg(encryption)]
use memberlist_proto::unwrap_transforms_with_encryption;
use memberlist_proto::{
  Instant, PingId,
  codec::{
    DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing, encode_outgoing_compound,
    parse_messages,
  },
  event::{Event, ExchangeKind, ExchangeStatus, PushPullKind, StreamId, Transmit},
  streams::{ExchangeId, StreamAction, StreamTransport},
  typed::Message,
};

#[cfg(checksum)]
use crate::command::SetChecksumOptionsCmd;
#[cfg(compression)]
use crate::command::SetCompressionOptionsCmd;
#[cfg(encryption)]
use crate::command::SetEncryptionOptionsCmd;
use crate::{
  NodeId, StreamEndpoint,
  cidr::{CidrFilter, cidr_blocks},
  command::{
    Command, JoinCmd, JoinReply, LeaveCmd, PingCmd, QueueUserBroadcastCmd, SendReliableCmd,
    SendUserCmd, SetAckPayloadCmd, SetLocalStateCmd, ShutdownCmd, UpdateNodeMetadataCmd,
  },
  driver::join_reply,
  error::{Error, JoinFailed},
  observation::observation_payload_bytes,
  shared::Shared,
  snapshot::snapshot_of,
};
use memberlist_proto::metrics::Metrics;
use smallvec::SmallVec;

/// IP-layer UDP payload maximum; caps the per-recv gossip buffer.
const GOSSIP_RECV_BUF_MAX: usize = 65507;

/// Wall-clock bound on deferring a DUE `handle_timeout` behind capped inbound
/// paths. Inbound drains roughly FIFO, so the pre-deadline backlog clears
/// within a bounded number of capped polls (microseconds at gossip datagram
/// sizes) — the grace exists so a sustained external flood that keeps every
/// poll capped cannot suppress failure detection, while a burst that merely
/// truncated one batch never prematurely times out the input it buffered.
const TIMEOUT_STALENESS_GRACE: core::time::Duration = core::time::Duration::from_millis(5);

/// The largest the encrypted wrapper can inflate a gossip datagram, or `0` when
/// no encryption backend is built in. The proto const exists only under an
/// encryption backend; with none the gossip frame goes out unencrypted, so the
/// wrapper adds nothing to the recv-buffer sizing.
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
enum DialStatus<R>
where
  R: Runtime,
{
  /// The connection succeeded; hand the stream and its write channel back.
  Connected(DialConnected<R>),
  /// The connection failed or timed out; the pump fails the exchange.
  Failed(ExchangeId),
}

/// Payload of [`DialStatus::Connected`].
struct DialConnected<R>
where
  R: Runtime,
{
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
  /// Peer addresses whose dispatched exchange terminated successfully — the
  /// reached set the call resolves `Ok` with. Each successful exchange
  /// contributes independently (duplicate seeds yield duplicate entries).
  contacted: SmallVec<[SocketAddr; 1]>,
  requested: usize,
  reply: oneshot::Sender<JoinReply>,
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
  #[cfg(compression)]
  compression: CompressionOptions,
  #[cfg(checksum)]
  checksum: ChecksumOptions,
  #[cfg(encryption)]
  encryption: EncryptionOptions,
  gossip_mtu: usize,
  label: Option<Bytes>,
}

/// Transform one inbound gossip datagram: decrypt + decompress (one pass) →
/// strip label → parse. Returns the source and the parsed messages — empty on
/// any malformed/undecryptable datagram (gossip is lossy; the driver drops it).
/// Runs inline on the pump (the transform is microsecond-scale per datagram).
fn transform_ingress<I>(
  ctx: &TransformCtx,
  from: SocketAddr,
  raw: Bytes,
) -> (SocketAddr, Vec<Message<I, SocketAddr>>)
where
  I: NodeId,
{
  // The only `Cow::Borrowed` result is the no-transform passthrough (the whole
  // input unchanged), so reuse the input `Bytes` instead of copying it — this is
  // the common plain-gossip path. A real transform yields `Owned`. With an
  // encryption backend built in the keyring-aware unwrap strips the outer
  // `Encrypted` wrapper; with none the base `unwrap_transforms` strips the
  // remaining (checksum/compression) wrappers, bounded by the gossip MTU.
  #[cfg(encryption)]
  let unwrapped = unwrap_transforms_with_encryption(&raw, ctx.gossip_mtu, &ctx.encryption);
  #[cfg(not(encryption))]
  let unwrapped = unwrap_transforms(&raw, ctx.gossip_mtu);
  let transformed = match unwrapped {
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
fn transform_egress<I>(
  ctx: &TransformCtx,
  transmit: Transmit<I, SocketAddr>,
) -> Option<(SocketAddr, Vec<u8>)>
where
  I: NodeId,
{
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
  // Apply the cross-transport transforms to the encoded frame before it hits the
  // wire: compress, then checksum, then encrypt, so the on-wire byte order is
  // `[Encrypted[Checksumed[Compressed[frame]]]]`. Each is present only when its
  // backend is built in; with none, the encoded frame goes out as-is.
  #[allow(unused_mut)]
  let mut on_wire: Vec<u8> = plain.to_vec();
  #[cfg(compression)]
  {
    on_wire = compress_gossip_datagram(&ctx.compression, &on_wire);
  }
  #[cfg(checksum)]
  {
    // Checksum configured but its backend was not built in — drop rather than
    // emit an unverifiable datagram on a checksum-configured path.
    on_wire = checksum_gossip_datagram(&ctx.checksum, &on_wire).ok()?;
  }
  #[cfg(encryption)]
  {
    // Encryption-configured + backend-rejected — drop the datagram. Emitting
    // plaintext on an encrypted-cluster path would silently bypass auth.
    on_wire = encrypt_gossip_datagram(&ctx.encryption, &on_wire).ok()?;
  }
  Some((peer, on_wire))
}

/// Build the transform context from the endpoint's current options.
fn build_transform<I, T, G>(
  endpoint: &StreamEndpoint<I, SocketAddr, T, G>,
  label: &Option<Bytes>,
) -> Arc<TransformCtx>
where
  T: StreamTransport,
{
  Arc::new(TransformCtx {
    // `compression`/`checksum` accessors return `&Options`; copy out (both are
    // `Copy`). `encryption_options` likewise returns a reference and is cloned.
    #[cfg(compression)]
    compression: *endpoint.compression(),
    #[cfg(checksum)]
    checksum: *endpoint.checksum(),
    #[cfg(encryption)]
    encryption: endpoint.encryption_options().clone(),
    gossip_mtu: endpoint.gossip_mtu(),
    label: label.clone(),
  })
}

/// The single-owner TCP driver future. Runs until shutdown (the last handle
/// dropped, or a `Shutdown` command).
pub(crate) struct StreamDriver<I, R, T, G = rand::rngs::StdRng>
where
  // Structurally required (§8 intrinsic exception): `socket`, `accept_join`,
  // `dial_rx` and the timer field name `<R::Net as Net>::UdpSocket`,
  // `<R::Spawner as AsyncSpawner>::JoinHandle`, `DialStatus<R>` (itself
  // `where R: Runtime`), and `R::Sleep` — none well-formed without this.
  R: Runtime,
  // Structurally required (§8 intrinsic exception): `endpoint` names
  // `StreamEndpoint<I, SocketAddr, T, G>`, whose own struct declares
  // `where R: StreamTransport` over this `T`, so the field type needs it.
  // `I: NodeId` and `G: rand::Rng` are NOT field-structural (the inner
  // `Endpoint<I, A, G>` is unbounded), so they live on the impls instead.
  T: StreamTransport,
{
  endpoint: StreamEndpoint<I, SocketAddr, T, G>,
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
  /// label. Baked into the cached `transform` at construction; retained here so a
  /// `rebuild_transform` (on a runtime transform-options change) re-derives the
  /// context with the same label. With no transform backend built in nothing can
  /// trigger a rebuild, so the field is only ever written.
  #[cfg_attr(
    not(any(compression, encryption, checksum)),
    allow(dead_code, reason = "only read by the transform-change rebuild path")
  )]
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
  /// Inbound transport bytes/EOF from the bridge read tasks. `Some` for the whole
  /// running lifetime and throughout the shutdown drain (which reads it to
  /// all-senders-gone); the shutdown reap then takes it once the drain completes,
  /// which also serves as the one-time reap guard.
  inbound_rx: Option<Receiver<BridgeInbound>>,
  /// The driver's template inbound sender, cloned into each spawned bridge task to
  /// report inbound bytes/EOF. Wrapped in `Option` so the shutdown freeze can drop
  /// it: with the template gone, the only remaining senders are the frozen
  /// bridges' clones, so the inbound channel reaches Disconnected exactly when the
  /// last frozen bridge exits — the drain's terminating condition. `Some` for the
  /// whole running lifetime (no bridge is spawned after the freeze drops it).
  inbound_tx: Option<Sender<BridgeInbound>>,
  /// Dial completions from the dial tasks.
  dial_rx: Receiver<DialStatus<R>>,
  /// Cloned into each spawned dial task to report its outcome.
  dial_tx: Sender<DialStatus<R>>,
  recv_buf: Vec<u8>,
  recv_batch: usize,
  /// Per-poll cap on each drained surface (bounds the work one poll performs;
  /// remaining work triggers a self-wake).
  transmit_batch: usize,
  timer: Option<Pin<Box<R::Sleep>>>,
  timer_deadline: Option<Instant>,
  /// Anchored the first poll a DUE deadline is held back by a capped inbound
  /// path; `handle_timeout` force-fires once
  /// [`TIMEOUT_STALENESS_GRACE`] has elapsed since this anchor, so a
  /// sustained inbound flood cannot suppress failure detection.
  timeout_stall_since: Option<Instant>,
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
  last_metrics: Metrics,
}

impl<I, R, T, G> StreamDriver<I, R, T, G>
where
  I: NodeId,
  R: Runtime,
  T: StreamTransport,
{
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    endpoint: StreamEndpoint<I, SocketAddr, T, G>,
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
      .saturating_add(ENCRYPTED_WRAPPER_OVERHEAD)
      .saturating_add(CHECKSUMED_WRAPPER_OVERHEAD)
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
      inbound_tx: Some(inbound_tx),
      dial_rx,
      dial_tx,
      recv_buf: vec![0u8; buf_len],
      // Clamp to at least 1: a 0 batch would do no receive work yet still set
      // `more` (the `== recv_batch` self-wake arms in both the gossip-recv and
      // inbound-transport loops), pegging the executor in a busy-loop that never
      // receives gossip or bridge data. `recv_batch == 0` therefore behaves as 1.
      // Mirrors the QUIC driver's construction-time clamp.
      recv_batch: recv_batch.max(1),
      transmit_batch,
      timer: None,
      timer_deadline: None,
      timeout_stall_since: None,
      idle_wake: Duration::from_secs(1),
      close_timeout,
      cidr_policy,
      transform,
      last_snapshot_version,
      last_metrics: Metrics::default(),
    }
  }

  /// Rebuild the cached transform context after a runtime options change
  /// (`Set{Compression,Checksum,Encryption}Options`), so the inline transform
  /// path observes the new options. Only the transform-setter command arms call
  /// it, so it is compiled only when a transform backend is built in.
  #[cfg(any(compression, encryption, checksum))]
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
      self
        .inbound_tx
        .as_ref()
        .expect("a bridge is only ever spawned while the driver is running, before the shutdown freeze drops the template inbound sender")
        .clone(),
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
  // `G: rand::Rng` only here among the methods: `start_push_pull` /
  // `start_user_message` draw the endpoint's gossip RNG; the non-scheduling
  // methods (spawn/obs/timer/account) carry no such bound.
  fn dispatch(&mut self, cmd: Command<I>, now: Instant)
  where
    G: rand::Rng,
  {
    match cmd {
      Command::Join(JoinCmd { addrs, wait, reply }) => {
        if !self.endpoint.is_running() {
          // Ignoring Err: the caller dropped its reply receiver. The
          // reached-so-far set is empty.
          let _ = reply.send(Err((SmallVec::new(), Error::NotRunning)));
          return;
        }
        if !wait {
          // Dispatch: fire-and-forget; reply the dispatched seed set. The set is
          // NOT deduplicated (the fire-and-forget count is the dispatched-exchange
          // count, so a duplicate seed contributes two).
          let mut dispatched: SmallVec<[SocketAddr; 1]> = SmallVec::new();
          for addr in &addrs {
            // Ignoring StreamId: per-seed outcome surfaces via poll_event.
            let _ = self
              .endpoint
              .start_push_pull(*addr, PushPullKind::Join, now);
            dispatched.push(*addr);
          }
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Ok(dispatched));
          return;
        }
        // WaitForCompletion: account_event replies the contacted set once every
        // exchange this join started completes. An empty seed list is a no-op
        // success, not a failure.
        if addrs.is_empty() {
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Ok(SmallVec::new()));
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
          // failure rather than an indefinite wait. The reached-so-far set is
          // empty.
          // Ignoring Err: the caller dropped its reply receiver.
          let _ = reply.send(Err((
            SmallVec::new(),
            Error::JoinFailed(JoinFailed::new(addrs.len(), 0)),
          )));
          return;
        }
        let id = self.next_pending_join_id;
        self.next_pending_join_id = self.next_pending_join_id.wrapping_add(1);
        self.pending_joins.insert(
          id,
          PendingJoin {
            pending_eids,
            contacted: SmallVec::new(),
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
          .map_err(Error::Proto);
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
      #[cfg(compression)]
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
      #[cfg(checksum)]
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
      #[cfg(encryption)]
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
            Ok(m) => self.endpoint.update_meta(m).map_err(Error::Proto),
            Err(e) => Err(Error::MetaTooLarge(e)),
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
            .map_err(Error::Proto)
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
            .map_err(Error::Proto)
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
          self.endpoint.set_ack_payload(payload).map_err(Error::Proto)
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
        let succeeded = matches!(p.outcome(), ExchangeStatus::Succeeded);
        // Driver events are `Event<I, SocketAddr>`, so the completed exchange's
        // peer is the contacted seed address itself.
        let peer = *p.peer();
        let mut completed = None;
        for (key, pj) in self.pending_joins.iter_mut() {
          if pj.pending_eids.remove(&eid) {
            // Push for each successful exchange — duplicate seeds that succeed
            // contribute independently (matching the count semantics the join
            // API has always had).
            if succeeded {
              pj.contacted.push(peer);
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
          let res = join_reply(pj.contacted, pj.requested);
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
        let failed = matches!(p.outcome(), ExchangeStatus::Failed);
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
  // `G: rand::Rng`: the CIDR-blocked `Connect` arm calls `handle_dial_failed`,
  // which drives the endpoint's gossip RNG.
  fn handle_stream_action(&mut self, action: StreamAction)
  where
    G: rand::Rng,
  {
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
        // at which point `DialStatus::Connected` spawns the bridge.
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
  // `G: rand::Rng`: the inbound-gossip pass feeds `handle_packet`, which drives
  // the endpoint's gossip RNG.
  /// Repeatedly runs the ordered surface pass to a FIXED POINT: a later surface
  /// can create work for an earlier one (an event answering enqueues a
  /// transmit; a fed inbound message enqueues an event), and a single ordered
  /// pass would report false quiescence for it. Each surface stays cap-bounded
  /// per pass, so a hit cap ends the fixed point (`more == true`, self-wake)
  /// rather than repeating; on `more == false` no ready machine work remains
  /// from this poll's input. The third return is whether the INGRESS surface
  /// specifically hit its cap — parsed gossip not yet fed to the machine —
  /// which gates the timer below exactly like a capped socket recv.
  fn drain_surfaces(&mut self, cx: &mut Context<'_>) -> (bool, bool, bool)
  where
    G: rand::Rng,
  {
    let mut worked = false;
    let mut ingress_capped = false;
    loop {
      let (pass_worked, pass_more, pass_ingress_capped) = self.drain_surfaces_pass(cx);
      worked |= pass_worked;
      ingress_capped |= pass_ingress_capped;
      if pass_more {
        return (worked, true, ingress_capped);
      }
      if !pass_worked {
        return (worked, false, ingress_capped);
      }
    }
  }

  /// One ordered surface pass — ingress → action → transport → gossip → event —
  /// each capped surface draining up to the transmit budget. The event surface
  /// is UNCAPPED (drained to empty) so a surfaced terminal
  /// (`ExchangeCompleted` / `LeftCluster`) folded by `send_observation` is
  /// never stranded behind a per-poll cap under a gossip flood. Sound:
  /// `poll_event` is pump-fed (bounded per poll by the already-capped feeds
  /// plus a `handle_timeout` burst), `send_observation` is non-blocking
  /// (overflow + drop), and there is no event→event feedback, so the drain
  /// terminates.
  fn drain_surfaces_pass(&mut self, cx: &mut Context<'_>) -> (bool, bool, bool)
  where
    G: rand::Rng,
  {
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
    let ingress_capped = ingress == budget;
    more |= ingress_capped;

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

    // Observation events: retry the overflow first, then drain to EMPTY (see
    // the pass doc for why this surface is uncapped).
    self.flush_obs_overflow();
    let mut events = false;
    while let Some(ev) = self.endpoint.poll_event() {
      events = true;
      self.send_observation(ev);
    }
    worked |= events;

    (worked, more, ingress_capped)
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

impl<I, R, T, G> Future for StreamDriver<I, R, T, G>
where
  T: Unpin,
  T::Options: Unpin,
  I: NodeId,
  R: Runtime,
  T: StreamTransport,
  // `poll` drives the endpoint's RNG (handle_timeout / handle_packet /
  // handle_transport_* / snapshot_of); a trait-method body cannot carry its own
  // `where`, so the bound stays on the impl.
  G: rand::Rng + Unpin,
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

    // Shutdown, in ordered phases: (1) FREEZE — best-effort leave, cancel every
    // bridge's reads, drop the template inbound sender, and release the bind
    // sockets; (2) DRAIN the bridge inbound channel to all-senders-gone, folding
    // every already-read completion into its join's contacted set; (3) REAP the
    // parked joins (their reached set now exact) and fail the other waiters; then
    // (4) await ONLY the accept task's exit before acking the caller. The
    // completion latch promises the bind address is free — the UDP gossip socket
    // and the TCP listener — not that every connected stream FD has closed.
    // Dropping `this` at the very end drops the dial channel ends, so any dial task
    // observes its send/recv error and exits.
    //
    // The teardown is re-entrant: the freeze runs once (guarded on
    // `accept_shutdown_tx` still being `Some`); the drain reads `inbound_rx` across
    // as many polls as it takes the frozen bridges to land their parked sends and
    // exit, woken by each bridge's on-exit `wake_driver()`; the reap runs once when
    // the drain disconnects (guarded on `inbound_rx` still being `Some`); every
    // shutdown poll then polls the retained `accept_join`. The accept task owns the
    // TCP listener
    // (its `listener` local), so the FD is released only when that task actually
    // EXITS — not when it is merely signalled. Polling the join handle here
    // registers the waker, so the driver re-polls when the accept task finishes;
    // the stashed reply fires only after that, so a same-address rebind resuming
    // from `shutdown().await` cannot race a still-open listener. The drain unblocks
    // the bridges' parked sends but awaits no bridge's join, so an exchange stalled
    // on the bounded inbound hand-off cannot wedge shutdown.
    if this.shared.is_shutdown() {
      // FREEZE (one-time). Stop every live bridge's reads WITHOUT preempting an
      // in-flight inbound hand-off, drop the driver's template inbound sender so the
      // channel can reach all-senders-gone, and release the bind sockets. Guarded on
      // `accept_shutdown_tx` still being `Some`; taking it below clears the guard so
      // this runs once.
      if this.accept_shutdown_tx.is_some() {
        // Ignoring Err: best-effort leave during shutdown.
        let _ = this.endpoint.leave(Instant::now());
        // FREEZE every live bridge. `cancel_tx.send(())` resolves each bridge's
        // biased-first cancel arm, so a bridge about to read breaks BEFORE it can
        // fold a racing peer-FIN into a fabricated EOF, and a bridge stalled
        // mid-write is preempted too. Dropping the handle additionally
        // disconnects its per-bridge out channel as a backstop. Neither preempts
        // an `inbound_tx` send the bridge is already parked on — that send is
        // awaited in an arm BODY, outside the select, so an already-read EOF still
        // lands first. Each frozen bridge drops its inbound sender on exit,
        // strictly after its parked send lands, and calls `wake_driver()` (see
        // `bridge_task`) so the drain below is re-polled to observe disconnect.
        for (_, handle) in this.bridges.drain() {
          // Ignoring Err: the bridge may have already exited (cancel receiver
          // gone); the freeze is best-effort.
          let _ = handle.cancel_tx.send(());
        }
        // Drop the driver's template inbound sender: the only remaining senders are
        // now the frozen bridges' clones, so the channel reaches Disconnected
        // exactly when the last one exits — the drain's terminating condition.
        this.inbound_tx = None;
        // Release the bind sockets. Dropping `accept_shutdown_tx` cancels the
        // accept task's pending `accept()` so it breaks its loop and drops the
        // `listener` local — but that drop happens when the task is next scheduled,
        // NOT here, which is why the join handle is polled below before the ack.
        // Dropping the gossip socket closes its UDP FD synchronously (the agnostic
        // `UdpSocket` has no async `close`, so the local going out of scope here
        // closes the FD). Taking `accept_shutdown_tx` also clears the freeze guard.
        drop(this.accept_shutdown_tx.take());
        drop(this.socket.take());
      }

      // DRAIN-TO-DISCONNECTED (re-entrant), then REAP (once, when the drain
      // completes). Fold every already-read completion (queued, or
      // parked on a saturated hand-off the drain unblocks) into its join's
      // contacted set BEFORE reaping, so a seed that effectively completed is not
      // dropped from the reaped Err((reached, Shutdown)) set. The drain reads
      // `inbound_rx` to all-senders-gone via `try_recv`; it deliberately holds NO
      // persistent flume `recv_async` waker (a per-poll temporary would deregister
      // on drop, and flume's own sender-drop disconnect would then wake nothing).
      // Instead each frozen bridge calls `wake_driver()` on exit (drop-then-wake),
      // so the last exit re-polls us to observe Disconnected. FREEZE guarantees no
      // bridge issues a new read, so nothing refills the channel: the drain cannot
      // spin, needs no counter, and reaching Disconnected means every parked send
      // has landed. `inbound_rx` stays `Some` through the drain and is taken once
      // it disconnects, which doubles as the one-time reap guard.
      if this.inbound_rx.is_some() {
        let drained_to_disconnect = loop {
          let mut channel_work = false;
          let mut hit_disconnect = false;
          if let Some(inbound_rx) = this.inbound_rx.as_ref() {
            loop {
              match inbound_rx.try_recv() {
                Ok(msg) => {
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
                  channel_work = true;
                }
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                  hit_disconnect = true;
                  break;
                }
              }
            }
          }
          // Pull the resulting machine surfaces; account_event folds every terminal
          // ExchangeCompleted into the matching pending join's contacted set. A
          // dispatched EOF reaps its bridge and emits the completion across one or
          // more surface passes (each bounded by transmit_batch), so keep looping
          // while there is channel or surface work — drain_surfaces must reach
          // QUIESCENCE before we decide, or a completion (or a second queued event)
          // could be left unfolded.
          let (_, surf_more, _) = this.drain_surfaces(cx);
          if channel_work || surf_more {
            continue;
          }
          // Quiesced: no inbound items pending and no surface backlog. If the
          // channel has disconnected, every parked send has landed and been folded
          // — proceed to reap. Otherwise a frozen bridge has not yet landed its
          // parked send and exited; park, and its on-exit `wake_driver()` re-polls
          // us.
          break hit_disconnect;
        };
        if !drained_to_disconnect {
          return Poll::Pending;
        }
        // The drain reached Disconnected: every already-read completion is folded.
        // Take `inbound_rx` (the one-time reap guard), then reap below.
        drop(this.inbound_rx.take());
        // Close the command queue and fail any still-queued commands, so a handle
        // that raced the shutdown gets a reply instead of hanging.
        for cmd in this.shared.close_and_drain() {
          match cmd {
            // Ignoring Err: the caller dropped its reply receiver. The
            // reached-so-far set is empty.
            Command::Join(JoinCmd { reply, .. }) => {
              let _ = reply.send(Err((SmallVec::new(), Error::Shutdown)));
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
            #[cfg(compression)]
            Command::SetCompressionOptions(SetCompressionOptionsCmd { reply, .. }) => {
              // Ignoring Err: the caller dropped its reply receiver.
              let _ = reply.send(Err(Error::Shutdown));
            }
            #[cfg(checksum)]
            Command::SetChecksumOptions(SetChecksumOptionsCmd { reply, .. }) => {
              // Ignoring Err: the caller dropped its reply receiver.
              let _ = reply.send(Err(Error::Shutdown));
            }
            #[cfg(encryption)]
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
        for (_, mut pj) in this.pending_joins.drain() {
          // Ignoring Err: the join caller dropped its reply receiver.
          // Carry the addresses already reached before shutdown raced
          // this waiter — callers see the partial contacted set, not empty.
          let _ = pj
            .reply
            .send(Err((std::mem::take(&mut pj.contacted), Error::Shutdown)));
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
    let recv_capped = recv_n == this.recv_batch;
    more |= recv_capped;

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
        DialStatus::Connected(DialConnected {
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
        DialStatus::Failed(eid) => {
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
    let inbound_capped = inbound_n == this.recv_batch;
    more |= inbound_capped;

    // Drain machine surfaces to a fixed point (each surface cap-bounded per
    // pass; the event surface uncapped so terminals fold before the timer
    // decision below).
    let (drained, drain_more, ingress_capped) = this.drain_surfaces(cx);
    progress |= drained;
    more |= drain_more;

    // Timer, gated on INBOUND quiescence. A resolving Ack (or any
    // deadline-refuting input) that arrived BEFORE a due deadline can still be
    // buffered behind the per-poll caps — in the kernel socket buffer
    // (`recv_capped`), the raw ingress buffer (`recv_gated`), the parsed-gossip
    // surface (`ingress_capped`), or the bridge-inbound channel
    // (`inbound_capped`). Firing `handle_timeout` past it would time out an
    // exchange or suspect a live peer prematurely, so a due deadline DEFERS
    // while any inbound path is capped: the `more` self-wake re-polls and each
    // capped path drains FIFO, so the pre-deadline backlog clears within a
    // bounded number of polls. Deferral is itself bounded by a wall-clock
    // staleness grace — a sustained flood that keeps every poll capped cannot
    // suppress failure detection past it. Outbound caps (actions, transport,
    // gossip egress) do NOT defer: outbound work cannot refute a deadline.
    let inbound_backlog = recv_capped || recv_gated || ingress_capped || inbound_capped;
    let target = match this.endpoint.poll_timeout() {
      Some(d) => d.min(now + this.idle_wake),
      None => now + this.idle_wake,
    };
    if target <= now {
      if inbound_backlog {
        this.timeout_stall_since.get_or_insert(now);
      }
      let grace_elapsed = this
        .timeout_stall_since
        .is_some_and(|t| now.saturating_duration_since(t) >= TIMEOUT_STALENESS_GRACE);
      if !inbound_backlog || grace_elapsed {
        this.endpoint.handle_timeout(now);
        this.timeout_stall_since = None;
        progress = true;
      }
      // Due (fired or deferred): self-wake either way — a fired deadline may
      // have queued transmits/events this pass did not drain, and a deferred
      // one needs the re-poll to drain the backlog toward quiescence.
      more = true;
    } else {
      this.timeout_stall_since = None;
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
    // `Endpoint::metrics` returns `&Metrics`; copy it out (`Metrics: Copy`) so the
    // comparison and store below work on owned values.
    let metrics = *this.endpoint.endpoint_ref().metrics();
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
pub(crate) async fn accept_task<I, L>(
  listener: L,
  accepted_tx: Sender<(L::Stream, SocketAddr)>,
  shutdown_rx: Receiver<()>,
  shared: Arc<Shared<I>>,
) where
  I: NodeId,
  L: TcpListener,
{
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
async fn dial_task<I, R>(
  eid: ExchangeId,
  peer: SocketAddr,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: oneshot::Receiver<()>,
  dial_tx: Sender<DialStatus<R>>,
  shared: Arc<Shared<I>>,
) where
  I: NodeId,
  R: Runtime,
{
  let outcome = match <R::Net as Net>::TcpStream::connect_timeout(&peer, DIAL_TIMEOUT).await {
    Ok(stream) => DialStatus::Connected(DialConnected {
      eid,
      stream,
      out_rx,
      cancel_rx,
    }),
    Err(_) => DialStatus::Failed(eid),
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
/// - An explicit `StreamAction::Abort` (and the shutdown freeze) sends `()` on
///   `cancel_rx` before dropping the handle. That resolves `cancel_fut`, the
///   biased-FIRST arm of the both-halves-live read select, so it preempts the
///   bridge ahead of a racing read — a peer-FIN readable just after the signal
///   is NOT read and folded into a fabricated EOF — and ahead of a write stalled
///   on an unresponsive peer, discarding the queued bytes. A cancel-break emits
///   no EOF; only a real `read == 0` does. An `inbound_tx` send already in flight
///   is awaited in an arm body, not the select, so it still completes first.
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
async fn bridge_task<I, R, S>(
  stream: S,
  eid: ExchangeId,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: oneshot::Receiver<()>,
  inbound_tx: Sender<BridgeInbound>,
  shared: Arc<Shared<I>>,
  close_timeout: Duration,
) where
  I: NodeId,
  R: Runtime,
  S: TcpStream,
{
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
    // Both halves live. Bias the shutdown/abort cancel AHEAD of the read so a
    // freeze (`cancel_tx.send(())` then handle drop) stops this bridge's reads
    // before a racing peer-FIN can be read and folded as a completed EOF: a FIN
    // unread at the freeze instant is genuinely in-flight and must be ABSENT
    // from the shutdown reached set. The `out` arm is also ahead of the read, so
    // a graceful Close (handle drop, no cancel) tears down on the out-channel
    // disconnect rather than a racing late read. An already-read EOF is still
    // preserved: its `inbound_tx` send is awaited in the read arm BODY, not in
    // this select, so once the bridge is parked on that send the cancel cannot
    // preempt it — cancel only wins when the loop comes back to a READ.
    select_biased! {
      // Cancel first: ONLY an explicit abort/freeze (`cancel_tx.send(())`)
      // resolves this future — a graceful-Close handle-drop is mapped to
      // `pending()`. It breaks at once WITHOUT emitting any EOF; only a real
      // `read == 0` below emits one, so a cancel-break never fabricates a seed.
      () = &mut cancel_fut => break,
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
    }
  }
  // Drop the inbound sender BEFORE waking the driver. The shutdown drain reads
  // `inbound_rx` to all-senders-gone via `try_recv` and deliberately does NOT
  // hold a persistent flume `recv_async` waker (a per-poll temporary registers
  // then deregisters on drop), so flume's own last-sender disconnect wakes
  // nothing — the driver future would never be re-polled and `shutdown().await`
  // would hang. Dropping first, then waking, guarantees the re-polled driver
  // observes the disconnect (the drop is release-ordered ahead of the wake). This
  // fires on EVERY exit path — a frozen bridge's out-channel disconnect, an
  // explicit cancel/abort, an inbound-send error, or a read/write error — which
  // is what lets the drain reach Disconnected without awaiting any bridge join.
  drop(inbound_tx);
  shared.wake_driver();
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
