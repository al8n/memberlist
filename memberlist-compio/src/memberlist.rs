//! Public [`Memberlist`] handle — a cheaply clonable wrapper around a single
//! driver task.
//!
//! The handle exposes a CQRS API:
//! - **Reads** (`snapshot`, `local_node`, `alive_count`, `member_count`) are
//!   served lock-free from an `ArcSwap<MemberlistSnapshot<I, A>>` the driver
//!   republishes after every state-affecting tick.
//! - **Writes** (`join`, `leave`, `update_node_metadata`,
//!   `set_compression_options`, `set_encryption_options`, `shutdown`)
//!   round-trip through the command channel: the handle sends a
//!   [`Command`] carrying a one-shot reply sender; the driver dispatches
//!   the command on the owned [`StreamEndpoint`] and replies.
//! - **Events** are observed via [`Memberlist::events`], which returns a
//!   fresh [`EventStream`] for every call (flume MPMC — see
//!   [`crate::events`] for the round-robin caveat).
//!
//! `Memberlist<I, A, R>` is honestly generic: the snapshot
//! ([`MemberlistSnapshot<I, A>`]) and events channel
//! ([`EventStream<I, A>`]) propagate `I`/`A` straight through. The
//! transport-discriminator parameter `R` is a phantom on the handle —
//! the trait bound lives in the per-transport spawn adapter
//! ([`TcpMemberlist`](crate::TcpMemberlist) /
//! [`TlsMemberlist`](crate::TlsMemberlist) /
//! [`QuicMemberlist`](crate::QuicMemberlist)), each of which fixes
//! `I = SmolStr, A = SocketAddr` at the alias level. Power users that
//! wire a custom [`StreamEndpoint`] / [`QuicEndpoint`] keep the full
//! `<I, A>` shape end-to-end without any driver-side projection.

use std::{
  marker::PhantomData,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use compio::runtime::JoinHandle;
use flume::{Receiver, Sender};
use memberlist_machine::event::Event;
use memberlist_wire::{CheapClone, CompressionOptions, EncryptionOptions, Node};

use crate::{
  Address, EventStream, JoinAllFailed, MemberlistError, MemberlistSnapshot, Result,
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, SetCompressionOptionsCmd, SetEncryptionOptionsCmd,
    ShutdownCmd, UpdateNodeMetadataCmd, WaitForCompletionArgs,
  },
  resolver::{OsResolver, Resolver},
};

/// Cheaply clonable handle to a running memberlist driver.
///
/// Every clone shares the same command channel, snapshot, events receiver,
/// and driver-task handle. The driver task is spawned once at construction
/// (see [`TcpMemberlist::new`](crate::TcpMemberlist) /
/// [`TlsMemberlist::new`](crate::TlsMemberlist)) and lives until either
/// [`Memberlist::shutdown`] resolves or every clone is dropped (the latter
/// closes the command channel, which the driver observes as a shutdown
/// request and exits its loop; the [`JoinHandle`] held inside the last
/// `Arc` cancels the task on drop, which is a no-op if the loop already
/// exited cleanly).
///
/// `Memberlist<I, A, R>` is honestly generic over the wire id /
/// address types — the snapshot and events channel both propagate
/// `<I, A>` through to their public types
/// ([`MemberlistSnapshot<I, A>`] and [`EventStream<I, A>`]). Most users
/// want [`TcpMemberlist`](crate::TcpMemberlist) /
/// [`TlsMemberlist`](crate::TlsMemberlist) /
/// [`QuicMemberlist`](crate::QuicMemberlist), the pinned aliases that
/// fix `I = SmolStr, A = SocketAddr`.
pub struct Memberlist<I, A, R> {
  /// Command channel into the driver task — every write API sends one
  /// command and awaits the one-shot reply.
  commands: Sender<Command>,
  /// Lock-free observable state, republished by the driver after every
  /// state-affecting tick.
  snapshot: Arc<ArcSwap<MemberlistSnapshot<I, A>>>,
  /// Shared events receiver — `events()` clones this into a fresh
  /// [`EventStream`].
  events_rx: Receiver<Event<I, A>>,
  /// Driver-task handle. Wrapped in `Arc` so clones share ownership;
  /// dropping the last `Arc` drops the inner [`JoinHandle`], which
  /// cancels the task. After [`Memberlist::shutdown`] the task has
  /// already exited and the cancel is a no-op.
  driver_handle: Arc<JoinHandle<()>>,
  /// Atomic shutdown flag — set by the driver's post-loop cleanup
  /// BEFORE the cleanup drain runs. Every command-sending method on
  /// this handle (and its clones) reads the flag at entry and returns
  /// `MemberlistError::Shutdown` immediately when set, so a clone
  /// racing the driver's shutdown cleanup cannot end up with its
  /// reply-receiver hanging on a command buffered in a channel that
  /// already has its Receiver gone.
  shutdown_flag: Arc<AtomicBool>,
  /// Monotonic count of events the driver dropped via `try_send` when
  /// the bounded events channel was full (slow-subscriber backpressure).
  /// `Disconnected` returns from `try_send` are NOT counted — those
  /// only fire when every `EventStream` has been dropped, which is the
  /// "no one is subscribing" terminal state and is not a gap. See
  /// [`Self::events_dropped`].
  events_dropped: Arc<AtomicU64>,
  /// Cached join deadline — the only `DriverOptions` field
  /// [`Self::join_with`] reads on the handle hot-path. Caching one
  /// scalar instead of the full options struct keeps `Memberlist<I, A,
  /// R>` free of a transport-options generic.
  cached_join_deadline: Duration,
  /// Phantom over the wire id type.
  _i: PhantomData<fn(I)>,
  /// Phantom over the wire address type.
  _a: PhantomData<fn(A)>,
  /// Phantom over the stream record layer.
  _r: PhantomData<fn(R)>,
}

impl<I, A, R> Clone for Memberlist<I, A, R> {
  fn clone(&self) -> Self {
    Self {
      commands: self.commands.clone(),
      snapshot: self.snapshot.clone(),
      events_rx: self.events_rx.clone(),
      driver_handle: self.driver_handle.clone(),
      shutdown_flag: self.shutdown_flag.clone(),
      events_dropped: self.events_dropped.clone(),
      cached_join_deadline: self.cached_join_deadline,
      _i: PhantomData,
      _a: PhantomData,
      _r: PhantomData,
    }
  }
}

impl<I, A, R> Memberlist<I, A, R> {
  /// Internal constructor used by the per-transport adapters
  /// (`TcpMemberlist::new`, `TlsMemberlist::new`, future `QuicMemberlist::new`).
  /// Not exposed publicly: the abstract `Memberlist<I, A, R>` has no way to
  /// spawn a driver task of its own — the spawn site lives in the transport
  /// adapter that knows the concrete `R` type and how to wire the
  /// transport-specific machinery (record layer, dial closure, or QUIC
  /// endpoint).
  #[allow(dead_code)]
  pub(crate) fn from_parts(
    commands: Sender<Command>,
    snapshot: Arc<ArcSwap<MemberlistSnapshot<I, A>>>,
    events_rx: Receiver<Event<I, A>>,
    events_dropped: Arc<AtomicU64>,
    driver_handle: JoinHandle<()>,
    shutdown_flag: Arc<AtomicBool>,
    cached_join_deadline: Duration,
  ) -> Self {
    Self {
      commands,
      snapshot,
      events_rx,
      driver_handle: Arc::new(driver_handle),
      shutdown_flag,
      events_dropped,
      cached_join_deadline,
      _i: PhantomData,
      _a: PhantomData,
      _r: PhantomData,
    }
  }

  /// Lock-free snapshot of the cluster's current observable state.
  ///
  /// Returns a strong [`Arc`] pointing at the immutable snapshot the
  /// driver last published. Subsequent driver mutations do not affect the
  /// returned `Arc` — call again to observe a fresh snapshot.
  #[inline]
  pub fn snapshot(&self) -> Arc<MemberlistSnapshot<I, A>> {
    self.snapshot.load_full()
  }

  /// The local node, taken from the latest published snapshot.
  ///
  /// Cheap clone via [`CheapClone`] on both `I` and `A` — for the
  /// pinned aliases (`SmolStr` + `SocketAddr`) this is `Arc`-bump and
  /// scalar copy respectively.
  #[inline]
  pub fn local_node(&self) -> Node<I, A>
  where
    I: CheapClone,
    A: CheapClone,
  {
    self.snapshot.load().local_ref().cheap_clone()
  }

  /// Number of alive members in the latest published snapshot.
  #[inline]
  pub fn alive_count(&self) -> usize {
    self.snapshot.load().alive_count()
  }

  /// Total member count (alive + suspect + dead/left, per the coordinator's
  /// `num_members` definition) in the latest published snapshot.
  #[inline]
  pub fn member_count(&self) -> usize {
    self.snapshot.load().member_count()
  }

  /// Synchronous join: dispatch a push/pull against each seed address
  /// and wait for actual contact.
  ///
  /// Convenience over [`Self::join_with`] for callers that do not
  /// need a custom DNS path. See that method's docstring for the
  /// per-seed completion semantics, the `JOIN_DEADLINE` bound, and
  /// the `JoinAllFailed` zero-contact error.
  pub async fn join(&self, seeds: &[Address]) -> Result<usize> {
    self.join_with(&OsResolver, seeds).await
  }

  /// Synchronous join with an explicit resolver.
  ///
  /// Each resolved address becomes one outbound push/pull exchange.
  /// The call resolves either when every dispatched exchange has
  /// terminated OR when the per-call deadline (`JOIN_DEADLINE`,
  /// currently 10s) elapses, whichever comes first.
  ///
  /// The returned count is the number of dispatched exchanges that
  /// terminated with
  /// [`ExchangeOutcome::Succeeded`](memberlist_machine::event::ExchangeOutcome::Succeeded)
  /// — i.e. the peer's response decoded cleanly, the record layer +
  /// frame + payload all accepted, and the peer's state was merged
  /// into membership. Each exchange counts independently, so passing
  /// the same address twice yields two contacts on a healthy peer.
  /// An exchange whose dial failed, whose handshake / decode
  /// rejected, whose peer hung up before completing the push/pull,
  /// or which crossed the FSM's per-exchange deadline before
  /// validating, does NOT count.
  ///
  /// On a non-empty input that resolved to ≥1 address, the return is:
  /// - `Ok(n)` with `n ≥ 1` — at least one exchange terminated
  ///   `Succeeded`.
  /// - `Err(MemberlistError::JoinAllFailed { requested, contacted: 0 })`
  ///   — every dispatched exchange terminated `Failed` or the
  ///   deadline elapsed before any could `Succeed` (the typical
  ///   "all seeds unreachable" cluster-bootstrap failure).
  ///
  /// An empty `seeds` slice returns `Ok(0)` without dispatching a
  /// command. A non-empty slice whose resolver returns zero
  /// addresses for every entry surfaces
  /// `Err(MemberlistError::JoinAllFailed { requested: seeds.len(), contacted: 0 })`.
  ///
  /// # Cancellation
  ///
  /// The returned future is cancel-safe at the suspension point:
  /// dropping it BEFORE the resolver finishes or the command is
  /// sent simply abandons the call. Dropping AFTER the command is
  /// sent and BEFORE the reply arrives drops the reply-receiver;
  /// the driver still runs the push/pull exchanges and updates
  /// membership, but the reply is silently discarded (the driver's
  /// `send_async` ignores the `SendError::Disconnected`). The
  /// caller should `select!` against an external cancel signal if a
  /// tighter bound than `JOIN_DEADLINE` is required.
  ///
  /// # Fire-and-forget alternative
  ///
  /// Callers that want to enqueue a join without waiting for
  /// completion (long-lived background re-discovery loops, etc.)
  /// should use [`Self::dispatch_join_with`].
  pub async fn join_with<Res>(&self, resolver: &Res, seeds: &[Res::Address]) -> Result<usize>
  where
    Res: Resolver,
    Res::Error: Into<std::io::Error>,
  {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    // Empty input is a trivial caller-side request — return `Ok(0)`
    // without sending a command. Non-empty input that RESOLVES to
    // zero socket addresses (a service-discovery resolver that finds
    // no endpoints) must NOT collapse to a silent success: surface
    // it as `JoinAllFailed` so a bootstrap / discovery outage is
    // never reported as a healthy zero-contact join.
    if seeds.is_empty() {
      return Ok(0);
    }
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for seed in seeds {
      let resolved = resolver
        .resolve(seed)
        .await
        .map_err(|e| MemberlistError::Resolve(e.into()))?;
      addrs.extend(resolved);
    }
    if addrs.is_empty() {
      return Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
        seeds.len(),
        0,
      )));
    }
    let deadline = Instant::now() + self.cached_join_deadline;
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::Join(JoinCmd {
        addrs,
        kind: JoinKind::WaitForCompletion(WaitForCompletionArgs { deadline }),
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Fire-and-forget join: dispatch a push/pull against each seed
  /// address and return immediately.
  ///
  /// Convenience over [`Self::dispatch_join_with`] for callers that
  /// do not need a custom DNS path. See [`Self::join`] for the
  /// synchronous variant that waits for actual contact.
  pub async fn dispatch_join(&self, seeds: &[Address]) -> Result<usize> {
    self.dispatch_join_with(&OsResolver, seeds).await
  }

  /// Fire-and-forget join with an explicit resolver.
  ///
  /// Returns the number of resolved seed addresses handed to the
  /// driver — i.e. the number of push/pull exchanges queued, NOT
  /// the number of seeds that successfully responded. The driver
  /// emits a [`NodeJoined`](memberlist_machine::event::Event) for
  /// every newly-Alive peer; per-seed dial failure surfaces through
  /// the membership FSM's normal suspicion / dead-node path.
  ///
  /// Applications that want to wait for actual contact should use
  /// [`Self::join_with`] instead, which handles the completion
  /// accounting and surfaces zero-contact failures as
  /// [`MemberlistError::JoinAllFailed`]. This method exists for
  /// callers that need to enqueue work without blocking on
  /// completion — typically long-lived background re-discovery
  /// loops where the caller observes [`Self::events`] separately.
  pub async fn dispatch_join_with<Res>(
    &self,
    resolver: &Res,
    seeds: &[Res::Address],
  ) -> Result<usize>
  where
    Res: Resolver,
    Res::Error: Into<std::io::Error>,
  {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let mut addrs: Vec<SocketAddr> = Vec::new();
    for seed in seeds {
      let resolved = resolver
        .resolve(seed)
        .await
        .map_err(|e| MemberlistError::Resolve(e.into()))?;
      addrs.extend(resolved);
    }
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::Join(JoinCmd {
        addrs,
        kind: JoinKind::Dispatch,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Leave the cluster gracefully. Returns when the local "leave"
  /// broadcast has been queued; subscribers observe an `Event::LeftCluster`
  /// once the broadcast completes.
  pub async fn leave(&self) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::Leave(LeaveCmd { reply: tx }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Replace the local node's metadata. The new bytes are gossiped
  /// through the standard alive-broadcast path.
  pub async fn update_node_metadata(&self, meta: Vec<u8>) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::UpdateNodeMetadata(UpdateNodeMetadataCmd {
        meta,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Reconfigure the gossip compression policy in place. Takes effect on
  /// the next outbound datagram.
  pub async fn set_compression_options(&self, opts: CompressionOptions) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::SetCompressionOptions(SetCompressionOptionsCmd {
        opts,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Reconfigure the gossip encryption policy in place. Takes effect on
  /// the next outbound datagram.
  ///
  /// # Not a trust-boundary cutoff
  ///
  /// This API is a key-ROTATION mechanism, not a key-REVOCATION
  /// mechanism. The coordinator's machine-side state — the
  /// keyring, the gossip ingress buffer, and every live bridge's
  /// outbound buffer — is updated to the new policy on the spot
  /// (see [`memberlist_machine::streams::StreamEndpoint::set_encryption_options`]
  /// for the full purge-and-reap protocol). But any reliable-stream
  /// bytes that were already encoded under the prior policy and
  /// queued in a per-bridge byte-mover's FIFO channel WILL be
  /// written to the wire under the prior policy before the bridge
  /// observes its `Close` and exits. The window is bounded by the
  /// FIFO drain through the socket's write half, and the bytes were
  /// legitimately authenticated under the prior policy at encode
  /// time — SWIM's eventual-consistency absorbs the brief
  /// inconsistency.
  ///
  /// **No public API in this crate provides a hard in-flight
  /// cutoff for reliable-stream bytes already queued in a bridge.**
  /// This includes [`Self::shutdown`] — the shutdown sequence
  /// sends a `Close` through each bridge's FIFO out-channel, so
  /// any queued `Bytes` ahead of that `Close` are written under
  /// the prior policy before the bridge exits, identical to the
  /// rotation case above. For applications that need a strict
  /// trust-boundary cutoff (e.g. on observed key compromise) the
  /// approved approach is OUT-OF-BAND key revocation at the peer
  /// — every peer rejects the compromised key on its keyring
  /// removal, and the leaked-window bytes become undecryptable
  /// the moment they land. A future `StreamAction::Cancel`
  /// machine-side extension is the documented path to in-driver
  /// hard cancellation; until then, treat this as a rotation API
  /// only.
  pub async fn set_encryption_options(&self, opts: EncryptionOptions) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
        opts,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Subscribe to the driver's event stream.
  ///
  /// Every call returns a fresh [`EventStream`] backed by a clone of the
  /// shared flume receiver. Per flume MPMC semantics, events ROUND-ROBIN
  /// between concurrent subscribers (NOT broadcast): for a single
  /// consumer (the common case) this is exactly the right shape; for
  /// multi-consumer broadcast, wrap with a dedicated broadcast layer
  /// above this stream.
  ///
  /// # Lossy under backpressure
  ///
  /// The events channel is bounded (1024). When the queue is full the
  /// driver drops the newest event rather than block — a slow
  /// subscriber that stops draining must not deadlock the membership
  /// FSM. Subscribers that need to detect a gap can poll
  /// [`Self::events_dropped`] before and after a window; a monotonic
  /// increase means the driver dropped at least that many events
  /// during the window and the subscriber should reconcile state from
  /// the lock-free snapshot ([`Self::snapshot`] /
  /// [`Self::alive_count`] / [`Self::member_count`]).
  #[inline]
  pub fn events(&self) -> EventStream<I, A> {
    EventStream::new(self.events_rx.clone())
  }

  /// Monotonic count of events the driver has dropped because the
  /// bounded events channel was full when the driver tried to publish
  /// them. Slow subscribers that need to detect that they missed
  /// events can sample this value before and after a window — a
  /// monotonic increase across the window indicates dropped events
  /// (the lock-free [`Self::snapshot`] / [`Self::alive_count`] /
  /// [`Self::member_count`] read path is unaffected and remains the
  /// authoritative source of current cluster state).
  #[inline]
  pub fn events_dropped(&self) -> u64 {
    self.events_dropped.load(Ordering::Relaxed)
  }

  /// Cleanly shut down the driver task.
  ///
  /// Consumes `self`. Sends `Command::Shutdown` and awaits the driver's
  /// acknowledgement — by the time the future resolves the driver has
  /// drained its bridges and closed the gossip socket. Other live clones
  /// (if any) become inert: their `commands` sender is still valid by
  /// construction but the receiving driver loop has exited, so any
  /// subsequent write call observes either `CommandSend` (if the channel
  /// is now closed) or `ReplyClosed` (if the send raced ahead of the
  /// shutdown).
  pub async fn shutdown(self) -> Result<()> {
    // First-caller-wins: the racing clone that flips the flag to true
    // owns the shutdown sequence; every other clone observing a
    // already-true flag returns immediately with Shutdown. The
    // command is still SENT (so the driver loop tears down) only if
    // we are the first caller; otherwise the driver has already
    // observed (or will observe) a prior Shutdown command and is
    // tearing down on its own.
    if self.shutdown_flag.swap(true, Ordering::AcqRel) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    self
      .commands
      .send_async(Command::Shutdown(ShutdownCmd { reply: tx }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.recv_async()
      .await
      .map_err(|_| MemberlistError::ReplyClosed)?
  }
}
