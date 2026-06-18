//! Public [`Memberlist`] handle — a cheaply clonable wrapper around a single
//! driver task.
//!
//! The handle exposes a CQRS API:
//! - **Reads** (`snapshot`, `local_node`, `alive_count`, `member_count`) are
//!   served lock-free from an `ArcSwap<MemberlistSnapshot<I, SocketAddr>>`
//!   the driver republishes after every state-affecting tick.
//! - **Writes** (`join`, `leave`, `update_node_metadata`,
//!   `queue_user_broadcast`, `set_local_state`, `set_ack_payload`,
//!   `set_compression_options`, `set_checksum_options`,
//!   `set_encryption_options`, `shutdown`)
//!   round-trip through the command channel: the handle sends a
//!   [`Command`] carrying a one-shot reply sender; the driver dispatches
//!   the command on the owned machine endpoint and replies.
//! - **Events** are observed via [`Memberlist::events`], which returns a
//!   fresh [`EventStream`] for every call (flume MPMC — see
//!   [`crate::events`] for the round-robin caveat).
//!
//! The handle is `Memberlist<I, A>`, parameterized by the wire id `I` and the
//! transport's unresolved address `A`. The [`Transport`] backend and
//! [`Delegate`] hook bundle are fn-level generics on the constructors, selected
//! by the [`Options<T>`](crate::Options) argument to [`Memberlist::new`].

use core::marker::PhantomData;
use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use compio::runtime::JoinHandle;
use flume::{Receiver, Sender};
#[cfg(checksum)]
use memberlist_proto::ChecksumOptions;
#[cfg(compression)]
use memberlist_proto::CompressionOptions;
#[cfg(encryption)]
use memberlist_proto::EncryptionOptions;
use memberlist_proto::{
  CheapClone, Instant, Node,
  event::Event,
  typed::{NodeState, State},
};

#[cfg(checksum)]
use crate::command::SetChecksumOptionsCmd;
#[cfg(compression)]
use crate::command::SetCompressionOptionsCmd;
#[cfg(encryption)]
use crate::command::SetEncryptionOptionsCmd;
use crate::{
  EventStream, JoinAllFailed, MemberlistError, MemberlistSnapshot, Options, Result,
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, PingCmd, QueueUserBroadcastCmd, SendReliableCmd,
    SendUserCmd, SetAckPayloadCmd, SetLocalStateCmd, ShutdownCmd, UpdateNodeMetadataCmd,
    WaitForCompletionArgs,
  },
  delegate::Delegate,
  maybe_resolved::MaybeResolved,
  resolver::{AdvertiseAddrResolver, Resolver},
  transport::{Transport, TransportRuntime},
};
use memberlist_proto::metrics::Metrics;
use rand::rngs::StdRng;

/// Cheaply clonable handle to a running memberlist driver.
///
/// Every clone shares the same command channel, snapshot, events receiver,
/// and driver-task handle. The driver task is spawned once at construction
/// (see [`Memberlist::new`]) and lives until either [`Memberlist::shutdown`]
/// resolves or every clone is dropped (the latter closes the command
/// channel, which the driver observes as a shutdown request and exits its
/// loop; the [`JoinHandle`] held inside the last `Arc` cancels the task on
/// drop, which is a no-op if the loop already exited cleanly).
///
/// `Memberlist<I, A>` carries the wire id type `I` and the transport's
/// unresolved address type `A`. `I` flows into the snapshot and events channel,
/// which carry `<I, SocketAddr>` to their public types
/// ([`MemberlistSnapshot<I, SocketAddr>`] and [`EventStream<I, SocketAddr>`]) —
/// the driver-layer address is always `SocketAddr`. `A` is held in no field; it
/// ties [`Memberlist::join`]'s seeds to the transport's address domain. The
/// [`Transport`] backend and [`Delegate`] hook bundle are fn-level generics on
/// the constructors only, since both move into the spawned driver task.
pub struct Memberlist<I, A> {
  /// Command channel into the driver task — every write API sends one
  /// command and awaits the one-shot reply. The id type `I` propagates from
  /// `Command<I>` — only `Command::Ping` carries a full
  /// `Node<I, SocketAddr>`.
  commands: Sender<Command<I>>,
  /// Lock-free observable state, republished by the driver after every
  /// state-affecting tick.
  snapshot: Arc<ArcSwap<MemberlistSnapshot<I, SocketAddr>>>,
  /// The machine's load-shedding counters, read lock-free via `metrics()`.
  metrics: Arc<ArcSwap<Metrics>>,
  /// Shared events receiver — `events()` clones this into a fresh
  /// [`EventStream`].
  events_rx: Receiver<Event<I, SocketAddr>>,
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
  /// Teardown-completion latch. The driver task holds the matching sender and
  /// drops it only after `run` returns (all sockets closed). A `shutdown` caller
  /// that LOST the flag race awaits this before returning, so it cannot resume
  /// and rebind the same port while the driver's listener / UDP socket is still
  /// bound. `recv_async` resolves (with `Disconnected`) the moment the sender
  /// drops.
  shutdown_done_rx: Receiver<()>,
  /// Monotonic count of events dropped at the `EventStream` (`events()`)
  /// fan-out: a slow subscriber let the bounded events queue fill, so the driver
  /// dropped the newest event rather than block. App-data is routed to the
  /// delegate only, so these are membership/control events — RECOVERABLE by
  /// reconciling from the snapshot. `Disconnected` returns from the `try_send`
  /// are NOT counted (every `EventStream` dropped — "no one is subscribing" — is
  /// not a gap). See [`Self::events_dropped`].
  events_dropped: Arc<AtomicU64>,
  /// Monotonic count of events dropped at the observation (delegate) channel:
  /// when it is configured `Channel::Bounded` (the default) and the delegate
  /// cannot keep up, the driver drops by count or by the payload byte backstop.
  /// A drop here means BOTH the delegate hook and the EventStream missed the
  /// event; for app-data (`UserPacket` / `RemoteStateReceived`) it is
  /// UNRECOVERABLE (absent from the snapshot). See [`Self::observation_dropped`].
  observation_dropped: Arc<AtomicU64>,
  /// Cached join deadline — the only `DriverOptions` field [`Self::join`]
  /// reads on the handle hot-path. Caching one scalar instead of the full
  /// options struct keeps the handle free of a transport-options generic.
  cached_join_deadline: Duration,
  /// Ties the handle to the transport's unresolved address type. Not held in
  /// any field — `join` enforces seeds resolve in this address domain.
  _a: PhantomData<fn(A)>,
}

impl<I, A> Clone for Memberlist<I, A> {
  fn clone(&self) -> Self {
    Self {
      commands: self.commands.clone(),
      snapshot: self.snapshot.clone(),
      metrics: self.metrics.clone(),
      events_rx: self.events_rx.clone(),
      driver_handle: self.driver_handle.clone(),
      shutdown_flag: self.shutdown_flag.clone(),
      shutdown_done_rx: self.shutdown_done_rx.clone(),
      events_dropped: self.events_dropped.clone(),
      observation_dropped: self.observation_dropped.clone(),
      cached_join_deadline: self.cached_join_deadline,
      _a: PhantomData,
    }
  }
}

impl<I, A> Memberlist<I, A>
where
  I: CheapClone + memberlist_proto::Data + 'static,
{
  /// Construct a memberlist, bind the transport's sockets, build the
  /// initial snapshot, and spawn the driver task on the current compio
  /// runtime.
  ///
  /// `options` bundles the per-backend transport options, the SWIM-level
  /// [`MemberlistOptions`](crate::MemberlistOptions), and the per-driver
  /// [`DriverOptions`](crate::driver_options::DriverOptions). `delegate` is
  /// the membership hook bundle (default
  /// [`VoidDelegate`](crate::delegate::VoidDelegate) is a no-op). `resolver`
  /// resolves the (possibly-unresolved) advertise address;
  /// `advertise_resolver` picks one `SocketAddr` from the resolved
  /// candidates.
  ///
  /// On return the driver task is already running. Reads (`snapshot`,
  /// `local_node`, `alive_count`, `member_count`) are served lock-free from
  /// the initial snapshot until the driver publishes its first refresh on
  /// entry into the loop.
  ///
  /// # Errors
  ///
  /// Returns [`MemberlistError::Io`] if the transport fails to construct
  /// (binding the UDP gossip socket / TCP reliable listener / quinn
  /// endpoint, most commonly `EADDRINUSE` on a port collision; or a
  /// required option such as `local_id` / `advertise_addr` was not set).
  pub async fn new<T, D, RES, AR>(
    options: Options<T>,
    delegate: D,
    resolver: &RES,
    advertise_resolver: &AR,
  ) -> Result<Self>
  where
    T: Transport<Id = I, Address = A>,
    D: Delegate<Id = I, Address = SocketAddr>,
    RES: Resolver<Address = A>,
    AR: AdvertiseAddrResolver,
  {
    Self::new_with_rng::<T, D, RES, AR, StdRng>(
      options,
      delegate,
      resolver,
      advertise_resolver,
      crate::gossip_rng()?,
    )
    .await
  }

  /// Like [`new`](Self::new) but with a caller-supplied gossip RNG `G`, mirroring
  /// [`Endpoint::new`]'s `rng` parameter — the caller owns seeding it. The
  /// machine's gossip schedule is reproducible iff `rng` is.
  pub async fn new_with_rng<T, D, RES, AR, G>(
    options: Options<T>,
    delegate: D,
    resolver: &RES,
    advertise_resolver: &AR,
    rng: G,
  ) -> Result<Self>
  where
    T: Transport<Id = I, Address = A>,
    D: Delegate<Id = I, Address = SocketAddr>,
    RES: Resolver<Address = A>,
    AR: AdvertiseAddrResolver,
    G: rand::Rng + Send + Unpin + 'static,
  {
    #[cfg(feature = "cidr")]
    let cidr_policy = options.cidr_policy().cloned();
    let (transport_opts, memberlist_opts, driver_opts, alive_delegate, merge_delegate) =
      options.into_parts();
    // Fail fast on an impossible `gossip_mtu` BEFORE binding any socket: a
    // gossip packet is one UDP datagram, so a `gossip_mtu` whose wire datagram
    // cannot fit a UDP packet would make near-MTU gossip silently unsendable.
    // Enforced here so every backend (TCP/TLS/QUIC) shares one check.
    crate::options::validate_gossip_mtu(&memberlist_opts)?;
    // Fail fast on a generic-free driver knob whose value would
    // deterministically break the driver loop (currently only a zero
    // `idle_wake_interval`, which would busy-spin a quiescent loop). Shared by
    // every backend through this single `Memberlist::new` path, like the
    // gossip-MTU check above.
    crate::options::validate_driver_options(&driver_opts)?;
    // Fail fast on a zero `max_stream_frame_size`: the reliable-stream frame
    // ceiling gates every push/pull and large user message, so a zero ceiling
    // would reject every reliable frame and the node could never join or
    // receive reliable user data. Checked before `initial_local_state`, which
    // is validated against this same ceiling.
    crate::options::validate_max_stream_frame_size(&memberlist_opts)?;
    // Fail fast on an `initial_meta` larger than the effective meta cap
    // (`min(meta_max_size, Meta::MAX_SIZE)`). The machine only debug-asserts this
    // invariant, so without a release check a node could start with a meta the
    // reliable-frame floor (sized for `meta_max_size`) under-counts, then fail
    // every push/pull at the receiver's frame-length gate.
    crate::options::validate_initial_meta(&memberlist_opts)?;
    // Fail fast on an `initial_local_state` snapshot whose framed PushPull
    // exceeds the reliable-stream frame budget: the snapshot rides every
    // push/pull exchange, so an over-budget snapshot would be rejected by
    // every receiver's frame-length gate and the application state would never
    // reach a peer. Validated here (before any socket is bound) against the
    // EFFECTIVE `max_stream_frame_size` (the configured override, or the
    // machine default when unset) that `apply_memberlist_options` installs into
    // every `T::run`.
    crate::options::validate_initial_local_state::<T::Id, SocketAddr>(&memberlist_opts)?;
    // Fail fast when the configured keyring references an AEAD algorithm whose
    // backend feature is absent from this build. A keyring is constructible
    // without the feature (only the `random_*` constructors are gated), but
    // every subsequent gossip encrypt/decrypt would return `UnsupportedAlgorithm`
    // — the cluster silently breaks after an apparently successful construction.
    // Every key in the ring (primary AND secondaries) is probed so a supported
    // primary paired with an unsupported secondary — a common key-rotation state
    // — is also caught. Checked before any socket is bound (before
    // `T::new`), so a misconfigured keyring fails fast with no resources held.
    #[cfg(encryption)]
    crate::options::validate_encryption_options(memberlist_opts.encryption())
      .map_err(MemberlistError::Encryption)?;
    // Fail fast when the configured gossip checksum algorithm's backend feature
    // is absent from this build. The options builder accepts the algorithm, but
    // every subsequent `checksum_gossip` would return a `ChecksumError` and the
    // driver would drop the datagram — so a "successful" checksum config would
    // silently disable ALL gossip. Caught before any socket is bound (before
    // `T::new`), mirroring the encryption probe above. Checksum is a gossip-plane
    // concern only; the reliable stream path carries no checksum.
    #[cfg(checksum)]
    crate::options::validate_checksum_options(memberlist_opts.checksum())
      .map_err(MemberlistError::Checksum)?;
    let transport = T::new(transport_opts, resolver, advertise_resolver)
      .await
      .map_err(|e| {
        // Preserve a typed `MemberlistError` raised by the backend's `T::new`
        // (e.g. a stream backend rejecting a zero `bridge_recv_buf_len` — a
        // `T::Options`-local knob not reachable at the generic-option checks
        // above) instead of flattening every backend error into `Io`. A custom
        // transport whose `Error` is some other type still surfaces as `Io`.
        let boxed: Box<dyn core::error::Error + Send + Sync> = Box::new(e);
        match boxed.downcast::<MemberlistError>() {
          Ok(typed) => *typed,
          Err(other) => MemberlistError::Io(std::io::Error::other(other.to_string())),
        }
      })?;

    // Fail fast on a node whose own mandatory single-datagram control packets
    // (probe Ping / self-Alive / Ack) — built from the ACTUAL local id and the
    // ACTUAL resolved advertise address — are unsendable, BEFORE any driver task
    // is spawned. Two ways they can be unsendable, both surfaced here:
    //   * the advertise address is not wire-encodable (e.g. a scoped IPv6 the
    //     compact `SocketAddrV6` encoder rejects) ⇒ every local-node-bearing
    //     packet would fail to encode at runtime — the node could not emit
    //     membership traffic — so construction is rejected with
    //     `AdvertiseAddrNotEncodable`;
    //   * the unbounded local id pushes a packet's plaintext frame over the
    //     gossip budget ⇒ silently unsendable / never gossiped and peers falsely
    //     suspect it — rejected with `GossipMtuTooSmall`.
    // The resolved id AND advertise address are first available through the
    // generic `Transport` trait here; the just-built transport is dropped on
    // `Err` (its socket closes) before any driver task is spawned.
    crate::options::validate_gossip_mtu_for_identity::<T::Id>(
      transport.local_id(),
      transport.advertise_address(),
      &memberlist_opts,
    )?;

    // Fail fast if `max_stream_frame_size` is too small to carry even the local
    // node's own minimal reliable push/pull frame. Every join / anti-entropy
    // exchange carries at least the local node, so a cap below that minimum
    // would have every receiver's frame-length gate reject the exchange while
    // the node still constructs Ok. Identity-aware (uses the ACTUAL id /
    // advertise address, sized for the `meta_max_size` ceiling so any future
    // `update_node_metadata` is guaranteed to fit): it accepts a small cap that
    // genuinely fits the node and rejects only caps too small for its own frame.
    crate::options::validate_stream_frame_for_identity::<T::Id>(
      transport.local_id(),
      transport.advertise_address(),
      &memberlist_opts,
    )?;

    // Fail fast on a resolved advertise address that is not a usable unicast
    // CONTACT — an unspecified IP (`0.0.0.0`/`::`, the wildcard-bind a node gets
    // when it binds `0.0.0.0:0` and forgets to set a concrete advertise), a
    // multicast / IPv4-broadcast IP, or a zero port. Such an address encodes
    // fine (so the encodability check above passes) but is undialable: the node
    // would join a cluster as a member no peer can route probes/dials to, then
    // be falsely suspected and reaped. Rejected with `InvalidAdvertiseAddr`
    // before any driver task is spawned (the transport is dropped on `Err`,
    // closing its socket). Same single site / all-backend coverage as the
    // identity check above; loopback / private / global unicast stay valid.
    crate::options::validate_advertise_addr(transport.advertise_address())?;

    // Build the initial snapshot from the transport's resolved local
    // identity — independent of the machine endpoint, which `T::run`
    // builds from the transport's stored config. The local node starts
    // alive at incarnation 0 (the machine sets incarnation 1 on first
    // alive broadcast after construction).
    //
    // Carry the configured `initial_meta` (empty when unset) so a
    // `local_node()` / `by_id(local)` read taken IMMEDIATELY after `new()`
    // — before the driver task republishes its first endpoint-derived
    // snapshot — already reflects the applied local metadata, matching the
    // machine endpoint's local member (`Endpoint::new` stamps the same
    // `initial_meta` onto its local `NodeState`). Protocol / delegate
    // versions default to `V1` in both `NodeState::new` and
    // `EndpointOptions`, and the compio options expose no override, so the
    // defaults already agree with the endpoint's local member.
    let initial_meta = memberlist_opts.initial_meta().cloned().unwrap_or_default();
    let local_ns = Arc::new(
      NodeState::new(
        transport.local_id().cheap_clone(),
        *transport.advertise_address(),
        State::Alive,
      )
      .with_meta(initial_meta),
    );
    let snapshot = Arc::new(ArcSwap::from_pointee(MemberlistSnapshot::new(
      vec![local_ns.clone()],
      local_ns,
      1,
      1,
      0,
    )));
    let metrics = Arc::new(ArcSwap::from_pointee(Metrics::default()));

    let (commands_tx, commands_rx) = flume::unbounded();
    let (events_tx, events_rx) = flume::bounded(driver_opts.event_queue_cap());
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let events_dropped = Arc::new(AtomicU64::new(0));
    let observation_dropped = Arc::new(AtomicU64::new(0));
    let cached_join_deadline = driver_opts.join_deadline();

    // A CIDR policy gates the advertised address as an AliveDelegate (composed
    // with any user-supplied one), and the SAME policy filters the gossip source
    // and stream peer inside the driver loop. Compose the membership delegate
    // here; the raw policy rides the runtime to the loop as `cidr_policy`.
    #[cfg(feature = "cidr")]
    let alive_delegate: Option<Box<dyn crate::delegate::AliveDelegate<T::Id, SocketAddr>>> =
      match (cidr_policy.clone(), alive_delegate) {
        (Some(policy), Some(user)) => Some(Box::new(memberlist_proto::CidrAnd::new(policy, user))),
        (Some(policy), None) => Some(Box::new(policy)),
        (None, user) => user,
      };
    #[cfg(not(feature = "cidr"))]
    let cidr_policy = crate::transport::runtime::CidrFilter;

    let runtime = TransportRuntime::new(
      delegate,
      commands_rx,
      events_tx,
      events_dropped.clone(),
      observation_dropped.clone(),
      snapshot.clone(),
      metrics.clone(),
      shutdown_flag.clone(),
      driver_opts,
      memberlist_opts,
      alive_delegate,
      merge_delegate,
      cidr_policy,
    );

    // Teardown-completion latch: moved into the driver task and dropped only
    // when `run` returns (all sockets closed), unblocking any `shutdown` caller
    // that lost the flag race (see `shutdown`).
    let (shutdown_done_tx, shutdown_done_rx) = flume::bounded::<()>(1);
    let driver_handle = compio::runtime::spawn(async move {
      transport.run(runtime, rng).await;
      drop(shutdown_done_tx);
    });

    Ok(Self {
      commands: commands_tx,
      snapshot,
      metrics,
      events_rx,
      driver_handle: Arc::new(driver_handle),
      shutdown_flag,
      shutdown_done_rx,
      events_dropped,
      observation_dropped,
      cached_join_deadline,
      _a: PhantomData,
    })
  }

  /// The local node, taken from the latest published snapshot.
  ///
  /// Cheap clone via [`CheapClone`] on both `I` and `SocketAddr` — for
  /// the common id type (`SmolStr` + `SocketAddr`) this is `Arc`-bump and
  /// scalar copy respectively.
  #[inline]
  pub fn local_node(&self) -> Node<I, SocketAddr> {
    let snap = self.snapshot.load();
    let ns = snap.local_ref();
    Node::new(ns.id_ref().cheap_clone(), ns.address_ref().cheap_clone())
  }

  /// The local node's id.
  #[inline]
  pub fn local_id(&self) -> I {
    self.snapshot.load().local_ref().id_ref().cheap_clone()
  }
}

impl<I, A> Memberlist<I, A> {
  /// Lock-free snapshot of the cluster's current observable state.
  ///
  /// Returns a strong [`Arc`] pointing at the immutable snapshot the
  /// driver last published. Subsequent driver mutations do not affect the
  /// returned `Arc` — call again to observe a fresh snapshot.
  #[inline]
  pub fn snapshot(&self) -> Arc<MemberlistSnapshot<I, SocketAddr>> {
    self.snapshot.load_full()
  }

  /// The machine's cumulative load-shedding counters, read lock-free. The counts
  /// are monotonic for the node's lifetime; difference successive reads for rates.
  /// See [`memberlist_proto::metrics::Metrics`].
  #[inline]
  pub fn metrics(&self) -> Metrics {
    *self.metrics.load_full()
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

  /// The local node's advertised address.
  #[inline]
  pub fn advertise_address(&self) -> SocketAddr {
    *self.snapshot.load().local_ref().address_ref()
  }

  /// The local node's full state from the latest published snapshot.
  #[inline]
  pub fn local_state(&self) -> Arc<memberlist_proto::typed::NodeState<I, SocketAddr>> {
    self.snapshot.load().local_ref().clone()
  }

  /// Look up a member by id in the latest published snapshot.
  /// Returns `None` if the id is not known.
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<Arc<memberlist_proto::typed::NodeState<I, SocketAddr>>>
  where
    I: PartialEq,
  {
    self.snapshot.load().by_id(id).cloned()
  }

  /// All members currently in the alive state, from the latest published
  /// snapshot.
  #[inline]
  pub fn online_members(&self) -> Vec<Arc<memberlist_proto::typed::NodeState<I, SocketAddr>>> {
    self.snapshot.load().online_members().cloned().collect()
  }

  /// Number of alive members. Equivalent to [`Self::alive_count`].
  #[inline]
  pub fn num_online_members(&self) -> usize {
    self.snapshot.load().alive_count()
  }

  /// All known members (alive + suspect + dead/left) from the latest
  /// published snapshot. Mirrors the legacy `Memberlist::members` name.
  #[inline]
  pub fn members(&self) -> Vec<Arc<memberlist_proto::typed::NodeState<I, SocketAddr>>> {
    self.snapshot.load().members().to_vec()
  }

  /// Total member count. Equivalent to [`Self::member_count`]; mirrors the
  /// legacy `Memberlist::num_members` name.
  #[inline]
  pub fn num_members(&self) -> usize {
    self.snapshot.load().member_count()
  }

  /// Members matching `pred`, from the latest published snapshot.
  #[inline]
  pub fn members_by(
    &self,
    pred: impl FnMut(&memberlist_proto::typed::NodeState<I, SocketAddr>) -> bool,
  ) -> Vec<Arc<memberlist_proto::typed::NodeState<I, SocketAddr>>> {
    self.snapshot.load().members_by(pred).cloned().collect()
  }

  /// Count of members matching `pred`.
  #[inline]
  pub fn num_members_by(
    &self,
    pred: impl FnMut(&memberlist_proto::typed::NodeState<I, SocketAddr>) -> bool,
  ) -> usize {
    self.snapshot.load().num_members_by(pred)
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  #[inline]
  pub fn members_map_by<O>(
    &self,
    f: impl FnMut(&memberlist_proto::typed::NodeState<I, SocketAddr>) -> Option<O>,
  ) -> Vec<O> {
    self.snapshot.load().members_map_by(f)
  }

  /// The local node's Lifeguard health score (`0` = healthy; higher = worse).
  #[inline]
  pub fn health_score(&self) -> usize {
    self.snapshot.load().health_score()
  }

  /// Synchronous join: dispatch a push/pull against each seed and wait for
  /// actual contact.
  ///
  /// Each seed is a [`MaybeResolved`] — either an already-resolved
  /// `SocketAddr` (used directly) or an unresolved `RES::Address` (resolved
  /// via the supplied `resolver`). Each resolved address becomes one
  /// outbound push/pull exchange. The call resolves either when every
  /// dispatched exchange has terminated OR when the per-call deadline
  /// (`JOIN_DEADLINE`, currently 10s) elapses, whichever comes first.
  ///
  /// The returned count is the number of dispatched exchanges that
  /// terminated with
  /// [`ExchangeStatus::Succeeded`](memberlist_proto::event::ExchangeStatus::Succeeded)
  /// — i.e. the peer's response decoded cleanly, the record layer +
  /// frame + payload all accepted, and the peer's state was merged
  /// into membership. Each exchange counts independently, so passing
  /// the same address twice yields two contacts on a healthy peer.
  /// An exchange whose dial failed, whose handshake / decode rejected,
  /// whose peer hung up before completing the push/pull, or which crossed
  /// the FSM's per-exchange deadline before validating, does NOT count.
  ///
  /// On a non-empty input that resolved to ≥1 address, the return is:
  /// - `Ok(n)` with `n ≥ 1` — at least one exchange terminated
  ///   `Succeeded`.
  /// - `Err(MemberlistError::JoinAllFailed { requested, contacted: 0 })`
  ///   — every dispatched exchange terminated `Failed` or the deadline
  ///   elapsed before any could `Succeed` (the typical "all seeds
  ///   unreachable" cluster-bootstrap failure).
  ///
  /// An empty `seeds` slice returns `Ok(0)` without dispatching a
  /// command. A non-empty slice whose resolver returns zero addresses for
  /// every entry surfaces
  /// `Err(MemberlistError::JoinAllFailed { requested: seeds.len(), contacted: 0 })`.
  ///
  /// # Cancellation
  ///
  /// The returned future is cancel-safe at the suspension point:
  /// dropping it BEFORE the resolver finishes or the command is sent
  /// simply abandons the call. Dropping AFTER the command is sent and
  /// BEFORE the reply arrives drops the reply-receiver; the driver still
  /// runs the push/pull exchanges and updates membership, but the reply
  /// is silently discarded. The caller should `select!` against an
  /// external cancel signal if a tighter bound than `JOIN_DEADLINE` is
  /// required.
  ///
  /// # Fire-and-forget alternative
  ///
  /// Callers that want to enqueue a join without waiting for completion
  /// (long-lived background re-discovery loops, etc.) should use
  /// [`Self::dispatch_join`].
  #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, fields(seeds = seeds.len())))]
  pub async fn join<RES>(
    &self,
    resolver: &RES,
    seeds: &[MaybeResolved<A, SocketAddr>],
  ) -> Result<usize>
  where
    RES: Resolver<Address = A>,
  {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    // Empty input is a trivial caller-side request — return `Ok(0)`
    // without sending a command. Non-empty input that RESOLVES to zero
    // socket addresses (a service-discovery resolver that finds no
    // endpoints) must NOT collapse to a silent success: surface it as
    // `JoinAllFailed` so a bootstrap / discovery outage is never reported
    // as a healthy zero-contact join.
    if seeds.is_empty() {
      return Ok(0);
    }
    let addrs = resolve_seeds(resolver, seeds).await?;
    if addrs.is_empty() {
      return Err(MemberlistError::JoinAllFailed(JoinAllFailed::new(
        seeds.len(),
        0,
      )));
    }
    let deadline = Instant::now() + self.cached_join_deadline;
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::Join(JoinCmd {
        addrs,
        kind: JoinKind::WaitForCompletion(WaitForCompletionArgs { deadline }),
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Fire-and-forget join: dispatch a push/pull against each seed and
  /// return immediately.
  ///
  /// Returns the number of resolved seed addresses handed to the driver —
  /// i.e. the number of push/pull exchanges queued, NOT the number of
  /// seeds that successfully responded. The driver emits a
  /// [`NodeJoined`](memberlist_proto::event::Event) for every newly-Alive
  /// peer; per-seed dial failure surfaces through the membership FSM's
  /// normal suspicion / dead-node path.
  ///
  /// Applications that want to wait for actual contact should use
  /// [`Self::join`] instead, which handles the completion accounting and
  /// surfaces zero-contact failures as [`MemberlistError::JoinAllFailed`].
  /// This method exists for callers that need to enqueue work without
  /// blocking on completion — typically long-lived background
  /// re-discovery loops where the caller observes [`Self::events`]
  /// separately.
  pub async fn dispatch_join<RES>(
    &self,
    resolver: &RES,
    seeds: &[MaybeResolved<A, SocketAddr>],
  ) -> Result<usize>
  where
    RES: Resolver<Address = A>,
  {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let addrs = resolve_seeds(resolver, seeds).await?;
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::Join(JoinCmd {
        addrs,
        kind: JoinKind::Dispatch,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Leave the cluster gracefully.
  ///
  /// Returns `Ok(())` once the leave broadcast has been flushed to
  /// peers — i.e. once the direct `Dead`-self notices queued for every
  /// live peer have been handed to the wire (the membership machine's
  /// `Event::LeftCluster` completion signal). Until then the call is
  /// in flight; subscribers observe `Event::NodeLeft(self)` on peers as
  /// the notices land.
  ///
  /// The wait races the driver's `leave_timeout` (see
  /// [`DriverOptions::with_leave_timeout`](crate::DriverOptions::with_leave_timeout)):
  /// if the flush does not complete within that budget the call returns
  /// [`MemberlistError::LeaveTimeout`] — the local node has still left,
  /// but the driver could not confirm peers were notified.
  #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
  pub async fn leave(&self) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::Leave(LeaveCmd { reply: tx }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Replace the local node's metadata. The new bytes are gossiped
  /// through the standard alive-broadcast path.
  ///
  /// Returns [`MemberlistError::NotRunning`] if the local node has left
  /// the cluster (after [`leave`](Self::leave)): the gossip schedulers
  /// are stopped, so the update could never be disseminated.
  pub async fn update_node_metadata(&self, meta: Vec<u8>) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::UpdateNodeMetadata(UpdateNodeMetadataCmd {
        meta,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Queue an application user-broadcast for cluster-wide gossip
  /// dissemination. The bytes ride the standard gossip path and surface on
  /// peers through [`NodeDelegate::notify_user_msg`](crate::NodeDelegate::notify_user_msg).
  ///
  /// Returns [`MemberlistError::NotRunning`] if the local node has left
  /// the cluster (after [`leave`](Self::leave)): the gossip scheduler is
  /// stopped, so the broadcast could never be disseminated.
  pub async fn queue_user_broadcast(&self, data: Bytes) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::QueueUserBroadcast(QueueUserBroadcastCmd::new(
        data, tx,
      )))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Set the application push/pull local-state snapshot. The snapshot is
  /// carried in subsequent push/pull exchanges and surfaces on peers through
  /// [`NodeDelegate::merge_remote_state`](crate::NodeDelegate::merge_remote_state).
  ///
  /// Returns [`MemberlistError::NotRunning`] if the local node has left
  /// the cluster (after [`leave`](Self::leave)): no further push/pull
  /// exchange will carry the snapshot.
  pub async fn set_local_state(&self, state: Bytes) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SetLocalState(SetLocalStateCmd::new(state, tx)))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Set the application payload attached to outbound probe acks. The payload
  /// surfaces on the probing peer through
  /// [`PingDelegate::notify_ping_complete`](crate::PingDelegate::notify_ping_complete).
  ///
  /// Returns [`MemberlistError::NotRunning`] if the local node has left
  /// the cluster (after [`leave`](Self::leave)): no further probe ack will
  /// carry the payload.
  ///
  /// Returns [`MemberlistError::Proto`] if the framed ack carrying
  /// `payload` would exceed the node's gossip packet budget. An ack is sent
  /// as a single UDP datagram, so an over-budget payload is rejected (and
  /// not stored): every probe reply would otherwise silently fail to send
  /// and peers would falsely suspect this node.
  pub async fn set_ack_payload(&self, payload: Bytes) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SetAckPayload(SetAckPayloadCmd::new(payload, tx)))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Reconfigure the gossip compression policy in place. Takes effect on
  /// the next outbound datagram.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  pub async fn set_compression_options(&self, opts: CompressionOptions) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SetCompressionOptions(SetCompressionOptionsCmd {
        opts,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Reconfigure the gossip (unreliable) checksum policy in place. Takes effect
  /// on the next outbound gossip datagram.
  ///
  /// Checksum is a GOSSIP-PLANE-ONLY concern: it wraps outbound gossip
  /// datagrams and verifies inbound ones. It is NOT applied on the
  /// reliable-stream path — the stream transport (TCP/TLS/QUIC) provides its
  /// own integrity guarantee there, so reliable bridges carry no checksum and
  /// this reconfiguration has no per-bridge fan-out.
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  pub async fn set_checksum_options(&self, opts: ChecksumOptions) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SetChecksumOptions(SetChecksumOptionsCmd {
        opts,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Reconfigure the gossip encryption policy in place. Takes effect on
  /// the next outbound datagram.
  ///
  /// # Not a trust-boundary cutoff
  ///
  /// This API is a key-ROTATION mechanism, not a key-REVOCATION
  /// mechanism. The coordinator's machine-side state — the keyring, the
  /// gossip ingress buffer, and every live bridge's outbound buffer — is
  /// updated to the new policy on the spot (see
  /// [`memberlist_proto::streams::StreamEndpoint::set_encryption_options`]
  /// for the full purge-and-reap protocol).
  ///
  /// On an INSECURE reliable transport (plain TCP) a policy change FAILS
  /// every live bridge, and the machine emits a `StreamAction::Abort` for
  /// each: the driver hard-cancels the bridge through its out-of-band
  /// cancel signal, so any reliable-stream bytes encoded under the prior
  /// policy and still queued in the bridge's FIFO channel are DISCARDED,
  /// not written. The affected exchanges are retried under fresh bridges
  /// built on the new policy. The remaining rotation window is the GOSSIP
  /// (UDP) path: a datagram already handed to the OS cannot be recalled.
  ///
  /// [`Self::shutdown`] is GRACEFUL, not a cutoff: the shutdown sequence
  /// sends a `Close` through each bridge's FIFO out-channel, so any queued
  /// `Bytes` ahead of that `Close` are written before the bridge exits (a
  /// clean teardown flushes in-flight responses). For applications that
  /// need a strict trust-boundary cutoff (e.g. on observed key compromise)
  /// the approved approach remains OUT-OF-BAND key revocation at the peer —
  /// every peer rejects the compromised key on its keyring removal, and any
  /// leaked-window gossip bytes become undecryptable the moment they land.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  pub async fn set_encryption_options(&self, opts: EncryptionOptions) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
        opts,
        reply: tx,
      }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
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
  /// # Membership / control events only
  ///
  /// The stream carries node-lifecycle and control events (joins, leaves,
  /// updates, conflicts, ...). It does NOT carry application payloads —
  /// [`Event::UserPacket`] and [`Event::RemoteStateReceived`] are delivered
  /// only through the [`Delegate`](crate::delegate::Delegate) hooks
  /// (`notify_user_msg` / `merge_remote_state`), which is their reliable path.
  /// A best-effort stream cannot reconstruct a dropped payload from the
  /// snapshot (which holds only membership), and buffering large reliable
  /// payloads here for a slow subscriber would be an unbounded-memory hazard;
  /// so app-data is the delegate's responsibility. Use the delegate to consume
  /// it.
  ///
  /// # Lossy under backpressure
  ///
  /// The events channel is bounded (1024). When the queue is full the
  /// driver drops the newest event rather than block — a slow subscriber
  /// that stops draining must not deadlock the membership FSM. A subscriber
  /// can miss an event two ways, counted separately: the EventStream queue
  /// overflowed ([`Self::events_dropped`]), or the upstream observation channel
  /// dropped it BEFORE fan-out ([`Self::observation_dropped`]). To detect ALL
  /// gaps, poll BOTH before and after a window; any increase means events were
  /// missed and the subscriber should reconcile state from the lock-free
  /// snapshot ([`Self::snapshot`] / [`Self::alive_count`] / [`Self::member_count`]).
  #[inline]
  pub fn events(&self) -> EventStream<I, SocketAddr> {
    EventStream::new(self.events_rx.clone())
  }

  /// Monotonic count of events dropped at the [`EventStream`] ([`Self::events`])
  /// fan-out: when a slow subscriber lets the bounded events queue fill, the
  /// driver drops the newest event rather than block. App-data is delivered to
  /// the [`Delegate`](crate::delegate::Delegate) only, so a gap here is in
  /// membership/control events and is RECOVERABLE — reconcile from the lock-free
  /// snapshot ([`Self::snapshot`] / [`Self::alive_count`] /
  /// [`Self::member_count`]).
  ///
  /// This counter does NOT cover every gap an EventStream subscriber sees: an
  /// upstream observation-channel drop suppresses an event BEFORE fan-out (so
  /// the subscriber misses it) yet increments [`Self::observation_dropped`], not
  /// this. A consumer detecting EventStream gaps must therefore watch BOTH
  /// counters; their sum is the subscriber's total loss over a window.
  #[inline]
  pub fn events_dropped(&self) -> u64 {
    self.events_dropped.load(Ordering::Relaxed)
  }

  /// Monotonic count of events dropped at the observation (delegate) channel:
  /// when it is configured [`Channel::Bounded`](crate::Channel::Bounded) (the
  /// default) and the [`Delegate`](crate::delegate::Delegate) cannot keep up,
  /// the driver drops the event — by count (the channel is at capacity) or by
  /// the payload byte backstop (a large reliable payload would push queued
  /// payload bytes over budget). A
  /// [`Channel::Unbounded`](crate::Channel::Unbounded) observation channel never
  /// drops here.
  ///
  /// Unlike [`Self::events_dropped`] (only a slow EventStream subscriber missed
  /// the event), a drop here means the event reached NEITHER consumer: the
  /// DELEGATE missed it AND it never reached EventStream fan-out, so EventStream
  /// subscribers miss it too (watch both counters for stream gaps). For app-data
  /// ([`Event::UserPacket`] / [`Event::RemoteStateReceived`]) it is
  /// UNRECOVERABLE, since those payloads are absent from the snapshot. A rising
  /// value means the delegate is not keeping up and application data may have
  /// been lost; the remedy is a faster delegate or a
  /// [`Channel::Unbounded`](crate::Channel::Unbounded) observation channel (at
  /// the cost of unbounded queue growth under a stuck handler).
  #[inline]
  pub fn observation_dropped(&self) -> u64 {
    self.observation_dropped.load(Ordering::Relaxed)
  }

  /// Ping `node` and return the measured round-trip time. Mirrors
  /// `memberlist-core`'s `Ping`. The call completes once the endpoint
  /// receives an Ack (reply `Ok(rtt)`) or the probe_timeout elapses
  /// without a response (reply `Err(PingTimeout)`).
  ///
  /// Requires the node to be running. Returns `Err(NotRunning)` after
  /// `leave()` and `Err(Shutdown)` after `shutdown()`.
  pub async fn ping(&self, node: Node<I, SocketAddr>) -> Result<Duration> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::Ping(PingCmd::new(node, tx)))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Send an unreliable directed user message to `to`. Best-effort
  /// delivery; no completion event. Mirrors `memberlist-core`'s `send`.
  ///
  /// Returns `Err(Proto)` if the framed `UserData` packet
  /// exceeds the gossip MTU, `Err(NotRunning)` after `leave()`, and
  /// `Err(Shutdown)` after `shutdown()`.
  #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, msg), fields(to = %to, len = msg.len())))]
  pub async fn send(&self, to: SocketAddr, msg: Bytes) -> Result<()> {
    self.send_many(to, core::iter::once(msg)).await
  }

  /// Send several unreliable directed user messages to `to`. Mirrors
  /// `memberlist-core`'s `send_many`. Two-or-more messages are
  /// compound-packed into one datagram (fewer syscalls); a single message
  /// degrades to `send`.
  pub async fn send_many(
    &self,
    to: SocketAddr,
    msgs: impl IntoIterator<Item = Bytes>,
  ) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let payloads: Vec<Bytes> = msgs.into_iter().collect();
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SendUser(SendUserCmd::new(to, payloads, tx)))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }

  /// Send a reliable directed user message to `to` over a dedicated reliable
  /// exchange. The future resolves once the stream exchange completes
  /// (success) or fails (e.g. dial failure, stream timeout). Mirrors
  /// `memberlist-core`'s `send_reliable`.
  #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, msg), fields(to = %to, len = msg.len())))]
  pub async fn send_reliable(&self, to: SocketAddr, msg: Bytes) -> Result<()> {
    self.send_many_reliable(to, core::iter::once(msg)).await
  }

  /// Send several reliable directed user messages to `to`, each on its own
  /// reliable exchange. The future resolves once ALL streams have
  /// completed. `Ok(())` if every exchange succeeded; `Err(SendFailed)` if
  /// any failed. Mirrors `memberlist-core`'s `send_many_reliable`.
  pub async fn send_many_reliable(
    &self,
    to: SocketAddr,
    msgs: impl IntoIterator<Item = Bytes>,
  ) -> Result<()> {
    if self.shutdown_flag.load(Ordering::Acquire) {
      return Err(MemberlistError::Shutdown);
    }
    let payloads: Vec<Bytes> = msgs.into_iter().collect();
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::SendReliable(SendReliableCmd::new(
        to, payloads, tx,
      )))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
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
  #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
  pub async fn shutdown(self) -> Result<()> {
    // First-caller-wins: the racing clone that flips the flag to true
    // owns the shutdown sequence; every other clone observing an
    // already-true flag returns immediately with Shutdown. The command
    // is still SENT (so the driver loop tears down) only if we are the
    // first caller; otherwise the driver has already observed (or will
    // observe) a prior Shutdown command and is tearing down on its own.
    if self.shutdown_flag.swap(true, Ordering::AcqRel) {
      // Lost the race: the winning caller owns the teardown. Await its
      // completion before returning so this caller cannot resume and rebind the
      // same port while the driver's sockets are still bound. `recv_async`
      // resolves (with `Disconnected`) the moment the driver drops the latch
      // sender after `run` returns. Ignoring the result: either arm means
      // teardown is done.
      let _ = self.shutdown_done_rx.recv_async().await;
      return Err(MemberlistError::Shutdown);
    }
    let (tx, rx) = futures_channel::oneshot::channel();
    self
      .commands
      .send_async(Command::Shutdown(ShutdownCmd { reply: tx }))
      .await
      .map_err(|_| MemberlistError::CommandSend)?;
    rx.await.map_err(|_| MemberlistError::ReplyClosed)?
  }
}

/// Resolve a slice of [`MaybeResolved`] seeds into a flat `Vec<SocketAddr>`.
///
/// `Resolved` entries are used directly; `Unresolved` entries are run
/// through `resolver` and their results appended. A resolver failure
/// surfaces as [`MemberlistError::Resolve`].
async fn resolve_seeds<RES>(
  resolver: &RES,
  seeds: &[MaybeResolved<RES::Address, SocketAddr>],
) -> Result<Vec<SocketAddr>>
where
  RES: Resolver,
{
  let mut addrs: Vec<SocketAddr> = Vec::new();
  for seed in seeds {
    match seed {
      MaybeResolved::Resolved(s) => addrs.push(*s),
      MaybeResolved::Unresolved(a) => {
        let resolved = resolver
          .resolve(a)
          .await
          .map_err(|e| MemberlistError::Resolve(std::io::Error::other(e.to_string())))?;
        addrs.extend(resolved);
      }
    }
  }
  Ok(addrs)
}

#[cfg(all(test, feature = "tcp"))]
mod tests;
