//! The cloneable [`Memberlist`] handle and node construction.
//!
//! [`Memberlist::new`] builds the shared [`Engine`](memberlist_embedded::Engine)
//! over the caller's embassy-net sockets, seeds the reliable-plane pool with the
//! `N` TCP slots, installs the first listener, and hands back the handle paired
//! with the [`Runner`] the caller drives. The handle is a thin shared reference
//! (`Rc<Shared>`); clone it freely (it is `Clone`) to issue commands and read
//! membership from multiple places on the single executor.

use core::{net::SocketAddr, time::Duration};

use alloc::{rc::Rc, vec::Vec};

use embassy_net::{tcp::TcpSocket, udp::UdpSocket};
use embassy_time::Timer;
use memberlist_embedded::{
  AliveDelegate, ControlError, Engine, MergeDelegate, Options as EngineConfig, TransformOptions,
  transform::{CompressionOptions, EncryptionOptions},
};
use memberlist_proto::{
  EndpointOptions, Instant, Node,
  event::{Event, PingId, StreamId},
  typed::NodeState,
};

use crate::{
  config::Options,
  error::{InitError, OpError, SocketTimeoutOutOfRange},
  mailbox::{Command, Mailbox},
  runner::Runner,
  shared::{self, Shared},
  stream_io::{SlotId, SlotWake},
  time,
};

/// The largest [`Options::socket_timeout`](crate::Options::socket_timeout)
/// [`Memberlist::new`] accepts. A per-socket inactivity backstop longer than a day is
/// nonsensical for memberlist (reliable exchanges complete in milliseconds), and
/// rejecting larger values keeps the timeout safely within EVERY downstream duration
/// domain — the `embassy_time` tick count, its `as_micros` conversion (which
/// multiplies before dividing), and smoltcp's `i64` `Instant` arithmetic — at ANY
/// tick rate, so no configurable value can overflow that chain into a wrapped,
/// effectively-past deadline that would abort a TCP slot immediately.
const MAX_SOCKET_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(86_400);

/// Floor a portable `core::Duration` to whole `embassy_time` ticks at `tick_hz`,
/// exactly as [`embassy_time::Duration::from_ticks`] will store it.
///
/// Pure and parameterized on `tick_hz` (rather than reading the
/// [`embassy_time::TICK_HZ`] constant directly) so the coarse-rate rounding behavior is
/// unit-testable without rebuilding `embassy-time` at another tick rate. The nanosecond
/// basis keeps the conversion exact at fine tick rates (a microsecond basis would
/// silently drop sub-microsecond resolution); the `u128` saturating multiply and `u64`
/// clamp keep it total for any input, so an out-of-range duration converts to a
/// saturated tick count rather than panicking.
fn duration_to_ticks(d: core::time::Duration, tick_hz: u128) -> u64 {
  let ticks = d.as_nanos().saturating_mul(tick_hz) / 1_000_000_000;
  u64::try_from(ticks).unwrap_or(u64::MAX)
}

/// The whole-microsecond timeout embassy-net actually installs into smoltcp for an
/// already-floored embassy tick count at `tick_hz`.
///
/// embassy-net hands smoltcp `embassy_time::Duration::as_micros()` — a SECOND floor on
/// top of the tick flooring: at a tick rate finer than 1 MHz the tick count carries
/// sub-microsecond resolution this floor discards. Reproduced here (saturating `u128`) so
/// validation reasons about the value smoltcp receives, microseconds, not the
/// intermediate tick count.
fn installed_micros(ticks: u64, tick_hz: u128) -> u128 {
  u128::from(ticks).saturating_mul(1_000_000) / tick_hz
}

/// Validate `socket_timeout` against the engine deadlines IN THE INSTALLED MICROSECOND
/// DOMAIN at `tick_hz`, returning the embassy tick count the worker installs when it is
/// in range.
///
/// The value that actually gates the TCP socket is `socket_timeout` floored twice — to
/// whole embassy ticks (`from_ticks`) and then to whole microseconds (embassy-net's
/// `as_micros`, see [`installed_micros`]). The ordering invariant must hold on that
/// microsecond value, not on the portable input or the intermediate tick count: a coarse
/// tick rate can floor a portable-valid value below a deadline, and a tick rate finer than
/// 1 MHz can clear a tick comparison yet still install the same (or zero) microsecond
/// value. The deadlines are floored to microseconds too — the engine enforces them at full
/// resolution, but an installed whole-microsecond timeout exceeds the real deadline exactly
/// when it exceeds the deadline's microsecond floor. `socket_us > close_us` with
/// non-negative `close_us` also forces at least one installed microsecond, so an accepted
/// timeout is never the zero value smoltcp treats as an immediate abort. The upper bound is
/// checked first, in the portable domain, so the conversions cannot overflow (see
/// [`MAX_SOCKET_TIMEOUT`]).
fn checked_socket_timeout(
  socket: core::time::Duration,
  close: core::time::Duration,
  stream: core::time::Duration,
  tick_hz: u128,
) -> Option<u64> {
  if socket > MAX_SOCKET_TIMEOUT {
    return None;
  }
  let socket_ticks = duration_to_ticks(socket, tick_hz);
  let socket_us = installed_micros(socket_ticks, tick_hz);
  (socket_us > close.as_micros() && socket_us > stream.as_micros()).then_some(socket_ticks)
}

/// A cloneable handle to an embassy-net memberlist node.
///
/// Holds a shared reference to the node's [`Engine`](memberlist_embedded::Engine)
/// and the run loop's coordination signals. The async ops (`join` / `leave` /
/// `ping` / `send*`) enqueue work on the engine and (where they await) park on a
/// per-request signal the [`Runner`] fires; the sync query accessors borrow the
/// engine directly. Every method takes `&self`, so the handle is shared across
/// the executor's tasks.
///
/// `I` is the node identifier type (e.g. `smol_str::SmolStr`); the address type
/// is pinned to [`SocketAddr`].
pub struct Memberlist<I>
where
  I: memberlist_proto::Id,
{
  shared: Rc<Shared<I>>,
}

impl<I> Clone for Memberlist<I>
where
  I: memberlist_proto::Id,
{
  fn clone(&self) -> Self {
    Self {
      shared: self.shared.clone(),
    }
  }
}

impl<I> Memberlist<I>
where
  I: memberlist_proto::Id,
{
  /// Construct a node over the caller's embassy-net sockets, returning the handle
  /// and the [`Runner`] to drive.
  ///
  /// The caller owns the embassy-net [`Stack`](embassy_net::Stack) and supplies a
  /// gossip [`UdpSocket`] and the reliable-plane pool of `N` [`TcpSocket`]s (each
  /// built from the stack with its own rx/tx buffers). `new` binds the UDP socket
  /// to `cfg.port`, wires up the transport-agnostic [`Engine`], seeds the engine's
  /// reliable-plane pool with the `N` slots, dedicates one slot to the listener,
  /// and posts that slot's worker a `Listen` directive so it begins accepting on
  /// the bound port. No I/O occurs here.
  ///
  /// Drive the returned [`Runner`] with [`Runner::run`] (spawn it as a task or
  /// `select` it against an operation), and the embassy-net stack `Runner`
  /// separately, before any handle op can make progress.
  ///
  /// # Errors
  ///
  /// - [`InitError::TcpPoolTooSmall`] — `N < 2` (a listener plus one dial/accept
  ///   socket is the functional minimum).
  /// - [`InitError::ZeroBridgeRing`] — a zero
  ///   [`Options::tcp_socket_rx_bytes`](crate::Options::tcp_socket_rx_bytes) /
  ///   [`tcp_socket_tx_bytes`](crate::Options::tcp_socket_tx_bytes).
  /// - [`InitError::SocketTimeoutOutOfRange`] —
  ///   [`Options::socket_timeout`](crate::Options::socket_timeout), as embassy-net installs
  ///   it into smoltcp (floored to whole microseconds at the platform tick rate), is not
  ///   at least one microsecond and strictly greater than both `close_timeout` and the
  ///   machine's `stream_timeout`, or it is larger than the sane maximum it can be safely
  ///   installed at.
  /// - [`InitError::Engine`] — the shared engine rejected the configuration (zero
  ///   port / close-timeout, a non-routable or port-mismatched advertise address,
  ///   an over-ceiling gossip MTU, an unusable encryption keyring, or a
  ///   machine-endpoint / entropy failure).
  ///
  /// # Panics
  ///
  /// Panics if binding the supplied `udp_socket` to `cfg.port` fails — which, with
  /// a non-zero port (the engine rejects port 0 before this) and a fresh socket,
  /// embassy-net does not do. Bind the socket yourself before calling if you need
  /// to handle a bind error.
  pub fn new<'a, const N: usize>(
    cfg: Options,
    transform: TransformOptions,
    ep_cfg: EndpointOptions<I, SocketAddr>,
    mut udp_socket: UdpSocket<'a>,
    tcp_sockets: [TcpSocket<'a>; N],
    now: Instant,
  ) -> Result<(Self, Runner<'a, I, N>), InitError> {
    // Validate the driver-side pool sizing before touching the engine.
    if N < 2 {
      return Err(InitError::TcpPoolTooSmall(N));
    }
    if cfg.tcp_socket_rx_bytes == 0 || cfg.tcp_socket_tx_bytes == 0 {
      return Err(InitError::ZeroBridgeRing);
    }
    // The per-socket inactivity timeout is a backstop that must fire strictly AFTER the
    // engine's own deadlines (the reliable-exchange `stream_timeout` and the
    // graceful-close `close_timeout`), so embassy-net never aborts a slow-but-valid
    // exchange before the machine's own policy does, and must stay within
    // `MAX_SOCKET_TIMEOUT` so its conversion cannot overflow. The ordering is enforced on
    // the whole-microsecond value embassy-net actually installs into smoltcp (see
    // `checked_socket_timeout`), not on the portable input, because the two floors on the
    // way there (to embassy ticks, then to microseconds) can round a portable-valid value
    // down to or below a deadline.
    let stream_timeout = ep_cfg.stream_timeout();
    let socket_ticks = checked_socket_timeout(
      cfg.socket_timeout,
      cfg.close_timeout,
      stream_timeout,
      embassy_time::TICK_HZ as u128,
    )
    .ok_or(InitError::SocketTimeoutOutOfRange(
      SocketTimeoutOutOfRange {
        socket_timeout: cfg.socket_timeout,
        close_timeout: cfg.close_timeout,
        stream_timeout,
        max: MAX_SOCKET_TIMEOUT,
        tick_hz: embassy_time::TICK_HZ,
      },
    ))?;
    let socket_timeout = embassy_time::Duration::from_ticks(socket_ticks);

    // Bind the gossip socket to the node's port. With a non-zero port (the engine
    // re-checks this) and a fresh socket, embassy-net's `bind` cannot fail; a
    // misuse (already-bound socket or zero port slipping through) is a programming
    // error, so surface it as a panic rather than a recoverable variant.
    udp_socket
      .bind(cfg.port)
      .expect("binding the gossip UDP socket to the configured port failed");

    // Build the transport-agnostic engine from the port / close-timeout config.
    // `try_new_at` (not `new_at`) so a machine entropy failure, an unusable
    // encryption keyring, or a non-routable / port-mismatched advertise address
    // becomes a typed `InitError::Engine` rather than a panic.
    let engine_cfg = EngineConfig::new()
      .with_port(cfg.port)
      .with_close_timeout(cfg.close_timeout);
    // Forward the CIDR policy into the engine, which enforces it at the gossip
    // source (recv), the reliable accept, and membership admission.
    #[cfg(feature = "cidr")]
    let engine_cfg = match cfg.cidr_policy.clone() {
      Some(policy) => engine_cfg.with_cidr_policy(policy),
      None => engine_cfg,
    };
    let mut engine: Engine<I, SlotId> =
      Engine::try_new_at(engine_cfg, transform, ep_cfg, now).map_err(InitError::from)?;

    // Seed the engine's reliable-plane pool with every slot id, then dedicate slot
    // 0 to the listener. The engine owns this pool (it reaches it directly, not
    // through the `StreamIo` view), exactly like the smoltcp driver.
    for i in 0..N {
      engine.plane_mut().pool.push(SlotId(i));
    }

    // Build the per-slot mailboxes and command wakes. The mailbox ring capacities
    // come from the driver config so a slot's bridge never holds more than a
    // socket buffer's worth of un-handed-off bytes.
    let mailboxes: [_; N] = core::array::from_fn(|_| {
      core::cell::RefCell::new(Mailbox::new(
        cfg.tcp_socket_rx_bytes,
        cfg.tcp_socket_tx_bytes,
      ))
    });
    let cmd_wakes: [SlotWake; N] = core::array::from_fn(|_| SlotWake::new());

    // Dedicate one pooled slot to the listener and post its worker a `Listen`
    // directive so it begins accepting on the bound port at startup. (The engine
    // replenishes subsequent listeners via the `StreamIo` view; this is the
    // construction-time seed of the first one.)
    if let Some(listener) = engine.plane_mut().pool.take() {
      mailboxes[listener.0].borrow_mut().command = Command::Listen(cfg.port);
      // The worker is not yet running, so no wake is needed; it reads the command
      // on its first poll. Install the slot as the engine's listener.
      engine.set_listener(listener);
    }

    // Arm the SWIM scheduler so the probe / gossip / push-pull periodic timers
    // are live from the first pump (the engine returns `None` for its deadline
    // until this runs, which would leave the pump parked with no periodic wake).
    // Done here so the caller does not have to remember a separate `start` call.
    engine.start(now);

    let shared = Rc::new(Shared::new(engine));
    let runner = Runner {
      shared: shared.clone(),
      udp: udp_socket,
      tcp: tcp_sockets,
      mailboxes,
      cmd_wakes,
      // Bounds every worker's blocking socket await so a stalled peer cannot wedge
      // a slot (and, via the reuse gate, the pool) indefinitely. Already converted +
      // validated in the embassy-time tick domain above.
      socket_timeout,
      // The driver-side free-list is the `StreamIo` pool mirror; the engine owns
      // and drives its own pool, so this starts empty (and stays unused — see
      // `EmbassyStream`'s pool methods).
      free: Vec::new(),
    };

    Ok((Self { shared }, runner))
  }

  // ── Async operations ────────────────────────────────────────────────────────
  //
  // Per the async-trait convention these are inherent `async fn` on the struct
  // (not on any public trait), which is allowed. They enqueue work on the engine,
  // wake the pump, and — where they wait — park on a signal the Runner fires.

  /// Record intent to join the cluster via these seed addresses, resolving once
  /// the node has learned at least one peer (a push/pull state exchange synced).
  ///
  /// Enqueues a push/pull to each seed on the engine, then parks until
  /// `is_joined()` (woken by the Runner on each membership change, with a short
  /// timer backstop so a missed wake never hangs). A non-routable seed is dropped
  /// by the engine. The caller owns the overall deadline (drive this under a
  /// `select` with a timeout). Returns `Err(OpError::NotRunning)` if the node
  /// has left: a left node initiates no new join.
  pub async fn join(&self, seeds: &[SocketAddr]) -> Result<(), OpError> {
    // Fast path: already joined (e.g. a peer was injected, or a prior join).
    if shared::is_joined(&self.shared) {
      return Ok(());
    }
    self
      .shared
      .engine
      .borrow_mut()
      .join(seeds)
      .map_err(|_| OpError::NotRunning)?;
    self.shared.wake_pump();

    // Park until joined. The Runner pulses `join_wake` on every membership change;
    // race it against a short timer so a wake the single-consumer signal delivered
    // to another concurrent joiner only costs an interval, never a hang.
    loop {
      if shared::is_joined(&self.shared) {
        return Ok(());
      }
      // Ignoring the `Either`: whichever of the membership wake or the timer fired,
      // the loop simply re-checks `is_joined`.
      let _ = embassy_futures::select::select(
        self.shared.join_wake.wait(),
        Timer::after(embassy_time::Duration::from_millis(20)),
      )
      .await;
    }
  }

  /// Begin leaving the cluster (gossip the departure). Returns immediately after
  /// enqueuing; the `LeftCluster` event surfaces via [`poll_event`](Self::poll_event).
  ///
  /// Returns `Err(OpError::NotRunning)` if the node is not in a running state
  /// (already left or never started).
  pub fn leave(&self) -> Result<(), OpError> {
    let now = time::now();
    let r = self
      .shared
      .engine
      .borrow_mut()
      .leave(now)
      .map_err(|_| OpError::NotRunning);
    self.shared.wake_pump();
    r
  }

  /// Send a direct UDP ping to `node`, resolving with the measured round-trip
  /// time or [`OpError::PingTimeout`] if the peer did not ack within the probe
  /// timeout.
  ///
  /// Issues the ping on the engine (correlation token: [`PingId`]) and parks on a
  /// per-request signal the Runner fires on the matching `PingCompleted` /
  /// `PingFailed`.
  pub async fn ping(&self, node: Node<I, SocketAddr>) -> Result<Duration, OpError> {
    let now = time::now();
    let ping_id: PingId = self
      .shared
      .engine
      .borrow_mut()
      .ping(node, now)
      .map_err(|_| OpError::NotRunning)?;
    let reply = self.shared.register_ping(ping_id);
    self.shared.wake_pump();
    reply.wait().await
  }

  /// Enqueue a directed unreliable UDP user-data packet to `to` (best-effort).
  ///
  /// Returns immediately. Returns `Err` when the framed payload exceeds the
  /// configured gossip MTU.
  pub fn send(&self, to: SocketAddr, payload: bytes::Bytes) -> Result<(), memberlist_proto::Error> {
    let r = self.shared.engine.borrow_mut().send(to, payload);
    self.shared.wake_pump();
    r
  }

  /// Enqueue multiple directed unreliable UDP user-data packets to `to`,
  /// compounding them into one datagram when they fit the gossip MTU together.
  ///
  /// Returns immediately. Returns `Err` when the compound frame exceeds the
  /// gossip MTU.
  pub fn send_many(
    &self,
    to: SocketAddr,
    payloads: &[bytes::Bytes],
  ) -> Result<(), memberlist_proto::Error> {
    let r = self.shared.engine.borrow_mut().send_many(to, payloads);
    self.shared.wake_pump();
    r
  }

  /// Reliably deliver `payload` to `to` over a dedicated TCP stream, resolving
  /// once the exchange completes (`Ok`) or fails ([`OpError::SendFailed`]).
  ///
  /// Issues the user-message exchange on the engine and parks on a signal the
  /// Runner fires when THIS send's exchange terminates. Completion is correlated
  /// by the [`StreamId`] the engine returns (mapped to the exchange at its
  /// `Connect`), so overlapping or out-of-order completions — concurrent sends, or
  /// sends to different peers finishing in any order — each resolve their own
  /// caller, never by arrival order.
  pub async fn send_reliable(&self, to: SocketAddr, payload: bytes::Bytes) -> Result<(), OpError> {
    let now = time::now();
    let sid: StreamId = self
      .shared
      .engine
      .borrow_mut()
      .send_reliable(to, payload, now)
      .map_err(|_| OpError::NotRunning)?;
    let reply = self.shared.register_send(sid);
    self.shared.wake_pump();
    reply.wait().await
  }

  /// Queue an application user-data payload for piggyback gossip to peers
  /// (best-effort). Returns immediately.
  ///
  /// Returns `Err(Error::UserBroadcastExceedsMtu)` when the lone framed datagram
  /// would exceed the gossip MTU.
  pub fn queue_user_broadcast(&self, data: bytes::Bytes) -> Result<(), memberlist_proto::Error> {
    let r = self.shared.engine.borrow_mut().queue_user_broadcast(data);
    self.shared.wake_pump();
    r
  }

  /// Replace this node's advertised metadata at runtime (best-effort gossip).
  /// Returns immediately; peers observe the change as `Event::NodeUpdated`.
  pub fn update_node_metadata(
    &self,
    meta: memberlist_proto::typed::Meta,
  ) -> Result<(), memberlist_proto::Error> {
    let r = self.shared.engine.borrow_mut().update_node_metadata(meta);
    self.shared.wake_pump();
    r
  }

  /// Set the application state snapshot exchanged during push/pull. Returns
  /// immediately; surfaces on the receiving peer as `Event::RemoteStateReceived`.
  pub fn set_local_state(&self, state: bytes::Bytes) -> Result<(), memberlist_proto::Error> {
    let r = self.shared.engine.borrow_mut().set_local_state(state);
    self.shared.wake_pump();
    r
  }

  /// Set the payload attached to outgoing ping acknowledgements. Returns
  /// immediately; a probing peer receives it in its `Event::PingCompleted`.
  pub fn set_ack_payload(&self, payload: bytes::Bytes) -> Result<(), memberlist_proto::Error> {
    let r = self.shared.engine.borrow_mut().set_ack_payload(payload);
    self.shared.wake_pump();
    r
  }

  /// Install a custom peer-admission predicate, composed with the built-in
  /// routable-address filter (both must admit). Call before [`join`](Self::join)
  /// so no peer is admitted before the policy applies.
  pub fn set_alive_delegate(&self, delegate: impl AliveDelegate<I, SocketAddr>) {
    self.shared.engine.borrow_mut().set_alive_delegate(delegate);
    self.shared.wake_pump();
  }

  /// Install a custom join-merge predicate, consulted on each join push/pull
  /// merge. A delegate that rejects the merge fails the join.
  pub fn set_merge_delegate(&self, delegate: impl MergeDelegate<I, SocketAddr>) {
    self.shared.engine.borrow_mut().set_merge_delegate(delegate);
    self.shared.wake_pump();
  }

  // ── Sync queries / accessors ────────────────────────────────────────────────
  //
  // Thin forwards over the live machine endpoint (a brief `RefCell` borrow), so
  // each read reflects the last pump tick with no snapshot lag.

  /// Drain one application-visible membership or lifecycle event, if any.
  ///
  /// Reads from the Runner-populated event buffer: the Runner is the sole
  /// `poll_event` caller on the engine (it drains events each pump to resolve
  /// parked ping/send waiters) and re-buffers every event here for the
  /// application, so no event is lost to that internal drain.
  #[inline]
  pub fn poll_event(&self) -> Option<Event<I, SocketAddr>> {
    self.shared.pop_app_event()
  }

  /// Number of known members, including the local node itself.
  #[inline]
  pub fn num_members(&self) -> usize {
    self.shared.engine.borrow().num_members()
  }

  /// Number of outbound reliable exchanges currently awaiting completion in the
  /// engine's send-correlation map — a diagnostic that returns to zero once every
  /// dispatched reliable exchange (join push/pull, user-message send) has completed
  /// and been drained, witnessing that the map does not leak per exchange.
  #[inline]
  pub fn outbound_correlation_len(&self) -> usize {
    self.shared.engine.borrow().outbound_correlation_len()
  }

  /// Whether this node has learned at least one peer.
  #[inline]
  pub fn is_joined(&self) -> bool {
    self.shared.engine.borrow().is_joined()
  }

  /// The `NodeState` for `id`, stamped with the current FSM liveness, or `None`
  /// if `id` is unknown.
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<NodeStateHandle<I>> {
    self.shared.engine.borrow().by_id(id)
  }

  /// All members currently in the `Alive` FSM state.
  #[inline]
  pub fn online_members(&self) -> Vec<NodeStateHandle<I>> {
    self.shared.engine.borrow().online_members()
  }

  /// Count of members currently in the `Alive` FSM state.
  #[inline]
  pub fn num_online_members(&self) -> usize {
    self.shared.engine.borrow().num_online_members()
  }

  /// All known members (Alive + Suspect + Dead/Left), each stamped with the
  /// current FSM liveness.
  #[inline]
  pub fn members(&self) -> Vec<NodeStateHandle<I>> {
    self.shared.engine.borrow().members()
  }

  /// Members matching `pred`, each stamped with the current FSM liveness.
  #[inline]
  pub fn members_by(
    &self,
    pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool,
  ) -> Vec<NodeStateHandle<I>> {
    self.shared.engine.borrow().members_by(pred)
  }

  /// Count of members matching `pred`.
  #[inline]
  pub fn num_members_by(&self, pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool) -> usize {
    self.shared.engine.borrow().num_members_by(pred)
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  #[inline]
  pub fn members_map_by<O>(&self, f: impl FnMut(&NodeState<I, SocketAddr>) -> Option<O>) -> Vec<O> {
    self.shared.engine.borrow().members_map_by(f)
  }

  /// The local node's Lifeguard health score (`0` = fully healthy).
  #[inline]
  pub fn health_score(&self) -> usize {
    self.shared.engine.borrow().health_score()
  }

  /// The local node's id.
  #[inline]
  pub fn local_id(&self) -> I {
    self.shared.engine.borrow().local_id()
  }

  /// The local node's advertised `SocketAddr`.
  #[inline]
  pub fn advertise_address(&self) -> SocketAddr {
    shared::advertise_address(&self.shared)
  }

  /// The local node's `NodeState`, stamped with the current FSM liveness.
  #[inline]
  pub fn local_state(&self) -> NodeStateHandle<I> {
    self.shared.engine.borrow().local_state()
  }

  /// Whether `id` is currently Alive from this node's perspective.
  #[inline]
  pub fn is_alive(&self, id: &I) -> bool {
    self.shared.engine.borrow().is_alive(id)
  }

  /// Whether `id` is currently Dead from this node's perspective.
  #[inline]
  pub fn is_dead(&self, id: &I) -> bool {
    self.shared.engine.borrow().is_dead(id)
  }

  /// Seed a statically-known peer as Alive (bootstrap membership without the TCP
  /// push-pull join path). A non-routable peer is dropped by the engine.
  pub fn inject_alive(&self, id: I, peer: SocketAddr) {
    let now = time::now();
    self.shared.engine.borrow_mut().inject_alive(id, peer, now);
    self.shared.wake_pump();
  }

  // ── Reliable-plane diagnostics (test/operator visibility) ────────────────────
  //
  // Thin `#[doc(hidden)]` reads over the engine's reliable plane, mirroring the
  // smoltcp driver's diagnostics so the lifecycle invariants (pool recovery,
  // abort/reuse, listener self-healing) can be witnessed directly rather than only
  // inferred from membership.

  /// Number of pooled TCP slots currently free (not assigned to an active
  /// exchange or the listener).
  ///
  /// A diagnostic for the reliable plane's pool-recovery invariant: a slot
  /// aborted/closed must return to the free-list once its worker has reset the
  /// socket, so repeated dial/abort churn must not permanently shrink the pool.
  #[doc(hidden)]
  #[inline]
  pub fn pool_free_count(&self) -> usize {
    self.shared.engine.borrow().pool_free_count()
  }

  /// Number of inbound reliable connections accepted on the TCP listener since
  /// construction. A diagnostic for the listener self-healing invariant.
  #[doc(hidden)]
  #[inline]
  pub fn accepted_inbound_count(&self) -> u64 {
    self.shared.engine.borrow().accepted_inbound_count()
  }

  /// Number of TCP slots currently parked mid-close (our FIN sent, the peer's not
  /// yet completed), awaiting reap.
  #[doc(hidden)]
  #[inline]
  pub fn closing_count(&self) -> usize {
    self.shared.engine.borrow().closing_count()
  }

  /// Number of reliable exchanges currently half-closed (local FIN emitted, still
  /// mapped awaiting the peer's reply and/or FIN).
  #[doc(hidden)]
  #[inline]
  pub fn half_closed_count(&self) -> usize {
    self.shared.engine.borrow().half_closed_count()
  }

  /// Whether a passive-open listener slot is currently installed.
  #[doc(hidden)]
  #[inline]
  pub fn listener_present(&self) -> bool {
    self.shared.engine.borrow().listener_present()
  }

  /// Number of reliable exchanges still in `PendingDial` (dial requested, no slot
  /// assigned yet).
  #[doc(hidden)]
  #[inline]
  pub fn pending_dial_count(&self) -> usize {
    self.shared.engine.borrow().pending_dial_count()
  }

  /// Replace the gossip+stream compression policy at runtime. Returns
  /// `NotRunning` after [`leave`](Self::leave).
  #[inline]
  pub fn set_compression_options(
    &self,
    opts: CompressionOptions,
  ) -> Result<(), memberlist_proto::Error> {
    let r = self
      .shared
      .engine
      .borrow_mut()
      .set_compression_options(opts);
    self.shared.wake_pump();
    r
  }

  /// Replace the gossip+stream encryption policy at runtime (key rotation). The
  /// keyring is validated before it is applied. Returns `NotRunning` after
  /// [`leave`](Self::leave) (gated before validation).
  #[inline]
  pub fn set_encryption_options(&self, opts: EncryptionOptions) -> Result<(), ControlError> {
    let r = self.shared.engine.borrow_mut().set_encryption_options(opts);
    self.shared.wake_pump();
    r
  }
}

/// The reference-counted [`NodeState`] the engine returns. The engine surfaces
/// `std::sync::Arc` (aliased to `alloc::sync::Arc` under no_std), so the handle
/// re-exports that exact type rather than re-wrapping in a single-executor `Rc`
/// (which would not interoperate with the engine's own `Arc`-typed returns).
pub type NodeStateHandle<I> = std::sync::Arc<NodeState<I, SocketAddr>>;

#[cfg(test)]
mod tests;
