//! The transport-agnostic driving core: construction, accessors, the membership
//! query/command API, and the link-layer-independent `pump`.
//!
//! [`Engine`] owns the memberlist SWIM machine ([`StreamEndpoint`]), the
//! reliable-plane connection state machine ([`ReliablePlane`]), the gossip
//! scratch buffer, and the join-seed queue. It performs NO socket I/O: a driver
//! supplies the link-layer stack tick plus a [`GossipIo`] and a [`StreamIo`], and
//! [`Engine::pump`] drives the machine over them. A driver wraps the engine,
//! owning the actual sockets/interface; see
//! [`memberlist-smoltcp`](https://docs.rs/memberlist-smoltcp).

use core::net::SocketAddr;

// Under `no_std + alloc` the prelude does not bring `Box` / `VecDeque` into
// scope; import them explicitly from the aliased `std` (which is `alloc` in
// that build).
#[cfg(feature = "std")]
use std::collections::VecDeque;
#[cfg(not(feature = "std"))]
use std::{boxed::Box, collections::VecDeque};

use memberlist_proto::{
  AliveDelegate, Endpoint, EndpointOptions, Instant, LabelOptions, MergeDelegate, Node,
  PushPullKind, RawRecords, StreamId,
  event::{PingId, Transmit},
  streams::{ExchangeId, StreamAction, StreamEndpoint},
  typed::{Alive, NodeState, State},
};

use hashbrown::HashMap;

use crate::{
  GossipIo, InitError, Options, StreamIo, TransformOptions,
  addr::socket_addr_is_routable,
  cidr::{CidrFilter, cidr_blocks},
  error::GossipMtuTooLarge,
  reliable::{ConnState, Connection, ReliablePlane},
};

/// The maximum UDP payload (`u16` length minus the 8-byte UDP header), the hard
/// ceiling for an on-wire gossip datagram. Matches the async drivers.
const UDP_PAYLOAD_MAX: usize = 65507;

/// Size the inbound-gossip receive scratch from the effective gossip MTU.
///
/// The machine caps an outbound gossip datagram's PLAINTEXT at the configured
/// [`EndpointOptions::gossip_mtu`]; the on-wire datagram can then exceed that by
/// up to [`memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD`] (the checksum wrapper)
/// plus [`memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD`] (30 B of wrapper header,
/// nonce, and AEAD tag) when both transforms are enabled. The buffer must hold
/// the largest such on-wire datagram, so it is sized to
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD`,
/// floored at 1500 (the common Ethernet payload) so the default
/// ([`memberlist_proto::DEFAULT_GOSSIP_MTU`], 1400) keeps a little headroom and
/// any sub-1500 MTU never under-sizes it.
///
/// A driver's datagram receive (e.g. smoltcp's `udp::Socket::recv_slice`) may
/// POP the datagram before checking the caller's slice length, so a datagram
/// larger than this buffer is consumed and lost. Sizing the buffer from the same
/// knob the machine uses to bound outbound gossip means a correctly-configured
/// cluster never truncates an in-budget datagram.
fn gossip_recv_buf_size(gossip_mtu: usize) -> usize {
  (gossip_mtu
    + memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD
    + memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD)
    .max(1500)
}

/// An [`AliveDelegate`] that admits a peer only when its advertised address is a
/// routable destination ([`socket_addr_is_routable`]).
///
/// The machine calls `notify_alive` inline for EVERY admitted Alive — gossip and
/// join push/pull alike (`Endpoint::process_alive`) — so this one filter drops a
/// non-routable address at admission on both planes. The bad address is never
/// stored as a member and so is never re-gossiped, stopping cluster-wide
/// propagation of a member address that no node could ever send a useful packet
/// to. It is the propagation-prevention layer; the egress chokepoints remain the
/// last-line drop for any address that reaches the driver by any other path.
struct RoutableAddrFilter;

impl<I> AliveDelegate<I, SocketAddr> for RoutableAddrFilter
where
  I: memberlist_proto::Id,
{
  fn notify_alive(&self, peer: &NodeState<I, SocketAddr>) -> bool {
    socket_addr_is_routable(peer.address_ref())
  }
}

/// An [`AliveDelegate`] that admits a peer only when BOTH the built-in routable
/// filter and a caller-supplied delegate accept it.
///
/// The routable filter is load-bearing on the no_std core — it stops a
/// non-routable address from being stored and re-gossiped cluster-wide — so a
/// caller's custom admission policy composes with it (logical AND) rather than
/// replacing it: a custom delegate can further restrict admission, never loosen
/// the routable guard.
struct RoutableAnd<D>(D);

impl<I, D> AliveDelegate<I, SocketAddr> for RoutableAnd<D>
where
  I: memberlist_proto::Id,
  D: AliveDelegate<I, SocketAddr>,
{
  fn notify_alive(&self, peer: &NodeState<I, SocketAddr>) -> bool {
    socket_addr_is_routable(peer.address_ref()) && self.0.notify_alive(peer)
  }
}

/// The transport-agnostic memberlist driving core.
///
/// Composes the memberlist SWIM machine with the pooled-stream reliable plane,
/// driving both through the [`GossipIo`] / [`StreamIo`] traits a driver supplies
/// to [`pump`](Engine::pump). The engine holds NO sockets — the driver owns the
/// link-layer stack and its UDP/stream sockets — so the same core runs under a
/// caller-driven poll loop (smoltcp) or an async executor (embassy-net).
///
/// `I` is the node identifier type (e.g. `SmolStr`); `A` is pinned to
/// `core::net::SocketAddr`. `C` is the driver's opaque connection handle
/// ([`StreamIo::Conn`]).
pub struct Engine<I, C>
where
  I: memberlist_proto::Id,
{
  endpoint: StreamEndpoint<I, SocketAddr, RawRecords>,
  /// Sizing / port configuration; retained for the reliable-plane paths.
  cfg: Options,
  /// Pooled connection handles and the exchange-to-handle map for the reliable
  /// plane.
  plane: ReliablePlane<C>,
  /// Heap scratch for one inbound gossip datagram, sized once at construction
  /// from the configured gossip MTU (see [`gossip_recv_buf_size`]) and reused
  /// every pump. Heap-resident (not a per-pump stack array) so a large MTU does
  /// not blow a constrained stack and the allocation happens exactly once.
  gossip_recv: std::vec::Vec<u8>,
  /// Seed addresses queued by `join` that have not yet been handed to the
  /// machine. Drained in the machine-pump phase of each `pump` tick: one
  /// `start_push_pull(seed, Join, now)` per entry, which queues a `DialRequested`
  /// the machine immediately services into a `Connect` action consumed later that
  /// same tick. Keeping the queue on the engine (rather than the reliable plane)
  /// because join intent is an engine-level policy — the machine drives the
  /// actual exchange state, while this queue records which seeds are still
  /// waiting for a first contact attempt.
  pending_seeds: VecDeque<SocketAddr>,
  /// Maps each outbound reliable exchange's [`ExchangeId`] to the [`StreamId`] the
  /// originating `send_reliable` / `join` / probe call returned, captured from the
  /// `Connect` action (which carries both ids). [`poll_event`](Self::poll_event)
  /// removes an entry when its exchange completes — for EVERY consumer, so the map
  /// cannot grow unbounded — and stashes the removed `StreamId` in
  /// [`last_completed_send`](Self::last_completed_send) for a driver that awaits a
  /// reliable send and resolves the exact waiter by that `StreamId`, never by
  /// arrival order (which cross-resolves overlapping or out-of-order completions).
  outbound_stream_ids: HashMap<ExchangeId, StreamId>,
  /// The `StreamId` that the most recent [`poll_event`](Self::poll_event) removed
  /// from `outbound_stream_ids` when it returned an `ExchangeCompleted`, or `None`
  /// otherwise. Valid only until the next `poll_event`.
  last_completed_send: Option<StreamId>,
  /// Cluster label applied to the gossip codec on both encode and decode.
  ///
  /// When `Some`, the gossip codec stamps a label prefix onto every outbound
  /// datagram and rejects any inbound datagram whose label does not match,
  /// isolating this cluster's gossip plane from nodes that carry a different
  /// label (or no label). `None` disables labeling, which is the default
  /// behaviour for an unlabelled cluster.
  label: Option<bytes::Bytes>,
  /// CIDR transport filter: a gossip datagram from a blocked source IP (recv) or
  /// a reliable connection from a blocked peer IP (accept) is dropped before the
  /// machine sees it. `()` when the `cidr` feature is off. The same policy also
  /// gates membership admission via the routable-address alive filter, which the
  /// constructor installs with this policy as its inner predicate.
  cidr_policy: CidrFilter,
}

impl<I, C> Engine<I, C>
where
  I: memberlist_proto::Id,
  C: Copy + Eq + core::hash::Hash,
{
  /// Construct an engine, panicking on a misconfiguration or entropy failure.
  ///
  /// This is the convenience wrapper over [`try_new_at`](Self::try_new_at); it
  /// has the same parameters and behaviour but unwraps the result. Use it only
  /// when the configuration is a static constant known to be valid and the build
  /// targets a host whose entropy source cannot fail.
  ///
  /// # Panics
  ///
  /// Panics if [`try_new_at`](Self::try_new_at) returns an [`InitError`] — e.g.
  /// on a zero/over-ceiling gossip MTU, a non-routable or port-mismatched
  /// advertise address, an entropy failure, or a machine-endpoint init failure.
  pub fn new_at(
    cfg: Options,
    transform: TransformOptions,
    ep_cfg: EndpointOptions<I, SocketAddr>,
    now: Instant,
  ) -> Self {
    Self::try_new_at(cfg, transform, ep_cfg, now)
      .expect("Engine::new_at: invalid configuration or entropy failure; use try_new_at to handle")
  }

  /// Fallibly construct an engine.
  ///
  /// Wires up the [`StreamEndpoint`] over the machine's `Endpoint` and sizes the
  /// gossip receive scratch. No sockets are bound — the driver owns the gossip
  /// and reliable-stream sockets — and no I/O occurs here.
  ///
  /// # Parameters
  ///
  /// - `cfg`: engine port / timeout configuration.
  /// - `transform`: cross-transport gossip + reliable-plane compression and
  ///   encryption, plus the reliable-plane (TCP) cluster label. A configured
  ///   encryption keyring is probed here (see Errors); the default is fully
  ///   disabled and unlabelled.
  /// - `ep_cfg`: machine identity (`id`, `advertise`, timing knobs, …).
  /// - `now`: the driver's clock reading at construction (passed to the
  ///   `Endpoint` so timers start from a consistent origin).
  ///
  /// # Errors
  ///
  /// Returns [`InitError`] instead of panicking when the configuration is
  /// invalid:
  ///
  /// - [`InitError::ZeroPort`] — `cfg.port` is zero.
  /// - [`InitError::ZeroCloseTimeout`] — `cfg.close_timeout` is zero.
  /// - [`InitError::GossipMtuTooLarge`] — the configured gossip MTU's on-wire
  ///   datagram cannot fit a UDP packet.
  /// - [`InitError::NonRoutableAdvertiseAddr`] — the advertise address is the
  ///   unspecified address, a multicast/broadcast IP, or port 0.
  /// - [`InitError::AdvertisePortMismatch`] — the advertised port differs from
  ///   the bound `cfg.port`.
  /// - [`InitError::Endpoint`] — the machine endpoint failed to initialize.
  /// - [`InitError::Encryption`] — `transform.encryption` carries a keyring with
  ///   a key whose AEAD backend was not compiled into this binary.
  pub fn try_new_at(
    cfg: Options,
    transform: TransformOptions,
    ep_cfg: EndpointOptions<I, SocketAddr>,
    now: Instant,
  ) -> Result<Self, InitError> {
    // Reject a zero port up front. A link layer's bind/listen rejects port 0 and
    // no peer can dial it; the reliable plane's listen and ephemeral-port dialing
    // both assume a non-zero bound port.
    if cfg.port == 0 {
      return Err(InitError::ZeroPort);
    }

    // Reject a gossip MTU whose on-wire datagram cannot fit a UDP packet. A
    // driver sizes its gossip arenas and the receive scratch from
    // `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD` (the
    // largest on-wire datagram the machine can emit); an over-ceiling
    // `gossip_mtu` would overflow that addition — a panic in a checked build, a
    // wrap to an undersized arena in release that then silently truncates
    // in-budget gossip. Bounding it here makes every downstream
    // `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD` safe
    // and mirrors the async drivers' reject-not-clamp doctrine.
    let gossip_mtu_ceiling = UDP_PAYLOAD_MAX
      - memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD
      - memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD;
    if ep_cfg.gossip_mtu() > gossip_mtu_ceiling {
      return Err(InitError::GossipMtuTooLarge(GossipMtuTooLarge {
        gossip_mtu: ep_cfg.gossip_mtu(),
        ceiling: gossip_mtu_ceiling,
      }));
    }

    // Reject a zero graceful-close timeout. `close_timeout` bounds the reliable
    // graceful-close drain: a connection still `Closing` past `now +
    // close_timeout` is force-aborted. Zero sets that deadline to `now`, so every
    // graceful close is force-aborted immediately — the drain never runs and an
    // in-flight push/pull response is truncated.
    if cfg.close_timeout.is_zero() {
      return Err(InitError::ZeroCloseTimeout);
    }

    // Size the inbound-gossip scratch from the configured gossip MTU BEFORE
    // `ep_cfg` is moved into `Endpoint::try_new_at`. The buffer must hold the
    // largest on-wire gossip datagram the machine will emit; reading the knob
    // here keeps the driver's ingress in lockstep with the machine's egress bound
    // (see `gossip_recv_buf_size`).
    let gossip_recv = std::vec![0u8; gossip_recv_buf_size(ep_cfg.gossip_mtu())];

    // Validate the encryption configuration before any endpoint exists, so an
    // unusable keyring is a typed construction error rather than a silent runtime
    // drop of every encrypted gossip datagram. Probe each configured key (primary
    // then secondaries) by encrypting an empty frame: a key whose AEAD backend was
    // not compiled into this binary fails here with
    // `EncryptionError::UnsupportedAlgorithm`.
    if let Some(keyring) = transform.encryption.keyring() {
      for key in core::iter::once(keyring.primary_ref()).chain(keyring.secondaries()) {
        memberlist_proto::encode_encrypted_frame(key.algorithm(), key, b"")
          .map_err(InitError::Encryption)?;
      }
    }

    // Validate the gossip checksum configuration before any endpoint exists, so
    // an algorithm whose backend feature is absent is a typed construction error
    // rather than a silent runtime drop of every gossip datagram. The probe is a
    // trial `apply` of an empty payload: a configured-but-unbuilt algorithm fails
    // here with a `ChecksumError`, while a disabled (no-algorithm) policy is
    // always usable. Checksum is a gossip-plane concern only; reliable streams
    // carry no checksum.
    transform.checksum.apply(&[]).map_err(InitError::Checksum)?;

    // Reject a non-routable advertise address before the endpoint exists. A node
    // must advertise an address its peers can route a reply to; an
    // unspecified/multicast/broadcast IP or port 0 would be gossiped cluster-wide
    // and then be useless to every peer that selected it as an egress destination.
    // Fail fast here rather than admit a self-description no peer can use.
    if !socket_addr_is_routable(ep_cfg.advertise_addr_ref()) {
      return Err(InitError::NonRoutableAdvertiseAddr(
        *ep_cfg.advertise_addr_ref(),
      ));
    }

    // The machine advertises ONE SocketAddr that peers use for BOTH gossip (UDP)
    // and reliable (TCP) — the single-port memberlist model (one `cfg.port` binds
    // both). The advertised port must match it; otherwise a peer reaches neither
    // plane (its gossip and its push/pull dial both route to a port nothing is
    // listening on) and join/state-sync silently partitions. A direct embedded
    // interface has no NAT, so the advertised port is the bound port.
    if ep_cfg.advertise_addr_ref().port() != cfg.port {
      return Err(InitError::AdvertisePortMismatch);
    }

    // Wire up the machine's stream endpoint. `peer_to_socket` is identity because
    // `A = SocketAddr`; `sni_provider` returns `None` (no TLS / no SNI).
    // `try_new_at` (not `new_at`) so a machine entropy failure becomes
    // `InitError::Endpoint` rather than a panic. The reliable-plane label, the
    // cross-transport compression/encryption, and the gossip-plane checksum all
    // come from `transform`; with a default `TransformOptions` they are disabled,
    // reproducing the plain no-label endpoint.
    let mut ep = Endpoint::try_new_at(ep_cfg, now).map_err(InitError::Endpoint)?;
    // The CIDR policy gates the alive delegate (composed just below) and the
    // transport-boundary recv/accept guards (stored on the engine). Cloned out of
    // `cfg` because `cfg` is moved into the engine below.
    #[cfg(feature = "cidr")]
    let cidr_policy: CidrFilter = cfg.cidr_policy.clone();
    #[cfg(not(feature = "cidr"))]
    let cidr_policy: CidrFilter = ();
    // Install the routable-address admission filter on the raw `Endpoint` BEFORE
    // it is moved into the `StreamEndpoint`. The machine consults it inline for
    // every inbound Alive (gossip AND join push/pull), so a peer advertising a
    // non-routable address is dropped at admission — never stored, never
    // re-gossiped — preventing cluster-wide propagation of an address no node
    // could send a useful packet to. When a CIDR policy is set, the routable
    // filter wraps it (routable AND in-policy), so one policy also gates
    // membership admission by the peer's self-advertised address.
    #[cfg(feature = "cidr")]
    match cidr_policy.clone() {
      Some(policy) => ep.set_alive_delegate(RoutableAnd(policy)),
      None => ep.set_alive_delegate(RoutableAddrFilter),
    }
    #[cfg(not(feature = "cidr"))]
    ep.set_alive_delegate(RoutableAddrFilter);
    // Build the reliable-plane label options from the single validated source.
    // The label is already validated at the TransformOptions setter; `new_in`
    // is infallible here.
    let label_bytes = transform.label.as_deref().map(|b| b.to_vec());
    let mut label_opts = LabelOptions::new_in(label_bytes, ());
    if transform.skip_inbound_label_check {
      label_opts = label_opts.skip_inbound_label_check();
    }
    // Retain the validated label for the gossip codec (same source, both planes
    // share one label so they cannot diverge).
    let label = transform.label.clone();
    let mut endpoint = StreamEndpoint::with_compression(
      ep,
      label_opts,
      Box::new(|_: &SocketAddr| -> Option<std::string::String> { None }),
      Box::new(|addr: &SocketAddr| *addr),
      transform.compression,
    )
    .with_encryption(transform.encryption);
    // Gossip-plane (unreliable) checksum. Unlike compression/encryption it is
    // not chainable on the builder because reliable streams carry no checksum
    // (no per-bridge fan-out); the in-place setter updates only the gossip
    // field. With a default `TransformOptions` no algorithm is selected and the
    // gossip codec stays identity.
    endpoint
      .set_checksum_options(transform.checksum)
      .map_err(InitError::Checksum)?;

    Ok(Self {
      endpoint,
      cfg,
      plane: ReliablePlane::new(),
      gossip_recv,
      pending_seeds: VecDeque::new(),
      outbound_stream_ids: HashMap::new(),
      last_completed_send: None,
      label,
      cidr_policy,
    })
  }

  /// Mutable access to the reliable plane's pool, for a driver to push its
  /// pre-created connection handles and install the initial listener at
  /// construction.
  ///
  /// The engine holds no sockets; the driver allocates its reliable-stream
  /// sockets and registers their handles here. After pushing the pool, the driver
  /// dedicates one handle to the listener via [`set_listener`](Self::set_listener).
  #[inline]
  pub fn plane_mut(&mut self) -> &mut ReliablePlane<C> {
    &mut self.plane
  }

  /// Install the initial passive-open listener handle, set by the driver after it
  /// has `listen`ed on that connection slot at construction.
  #[inline]
  pub fn set_listener(&mut self, c: C) {
    self.plane.listener = Some(c);
  }

  /// The configured local port (gossip + reliable listener both bind it).
  #[inline]
  pub fn port(&self) -> u16 {
    self.cfg.port
  }

  // ── Reliable-plane diagnostics ──────────────────────────────────────────────
  //
  // Thin reads over the reliable plane, surfaced for the driver to re-export as
  // its own `#[doc(hidden)]` test diagnostics.

  /// Number of inbound reliable connections accepted on the listener since
  /// construction.
  #[inline]
  pub fn accepted_inbound_count(&self) -> u64 {
    self.plane.accepted_inbound
  }

  /// Number of pooled connection slots currently free.
  #[inline]
  pub fn pool_free_count(&self) -> usize {
    self.plane.pool.free_len()
  }

  /// Number of connection slots currently parked mid-close.
  #[inline]
  pub fn closing_count(&self) -> usize {
    self.plane.closing.len()
  }

  /// Number of reliable exchanges currently half-closed (local FIN emitted, still
  /// mapped awaiting the peer's reply and/or FIN).
  #[inline]
  pub fn half_closed_count(&self) -> usize {
    self.plane.half_closed_count()
  }

  /// Whether a passive-open listener slot is currently installed.
  #[inline]
  pub fn listener_present(&self) -> bool {
    self.plane.listener.is_some()
  }

  /// Number of reliable exchanges still in `PendingDial` (dial requested, pool
  /// exhausted, no slot assigned yet).
  #[inline]
  pub fn pending_dial_count(&self) -> usize {
    self.plane.pending_dial_count()
  }

  // ── Membership queries ──────────────────────────────────────────────────────
  //
  // These are thin `&self` reads over the live `Endpoint` inside the
  // `StreamEndpoint`. Unlike the async drivers (compio, reactor) there is no
  // `ArcSwap` snapshot: reads go directly to the machine state, so they always
  // reflect the result of the last `pump` tick with no snapshot lag.
  //
  // FSM liveness: the `NodeState.state()` wire field is frozen at the last
  // `Alive` broadcast. The real, gossip-tracked liveness is
  // `endpoint.member_liveness(id)`. Every method that returns a `NodeState`
  // stamps it with the FSM liveness via `ns.as_ref().clone().with_state(fsm)` so
  // that `online_members()` and `is_alive()` agree on the same ground truth.

  /// Number of known members, including the local node itself.
  ///
  /// A freshly constructed engine has exactly one member (itself). Peers join
  /// after push/pull exchanges or gossip convergence during the pump loop.
  #[inline]
  pub fn num_members(&self) -> usize {
    self.endpoint.endpoint_ref().num_members()
  }

  /// Drain one application-visible membership or lifecycle event, if any.
  ///
  /// Returns events emitted by the machine during the last `pump` tick. Returns
  /// `None` when the event queue is empty; call again after the next `pump` tick.
  #[inline]
  pub fn poll_event(&mut self) -> Option<memberlist_proto::event::Event<I, SocketAddr>> {
    let ev = self.endpoint.poll_event();
    // Prune the outbound-StreamId correlation entry for a completing exchange, for
    // EVERY consumer (not only a driver that awaits sends), so the map cannot grow
    // unbounded; stash the removed StreamId for the immediately-following caller (a
    // send-awaiting driver reads it via `last_completed_send`).
    self.last_completed_send = None;
    if let Some(memberlist_proto::event::Event::ExchangeCompleted(ec)) = &ev {
      self.last_completed_send = self.outbound_stream_ids.remove(&ec.eid());
    }
    ev
  }

  /// The originating [`StreamId`] of the exchange whose `ExchangeCompleted` the
  /// most recent [`poll_event`](Self::poll_event) returned, or `None` if that event
  /// was not a completion or the exchange had no recorded `StreamId` (it never
  /// reached `Connect`). Valid only until the next `poll_event`. A driver awaiting
  /// reliable-send completion reads this right after a `poll_event` that yielded an
  /// `ExchangeCompleted { kind: UserMessage }` to resolve the exact send by
  /// `StreamId`, never by arrival order.
  #[inline]
  pub fn last_completed_send(&self) -> Option<StreamId> {
    self.last_completed_send
  }

  /// The number of outbound reliable exchanges currently awaiting completion in the
  /// correlation map — a diagnostic the driver uses to witness that the map is
  /// pruned (returns to zero) once every dispatched exchange has completed.
  #[inline]
  pub fn outbound_correlation_len(&self) -> usize {
    self.outbound_stream_ids.len()
  }

  /// Arm the SWIM scheduler at `now`. Call once before the first `pump`.
  ///
  /// Forwards to `StreamEndpoint::start_scheduling`, which arms the probe,
  /// gossip, and push-pull periodic timers so `poll_timeout` returns a finite
  /// deadline on the very next call.
  pub fn start(&mut self, now: Instant) {
    self.endpoint.start_scheduling(now);
  }

  /// Seed a statically-known peer as Alive, bootstrapping membership without the
  /// TCP push-pull join path.
  ///
  /// Builds a synthetic `Alive` message for `id` at `peer` (incarnation 1) and
  /// feeds it into the machine via `handle_alive`, exactly as if the node had been
  /// learned through gossip. Useful for static embedded clusters and for tests
  /// that skip the join phase.
  ///
  /// A non-routable `peer` (unspecified/multicast/broadcast IP or port 0) is
  /// dropped: it could only be stored as a member no node can send a useful packet
  /// to. The machine's admission filter would reject it too (the same
  /// routable-address delegate runs inline on `handle_alive`), but rejecting at
  /// this public entry makes the contract explicit and avoids building the
  /// synthetic `Alive` at all.
  pub fn inject_alive(&mut self, id: I, peer: SocketAddr, now: Instant) {
    if !socket_addr_is_routable(&peer) {
      return;
    }
    let alive = Alive::new(1, Node::new(id.cheap_clone(), peer));
    // Route the injected Alive through the gated inbound chokepoint so a left
    // node admits no injected peer either (handle_packet is the sole inbound
    // entry; the typed handlers are crate-private).
    self
      .endpoint
      .handle_packet(peer, memberlist_proto::typed::Message::Alive(alive), now);
  }

  /// Whether `id` is currently Alive from this node's perspective.
  ///
  /// Returns `false` for unknown ids or ids in any non-Alive state.
  #[inline]
  pub fn is_alive(&self, id: &I) -> bool {
    self
      .endpoint
      .endpoint_ref()
      .member_liveness(id)
      .map(|s| s == State::Alive)
      .unwrap_or(false)
  }

  /// Whether `id` is currently Dead from this node's perspective.
  ///
  /// Returns `false` for unknown ids or ids in any non-Dead state.
  #[inline]
  pub fn is_dead(&self, id: &I) -> bool {
    self
      .endpoint
      .endpoint_ref()
      .member_liveness(id)
      .map(|s| s == State::Dead)
      .unwrap_or(false)
  }

  /// Return the `NodeState` for `id`, stamped with the current FSM liveness.
  ///
  /// Returns `None` when `id` is unknown to this node. The `NodeState.state()`
  /// field reflects the live gossip-FSM state (`Alive` / `Suspect` / `Dead` /
  /// `Unknown`), not the frozen wire-format value.
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    let ns = ep.member(id)?;
    let fsm = ep.member_liveness(id).unwrap_or(State::Unknown(0));
    Some(std::sync::Arc::new(ns.as_ref().clone().with_state(fsm)))
  }

  /// All members currently in the `Alive` FSM state.
  ///
  /// Each returned `NodeState` is stamped with the FSM liveness, so
  /// `online_members()[i].state() == State::Alive` always holds. Consistent with
  /// `is_alive`: if `is_alive(id)` is `true`, `id` appears here.
  #[inline]
  pub fn online_members(&self) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter_map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        if fsm == State::Alive {
          Some(std::sync::Arc::new(ns.as_ref().clone().with_state(fsm)))
        } else {
          None
        }
      })
      .collect()
  }

  /// Count of members currently in the `Alive` FSM state.
  ///
  /// Equivalent to `online_members().len()` but avoids allocating a `Vec`.
  #[inline]
  pub fn num_online_members(&self) -> usize {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter(|ns| {
        ep.member_liveness(ns.id_ref())
          .map(|s| s == State::Alive)
          .unwrap_or(false)
      })
      .count()
  }

  /// All known members (Alive + Suspect + Dead/Left), each stamped with the
  /// current FSM liveness.
  ///
  /// Mirrors the legacy `Memberlist::members` name.
  #[inline]
  pub fn members(&self) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        std::sync::Arc::new(ns.as_ref().clone().with_state(fsm))
      })
      .collect()
  }

  /// Members matching `pred`, each stamped with the current FSM liveness.
  #[inline]
  pub fn members_by(
    &self,
    mut pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool,
  ) -> std::vec::Vec<std::sync::Arc<NodeState<I, SocketAddr>>> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter_map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        let stamped = ns.as_ref().clone().with_state(fsm);
        if pred(&stamped) {
          Some(std::sync::Arc::new(stamped))
        } else {
          None
        }
      })
      .collect()
  }

  /// Count of members matching `pred`.
  #[inline]
  pub fn num_members_by(&self, mut pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool) -> usize {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        let stamped = ns.as_ref().clone().with_state(fsm);
        pred(&stamped)
      })
      .count()
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  ///
  /// Each `NodeState` passed to `f` is stamped with the current FSM liveness.
  #[inline]
  pub fn members_map_by<O>(
    &self,
    mut f: impl FnMut(&NodeState<I, SocketAddr>) -> Option<O>,
  ) -> std::vec::Vec<O> {
    let ep = self.endpoint.endpoint_ref();
    ep.members()
      .filter_map(|ns| {
        let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
        let stamped = ns.as_ref().clone().with_state(fsm);
        f(&stamped)
      })
      .collect()
  }

  /// The local node's Lifeguard health score (`0` = fully healthy; higher = worse).
  ///
  /// Read directly from the live machine endpoint — no snapshot lag.
  #[inline]
  pub fn health_score(&self) -> usize {
    self.endpoint.endpoint_ref().health_score()
  }

  /// The local node's id, cheap-cloned from the machine endpoint.
  #[inline]
  pub fn local_id(&self) -> I {
    self.endpoint.endpoint_ref().local_id_ref().cheap_clone()
  }

  /// The local node's advertised `SocketAddr`.
  #[inline]
  pub fn advertise_address(&self) -> SocketAddr {
    *self.endpoint.endpoint_ref().advertise_ref()
  }

  /// The local node's `NodeState`, stamped with the current FSM liveness.
  #[inline]
  pub fn local_state(&self) -> std::sync::Arc<NodeState<I, SocketAddr>> {
    let ep = self.endpoint.endpoint_ref();
    let local_id = ep.local_id_ref();
    let ns = ep
      .member(local_id)
      .expect("local node is always in the membership map");
    // Alive is the correct fallback for the LOCAL node specifically: from its own
    // perspective the node is always alive, and before start() is called
    // member_liveness may return None.  Unknown(0) would be wrong here because it
    // implies the node's health is uncertain, which it never is locally.
    let fsm = ep.member_liveness(local_id).unwrap_or(State::Alive);
    std::sync::Arc::new(ns.as_ref().clone().with_state(fsm))
  }

  // ── Directed I/O ──────────────────────────────────────────────────────────
  //
  // These are `&mut self` thin forwarders. The pump loop drives all actual I/O;
  // the caller correlates completion by draining `poll_event()` after subsequent
  // `pump` ticks.

  /// Send a direct UDP ping to `node`.
  ///
  /// Returns a [`PingId`] token, or `NotRunning` after `leave()`. The caller
  /// should drain `poll_event()` after subsequent `pump` ticks to observe the
  /// terminal event (`Event::PingCompleted` / `Event::PingFailed`).
  ///
  /// Unlike a SWIM failure-detection probe, an application ping is direct-only: it
  /// does not fan out to indirect peers, request a reliable fallback, or mark the
  /// target as suspect on timeout.
  #[inline]
  pub fn ping(
    &mut self,
    node: Node<I, SocketAddr>,
    now: Instant,
  ) -> Result<PingId, memberlist_proto::Error> {
    self.endpoint.ping(node, now)
  }

  /// Enqueue a directed unreliable UDP user-data packet to `to`.
  ///
  /// The payload is encoded as a `UserData` gossip message and emitted on the next
  /// gossip drain in `pump`. The peer observes it as `Event::UserPacket` via
  /// `poll_event()`. Delivery is best-effort (UDP); callers that need guaranteed
  /// delivery should use [`Self::send_reliable`].
  ///
  /// Returns `Err` when the framed payload exceeds the configured gossip MTU.
  #[inline]
  pub fn send(
    &mut self,
    to: SocketAddr,
    payload: bytes::Bytes,
  ) -> Result<(), memberlist_proto::Error> {
    if cidr_blocks(&self.cidr_policy, to.ip()) {
      // Our own policy excludes the destination: drop the directed unreliable
      // packet before enqueueing rather than emit it to a blocked peer (the
      // reliable plane's dial is rejected in `dial`; this is the unreliable
      // counterpart, with the `drain_gossip_transmits` egress drop as last line).
      // Best-effort UDP, so a clean drop matches the delivery contract.
      return Ok(());
    }
    self.endpoint.send_user_packet(to, payload)
  }

  /// Enqueue multiple directed unreliable UDP user-data packets to `to`.
  ///
  /// When `payloads` contains two or more entries they are compounded into a
  /// single gossip datagram if they fit together within the configured gossip
  /// MTU. The peer observes each payload separately as `Event::UserPacket` via
  /// `poll_event()`.
  ///
  /// Returns `Err` when the compound frame exceeds the gossip MTU.
  #[inline]
  pub fn send_many(
    &mut self,
    to: SocketAddr,
    payloads: &[bytes::Bytes],
  ) -> Result<(), memberlist_proto::Error> {
    if cidr_blocks(&self.cidr_policy, to.ip()) {
      // See `send`: drop a directed unreliable batch to a CIDR-blocked
      // destination before enqueueing (best-effort UDP).
      return Ok(());
    }
    self.endpoint.send_user_packets(to, payloads)
  }

  /// Initiate a reliable TCP user-message delivery to `to`.
  ///
  /// The payload is encoded and sent over a dedicated TCP stream. Returns a
  /// [`StreamId`] token, or `NotRunning` after `leave()`. Completion surfaces as
  /// `Event::ExchangeCompleted { kind: ExchangeKind::UserMessage, .. }` via
  /// `poll_event()` after subsequent `pump` ticks.
  ///
  /// The pump loop services the resulting `DialRequested` generically — the same
  /// `Connect` path used for join push/pull — so no additional driver changes are
  /// needed to carry user messages over TCP.
  ///
  /// **Reliable exchanges share a single listener, so a peer accepts inbound
  /// reliable streams one at a time.** To send multiple reliable messages to the
  /// same peer, issue them sequentially: call `send_reliable`, drive `pump` until
  /// the matching `Event::ExchangeCompleted { kind: UserMessage }` arrives via
  /// `poll_event`, then send the next. Concurrent reliable streams to one peer
  /// would collide at the listener (the second SYN is RST'd during the first's
  /// handshake).
  #[inline]
  pub fn send_reliable(
    &mut self,
    to: SocketAddr,
    payload: bytes::Bytes,
    now: Instant,
  ) -> Result<StreamId, memberlist_proto::Error> {
    self.endpoint.start_user_message(to, payload, now)
  }

  // ── Join ──────────────────────────────────────────────────────────────────

  /// Record intent to join the cluster via these seed addresses.
  ///
  /// Returns immediately; the pump loop initiates a push/pull state exchange to
  /// each seed on the next tick. The caller should watch `is_joined()` or drain
  /// `poll_event()` for `Event::PushPullReplyReceived` / membership changes, and
  /// enforce its own join deadline — this method performs no I/O and imposes no
  /// timeout. Returns `NotRunning` after `leave()`: a left node initiates no new
  /// join.
  pub fn join(&mut self, seeds: &[SocketAddr]) -> Result<(), memberlist_proto::Error> {
    // A left node initiates no new join — the machine merges no remote state
    // during the graceful-leave drain, so queued seeds could never take effect.
    // Reject rather than enqueue work the pump would skip.
    self.ensure_running()?;
    for s in seeds {
      // Drop a non-routable seed (unspecified/multicast/broadcast IP or port 0):
      // it could only produce a doomed dial — the link layer's `connect` rejects
      // the unspecified address and port 0, and the rest are addresses no dial can
      // usefully reach. Queue only seeds a dial can actually complete.
      if socket_addr_is_routable(s) {
        self.pending_seeds.push_back(*s);
      }
    }
    Ok(())
  }

  /// Queue an application user-data payload for piggyback gossip to peers.
  ///
  /// The payload rides the next gossip rounds as a `UserData` message and surfaces
  /// on each receiving node as `Event::UserPacket` via `poll_event()`. A payload
  /// whose lone framed datagram would exceed the configured gossip MTU is rejected
  /// with `Error::UserBroadcastExceedsMtu` and not stored (it could never be
  /// gossiped even alone). Returns `NotRunning` after `leave()` (the gossip
  /// scheduler is stopped, so the broadcast could never drain). Delivery is
  /// otherwise best-effort, like all gossip.
  pub fn queue_user_broadcast(
    &mut self,
    data: bytes::Bytes,
  ) -> Result<(), memberlist_proto::Error> {
    self.endpoint.queue_user_broadcast(data)
  }

  /// Begin leaving the cluster.
  ///
  /// Forwards to the machine's graceful-leave path, which gossips the departure
  /// and ultimately emits `Event::LeftCluster` via `poll_event()`. The caller
  /// enforces its own leave timeout, then stops pumping.
  ///
  /// Returns an error if the node is not in a running state (e.g. already left or
  /// never started); the caller may choose to ignore this when tearing down
  /// unconditionally.
  pub fn leave(&mut self, now: Instant) -> Result<(), memberlist_proto::Error> {
    // Drop any seeds still queued from a pre-leave join: the drain initiates no
    // new push/pull, so they must not dial once we begin leaving.
    self.pending_seeds.clear();
    self.endpoint.leave(now)
  }

  /// Replace the gossip + reliable-plane compression policy in place.
  ///
  /// The new policy takes effect for all gossip datagrams emitted after this
  /// call and is fanned out to every live reliable bridge so long-lived
  /// push/pull exchanges adopt it on their next outbound encode. Returns
  /// `NotRunning` after `leave()` (the change could never reach the wire).
  pub fn set_compression_options(
    &mut self,
    opts: memberlist_proto::CompressionOptions,
  ) -> Result<(), memberlist_proto::Error> {
    self.ensure_running()?;
    self.endpoint.set_compression_options(opts);
    Ok(())
  }

  /// Replace the gossip + reliable-plane encryption policy in place.
  ///
  /// Validates every key in the keyring before mutating state — an unusable key
  /// (whose AEAD backend was not compiled into this binary) is rejected without
  /// touching the live policy, so a node never starts dropping traffic
  /// mid-rotation under a bad key. A valid update fans the new policy out to
  /// every live reliable bridge immediately.
  ///
  /// # Errors
  ///
  /// Returns [`ControlError::NotRunning`](crate::error::ControlError::NotRunning)
  /// after `leave()` (gated before any validation), or
  /// [`ControlError::Encryption`](crate::error::ControlError::Encryption) when a
  /// key in the supplied keyring cannot be used by this build (e.g.
  /// `EncryptionError::UnsupportedAlgorithm`). The existing policy is unchanged
  /// in either case.
  pub fn set_encryption_options(
    &mut self,
    opts: memberlist_proto::EncryptionOptions,
  ) -> Result<(), crate::error::ControlError> {
    if !self.endpoint.is_running() {
      return Err(crate::error::ControlError::NotRunning);
    }
    if let Some(keyring) = opts.keyring() {
      for key in core::iter::once(keyring.primary_ref()).chain(keyring.secondaries()) {
        memberlist_proto::encode_encrypted_frame(key.algorithm(), key, b"")?;
      }
    }
    self.endpoint.set_encryption_options(opts);
    Ok(())
  }

  /// `Ok` only while the node is running. After `leave()` the periodic
  /// schedulers stop and the machine merges no remote state, so the runtime
  /// operations that gate on this reject with `NotRunning` rather than queue or
  /// store a change no peer would observe. The core data-state setters gate
  /// inside the machine itself.
  fn ensure_running(&self) -> Result<(), memberlist_proto::Error> {
    if self.endpoint.is_running() {
      Ok(())
    } else {
      Err(memberlist_proto::Error::NotRunning)
    }
  }

  /// Replace this node's advertised metadata in place.
  ///
  /// Bumps the node's incarnation and gossips the new metadata; peers observe
  /// the change as `Event::NodeUpdated` via `poll_event()`. Returns an error if
  /// the metadata exceeds the configured cap or the node is not running.
  pub fn update_node_metadata(
    &mut self,
    meta: memberlist_proto::typed::Meta,
  ) -> Result<(), memberlist_proto::Error> {
    self.endpoint.update_meta(meta)
  }

  /// Set the application state snapshot exchanged during push/pull.
  ///
  /// The bytes ride the next join / push-pull exchange and surface on the
  /// receiving node as `Event::RemoteStateReceived` via `poll_event()`. Returns
  /// `NotRunning` after `leave()`, or a size error if the framed snapshot
  /// exceeds the reliable-stream frame budget.
  pub fn set_local_state(&mut self, state: bytes::Bytes) -> Result<(), memberlist_proto::Error> {
    self.endpoint.set_local_state_snapshot(state)
  }

  /// Set the payload attached to outgoing ping acknowledgements.
  ///
  /// A peer that probes this node receives the payload in its
  /// `Event::PingCompleted` via `poll_event()`. Returns `NotRunning` after
  /// `leave()`, or a size error if the framed ack exceeds the gossip budget.
  pub fn set_ack_payload(&mut self, payload: bytes::Bytes) -> Result<(), memberlist_proto::Error> {
    self.endpoint.set_ack_payload(payload)
  }

  /// Install a custom peer-admission predicate, composed with the built-in
  /// routable-address filter (both must admit). The machine consults it inline
  /// for every inbound Alive — gossip and join push/pull alike. Set it before
  /// [`start`](Self::start) so no peer is admitted before the policy applies.
  ///
  /// The caller's delegate can only further restrict admission: the built-in
  /// routable filter always runs first (see `RoutableAnd`), and — when a CIDR
  /// policy was set via [`Options::with_cidr_policy`](crate::Options::with_cidr_policy)
  /// — that policy is folded in too, so installing a delegate here never drops the
  /// configured CIDR admission. A peer must pass routable AND the policy AND the
  /// caller's delegate.
  ///
  /// Installing after `leave()` is inert: the machine admits no inbound Alive
  /// once leaving, so the delegate is never consulted again.
  pub fn set_alive_delegate(&mut self, delegate: impl AliveDelegate<I, SocketAddr>) {
    // Re-fold the stored CIDR policy so a caller's delegate composes with it
    // (routable AND in-policy AND delegate) rather than replacing it. The
    // transport-boundary recv/accept guards read `self.cidr_policy` directly and
    // are unaffected either way; this keeps the membership-admission half in sync.
    #[cfg(feature = "cidr")]
    match self.cidr_policy.clone() {
      Some(policy) => {
        self
          .endpoint
          .set_alive_delegate(RoutableAnd(memberlist_proto::CidrAnd::new(
            policy, delegate,
          )))
      }
      None => self.endpoint.set_alive_delegate(RoutableAnd(delegate)),
    }
    #[cfg(not(feature = "cidr"))]
    self.endpoint.set_alive_delegate(RoutableAnd(delegate));
  }

  /// Install a custom join-merge predicate, consulted on each join push/pull
  /// merge (never an anti-entropy refresh). A delegate that rejects the merge
  /// fails the join. Set it before [`start`](Self::start) / [`join`](Self::join).
  ///
  /// Installing after `leave()` is inert: the machine merges no remote state
  /// once leaving, so the delegate is never consulted again.
  pub fn set_merge_delegate(&mut self, delegate: impl MergeDelegate<I, SocketAddr>) {
    self.endpoint.set_merge_delegate(delegate);
  }

  /// Whether this node has learned at least one peer.
  ///
  /// `num_members() > 1` means a join push/pull has synced remote state, or a peer
  /// was injected via `inject_alive`. A coarse readiness signal; the caller owns
  /// the real join deadline.
  #[inline]
  pub fn is_joined(&self) -> bool {
    self.num_members() > 1
  }

  /// Advance the memberlist state machine once over the driver's already-ticked
  /// sockets. Returns the next wakeup deadline: the minimum of the machine's next
  /// timer AND any engine-owned deadline (the soonest gracefully-closing
  /// connection's force-abort instant). The driver folds in its own link-layer
  /// next-event deadline.
  ///
  /// The driver owns the super-loop: it ticks its link-layer stack (e.g.
  /// `iface.poll`), calls `pump`, then sleeps until `min(driver_stack_next,
  /// pump_result)`, advances the clock, and loops. Because every deadline the
  /// engine enforces on a tick is folded into the returned instant, a caller that
  /// sleeps exactly to it always wakes in time to honor them — including
  /// reclaiming a closing connection by `Options::close_timeout`.
  ///
  /// # Order
  ///
  /// `pump` runs the SAME ordered phases the smoltcp `poll` did EXCEPT the
  /// driver's own step-1 stack tick (`iface.poll`), which the driver performs
  /// before calling `pump`:
  ///
  /// 1a. **Reap** — return gracefully-closed (or close-timed-out) connections to
  ///    the pool.
  /// 1b. **Accept inbound** — if the listener completed a passive open, hand it
  ///    to the machine and replenish the listener from the pool.
  /// 1c. **Rebalance** — self-heal a missing listener, then assign any remaining
  ///    free slots to deferred dials (listener-first).
  /// 2a/2b. **Gossip ingress** — drain received datagrams into `handle_gossip`,
  ///    then decode buffered raw frames through the codec and feed typed messages
  ///    back via `handle_packet`.
  /// 3. **Reliable ingress pump** — drain each connection's rx into
  ///    `handle_transport_data`; deliver a one-shot EOF on peer FIN.
  /// 5. **Join-seed drain** — `start_push_pull(seed, Join, now)` per queued seed.
  /// 6. **Machine tick** — `handle_timeout` fires due timers.
  /// 7a–7e. **Stream actions + egress** — drain `poll_action`, promote, pump
  ///    outbound, flush deferred FINs, complete `Closing` drains, re-rebalance,
  ///    then drain + send outbound gossip.
  /// 8. **Deadline** — `min(machine_next, closing_next)`.
  pub fn pump<G: GossipIo, S: StreamIo<Conn = C>>(
    &mut self,
    now: Instant,
    gossip: &mut G,
    stream: &mut S,
  ) -> Option<Instant> {
    // 1a. Reap gracefully-closing connections. The driver's step-1 stack tick may
    // have advanced FIN exchanges to completion; reclaim any that are now fully
    // closed (or have exceeded `cfg.close_timeout`) so the freed handles back new
    // dials/accepts this same tick.
    self.reap_closing(now, stream);

    // The accept/replenish/dial phase is ordered LISTENER-FIRST: the inbound
    // listener gets first claim on a free slot and a deferred outbound dial takes
    // only what remains. The whole reliable plane can be driven from a single
    // spare slot, so an inbound peer must never be starved of a slot by outbound
    // intent. The two steps below run in this exact order:
    //
    //   1b. `check_listener`  — consume an accept-ready listener + replenish
    //   1c. `rebalance_pool`  — self-heal a missing listener, then deferred dials
    //                           take any remaining slots (listener-first)
    //
    // 1b. Accept an inbound connection completed on the listener and replenish the
    // listener from the pool.
    //
    // The driver's step-1 stack tick promotes a listener whose three-way handshake
    // just finished from Listen to Established; `check_listener` sees that here (it
    // needs only that prior `iface.poll`, nothing later in the phase), hands the
    // connection to the machine, registers the exchange↔slot mapping, then
    // immediately re-`listen`s a fresh listener from the pool so the next inbound
    // SYN has a slot ready. Running this BEFORE the dial rebalance is what enforces
    // listener priority: with one spare slot and an outbound backlog, draining
    // dials first would steal that slot and leave the listener `None` and
    // unreplenishable — starving inbound until some later exchange frees a slot.
    // Accept-and-replenish first claims it for the listener instead.
    self.check_listener(now, stream);

    // 1c. Self-heal a still-missing listener, then assign any remaining free slots
    // to deferred dials (listener-first). This runs the SAME rebalance as the late
    // call after the in-tick frees below (step 7), here over whatever
    // `reap_closing` freed plus the spare pool. Running it BEFORE the machine tick
    // (step 6) is required: a `PendingDial` deferred on a PRIOR tick must be
    // assigned a freed slot and dialed before step 6's `handle_timeout` could
    // elapse its bridge and tear it down — so the early site cannot move later.
    self.rebalance_pool(now, stream);

    // 2a. Drain inbound gossip datagrams into the machine's raw ingress buffer.
    {
      // The reusable receive scratch and the machine endpoint are separate fields,
      // so both can be borrowed across the drain loop at once.
      let buf = self.gossip_recv.as_mut_slice();
      let endpoint = &mut self.endpoint;
      let cidr_policy = &self.cidr_policy;
      // The driver's datagram I/O pops one datagram per `recv` call and returns
      // `None` once the rx ring is empty (an over-budget datagram larger than this
      // buffer is consumed and skipped by the driver, so one oversized datagram
      // cannot stall the in-budget datagrams queued behind it).
      while let Some((src, n)) = gossip.recv(buf) {
        // Drop a gossip datagram from a CIDR-blocked source before the machine
        // sees it (the transport-source filter; the advertised-address filter is
        // the composed routable-address alive delegate).
        if !cidr_blocks(cidr_policy, src.ip()) {
          endpoint.handle_gossip(src, &buf[..n], now);
        }
      }
    }

    // 2b. Unwrap the encryption/compression transforms, then decode each raw
    // gossip frame and feed typed messages back.
    while let Some((src, raw)) = self.endpoint.poll_memberlist_ingress() {
      // Strip the Encrypted-then-Compressed wrapper stack FIRST (each layer is
      // identity when its wrapper is absent, and the whole call is identity when
      // no keyring is configured, so the plaintext path is preserved exactly). On
      // an encrypted cluster the strict-mode entry check rejects a non-Encrypted
      // datagram here; a corrupt frame, an unknown algorithm, or an oversized
      // wrapper is likewise an Err. Drop on Err — a plaintext datagram on an
      // encrypted node, or a corrupt frame, must not reach the decoder. Gossip is
      // lossy and self-healing.
      let decrypted = match self.endpoint.decrypt_gossip(&raw) {
        Ok(p) => bytes::Bytes::from(p),
        Err(_) => continue,
      };
      let opts = memberlist_proto::codec::DecodeOptions::new(self.label.clone());
      // Drop malformed inbound datagrams silently — bad network input must not
      // panic the node; SWIM is self-healing. (`decode_incoming` Err = label
      // mismatch / framing error; `parse_messages` Err = malformed frame.)
      if let Ok(plain) = memberlist_proto::codec::decode_incoming(decrypted, &opts) {
        if let Ok(msgs) = memberlist_proto::codec::parse_messages::<I, SocketAddr>(plain) {
          for msg in msgs {
            self.endpoint.handle_packet(src, msg, now);
          }
        }
      }
    }

    // 3. Reliable ingress pump: drain each active exchange's socket rx buffer into
    // the machine. Must run before the machine tick so the machine sees fresh
    // inbound bytes (including the peer-FIN EOF) when firing timers.
    self.pump_inbound_reliable(now, stream);

    // 5. Drain join seeds: each seed queued by `join()` gets a push/pull exchange
    // initiated now. `start_push_pull` internally calls `service_dials` +
    // `flush_outbound`, queuing a `Connect` action that step 7a below will consume
    // this same tick, so the first TCP dial bytes are emitted without requiring an
    // additional pump.
    //
    // Skip the drain once leaving/left: `join()` rejects post-leave and `leave()`
    // clears the queue, so this guards the one site that dials against ever
    // initiating a join push/pull the machine would merge nothing from.
    if self.endpoint.is_running() {
      while let Some(seed) = self.pending_seeds.pop_front() {
        // StreamId is the machine's correlation token for this exchange. The dial is
        // correlated via the ExchangeId carried in the resulting Connect action, not
        // by the StreamId; the driver does not need to retain it.
        let _sid = self.endpoint.start_push_pull(seed, PushPullKind::Join, now);
      }
    }

    // 6. Machine tick: fire due timers (probe / gossip / push-pull).
    self.endpoint.handle_timeout(now);

    // 7a. Drain stream actions: open dials, half-close, or tear down exchanges.
    //
    // Actions are drained BEFORE transport transmits because a Connect must
    // install the exchange↔slot mapping before this same tick's outbound bytes for
    // that exchange are written (see the stream-endpoint ordering contract in
    // memberlist-proto/src/streams/mod.rs).
    self.drain_stream_actions(now, stream);

    // 7b. Promote dialing connections whose handshake completed this tick to
    // Established, so the egress pump and the deferred-FIN gate below see an
    // accurate lifecycle state.
    self.promote_established(stream);

    // 7c. Reliable egress pump: append new transmits to each connection's out
    // queue, then flush every connection's queue to its socket.
    self.pump_outbound_reliable(stream);

    // 7d. Emit any deferred graceful write-half FINs whose connection is now
    // Established and whose outbound bytes have fully drained to the peer. The
    // connection stays mapped in `connections` (its inbound reply + FIN still
    // pump); the socket is reclaimed only later by the machine's `Close`.
    self.flush_pending_shutdowns(stream);

    // 7d'. Complete the deferred terminal close of any connection draining in
    // `Closing`: a graceful `Close` whose send-capable socket still held
    // undelivered outbound bytes was kept mapped so 7c could keep flushing them.
    // Now that this tick's flush has run, emit the terminal FIN + detach for any
    // whose `out` and tx ring are fully drained, or force-abort one past its close
    // deadline. Runs AFTER 7c so a connection that finished draining this very tick
    // FINs the same tick rather than waiting for the next.
    self.flush_closing(now, stream);

    // 7d''. Re-run the listener/dial rebalance over every slot the machine tick and
    // teardown just freed back to the pool IN THIS TICK. The early 1c rebalance ran
    // before step 6 and saw only what `reap_closing` had freed; the slot-freeing
    // close paths run LATER (after the machine tick fires the `Close` actions), so a
    // slot freed here would otherwise sit idle until the next pump — stranding a
    // deferred `PendingDial` or a missing listener until some unrelated timer
    // happened to wake the driver, possibly past the waiting exchange's own bridge
    // deadline (which would then kill it before it ever got the slot). Servicing the
    // frees in-tick lets a freed slot immediately back the oldest waiting dial — its
    // SYN egress is then driven by the stack deadline, which the driver reports as
    // ~now — or restore the listener, so the returned wakeup is naturally correct
    // with no `pending_dial` deadline term needed.
    //
    // This MUST stay positioned after EVERY late `pool.give()` path so it dominates
    // all of them. Today those are exactly three, all upstream here:
    //   - 7a `drain_stream_actions` → `teardown`: the `Closed | TimeWait` branch
    //     and the abrupt `abort()` branch both `pool.give(h)`.
    //   - 7d' `flush_closing`: the deadline-`Abort` branch `pool.give(h)`.
    // (`teardown`'s and `flush_closing`'s graceful-FIN branches `closing.insert`
    // instead of `give`; those handles are reaped to the pool by a LATER tick's 1a
    // `reap_closing`, whose freed slots the next tick's 1c rebalance claims — so they
    // need no in-tick rebalance here.) If a new late free path is ever added, it must
    // precede this call or the end-of-tick invariant below regresses.
    self.rebalance_pool(now, stream);

    // Invariant held at end-of-tick: if the reliable pool holds a REUSE-READY slot
    // then a listener is present AND no connection remains in `PendingDial`. The late
    // rebalance above is the last pool-touching reliable phase — the gossip egress
    // touches only the gossip socket, never the reliable pool — so the invariant
    // cannot be disturbed before the deadline is computed below. The readiness
    // qualifier matters for an async-teardown driver: a freed-but-still-resetting
    // slot is intentionally NOT reused this tick (its worker has not reset the
    // socket yet), so a pool holding only such slots is not a rebalance miss — the
    // listener / deferred dial is serviced on the later tick the worker completes its
    // reset. For a synchronous driver every freed slot is ready, so this is the same
    // "non-empty pool ⇒ listener present and no PendingDial" guarantee as before.
    debug_assert!(
      !self.plane.pool.any_where(|&c| stream.reuse_ready(c))
        || (self.plane.listener.is_some() && self.plane.pending_dial_count() == 0),
      "end-of-tick: a reuse-ready reliable slot left a listener missing or a PendingDial unserviced"
    );

    // 7e. Egress: drain outbound gossip transmits, encode, and send.
    self.drain_gossip_transmits(gossip);

    // 8. Next deadline = min(machine, closing).
    //
    // The returned instant is the wake contract: a caller that sleeps until it
    // (combined with its own link-layer next-event deadline) must wake in time to
    // enforce every deadline the engine owns. Two engine-owned classes contribute:
    //
    // - `machine` — the SWIM machine's next timer (probe / gossip / push-pull /
    //   bridge handshake-and-dial deadlines).
    // - `closing` — the soonest force-abort deadline among connections parked
    //   mid-close. Two engine-owned deadline sources feed this class, both enforced
    //   only on a tick that actually runs: the `plane.closing` map (a detached
    //   handle whose FIN is in flight, reaped by `reap_closing`) and the
    //   `close_deadline` of any connection still draining in `Closing` before its
    //   terminal FIN (the backstop in `flush_closing`). If either were omitted, a
    //   peer that vanished mid-close would keep its socket `is_open()` forever (the
    //   link layer sets no TCP timeout), and a deadline-driven caller could sleep
    //   arbitrarily past `close_timeout`. Folding the soonest of BOTH in guarantees
    //   the caller wakes by `close_timeout` to reap it, so pool / listener recovery
    //   is bounded by `Options::close_timeout` as documented.
    //
    // `pending_dial` deliberately contributes NO deadline of its own: a buffered
    // dial is serviced when a slot frees, never on a clock of its own, and every
    // free either is handled THIS tick or already feeds one of the terms above. A
    // slot freed straight to the pool by a teardown / close this tick (7a / 7d') is
    // spent on the oldest waiting dial by the late 7d'' rebalance immediately, so its
    // SYN is queued before this deadline is computed and surfaces in the driver's
    // stack term. A slot whose graceful FIN is still in flight is reaped to the pool
    // only on a LATER tick by `reap_closing`, but that tick is itself guaranteed by
    // the `closing` deadline folded in here; the next tick's early rebalance then
    // dials the waiting connection. Either way the unblocking event is already
    // covered, so a `pending_dial` term would be redundant.
    //
    // The driver folds its own link-layer next-event deadline into the returned
    // instant before sleeping.
    let machine = self.endpoint.poll_timeout();
    // Soonest force-abort deadline across BOTH close backstops: detached handles
    // parked in `plane.closing`, and connections still draining in `Closing` before
    // their terminal FIN (their `close_deadline`).
    let closing = self
      .plane
      .closing
      .values()
      .chain(
        self
          .plane
          .connections
          .values()
          .filter_map(|c| c.close_deadline.as_ref()),
      )
      .min()
      .copied();
    min_opt(machine, closing)
  }

  /// Reclaim gracefully-closing connections that have finished closing or whose
  /// close has exceeded `cfg.close_timeout`.
  ///
  /// A graceful `teardown` issues a FIN and parks the handle in `plane.closing`
  /// with a deadline (see `teardown`). The socket then works through the TCP FIN
  /// states (FinWait / Closing / LastAck / TimeWait) before becoming reusable.
  /// This pass returns to the pool every parked handle that has reached a reusable
  /// state and leaves the rest parked for a later tick.
  ///
  /// "Reusable" is `!is_open()` — false only in the `Closed` and `TimeWait`
  /// states, exactly the states in which the socket's next consumer (`connect()`
  /// dial or `listen()` replenish) is accepted; both reject an open socket and
  /// both reset it on reuse, discarding any `TimeWait` 2MSL remainder. A socket
  /// still flushing its FIN (FinWait1/2, Closing, LastAck) is still `is_open()` and
  /// stays parked, so a socket is never reclaimed before its FIN completes.
  ///
  /// The deadline bounds that wait: the link layer applies no TCP timeout by
  /// default, so a peer that vanishes mid-FIN leaves the socket `is_open()` forever
  /// and the handle would leak. When `now >= deadline`, this pass `abort()`s the
  /// socket (forcing it straight to `Closed`) before returning it, so a stuck
  /// graceful close cannot permanently shrink the pool. A healthy close reaches
  /// `Closed` long before the deadline and is reclaimed on the `!is_open()` path.
  fn reap_closing<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    // Retain the still-closing handles; give the finished (or timed-out) ones back
    // to the pool.
    let pool = &mut self.plane.pool;
    self.plane.closing.retain(|&c, &mut deadline| {
      if !stream.is_open(c) {
        // Clean close completed (Closed / TimeWait): reclaim.
        pool.give(c);
        return false;
      }
      if now >= deadline {
        // Peer vanished mid-FIN: force the socket to Closed and reclaim so the pool
        // (and the listener replenished from it) recover.
        stream.abort(c);
        pool.give(c);
        return false;
      }
      // Still flushing its FIN within the deadline — keep parked.
      true
    });
  }

  /// Check whether the listener slot accepted an inbound connection.
  ///
  /// After the driver's stack tick, a passive open is *complete* only once the
  /// listener socket reaches Established — i.e. the remote's final ACK of the
  /// three-way handshake has arrived. The peer address is then available
  /// ([`StreamIo::accepted_peer`]) and the connection can carry the push/pull byte
  /// exchange. We swap the now-connected slot out of `plane.listener` and replenish
  /// a fresh listener from the pool if one is available.
  ///
  /// The accept gate is `accepted_peer(c).is_some()`, which a driver returns only
  /// once the socket is at/after Established with a known remote (gating on
  /// `may_send()` and a populated remote endpoint, NOT `is_active()` — that would
  /// accept a not-yet-established SynReceived socket whose handshake an
  /// RST/retransmit could still revert to Listen, silently turning the exchange's
  /// socket into a second listener and wedging the join).
  fn check_listener<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    let c = match self.plane.listener {
      Some(c) => c,
      None => return,
    };

    // The passive open is complete and a remote is known only when `accepted_peer`
    // is `Some`; otherwise the handshake has not settled this tick.
    let Some(peer) = stream.accepted_peer(c) else {
      return;
    };

    // Reject a reliable connection from a CIDR-blocked peer at the transport
    // boundary: abort the connected listener socket and return it to the pool
    // WITHOUT registering the exchange, then re-arm a fresh listener — the same
    // abort-and-reclaim shape the `dial` reject path uses. (The advertised-address
    // filter is the composed routable-address alive delegate.)
    if cidr_blocks(&self.cidr_policy, peer.ip()) {
      stream.abort(c);
      self.plane.pool.give(c);
      self.plane.listener = None;
      self.ensure_listener(stream);
      return;
    }

    // Register the inbound exchange with the machine; it returns the ExchangeId the
    // driver uses to route subsequent inbound bytes. The listener socket already
    // completed its handshake, so the Connection starts Established.
    let eid = self.endpoint.accept_connection(peer, now);
    self
      .plane
      .connections
      .insert(eid, Connection::accepted(peer, c));
    self.plane.accepted_inbound += 1;

    // The listener slot is now the exchange; the slot is empty.
    self.plane.listener = None;

    // Replenish immediately if a slot is free. This is the SAME path the poll-phase
    // `ensure_listener` self-heal uses, so there is exactly one
    // listener-establishment routine and the two cannot drift. Replenishing here —
    // before `drain_pending_dials` runs later this tick — is what gives the listener
    // first claim on a free slot over any deferred outbound dial. When the pool is
    // empty here, the slot stays empty this tick and a later pump re-establishes it
    // once a slot is freed (this exchange completing, or a concurrent one reaped),
    // preserving the self-healing invariant.
    self.ensure_listener(stream);
  }

  /// Re-establish the passive-open listener if it is missing and the pool can
  /// supply a slot.
  ///
  /// The accept path (`check_listener`) moves the listener slot into the accepted
  /// exchange and replenishes the listener only from whatever is free at that
  /// instant; a momentarily exhausted pool therefore leaves the listener slot empty
  /// with no other path to restore it. Calling this at the top of each `pump`
  /// (after the reap pass) closes that gap: as soon as a slot is free again, the
  /// listener is rebuilt, so a transient full pool never permanently stops the node
  /// from accepting inbound reliable connections.
  ///
  /// A no-op when a listener already exists or the pool is empty.
  fn ensure_listener<S: StreamIo<Conn = C>>(&mut self, stream: &mut S) {
    if self.plane.listener.is_some() {
      return;
    }
    // Take only a slot the driver reports as fully reset and reuse-ready. An
    // async-teardown driver (embassy-net) returns a just-aborted slot to the pool
    // before its worker has reset the socket; re-`listen`ing it now would clobber
    // the pending abort and leak the prior connection into the listener. A
    // still-resetting slot is left in the pool and a later tick re-establishes the
    // listener once the worker has finished. For a synchronous driver every slot is
    // ready, so this is unchanged.
    if let Some(c) = self.plane.pool.take_where(|&c| stream.reuse_ready(c)) {
      // `listen()` only fails on port 0 or an already-open socket. Neither applies:
      // a pooled socket is Closed (freshly created, or reset on reuse out of
      // TimeWait/Closed) and `cfg.port` is the user-supplied non-zero port.
      // Ignoring Err: the two failure modes above are both unreachable here.
      let _ = stream.listen(c, self.cfg.port);
      self.plane.listener = Some(c);
    }
  }

  /// Open a TCP dial for the `Dialing` connection `eid` on its assigned slot `c`.
  ///
  /// The caller has already created/transitioned the [`Connection`] to
  /// [`ConnState::Dialing`] with `socket: Some(c)` (so this same tick's
  /// parked/outbound push-pull bytes and later inbound bytes route to `c`). This
  /// issues the link-layer `connect()`. On a connect rejection — or a peer that is
  /// not a routable destination, screened up front before `connect` is ever called
  /// — the socket is aborted and returned to the pool — never leaked — the
  /// `Connection` is removed, and an EOF is latched on the bridge as a best-effort
  /// cancel. (For a `Handshaking` bridge that EOF is consumed only post-promotion,
  /// so the bridge is ultimately retired by its own dial/handshake deadline; the
  /// driver has no prompt dial-cancel path.) Shared by the live `Connect` drain and
  /// the deferred `drain_pending_dials` retry so both dial identically.
  fn dial<S: StreamIo<Conn = C>>(
    &mut self,
    eid: ExchangeId,
    peer: SocketAddr,
    c: C,
    now: Instant,
    stream: &mut S,
  ) {
    // Reject an outbound dial to a CIDR-blocked peer before `connect`: a blocked
    // peer forms no reliable connection in either direction (the accept guard
    // drops its passive opens; this drops our active dials, so a join toward a
    // blocked seed fails rather than completing the push/pull). Reclaim the
    // freshly-assigned socket and terminalize via `handle_dial_failed`: a
    // never-connected dial must FAIL the exchange, not feed a benign EOF that a
    // one-way user-message send would read as success.
    if cidr_blocks(&self.cidr_policy, peer.ip()) {
      stream.abort(c);
      self.plane.pool.give(c);
      self.plane.connections.remove(&eid);
      self.endpoint.handle_dial_failed(eid, now);
      return;
    }

    // Screen a non-routable peer BEFORE `connect`. A non-routable peer can never be
    // a useful TCP destination: the link layer's `connect` rejects the unspecified
    // address and port 0 with `Unaddressable`, and a multicast/broadcast remote
    // resolves only to a derived L2 multicast/broadcast MAC. Screen here on the same
    // `is_unicast` predicate so no doomed connect is started, and reclaim cleanly
    // exactly as the connect-rejection path does: the freshly-assigned socket is
    // still Closed, so `abort()` is a no-op that returns it reusable (never leaked),
    // and terminalize the exchange as a dial failure.
    if !socket_addr_is_routable(&peer) {
      stream.abort(c);
      self.plane.pool.give(c);
      self.plane.connections.remove(&eid);
      self.endpoint.handle_dial_failed(eid, now);
      return;
    }

    // Derive an ephemeral local port from the ExchangeId so each dial uses a
    // distinct port within the IANA ephemeral range (49152–65535, 16 384 ports). The
    // ExchangeId is a monotonically increasing u64 per endpoint, making this a cheap,
    // collision-resistant scheme without an explicit port allocator.
    let local_port = 49152u16 + (eid.get() as u16 % 16384);
    if stream.connect(c, peer, local_port).is_err() {
      // The connect was rejected before any SYN: abort, reclaim the socket, drop the
      // Connection (with all its parked state), and terminalize the exchange as a
      // dial failure.
      stream.abort(c);
      self.plane.pool.give(c);
      self.plane.connections.remove(&eid);
      self.endpoint.handle_dial_failed(eid, now);
    }
  }

  /// Assign a freed slot to each connection still waiting in
  /// [`ConnState::PendingDial`], one per freed slot, and dial it.
  ///
  /// Services the waiting connections oldest-first — by ascending `ExchangeId`,
  /// which is the machine's monotonically increasing per-endpoint correlation
  /// token, so the oldest deferred dial is dialed first. Stops the moment the pool
  /// empties again so the rest stay parked for a later tick. Called each `pump` LAST
  /// in the accept/replenish/dial phase — after `reap_closing`, `check_listener`,
  /// and the `ensure_listener` self-heal — so the inbound listener has already taken
  /// its slot and a deferred dial claims only what remains, yet a slot freed by a
  /// timed-out dead-seed bridge is still promptly spent on the oldest waiting viable
  /// dial rather than the intent being lost.
  ///
  /// A connection already retired (its bridge timed out and issued a `Close`) was
  /// removed from `connections` by `teardown`, so this never dials a dead exchange.
  /// Each assigned connection's parked `out` bytes and `fin_pending` FIN survive the
  /// transition and flush/fire once the socket is Established.
  fn drain_pending_dials<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    // Collect the waiting dials oldest-first. The borrow of `connections` is
    // released before the dial loop mutates the plane.
    let mut waiting: std::vec::Vec<(ExchangeId, SocketAddr)> = self
      .plane
      .connections
      .iter()
      .filter(|(_, c)| c.state == ConnState::PendingDial)
      .map(|(&eid, c)| (eid, c.peer))
      .collect();
    if waiting.is_empty() {
      return;
    }
    waiting.sort_by_key(|(eid, _)| eid.get());

    for (eid, peer) in waiting {
      // Only a reset, reuse-ready slot may back a fresh dial (see `ensure_listener`
      // for the async-teardown rationale). When none is ready — the pool is empty,
      // or every freed slot is still resetting — leave the rest parked for a later
      // tick; the deferred dials are retried once a worker finishes its reset.
      let Some(c) = self.plane.pool.take_where(|&c| stream.reuse_ready(c)) else {
        break;
      };
      // Assign the freed slot and transition PendingDial → Dialing, then issue the
      // connect. `assign_socket` retains any parked `out` / `fin_pending`. The
      // connection is still present (a racing Close would have removed it, but
      // `waiting` was just collected from `connections` this same tick with no
      // intervening machine call), so the assignment always finds it.
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.assign_socket(c);
      }
      self.dial(eid, peer, c, now, stream);
    }
  }

  /// Give the listener and any deferred dials first claim on whatever is currently
  /// in the pool: self-heal a missing listener, then assign the rest to
  /// `PendingDial` connections oldest-first.
  ///
  /// Idempotent and LISTENER-FIRST — `ensure_listener` claims at most one slot
  /// before `drain_pending_dials` touches the pool, so an inbound listener is never
  /// starved by deferred outbound intent — and a no-op when the pool is empty or
  /// there is no unmet demand. Run at two points each `pump`: early (1c), over the
  /// slots `reap_closing` freed plus the spare pool, BEFORE the machine tick so a
  /// prior-tick `PendingDial` is dialed before its bridge can time out; and late
  /// (7d''), over every slot the machine's teardown / close paths freed THIS tick, so
  /// an in-tick free immediately backs a waiting dial or restores the listener
  /// rather than sitting idle until the next pump.
  fn rebalance_pool<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    self.ensure_listener(stream);
    self.drain_pending_dials(now, stream);
  }

  /// Drain all `StreamAction`s emitted by the machine this tick.
  ///
  /// `Connect` — take a pooled slot, create a `Dialing` [`Connection`] (slot
  /// assigned), and `dial` it. When the pool is exhausted a `PendingDial` connection
  /// (no slot) is recorded instead and `drain_pending_dials` assigns a slot once one
  /// frees, so the intent is never lost. If the connect call itself errors the slot
  /// is returned to the pool, the connection is removed, and the bridge is retired by
  /// its own dial/handshake deadline, since the driver has no prompt dial-cancel
  /// path.
  ///
  /// `Shutdown` — the SEND-half close signal: the local side finished sending but
  /// the bridge is still awaiting the peer's reply and/or FIN. The graceful
  /// write-half FIN (`close`) is DEFERRED by setting the connection's `fin_pending`
  /// flag; `flush_pending_shutdowns` emits it once the connection is `Established`
  /// and the tx ring has drained, then transitions it to `HalfClosed` (still mapped)
  /// so the peer's reply still pumps inbound. The socket is reclaimed only later, by
  /// this exchange's `Close`.
  ///
  /// `Close` — the exchange completed GRACEFULLY (the bridge reached `BothClosed`):
  /// tear down via `teardown`, which removes the connection (dropping all its
  /// per-exchange state) and reclaims its slot by socket state — directly to the pool
  /// if both FINs already completed, parked in `closing` if our FIN is in flight (we
  /// half-closed, or an acceptor graceful-closes its CloseWait socket to flush its
  /// reply + a clean EOF), or — when buffered bytes remain — parked in `Closing` to
  /// finish flushing them before the terminal FIN. A graceful close never discards
  /// undelivered bytes.
  ///
  /// `Abort` — the exchange FAILED (dial failure, label/encryption rejection, or an
  /// elapsed exchange deadline): tear down via `abort_exchange`, which removes the
  /// connection (DISCARDING its buffered `out` bytes), hard-`abort()`s the socket
  /// (RST), and returns it straight to the pool. No `Closing` drain, no graceful FIN
  /// — the bytes are stale and the peer is given up on.
  fn drain_stream_actions<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    // The machine guarantees all Connects surface before any Shutdown/Close/Abort
    // (see StreamEndpoint::poll_action ordering contract). Draining fully in one pass
    // is therefore safe: no teardown can precede its Connect.
    while let Some(action) = self.endpoint.poll_action() {
      match action {
        StreamAction::Connect(info) => {
          let eid = info.id();
          let peer = info.peer();
          // Record the originating StreamId so a driver awaiting this exchange can
          // correlate its terminal `ExchangeCompleted` by StreamId, not by arrival
          // order. Recorded unconditionally (even when the pool is exhausted and the
          // dial defers to `PendingDial`): the exchange exists and will complete.
          self.outbound_stream_ids.insert(eid, info.stream_id());

          // Only a reset, reuse-ready slot may back a fresh dial (see
          // `ensure_listener`); a freed-but-still-resetting slot (async-teardown
          // driver) is skipped and the dial defers to `PendingDial` until its worker
          // finishes the reset.
          match self.plane.pool.take_where(|&c| stream.reuse_ready(c)) {
            Some(c) => {
              // A slot is free: create the Dialing connection (slot assigned) and
              // issue the connect this same tick.
              self
                .plane
                .connections
                .insert(eid, Connection::dialing(peer, c));
              self.dial(eid, peer, c, now, stream);
            }
            None => {
              // Pool exhausted (or every freed slot still resetting): no slot free to
              // back this dial right now. The Connect action was consumed by
              // poll_action and is never re-emitted, so dropping it would LOSE the
              // dial intent. Record a PendingDial connection (no slot yet) instead;
              // this same tick's request bytes and a same-tick Shutdown accumulate on
              // it, and `drain_pending_dials` assigns a slot once `reap_closing` frees
              // one (e.g. when a dead seed's bridge times out). This is what lets a
              // multi-seed `join()` reach a viable later seed even when earlier dead
              // seeds momentarily hold every slot.
              self
                .plane
                .connections
                .insert(eid, Connection::pending_dial(peer));
            }
          }
        }

        StreamAction::Shutdown(r) => {
          // Graceful write-side half-close (TCP FIN), DEFERRED. Issuing the link
          // layer's `close()` now would destroy a pre-Established socket and its
          // buffered push/pull bytes. Set the connection's `fin_pending` flag;
          // `flush_pending_shutdowns` emits the FIN once the socket is Established and
          // its tx ring has fully drained, then transitions the connection to
          // HalfClosed (still mapped) so the reply still pumps inbound. A Shutdown for
          // a PendingDial connection (slot not yet assigned) is honored too: the flag
          // rides the connection until the slot is assigned and drains.
          if let Some(conn) = self.plane.connections.get_mut(&r.id()) {
            conn.fin_pending = true;
          }
        }

        StreamAction::Close(r) => {
          // The exchange is done — tear it down and reclaim its slot. `teardown`
          // removes the Connection and reclaims by socket state (clean both-FIN done →
          // pool; our FIN already sent but peer's pending → park in `closing`; nothing
          // left to deliver → graceful FIN then park; abrupt / never-established →
          // abort). The one case it does NOT remove on the spot is a graceful close
          // whose send-capable socket still holds undelivered bytes: it parks the
          // Connection in `Closing` so the egress pump can finish flushing them, and
          // `flush_closing` removes it and FINs once they are delivered — draining the
          // reply rather than truncating it.
          self.teardown(r.id(), now, stream);
        }

        StreamAction::Abort(r) => {
          // The exchange FAILED (dial failure, label/encryption rejection, or an
          // elapsed exchange deadline): its buffered `out` bytes are stale and MUST be
          // discarded, not drained. Hard-reset the socket (RST) and reclaim it
          // immediately — no `Closing` drain, no graceful FIN. Unlike `Close`,
          // `abort_exchange` never parks the connection: a failed exchange owes nothing
          // to the peer, so its socket returns straight to the pool.
          self.abort_exchange(r.id(), stream);
        }
      }
    }
  }

  /// Abort the exchange for the machine's `StreamAction::Abort`, discarding any
  /// buffered outbound bytes and reclaiming the connection slot immediately.
  ///
  /// `Abort` is the machine's FAILED-terminal signal: the bridge reached a failed
  /// phase (dial failure, label/encryption rejection, or an elapsed exchange
  /// deadline). The buffered `out` bytes belong to an exchange the coordinator has
  /// given up on — flushing them would leak membership state from a failed push/pull
  /// (or hold the socket open until `close_timeout` draining bytes the peer will
  /// never act on), so they are discarded with the whole `Connection`.
  ///
  /// This is the unconditional analog of `teardown`'s abrupt `else` branch: the
  /// socket (if one was assigned) is `abort()`ed — a TCP RST that moves it to
  /// `Closed` in one step — and returned straight to the pool, regardless of the
  /// connection's prior state (`Dialing`, `Established`, `HalfClosed`, or `Closing`).
  /// A `PendingDial` connection (pool was exhausted, no slot assigned) has nothing to
  /// reset or reclaim: removing it is the whole abort, so a failed exchange is never
  /// later dialed.
  fn abort_exchange<S: StreamIo<Conn = C>>(&mut self, eid: ExchangeId, stream: &mut S) {
    let Some(conn) = self.plane.connections.remove(&eid) else {
      return;
    };
    // Removing the `Connection` already dropped its parked `out` bytes, the deferred
    // FIN flag, and the EOF-delivered flag. Reclaim the socket (if any) with a hard
    // reset so a half-delivered frame is not flushed.
    if let Some(c) = conn.socket {
      stream.abort(c);
      self.plane.pool.give(c);
    }
  }

  /// Tear down the exchange for the machine's `StreamAction::Close`, draining any
  /// undelivered outbound bytes before the terminal FIN and reclaiming the
  /// connection slot.
  ///
  /// `Close` is the machine's GRACEFUL terminal signal: the bridge reached
  /// `BothClosed` (peer replied + FIN'd). The failed-terminal case (an elapsed
  /// exchange deadline, a dial/label/encryption failure) is signalled separately by
  /// `StreamAction::Abort` → `abort_exchange`, which discards rather than drains. The
  /// connection is removed (dropping ALL of its per-exchange state at once — the
  /// parked `out` bytes, the deferred `fin_pending` FIN, and the `eof_delivered`
  /// flag) ONLY on a path that completes the teardown this tick; a graceful close
  /// with bytes still to deliver is instead deferred (see the drain branch below).
  /// The socket (if one was assigned) is handled by its TCP state:
  ///
  /// - `!is_open()` (Closed / TimeWait) — both FINs already exchanged (the clean
  ///   `BothClosed` case where our graceful FIN went out in `flush_pending_shutdowns`
  ///   and the peer's FIN was pumped in): remove the connection and return the handle
  ///   straight to the pool, no close handshake left to wait on.
  /// - `is_open()` after a graceful half-close (the connection was in
  ///   [`ConnState::HalfClosed`]): our FIN is already in flight, so `out` can no
  ///   longer be flushed (the tx half is closed). Remove the connection and park the
  ///   handle in `closing` with a `now + close_timeout` deadline; the reap pass
  ///   reclaims it once it reaches Closed, or force-`abort()`s it at the deadline so a
  ///   vanished peer cannot leak the socket.
  /// - send-capable (`Established` / `CloseWait`) with outbound bytes still
  ///   undelivered (`!out.is_empty()` OR `send_queue() != 0`) — a push/pull reply (or
  ///   request) larger than the tx ring whose remainder is parked in `out` from
  ///   partial-write backpressure, or still in the tx ring awaiting ACK. Issuing the
  ///   FIN now would truncate it: `close()` only FINs after the bytes already IN the
  ///   tx ring, never the remainder still in `out`, and an `abort()` would RST over a
  ///   partial frame — collapsing the reliable push/pull to a gossip-only sync (or
  ///   losing the reply entirely). Instead transition the connection to
  ///   [`ConnState::Closing`] with a `now + close_timeout` deadline and KEEP it
  ///   mapped, so `pump_outbound_reliable` keeps flushing `out` into the tx ring;
  ///   `flush_closing` emits the terminal FIN and detaches the socket only once `out`
  ///   is empty and the tx ring is fully acknowledged, or force-aborts at the deadline
  ///   if the peer never drains it.
  /// - send-capable with nothing left to deliver (`out` empty AND `send_queue() == 0`)
  ///   — an acceptor in `CloseWait` whose reply already reached the wire, or an
  ///   `Established` one-shot teardown with an empty tx ring. There is nothing to
  ///   drain: emit the graceful FIN immediately (`close()` → LastAck / FinWait1,
  ///   giving the peer a clean EOF so its initiator commits the response) and park the
  ///   handle in `closing` for the reap pass.
  /// - any other `is_open()` state with no prior half-close (an abrupt `Close` — a
  ///   failed dial, an admission-rejected exchange, a never-promoted bridge in
  ///   SynSent): `abort()` (TCP RST) sets the state to Closed in one step, so the
  ///   handle returns to the pool at once with no close handshake and the failed
  ///   exchange's stale tx bytes are discarded rather than flushed — there is nothing
  ///   to deliver over a connection the peer never established.
  ///
  /// A connection still in `PendingDial` (its bridge timed out before a slot freed)
  /// has no socket: removing it is the whole teardown, so a retired exchange is never
  /// later dialed.
  fn teardown<S: StreamIo<Conn = C>>(&mut self, eid: ExchangeId, now: Instant, stream: &mut S) {
    // Inspect the connection WITHOUT removing it: a graceful close that still has
    // bytes to deliver must stay mapped (transition to `Closing`) so the egress pump
    // can finish flushing them. Only the paths that complete the teardown this tick
    // remove the connection, and each drops every per-exchange entry for `eid` —
    // parked `out` bytes, the deferred FIN flag, the EOF-delivered flag — in one
    // mutation, so no exchange state outlives a completed `Close`.
    let Some(conn) = self.plane.connections.get(&eid) else {
      return;
    };

    let Some(c) = conn.socket else {
      // PendingDial: no socket was ever assigned, so there is nothing to reclaim and
      // nothing to deliver. Removing the connection is the whole teardown.
      self.plane.connections.remove(&eid);
      return;
    };

    // Whether the connection already half-closed (its graceful FIN was emitted by
    // `flush_pending_shutdowns`), so a still-open socket is parked for the close
    // backstop rather than reset with a RST — and its `out` can no longer be flushed
    // (the tx half is closed).
    let was_half_closed = conn.state == ConnState::HalfClosed;
    let out_pending = !conn.out_is_empty();

    // Read the socket's send/recv capability and tx-ring depth once.
    let is_open = stream.is_open(c);
    let may_send = stream.may_send(c);
    let tx_unacked = stream.send_queue(c);

    if !is_open {
      // `!is_open()` is exactly `Closed | TimeWait`: both FINs already exchanged (or
      // the socket was already aborted). Reclaim directly, no close handshake left to
      // wait on.
      self.plane.connections.remove(&eid);
      self.plane.pool.give(c);
    } else if was_half_closed {
      // Our graceful FIN is in flight but the peer has not finished the close. Do NOT
      // close()/abort() now: the FIN was already sent and the tx half is closed, so
      // any `out` remainder is undeliverable. Park the handle in `closing` with a
      // `now + close_timeout` deadline so the reap pass reclaims it once it reaches
      // Closed, or force-aborts it at the deadline if the peer vanished mid-FIN.
      self.plane.connections.remove(&eid);
      self.plane.closing.insert(c, now + self.cfg.close_timeout);
    } else if may_send && (out_pending || tx_unacked != 0) {
      // Send-capable (Established / CloseWait) with outbound bytes the peer has NOT yet
      // received — parked in `out` (partial-write backpressure) and/or still
      // unacknowledged in the tx ring. FIN-ing now truncates the reply: `close()`
      // flushes only what is already in the tx ring, never the `out` remainder, and
      // `abort()` would RST over a partial frame. Defer the close: transition to
      // `Closing` (KEEP the connection mapped) with a deadline so
      // `pump_outbound_reliable` keeps draining `out` into the tx ring; the FIN + socket
      // detach happen in `flush_closing` once everything is delivered, or the deadline
      // force-aborts a permanently-backpressured / vanished peer.
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.state = ConnState::Closing;
        conn.close_deadline = Some(now + self.cfg.close_timeout);
        // Seed the drain-progress mark with the current undelivered count (parked `out`
        // + unacked tx ring). `flush_closing` re-arms `close_deadline` each tick the
        // count shrinks, so `close_timeout` bounds a STALL, not the total drain — a
        // slow-but-progressing peer is never truncated.
        conn.close_drain_mark = conn.out_bytes() + tx_unacked;
        // A still-pending Shutdown FIN is subsumed by the Closing drain's own terminal
        // FIN; clear it so `flush_pending_shutdowns` does not also act.
        conn.fin_pending = false;
      }
    } else if may_send {
      // Send-capable with nothing left to deliver (`out` empty AND tx ring fully
      // acknowledged): an acceptor whose reply already reached the wire, or an
      // Established one-shot teardown with an empty ring. Emit the graceful FIN
      // immediately — `close()` (CloseWait → LastAck, Established → FinWait1) sends our
      // FIN, giving the peer a clean EOF so its initiator commits the response — and
      // park the handle in `closing` for the reap backstop.
      self.plane.connections.remove(&eid);
      stream.close(c);
      self.plane.closing.insert(c, now + self.cfg.close_timeout);
    } else {
      // Abrupt teardown: a graceful `Close` whose socket is not send-capable and never
      // half-closed (e.g. a connection still in SynSent / never promoted). FAILED
      // exchanges no longer reach here — they arrive via `StreamAction::Abort` →
      // `abort_exchange` — but a graceful `Close` over a socket the peer never
      // established is handled defensively the same way: RST and reclaim at once.
      // `abort()` sets the state to Closed immediately, so reuse is safe without waiting
      // for a close handshake, and any stale tx bytes are discarded rather than flushed
      // — there is nothing to deliver over a connection the peer never established.
      self.plane.connections.remove(&eid);
      stream.abort(c);
      self.plane.pool.give(c);
    }
  }

  /// Complete the deferred terminal close of every connection draining in
  /// [`ConnState::Closing`].
  ///
  /// A graceful `StreamAction::Close` whose send-capable socket still held
  /// undelivered outbound bytes does NOT FIN on the spot — that would truncate a
  /// push/pull reply (or request) larger than the tx ring, whose remainder is parked
  /// in the connection's `out`. `teardown` instead moves it to `Closing` and leaves it
  /// mapped so `pump_outbound_reliable` keeps flushing `out` into the tx ring across
  /// ticks. This pass, run each tick right after that pump, drives each `Closing`
  /// connection to completion:
  ///
  /// - **Drained** — `out` is empty AND the tx ring is fully acknowledged
  ///   (`send_queue() == 0`): every byte reached the peer. Emit the terminal FIN
  ///   (`close()`, closing only the transmit half — CloseWait → LastAck or Established
  ///   → FinWait1) so the peer reads a clean EOF and commits the full response, remove
  ///   the `Connection` (dropping its remaining per-exchange state), and park the
  ///   detached handle in `closing` with a fresh `now + close_timeout` deadline for the
  ///   reap pass to reclaim once the close completes.
  /// - **Deadline elapsed** — `now >= close_deadline` while bytes are still
  ///   undelivered (the peer stopped draining the tx ring: permanent backpressure or a
  ///   vanished peer): force-`abort()` the socket (RST → Closed), remove the
  ///   `Connection`, and return the handle straight to the pool. The drain is
  ///   best-effort and bounded; it must never wedge a pooled socket.
  /// - Otherwise the connection is still draining within its deadline — leave it
  ///   mapped for a later tick.
  ///
  /// The `Closing` deadline is folded into `pump()`'s returned wakeup (alongside the
  /// `closing`-map deadlines) so a deadline-driven caller wakes in time to run this
  /// abort backstop by `close_timeout`.
  fn flush_closing<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    // Classify each Closing connection without holding the `connections` borrow across
    // the mutating socket / pool / closing-map calls below.
    enum Outcome<C> {
      /// Drained: emit the FIN and park the handle in `closing`.
      Fin(C),
      /// No drain progress for the full `close_timeout`: abort and reclaim.
      Abort(C),
      /// The drain made progress this tick (the peer acked bytes): re-arm the idle
      /// deadline with the new undelivered mark.
      Progress(usize),
    }

    let mut actions: std::vec::Vec<(ExchangeId, Outcome<C>)> = std::vec::Vec::new();
    for (&eid, conn) in self.plane.connections.iter() {
      if conn.state != ConnState::Closing {
        continue;
      }
      // A Closing connection always has an assigned socket (it reached Established /
      // CloseWait before the close); skip defensively if not.
      let Some(c) = conn.socket else { continue };
      // Undelivered = bytes still parked in `out` plus bytes in the tx ring the peer has
      // not yet acked. It only ever shrinks during a close (no new bytes are queued once
      // Closing), and shrinks ONLY when the peer acks — so a shrink is the peer-liveness
      // signal. `close_timeout` therefore bounds the time since the peer last acked (a
      // stall), not the total drain duration: a slow-but-progressing peer re-arms the
      // deadline every tick it acks.
      let undelivered = conn.out_bytes() + stream.send_queue(c);
      if undelivered == 0 {
        actions.push((eid, Outcome::Fin(c)));
      } else if undelivered < conn.close_drain_mark {
        actions.push((eid, Outcome::Progress(undelivered)));
      } else if conn.close_deadline.is_some_and(|d| now >= d) {
        // No progress for the full `close_timeout`: the peer stalled / vanished. Give up
        // on the remainder and reclaim the socket so the pool cannot wedge.
        actions.push((eid, Outcome::Abort(c)));
      }
    }

    for (eid, outcome) in actions {
      match outcome {
        Outcome::Fin(c) => {
          // Every byte was delivered: FIN the transmit half so the peer reads a clean
          // EOF, then park for the reap backstop.
          self.plane.connections.remove(&eid);
          stream.close(c);
          self.plane.closing.insert(c, now + self.cfg.close_timeout);
        }
        Outcome::Abort(c) => {
          // Idle deadline elapsed: RST and reclaim at once.
          self.plane.connections.remove(&eid);
          stream.abort(c);
          self.plane.pool.give(c);
        }
        Outcome::Progress(mark) => {
          // Re-arm the idle deadline from `now`; keep the connection mapped so the egress
          // pump keeps draining.
          if let Some(conn) = self.plane.connections.get_mut(&eid) {
            conn.close_drain_mark = mark;
            conn.close_deadline = Some(now + self.cfg.close_timeout);
          }
        }
      }
    }
  }

  /// Drain each active connection's socket rx buffer into the machine, and deliver a
  /// one-shot EOF once the peer's FIN has been received and drained.
  ///
  /// For every connection in `connections` that has an assigned slot, reads available
  /// bytes from the socket and feeds them to `handle_transport_data`. The peer's FIN
  /// is signalled by [`StreamIo::recv_finished`] once the FIN has been received AND the
  /// receive buffer is fully drained (the driver's equivalent of smoltcp's
  /// `RecvError::Finished`, true from ANY post-FIN state — CloseWait for a connection
  /// whose own send half is still open, or FinWait2 / TimeWait for one already in
  /// [`ConnState::HalfClosed`]). The EOF is delivered exactly once per connection —
  /// `Connection::eof_delivered` gates it so the bridge FSM receives a single
  /// half-close signal.
  ///
  /// A connection still in `PendingDial` has no slot, so it is skipped (there is
  /// nothing to receive on a dial that has not even been issued).
  ///
  /// The drain loop reads via `recv` unconditionally rather than gating on a
  /// readability check: an empty rx buffer is NOT proof there is nothing to deliver —
  /// the peer's drained FIN surfaces only as `recv_finished`, never as readable bytes,
  /// so the loop must consult `recv_finished` on a `None`/`Some(0)` read to deliver the
  /// EOF. `Some(n>0)` is data, `Some(0)`/`None` an empty ring (which is the peer FIN
  /// iff `recv_finished`), so no spurious EOF is delivered before the handshake
  /// completes.
  fn pump_inbound_reliable<S: StreamIo<Conn = C>>(&mut self, now: Instant, stream: &mut S) {
    // Scratch buffer for one read. 4 KiB matches the default socket rx ring size;
    // reads chunk that size at most, and the machine reassembles frames across multiple
    // calls to handle_transport_data.
    const READ_BUF: usize = 4096;
    let mut buf = [0u8; READ_BUF];

    // Collect the active (eid, handle) pairs first to avoid holding a `&connections`
    // borrow across the mutable `stream` + `endpoint` calls. PendingDial connections (no
    // slot) contribute no pair.
    let pairs: std::vec::Vec<_> = self
      .plane
      .connections
      .iter()
      .filter_map(|(&eid, c)| c.socket.map(|h| (eid, h)))
      .collect();

    for (eid, c) in pairs {
      // Drain the socket: read all buffered bytes, then observe a drained peer FIN.
      //
      // `recv` returns `Some(n)` for `n` readable bytes; `Some(0)` (a momentarily empty
      // ring) and `None` (nothing ready) both mean "no data this tick". Neither is by
      // itself an EOF — an Established socket with an empty ring also reports it. The peer
      // FIN is `recv_finished`: the FIN has been received AND the rx buffer is fully
      // drained. So on a no-data read, consult `recv_finished` and, if set, deliver
      // exactly one EOF to the machine, gated by the connection's `eof_delivered` flag.
      // A still-handshaking socket reports no data and `recv_finished` false, so no
      // spurious EOF is delivered before the handshake completes.
      loop {
        match stream.recv(c, &mut buf) {
          Some(n) if n > 0 => {
            self
              .endpoint
              .handle_transport_data(eid, &buf[..n], false, now);
          }
          _ => {
            // No data this tick (`Some(0)` or `None`). Deliver the peer FIN exactly once
            // when the receive half is gracefully closed and drained.
            if stream.recv_finished(c) {
              if let Some(conn) = self.plane.connections.get_mut(&eid) {
                if !conn.eof_delivered {
                  conn.eof_delivered = true;
                  self.endpoint.handle_transport_data(eid, &[], true, now);
                }
              }
            }
            break;
          }
        }
      }
    }
  }

  /// Flush partially-written outbound TCP bytes and drain new transport transmits from
  /// the machine.
  ///
  /// Outbound bytes are written to the connection's socket tx ring via `send`. Because
  /// the ring has finite capacity, `send` may accept fewer bytes than offered (partial
  /// write); the unwritten remainder stays at the front of the connection's `out` queue
  /// (oldest-first). Each tick this method:
  ///
  /// 1. **Appends new transmits** — calls `poll_transport_transmit()` until `None`,
  ///    pushing each `(eid, _peer, bytes)` onto the matching connection's `out` queue
  ///    (preserving emission order). A connection in `PendingDial` parks them too — the
  ///    machine emits a push/pull's request bytes the same tick as its dial, which for a
  ///    pool-exhausted dial is before any socket exists; the bytes flush once
  ///    `drain_pending_dials` assigns a slot. Bytes for an exchange with no connection
  ///    (torn down) are dropped.
  /// 2. **Flushes each connection's `out`** — for every connection whose socket is past
  ///    the handshake, drains its `out` front-to-back via `send`, stopping at the first
  ///    partial write so the unsent tail stays at the front and per-exchange byte order
  ///    is preserved. This includes a connection in [`ConnState::Closing`]: a graceful
  ///    close that still had buffered bytes stays mapped specifically so this pass keeps
  ///    flushing them until they are all delivered, which is what `flush_closing` then
  ///    waits on before emitting the terminal FIN — the drain-before-close guarantee.
  ///
  /// Appending before flushing (rather than writing new bytes directly) keeps a single
  /// ordered queue per connection: new bytes can never overtake an older parked
  /// remainder, and they still reach the tx ring this same tick via the flush pass
  /// below.
  ///
  /// # Writing to a still-opening socket
  ///
  /// A freshly dialled socket is in `SynSent` until its handshake completes, and the
  /// machine commonly hands the push/pull initiator's first bytes in the SAME tick the
  /// dial opens — before the socket is writable. The link layer rejects writes until
  /// Established, so the flush pass skips a connection whose socket is not yet
  /// send-capable (`!may_send`, which covers both a still-opening socket and a
  /// `PendingDial` connection with no socket at all); doing the write and treating the
  /// rejection as "tx half closed" would silently drop the entire push/pull half and
  /// wedge the join. The bytes stay in `out` and retry each tick until the socket
  /// reaches Established. A connection that has already half-closed its own tx half
  /// (FinWait*) carries no `out` bytes — `flush_pending_shutdowns` emits its FIN only
  /// after `out` drained — so it is never selected here, and the eventual `Close`
  /// reclaims it.
  fn pump_outbound_reliable<S: StreamIo<Conn = C>>(&mut self, stream: &mut S) {
    // --- Pass 1: append new transmits to their connection's out queue ---
    while let Some((eid, _peer, bytes)) = self.endpoint.poll_transport_transmit() {
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        // Park in order regardless of state: an Established connection's bytes are written
        // by the flush pass below this same tick; a Dialing / PendingDial connection holds
        // them until its socket is writable.
        conn.out.push_back(bytes);
      }
      // Otherwise no connection: the exchange was torn down before these bytes arrived, so
      // they are dropped — the exchange is dead.
    }

    // --- Pass 2: flush each connection's out queue to its socket ---
    //
    // Collect the active (eid, handle) pairs first so the `connections` borrow is released
    // before the mutable `stream` access inside the loop.
    let pairs: std::vec::Vec<_> = self
      .plane
      .connections
      .iter()
      .filter_map(|(&eid, c)| {
        // A PendingDial connection (no socket) or one with an empty queue has nothing to
        // flush this tick.
        if c.out.is_empty() {
          return None;
        }
        c.socket.map(|h| (eid, h))
      })
      .collect();

    for (eid, c) in pairs {
      // Not yet send-capable: a still-handshaking socket (the common push/pull-initiator
      // case, where the first bytes arrive the same tick as the dial) is `!may_send` while
      // SynSent/SynReceived. Leave the queue parked and retry once the socket is
      // Established — writing now and treating the rejection as a closed tx half would drop
      // the whole push/pull half and wedge the join. A genuinely closed tx half (the
      // exchange torn down) drops its bytes when its `Connection` is removed, so no path
      // leaks a parked queue.
      if !stream.may_send(c) {
        continue;
      }
      // Drain front-to-back. Stop at the first partial write so the unsent tail stays at
      // the front of the queue and later entries are not reordered.
      while let Some(front) = self
        .plane
        .connections
        .get(&eid)
        .and_then(|conn| conn.out.front().cloned())
      {
        let sent = stream.send(c, &front);
        if sent >= front.len() {
          // Fully written — pop it and continue with the next buffer.
          if let Some(conn) = self.plane.connections.get_mut(&eid) {
            conn.out.pop_front();
          }
        } else {
          // Partial write (the tx ring is full this tick, or — for `sent == 0` — the socket
          // is momentarily not accepting): replace the front with its unsent tail and stop
          // flushing this connection so the unsent tail stays at the front and later entries
          // are not reordered.
          if let Some(conn) = self.plane.connections.get_mut(&eid) {
            if let Some(slot) = conn.out.front_mut() {
              *slot = front.slice(sent..);
            }
          }
          break;
        }
      }
    }
  }

  /// Promote each `Dialing` connection whose TCP handshake has completed to
  /// `Established`.
  ///
  /// A connection is created `Dialing` (slot assigned, SynSent) and stays so while the
  /// three-way handshake is in flight. Once the socket can send (`may_send()` —
  /// Established, and also CloseWait if the peer FIN'd before we did), the connection is
  /// writable: its parked `out` flushes and a deferred FIN may fire. Recording that as
  /// `ConnState::Established` makes the FIN gate in `flush_pending_shutdowns` a precise
  /// `state == Established` check rather than re-deriving readiness from the socket, and
  /// keeps `ConnState` an honest reflection of the lifecycle. `PendingDial` connections
  /// (no socket) and ones already `Established`/`HalfClosed` are left as-is.
  fn promote_established<S: StreamIo<Conn = C>>(&mut self, stream: &mut S) {
    let promote: std::vec::Vec<ExchangeId> = self
      .plane
      .connections
      .iter()
      .filter(|(_, c)| c.state == ConnState::Dialing)
      .filter_map(|(&eid, c)| c.socket.map(|h| (eid, h)))
      .filter(|&(_, h)| stream.may_send(h))
      .map(|(eid, _)| eid)
      .collect();
    for eid in promote {
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.state = ConnState::Established;
      }
    }
  }

  /// Emit deferred graceful write-half FINs for connections whose socket can now carry
  /// one losslessly — KEEPING the connection mapped so its inbound reply still pumps.
  ///
  /// `StreamAction::Shutdown` is the machine's SEND-half close signal: the local side
  /// finished sending (a push/pull initiator wrote its full request), but the bridge is
  /// STILL awaiting the peer's reply and/or FIN. It sets the connection's `fin_pending`
  /// flag rather than closing the socket on the spot (see the `Shutdown` arm of
  /// `drain_stream_actions`). This pass, run each tick after the outbound byte pump,
  /// promotes a parked FIN to an actual `close()` once ALL of:
  ///
  /// - the connection is `Established` (its handshake completed, so `close()` issues a
  ///   real FIN instead of the SynSent/Listen abort that would discard buffered bytes);
  ///   AND
  /// - the tx ring has fully drained and been acknowledged (`send_queue() == 0`) and no
  ///   remainder is parked in the connection's `out`, so every push/pull byte has reached
  ///   the peer before the FIN.
  ///
  /// When all hold, `close()` issues the FIN (closing only the TRANSMIT half) and the
  /// connection transitions `Established → HalfClosed` — but it STAYS in `connections`.
  /// This is the half-close correctness invariant: a FinWait socket still receives, so
  /// the connection must remain mapped for `pump_inbound_reliable` to drain the peer's
  /// later reply and FIN (EOF) into the bridge. Detaching here would strand the peer's
  /// reply — the peer normally ACKs the request in a separate segment BEFORE sending its
  /// reply, so the FIN fires first — and the exchange would time out at its bridge
  /// deadline despite a valid response. The socket is reclaimed only later, by the
  /// `StreamAction::Close` the machine emits once the bridge reaches `BothClosed` (peer
  /// replied + FIN) or its exchange deadline elapses (peer vanished); see `teardown`.
  ///
  /// Emitting exactly once is structural: the transition to `HalfClosed` clears the
  /// connection from the `Established` set this pass selects, and resets `fin_pending`. A
  /// connection still `Dialing`/`PendingDial` (socket not yet writable) keeps its
  /// `fin_pending` flag and fires on a later tick once it reaches `Established`; the
  /// machine-issued abrupt `Close` on the bridge's deadline bounds the wait.
  fn flush_pending_shutdowns<S: StreamIo<Conn = C>>(&mut self, stream: &mut S) {
    // Collect the connections ready to emit their FIN: `fin_pending` set, in
    // `Established`, socket fully drained and acknowledged, `out` empty. The
    // `connections` borrow is released before the mutation below.
    let ready: std::vec::Vec<_> = self
      .plane
      .connections
      .iter()
      // `fin_pending` requested, handshake complete (Established), and no outbound bytes
      // still parked in `out`.
      .filter(|(_, c)| {
        c.fin_pending && c.state == ConnState::Established && c.out_is_empty()
      })
      .filter_map(|(&eid, c)| c.socket.map(|h| (eid, h)))
      // …and the socket's tx ring is fully drained and acknowledged, so every push/pull
      // byte reached the peer before the FIN.
      .filter(|&(_, h)| stream.may_send(h) && stream.send_queue(h) == 0)
      .collect();

    for (eid, c) in ready {
      // Socket is Established with an empty, acknowledged tx ring: issue the graceful FIN
      // on the TRANSMIT half only. The connection stays in `connections` so the peer's
      // reply + FIN still pump inbound; the transition to HalfClosed (and clearing
      // `fin_pending`) records the FIN is sent so a later flush tick does not `close()`
      // twice, and the eventual `StreamAction::Close` reclaims the socket.
      stream.close(c);
      if let Some(conn) = self.plane.connections.get_mut(&eid) {
        conn.fin_pending = false;
        conn.state = ConnState::HalfClosed;
      }
    }
  }

  /// Drain all outbound gossip transmits from the machine, encode each one with the
  /// shared no-std codec, and write it to the gossip socket.
  ///
  /// A single-message transmit (`Transmit::Packet`) is encoded as a plain frame; a
  /// multi-message batch (`Transmit::Compound`) is encoded as a compound frame. Encoding
  /// errors and a full tx ring both silently drop the datagram — gossip is best-effort
  /// and SWIM recovers on the next round.
  fn drain_gossip_transmits<G: GossipIo>(&mut self, gossip: &mut G) {
    let enc = memberlist_proto::codec::EncodeOptions::new(self.label.clone());
    while let Some(transmit) = self.endpoint.poll_memberlist_transmit() {
      let (dest, bytes) = match encode_transmit::<I>(transmit, &enc) {
        Some(pair) => pair,
        None => continue,
      };
      // Apply the cross-transport transforms to the encoded frame before it hits the
      // wire: compress, then checksum, then encrypt, so the on-wire byte order is
      // `[Encrypted[Checksumed[Compressed[frame]]]]`. All are identity when disabled, so a
      // default `TransformOptions` sends the same plaintext frame as before. Staged into
      // owned `Vec`s here so the `&self.endpoint` transform borrows end before the gossip
      // send below.
      let compressed = self.endpoint.compress_gossip(&bytes);
      let checksummed = match self.endpoint.checksum_gossip(&compressed) {
        Ok(b) => b,
        // Checksum is configured but its backend was not built into this binary. Drop
        // rather than emit an unverifiable datagram on a checksum-configured path.
        // Gossip is lossy and self-healing.
        Err(_) => continue,
      };
      let on_wire = match self.endpoint.encrypt_gossip(&checksummed) {
        Ok(b) => b,
        // Encryption is configured but the backend rejected the request (e.g. a primary
        // key whose AEAD algorithm was not built into this binary). Drop: emitting the
        // plaintext frame on an encrypted-cluster path would silently bypass
        // authentication. Gossip is lossy and self-healing.
        Err(_) => continue,
      };
      // Last-line egress drop: a non-routable destination (unspecified/multicast/broadcast
      // IP or port 0) is screened here so no bad address from ANY source (gossip,
      // push/pull, config) reaches the gossip socket. The link layer would itself reject
      // the unspecified address and port 0 (a silent per-datagram drop); skipping it up
      // front is a clean drop on the same predicate the route / neighbor lookup asserts,
      // and gossip is lossy so SWIM recovers regardless.
      if !socket_addr_is_routable(&dest) {
        continue;
      }
      // Last-line CIDR egress drop: never emit a gossip or directed-user datagram to a
      // destination our own policy excludes — the transmit-side counterpart to the recv
      // source filter. The `send` / `send_many` paths already drop a blocked destination
      // before enqueueing; this is the defense-in-depth catch for any other transmit
      // (and a no-op without the `cidr` feature).
      if cidr_blocks(&self.cidr_policy, dest.ip()) {
        continue;
      }
      // Best-effort enqueue: a full or errored gossip tx ring drops this datagram and SWIM
      // recovers on the next gossip round.
      gossip.send(&on_wire, dest);
    }
  }
}

/// Returns the earlier of two optional deadlines. If only one is `Some`, that deadline
/// wins; if both are `None` the result is `None`.
fn min_opt(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
  match (a, b) {
    (Some(x), Some(y)) => Some(core::cmp::min(x, y)),
    (x, y) => x.or(y),
  }
}

/// Encode one outbound gossip transmit using the shared no-std codec.
///
/// Returns `(dest, encoded_bytes)` on success, or `None` if encoding fails (in which
/// case the caller silently skips the datagram — gossip is lossy).
///
/// - `Transmit::Packet` → plain frame (single message).
/// - `Transmit::Compound` → compound frame (two or more messages piggybacked).
fn encode_transmit<I>(
  t: Transmit<I, SocketAddr>,
  enc: &memberlist_proto::codec::EncodeOptions,
) -> Option<(SocketAddr, bytes::Bytes)>
where
  I: memberlist_proto::Data,
{
  match t {
    Transmit::Packet(pkt) => {
      let (to, msg) = pkt.into_parts();
      let bytes = memberlist_proto::codec::encode_outgoing(&msg, enc).ok()?;
      Some((to, bytes))
    }
    Transmit::Compound(cmp) => {
      let (to, msgs) = cmp.into_parts();
      let bytes = memberlist_proto::codec::encode_outgoing_compound(&msgs, enc).ok()?;
      Some((to, bytes))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use core::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
  };

  use memberlist_proto::{CompressionOptions, EncryptionOptions, Keyring, SecretKey};
  use smol_str::SmolStr;

  struct NoGossip;

  impl GossipIo for NoGossip {
    fn recv(&mut self, _buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
      None
    }

    fn send(&mut self, _bytes: &[u8], _dest: SocketAddr) {}
  }

  /// A [`GossipIo`] that records every outbound datagram so a test can inspect
  /// the actual on-wire bytes (e.g. the transform wrapper tag).
  struct CaptureGossip {
    sent: std::vec::Vec<std::vec::Vec<u8>>,
  }

  impl CaptureGossip {
    fn new() -> Self {
      Self {
        sent: std::vec::Vec::new(),
      }
    }
  }

  impl GossipIo for CaptureGossip {
    fn recv(&mut self, _buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
      None
    }

    fn send(&mut self, bytes: &[u8], _dest: SocketAddr) {
      self.sent.push(bytes.to_vec());
    }
  }

  /// Drive `engine` until it emits at least one outbound gossip datagram (or the
  /// budget of pumps elapses), returning the captured datagrams. A peer is
  /// injected first so gossip has a destination.
  fn capture_gossip(transform: TransformOptions) -> std::vec::Vec<std::vec::Vec<u8>> {
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
      .with_rng_seed(42);
    let mut engine = Engine::try_new_at(cfg, transform, ep_cfg, now).expect("valid configuration");
    engine.start(now);
    // A peer to gossip TO, so `pump` emits at least one outbound gossip datagram.
    engine.inject_alive(SmolStr::new("peer"), node_addr(7947), now);

    let mut gossip = CaptureGossip::new();
    let mut stream = NoStream::with_pool(2);
    let mut t = now;
    for _ in 0..40 {
      engine.pump(t, &mut gossip, &mut stream);
      if !gossip.sent.is_empty() {
        break;
      }
      t += Duration::from_millis(50);
    }
    gossip.sent
  }

  struct NoStream {
    free: std::vec::Vec<u32>,
  }

  impl NoStream {
    fn with_pool(size: u32) -> Self {
      Self {
        free: (0..size).collect(),
      }
    }
  }

  impl StreamIo for NoStream {
    type Conn = u32;

    fn take_free(&mut self) -> Option<u32> {
      self.free.pop()
    }

    fn give(&mut self, c: u32) {
      self.free.push(c);
    }

    fn free_count(&self) -> usize {
      self.free.len()
    }

    fn listen(&mut self, _c: u32, _port: u16) -> Result<(), crate::StreamIoError> {
      Ok(())
    }

    fn accepted_peer(&self, _c: u32) -> Option<SocketAddr> {
      None
    }

    fn connect(
      &mut self,
      _c: u32,
      _remote: SocketAddr,
      _local_port: u16,
    ) -> Result<(), crate::StreamIoError> {
      Err(crate::StreamIoError::Busy)
    }

    fn may_send(&self, _c: u32) -> bool {
      false
    }

    fn may_recv(&self, _c: u32) -> bool {
      false
    }

    fn is_open(&self, _c: u32) -> bool {
      false
    }

    fn is_established(&self, _c: u32) -> bool {
      false
    }

    fn recv(&mut self, _c: u32, _buf: &mut [u8]) -> Option<usize> {
      None
    }

    fn recv_finished(&self, _c: u32) -> bool {
      false
    }

    fn send(&mut self, _c: u32, _bytes: &[u8]) -> usize {
      0
    }

    fn send_queue(&self, _c: u32) -> usize {
      0
    }

    fn close(&mut self, _c: u32) {}

    fn abort(&mut self, _c: u32) {}
  }

  fn node_addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
  }

  fn make_engine() -> Engine<SmolStr, u32> {
    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
      .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now)
      .expect("valid configuration must construct without error")
  }

  /// `set_compression_options` is accepted and the engine remains operational
  /// (a subsequent `pump` does not panic or error).
  #[test]
  fn set_compression_options_accepted_and_engine_still_pumps() {
    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));

    engine
      .set_compression_options(CompressionOptions::default())
      .expect("compression accepted while running");
    engine.start(now);

    let mut gossip = NoGossip;
    let mut stream = NoStream::with_pool(2);
    // `pump` must not panic after a compression-options update.
    let _deadline = engine.pump(now, &mut gossip, &mut stream);
    assert_eq!(
      engine.num_members(),
      1,
      "single-node engine has exactly one member"
    );
  }

  /// A caller `AliveDelegate` composes with the built-in routable filter: it can
  /// reject an otherwise-admissible (routable) peer, while a peer it accepts is
  /// admitted.
  #[test]
  fn custom_alive_delegate_restricts_admission() {
    struct RejectId(SmolStr);
    impl AliveDelegate<SmolStr, SocketAddr> for RejectId {
      fn notify_alive(
        &self,
        peer: &memberlist_proto::typed::NodeState<SmolStr, SocketAddr>,
      ) -> bool {
        peer.id_ref() != &self.0
      }
    }

    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));
    engine.set_alive_delegate(RejectId(SmolStr::new("blocked")));
    engine.start(now);

    // Rejected by the custom delegate even though its address is routable.
    engine.inject_alive(SmolStr::new("blocked"), node_addr(7001), now);
    assert!(
      !engine.is_alive(&SmolStr::new("blocked")),
      "a peer the custom delegate rejects must not be admitted"
    );

    // Passes both the routable filter and the custom delegate.
    engine.inject_alive(SmolStr::new("allowed"), node_addr(7002), now);
    assert!(
      engine.is_alive(&SmolStr::new("allowed")),
      "a peer that passes both the routable filter and the custom delegate is admitted"
    );
  }

  /// A CIDR policy set via `Options::with_cidr_policy` gates membership admission
  /// by the peer's self-advertised address: a routable peer outside the allow-list
  /// is rejected, while one inside is admitted. (The transport-boundary recv/accept
  /// guards share the same `cidr_blocks` predicate and are exercised end-to-end by
  /// the std drivers' integration tests; this pins the membership half on the
  /// shared no_std core that smoltcp and embassy both drive.)
  #[cfg(feature = "cidr")]
  #[test]
  fn cidr_policy_gates_membership_admission_by_advertised_address() {
    use memberlist_proto::CidrPolicy;

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
      .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
      .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let mut engine: Engine<SmolStr, u32> =
      Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now).expect("construct");
    engine.start(now);

    // Routable but outside 10.0.0.0/8 — rejected by the policy.
    let outside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
    engine.inject_alive(SmolStr::new("outside"), outside, now);
    assert!(
      !engine.is_alive(&SmolStr::new("outside")),
      "a routable peer outside the CIDR allow-list must not be admitted"
    );

    // Inside 10.0.0.0/8 — admitted (non-vacuity: the policy gates by IP, not all).
    let inside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7002);
    engine.inject_alive(SmolStr::new("inside"), inside, now);
    assert!(
      engine.is_alive(&SmolStr::new("inside")),
      "a peer inside the CIDR allow-list is admitted"
    );
  }

  /// Installing a caller alive delegate after a CIDR policy was set does NOT drop
  /// the policy: `set_alive_delegate` re-folds the stored policy, so admission
  /// stays routable AND in-policy AND delegate. Without the re-fold an accept-all
  /// delegate would re-admit an out-of-policy peer — this is the regression guard
  /// for that composition.
  #[cfg(feature = "cidr")]
  #[test]
  fn set_alive_delegate_preserves_the_cidr_policy() {
    use memberlist_proto::CidrPolicy;

    struct AcceptAll;
    impl AliveDelegate<SmolStr, SocketAddr> for AcceptAll {
      fn notify_alive(&self, _: &memberlist_proto::typed::NodeState<SmolStr, SocketAddr>) -> bool {
        true
      }
    }

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
      .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
      .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let mut engine: Engine<SmolStr, u32> =
      Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now).expect("construct");
    // An accept-all delegate installed AFTER the policy must not loosen it.
    engine.set_alive_delegate(AcceptAll);
    engine.start(now);

    let outside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
    engine.inject_alive(SmolStr::new("outside"), outside, now);
    assert!(
      !engine.is_alive(&SmolStr::new("outside")),
      "the CIDR policy must survive a later set_alive_delegate (accept-all must not re-admit an \
       out-of-policy peer)"
    );

    // Non-vacuity: an in-policy peer the accept-all delegate also accepts is admitted.
    let inside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7002);
    engine.inject_alive(SmolStr::new("inside"), inside, now);
    assert!(
      engine.is_alive(&SmolStr::new("inside")),
      "an in-policy peer is still admitted with the accept-all delegate"
    );
  }

  /// A reliable user-message (`send_reliable`) to a CIDR-blocked peer terminalizes
  /// as `Failed`, NOT a benign success. The outbound dial is rejected before
  /// connect via `handle_dial_failed`: a clean EOF on a never-connected one-way
  /// `UserMessage` would otherwise complete the exchange as `Succeeded`, falsely
  /// reporting the send delivered when the bytes were dropped with the reclaimed
  /// connection.
  #[cfg(feature = "cidr")]
  #[test]
  fn cidr_blocked_send_reliable_fails_not_succeeds() {
    use memberlist_proto::{
      CidrPolicy,
      event::{Event, ExchangeKind, ExchangeOutcome},
    };

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
      .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
      .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let mut engine: Engine<SmolStr, u32> =
      Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now).expect("construct");

    // Seed a reliable slot for the dial plus a listener slot, so the Connect drives
    // a real dial this tick rather than deferring to PendingDial.
    engine.set_listener(1);
    engine.plane_mut().pool.push(0);
    engine.start(now);

    // A one-way reliable user-message to a routable-but-out-of-policy peer; the
    // CIDR screen (which precedes the routable screen) rejects the dial.
    let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
    engine
      .send_reliable(blocked, bytes::Bytes::from_static(b"blocked-bytes"), now)
      .expect("send_reliable queues the exchange");

    // Pump until the exchange terminalizes: Connect -> dial(blocked) -> reject.
    let mut gossip = NoGossip;
    let mut stream = NoStream::with_pool(0);
    let mut outcome = None;
    for _ in 0..4 {
      engine.pump(now, &mut gossip, &mut stream);
      while let Some(ev) = engine.poll_event() {
        if let Event::ExchangeCompleted(ec) = ev {
          if ec.kind() == ExchangeKind::UserMessage {
            outcome = Some(ec.outcome());
          }
        }
      }
      if outcome.is_some() {
        break;
      }
    }
    assert_eq!(
      outcome,
      Some(ExchangeOutcome::Failed),
      "a CIDR-blocked send_reliable must complete as Failed (a benign EOF would falsely succeed it)"
    );
  }

  /// A directed unreliable `send` to a CIDR-blocked destination emits NO gossip
  /// datagram — the outbound counterpart to the recv source filter. An in-policy
  /// destination still emits, proving the drop is the policy and not a vacuous
  /// no-send.
  #[cfg(feature = "cidr")]
  #[test]
  fn cidr_blocked_unreliable_send_emits_no_datagram() {
    use memberlist_proto::CidrPolicy;

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
      .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
      .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let mut engine: Engine<SmolStr, u32> =
      Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now).expect("construct");
    engine.start(now);
    let mut stream = NoStream::with_pool(0);

    // A routable-but-out-of-policy destination: the send is dropped before
    // enqueueing, so the gossip drain emits nothing.
    let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
    engine
      .send(blocked, bytes::Bytes::from_static(b"blocked"))
      .expect("best-effort send returns Ok");
    let mut gossip = CaptureGossip::new();
    engine.pump(now, &mut gossip, &mut stream);
    assert!(
      gossip.sent.is_empty(),
      "no datagram may be emitted to a CIDR-blocked destination, saw {}",
      gossip.sent.len()
    );

    // Non-vacuity: an in-policy destination DOES emit the directed datagram.
    let allowed = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7002);
    engine
      .send(allowed, bytes::Bytes::from_static(b"allowed"))
      .expect("send");
    let mut gossip2 = CaptureGossip::new();
    engine.pump(now, &mut gossip2, &mut stream);
    assert!(
      !gossip2.sent.is_empty(),
      "an in-policy directed send must emit a datagram (the block is by IP, not unconditional)"
    );
  }

  /// A caller `MergeDelegate` installs cleanly and the engine stays operational.
  /// (Merge-rejection behaviour itself is covered by the machine's own tests; the
  /// engine only forwards the predicate.)
  #[test]
  fn custom_merge_delegate_installs_and_engine_still_pumps() {
    struct RejectAllMerges;
    impl MergeDelegate<SmolStr, SocketAddr> for RejectAllMerges {
      fn notify_merge(
        &self,
        _peers: &[memberlist_proto::typed::NodeState<SmolStr, SocketAddr>],
      ) -> bool {
        false
      }
    }

    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));
    engine.set_merge_delegate(RejectAllMerges);
    engine.start(now);

    let mut gossip = NoGossip;
    let mut stream = NoStream::with_pool(2);
    let _deadline = engine.pump(now, &mut gossip, &mut stream);
    assert_eq!(
      engine.num_members(),
      1,
      "single-node engine has exactly one member after installing a merge delegate"
    );
  }

  /// After `leave()`, every runtime data- and policy-state setter rejects with
  /// `NotRunning` rather than a false `Ok`, since a post-leave mutation could
  /// never reach the wire.
  #[test]
  fn control_setters_reject_after_leave() {
    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));
    engine.start(now);
    engine.leave(now).expect("leave from a running node");

    let meta =
      memberlist_proto::typed::Meta::try_from(bytes::Bytes::from_static(b"x")).expect("meta");
    assert!(
      matches!(
        engine.update_node_metadata(meta),
        Err(memberlist_proto::Error::NotRunning)
      ),
      "update_node_metadata must reject after leave"
    );
    assert!(
      matches!(
        engine.set_local_state(bytes::Bytes::from_static(b"s")),
        Err(memberlist_proto::Error::NotRunning)
      ),
      "set_local_state must reject after leave"
    );
    assert!(
      matches!(
        engine.set_ack_payload(bytes::Bytes::from_static(b"a")),
        Err(memberlist_proto::Error::NotRunning)
      ),
      "set_ack_payload must reject after leave"
    );
    assert!(
      matches!(
        engine.queue_user_broadcast(bytes::Bytes::from_static(b"b")),
        Err(memberlist_proto::Error::NotRunning)
      ),
      "queue_user_broadcast must reject after leave"
    );
    assert!(
      matches!(
        engine.set_compression_options(CompressionOptions::default()),
        Err(memberlist_proto::Error::NotRunning)
      ),
      "set_compression_options must reject after leave"
    );
    assert!(
      matches!(
        engine.set_encryption_options(EncryptionOptions::default()),
        Err(crate::error::ControlError::NotRunning)
      ),
      "set_encryption_options must reject after leave"
    );
  }

  /// After `leave()` the machine admits no inbound Alive, so installing an
  /// admission delegate is inert — it succeeds (matching the core machine's
  /// infallible setter) but is never consulted.
  #[test]
  fn admission_delegate_install_after_leave_is_inert() {
    struct AcceptAll;
    impl AliveDelegate<SmolStr, SocketAddr> for AcceptAll {
      fn notify_alive(&self, _: &memberlist_proto::typed::NodeState<SmolStr, SocketAddr>) -> bool {
        true
      }
    }

    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));
    engine.start(now);
    engine.leave(now).expect("leave from a running node");

    // Installing succeeds but the machine never consults it: an inbound Alive
    // during the drain is not admitted, accept-all delegate notwithstanding.
    engine.set_alive_delegate(AcceptAll);
    engine.inject_alive(SmolStr::new("late"), node_addr(7050), now);
    assert!(
      !engine.is_alive(&SmolStr::new("late")),
      "a left node admits no Alive even with an accept-all delegate installed"
    );
  }

  /// The `Checksumed` wrapper tag on the wire. With neither encryption nor a
  /// label configured, the checksum wrapper is the OUTERMOST frame, so an
  /// outbound gossip datagram begins with this tag exactly when checksum is
  /// applied on the send path.
  #[cfg(feature = "checksum-crc32")]
  const CHECKSUMED_TAG: u8 = 15;

  /// With checksum enabled, every outbound gossip datagram carries the
  /// `Checksumed` wrapper tag — proving the engine's send path actually applies
  /// `checksum_gossip` (a wire-shape assertion, not mere convergence: an
  /// unwrapped datagram would still be accepted by a peer, so convergence alone
  /// cannot detect a send path that skips the checksum wrap).
  #[cfg(feature = "checksum-crc32")]
  #[test]
  fn enabled_checksum_stamps_the_checksumed_tag_on_outbound_gossip() {
    use memberlist_proto::{ChecksumAlgorithm, ChecksumOptions};

    let transform = TransformOptions::default()
      .with_checksum(ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32));
    let sent = capture_gossip(transform);

    assert!(
      !sent.is_empty(),
      "engine must emit at least one gossip datagram"
    );
    for dg in &sent {
      assert_eq!(
        dg.first().copied(),
        Some(CHECKSUMED_TAG),
        "every outbound gossip datagram must begin with the Checksumed tag when \
         checksum is enabled; got first byte {:?}",
        dg.first()
      );
    }
  }

  /// With checksum disabled (the default), no outbound gossip datagram carries
  /// the `Checksumed` wrapper tag — confirming the wrap is opt-in and the
  /// positive test above is discriminating rather than vacuous.
  #[cfg(feature = "checksum-crc32")]
  #[test]
  fn disabled_checksum_leaves_no_checksumed_tag_on_outbound_gossip() {
    let sent = capture_gossip(TransformOptions::default());

    assert!(
      !sent.is_empty(),
      "engine must emit at least one gossip datagram"
    );
    for dg in &sent {
      assert_ne!(
        dg.first().copied(),
        Some(CHECKSUMED_TAG),
        "a default (checksum-disabled) node must not stamp the Checksumed tag"
      );
    }
  }

  /// `set_encryption_options` with no keyring (disabled) is always accepted.
  #[test]
  fn set_encryption_options_disabled_is_always_ok() {
    let mut engine = make_engine();
    let result = engine.set_encryption_options(EncryptionOptions::default());
    assert!(result.is_ok(), "disabling encryption must always succeed");
  }

  /// `set_encryption_options` rejects a keyring whose algorithm backend is not
  /// compiled into this build, leaving the prior policy intact so the engine
  /// continues to pump without disruption.
  ///
  /// This test runs only when `encryption-aes-gcm` is absent; with the backend
  /// present the AES-256 key is valid and the test is logically inverted (the
  /// round-trip smoke test below covers that path).
  #[cfg(not(feature = "encryption-aes-gcm"))]
  #[test]
  fn set_encryption_options_rejects_unsupported_keyring_and_engine_still_pumps() {
    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));

    // AES-256 key whose algorithm backend (`encryption-aes-gcm`) is absent in
    // this build; the probe inside `set_encryption_options` must reject it.
    let bad_key = SecretKey::Aes256([0x5a; 32]);
    let bad_opts = EncryptionOptions::new().with_keyring(Keyring::new(bad_key));
    let err = engine
      .set_encryption_options(bad_opts)
      .expect_err("keyring with unsupported algorithm must be rejected");
    assert!(
      matches!(
        err,
        crate::error::ControlError::Encryption(
          memberlist_proto::EncryptionError::UnsupportedAlgorithm(_)
        )
      ),
      "expected Encryption(UnsupportedAlgorithm), got {err:?}"
    );

    // The engine must still function under its original (no-encryption) policy.
    engine.start(now);
    let mut gossip = NoGossip;
    let mut stream = NoStream::with_pool(2);
    let _deadline = engine.pump(now, &mut gossip, &mut stream);
    assert_eq!(
      engine.num_members(),
      1,
      "engine remains functional after rejected encryption update"
    );
  }

  /// `set_encryption_options` with a valid AES-256 keyring succeeds when the
  /// `encryption-aes-gcm` backend is compiled in, and the engine pumps normally
  /// afterward.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_accepts_valid_aes256_keyring_and_engine_still_pumps() {
    let mut engine = make_engine();
    let now = Instant::from_origin(Duration::from_secs(86_400));

    let key = SecretKey::Aes256([0x42; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
    engine
      .set_encryption_options(opts)
      .expect("valid AES-256 keyring must be accepted when encryption-aes-gcm is compiled in");

    engine.start(now);
    let mut gossip = NoGossip;
    let mut stream = NoStream::with_pool(2);
    let _deadline = engine.pump(now, &mut gossip, &mut stream);
    assert_eq!(
      engine.num_members(),
      1,
      "engine remains functional after encryption update"
    );
  }

  /// `try_new_at` rejects a gossip checksum algorithm whose backend feature is
  /// not compiled into this build, with a typed `InitError::Checksum`. The
  /// options builder accepts the algorithm, but every later `checksum_gossip`
  /// would fail and the driver would drop the datagram — silently disabling ALL
  /// gossip. This is the construction-time analogue of the encryption keyring
  /// rejection (the embedded engine has no runtime checksum setter); it runs only
  /// when `checksum-murmur3` is absent so the Murmur3 backend is genuinely
  /// missing.
  #[cfg(not(feature = "checksum-murmur3"))]
  #[test]
  fn try_new_at_rejects_unsupported_checksum_algorithm() {
    use memberlist_proto::{ChecksumAlgorithm, ChecksumOptions};

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10));
    let ep_cfg =
      memberlist_proto::EndpointOptions::new(SmolStr::new("bad-checksum"), node_addr(7946))
        .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));

    // Murmur3 is absent in this build. The selection is accepted by
    // `with_checksum`, but the trial `apply` probe inside `try_new_at` returns
    // `ChecksumError::Disabled`.
    let transform = TransformOptions::default()
      .with_checksum(ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Murmur3));

    let err = Engine::<SmolStr, u32>::try_new_at(cfg, transform, ep_cfg, now)
      .map(|_| ())
      .expect_err("unsupported checksum algorithm must be rejected at construction");
    assert!(
      matches!(err, InitError::Checksum(_)),
      "expected InitError::Checksum, got {err:?}"
    );
  }

  /// A disabled (no-algorithm) checksum policy always constructs cleanly — there
  /// is no backend to probe, so `try_new_at` succeeds regardless of feature set.
  #[test]
  fn try_new_at_accepts_disabled_checksum() {
    use memberlist_proto::ChecksumOptions;

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10));
    let ep_cfg =
      memberlist_proto::EndpointOptions::new(SmolStr::new("no-checksum"), node_addr(7946))
        .with_rng_seed(42);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let transform = TransformOptions::default().with_checksum(ChecksumOptions::new());

    assert!(
      Engine::<SmolStr, u32>::try_new_at(cfg, transform, ep_cfg, now).is_ok(),
      "a disabled checksum policy must always construct"
    );
  }

  struct QueueGossip {
    /// Pending inbound datagrams: each entry is `(src, bytes)`.
    inbound: std::vec::Vec<(SocketAddr, std::vec::Vec<u8>)>,
    /// Outbound datagrams captured from `send`.
    outbound: std::vec::Vec<(std::vec::Vec<u8>, SocketAddr)>,
  }

  impl QueueGossip {
    fn new() -> Self {
      Self {
        inbound: std::vec::Vec::new(),
        outbound: std::vec::Vec::new(),
      }
    }

    fn push(&mut self, src: SocketAddr, bytes: std::vec::Vec<u8>) {
      self.inbound.push((src, bytes));
    }
  }

  impl GossipIo for QueueGossip {
    fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
      if self.inbound.is_empty() {
        return None;
      }
      let (src, bytes) = self.inbound.remove(0);
      let n = bytes.len().min(buf.len());
      buf[..n].copy_from_slice(&bytes[..n]);
      Some((src, n))
    }

    fn send(&mut self, bytes: &[u8], dest: SocketAddr) {
      self.outbound.push((bytes.to_vec(), dest));
    }
  }

  /// An engine with a cluster label must:
  ///
  /// - Reject gossip datagrams that carry no label (or a wrong label) — the
  ///   `decode_incoming` label check drops them before the machine sees them, so
  ///   no membership change occurs.
  /// - Accept gossip datagrams that carry the matching label — the machine
  ///   processes the Alive and the member count rises.
  /// - Stamp the cluster label onto every outbound gossip datagram — the on-wire
  ///   bytes decode successfully with the matching label and fail when no label
  ///   (or the wrong label) is expected.
  #[test]
  fn gossip_carries_and_checks_the_configured_label() {
    use memberlist_proto::{
      DecodeOptions, EncodeOptions, Node, encode_outgoing,
      typed::{Alive, Message},
    };

    let cfg = Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10));
    let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("alpha"), node_addr(7946))
      .with_rng_seed(1);
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let transform = TransformOptions::new()
      .with_label(Some(b"alpha".to_vec()))
      .expect("valid label");
    let mut engine = Engine::try_new_at(cfg, transform, ep_cfg, now).expect("valid config");
    engine.start(now);

    // ── Ingress: unlabeled datagram must be dropped. ─────────────────────────
    // Build a plaintext Alive for a fake peer. Incarnation > 0 passes SWIM's
    // freshness check for a node this engine has never seen.
    let peer_addr: SocketAddr = SocketAddr::new(
      core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 2)),
      7946,
    );
    let ghost_node = Node::new(SmolStr::new("ghost"), peer_addr);
    let alive_msg = Alive::new(1, ghost_node.clone());
    let unlabeled = encode_outgoing::<SmolStr, SocketAddr>(
      &Message::Alive(alive_msg),
      &EncodeOptions::default(), // no label
    )
    .expect("encode unlabeled Alive");

    let src: SocketAddr = SocketAddr::new(
      core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 3)),
      7946,
    );
    let mut gossip = QueueGossip::new();
    gossip.push(src, unlabeled.to_vec());
    let mut stream = NoStream::with_pool(2);
    let _ = engine.pump(now, &mut gossip, &mut stream);

    assert_eq!(
      engine.num_members(),
      1,
      "unlabeled inbound gossip must be rejected — ghost must not appear"
    );

    // ── Ingress: wrong-label datagram must also be dropped. ──────────────────
    let alive_msg2 = Alive::new(1, ghost_node.clone());
    let beta_labeled = encode_outgoing::<SmolStr, SocketAddr>(
      &Message::Alive(alive_msg2),
      &EncodeOptions::new(Some(bytes::Bytes::from_static(b"beta"))),
    )
    .expect("encode beta-labeled Alive");

    gossip.push(src, beta_labeled.to_vec());
    let _ = engine.pump(now, &mut gossip, &mut stream);

    assert_eq!(
      engine.num_members(),
      1,
      "wrong-label inbound gossip must be rejected — ghost must not appear"
    );

    // ── Ingress: correctly-labeled datagram must be accepted. ─────────────────
    let alive_msg3 = Alive::new(1, ghost_node);
    let alpha_labeled = encode_outgoing::<SmolStr, SocketAddr>(
      &Message::Alive(alive_msg3),
      &EncodeOptions::new(Some(bytes::Bytes::from_static(b"alpha"))),
    )
    .expect("encode alpha-labeled Alive");

    gossip.push(src, alpha_labeled.to_vec());
    let _ = engine.pump(now, &mut gossip, &mut stream);

    assert_eq!(
      engine.num_members(),
      2,
      "alpha-labeled inbound gossip must be accepted — ghost must appear"
    );

    // ── Egress: outbound gossip must carry the cluster label. ────────────────
    // Advance time enough that the machine emits at least one gossip transmit.
    // The gossip timer fires on the first tick; pump once more to drain it.
    let later = Instant::from_origin(Duration::from_secs(86_400 + 2));
    let _ = engine.pump(later, &mut gossip, &mut stream);

    // Collect the first outbound datagram (if any).  The machine may or may
    // not emit one on the first ticked pump; iterate until we see a send or
    // exhaust a few more ticks.
    let mut tries = 0u32;
    while gossip.outbound.is_empty() && tries < 10 {
      let t = Instant::from_origin(Duration::from_secs(86_400 + 2 + tries as u64));
      let _ = engine.pump(t, &mut gossip, &mut stream);
      tries += 1;
    }

    assert!(
      !gossip.outbound.is_empty(),
      "engine must emit at least one gossip transmit after the timer fires"
    );

    let (wire_bytes, _dest) = &gossip.outbound[0];
    // Decoding with the matching label must succeed.
    let ok = memberlist_proto::codec::decode_incoming(
      bytes::Bytes::copy_from_slice(wire_bytes),
      &DecodeOptions::new(Some(bytes::Bytes::from_static(b"alpha"))),
    );
    assert!(
      ok.is_ok(),
      "outbound gossip must be decodable with the cluster label; got {:?}",
      ok.err()
    );

    // Decoding with no expected label must fail (labeled frame on an unlabeled
    // receiver is rejected with DoubleLabel).
    let no_label = memberlist_proto::codec::decode_incoming(
      bytes::Bytes::copy_from_slice(wire_bytes),
      &DecodeOptions::new(None),
    );
    assert!(
      no_label.is_err(),
      "outbound gossip must NOT be accepted by a no-label decoder"
    );

    // Decoding with the wrong label must fail (LabelMismatch).
    let wrong_label = memberlist_proto::codec::decode_incoming(
      bytes::Bytes::copy_from_slice(wire_bytes),
      &DecodeOptions::new(Some(bytes::Bytes::from_static(b"beta"))),
    );
    assert!(
      wrong_label.is_err(),
      "outbound gossip must NOT be accepted by a wrong-label decoder"
    );
  }
}
