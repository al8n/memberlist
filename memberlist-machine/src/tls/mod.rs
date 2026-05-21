//! Composed TLS-over-TCP + memberlist Sans-I/O coordinator.
//!
//! rustls carries reliable exchanges over a per-exchange TLS-over-TCP
//! connection; plain UDP carries unreliable gossip. TCP and UDP are separate
//! sockets, so there is no first-byte demux. rustls is itself Sans-I/O — the
//! TLS record layer lives inside this coordinator and the driver moves only
//! raw TCP/UDP bytes.
//!
//! Two ingress paths replace the QUIC coordinator's single demuxed UDP socket:
//! gossip arrives via [`TlsEndpoint::handle_gossip`] (buffered raw for the
//! codec-owning driver, exactly like the QUIC `Class::Memberlist` path), and
//! per-exchange ciphertext arrives via [`TlsEndpoint::handle_transport_data`]
//! (routed into the owning bridge, then a coordinator tick). Outbound, the
//! coordinator surfaces ciphertext keyed by exchange handle + peer
//! ([`TlsEndpoint::poll_transport_transmit`]), connect / half-close / teardown
//! signals ([`TlsEndpoint::poll_action`]), and the unreliable gossip
//! [`Transmit`] stream ([`TlsEndpoint::poll_memberlist_transmit`]).
//!
//! The fixed per-tick step order keeps the load-bearing invariant — the
//! stream-endpoint-event drain STRICTLY precedes `Endpoint::handle_timeout`
//! (else a reliable-fallback ping ack that lands the same tick the probe
//! cumulative deadline expires is lost and the peer is wrongly Suspected).

mod bridge;
mod conn;
mod crypto;
mod records;

use std::{
  collections::HashMap,
  net::SocketAddr,
  time::{Duration, Instant},
};

use bytes::Bytes;
use rustls::pki_types::ServerName;

use crate::{
  addr_bridge::AddrBridge,
  endpoint::Endpoint,
  event::{Event, PushPullKind, StreamId, Transmit},
};
use bridge::TlsBridge;
use records::TlsRecords;

pub use conn::ExchangeId;
pub use crypto::TlsConfig;

use conn::TlsConns;

/// Handshake completion budget for an inbound (accepted) exchange.
///
/// The outbound dial deadline is the membership exchange deadline carried on
/// `Event::DialRequested`; an accepted connection has no such intent, so the
/// coordinator bounds its handshake with this fixed budget (matching the
/// default `EndpointConfig::stream_timeout`, which the membership `Endpoint`
/// applies to the accepted `Stream`'s exchange once it is minted). A handshake
/// that has not completed by `now + ACCEPT_HANDSHAKE_DEADLINE` is reaped by the
/// bridge's `poll_timeout` / `pump_out` deadline path with no `Stream` minted.
const ACCEPT_HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// One pending dial intent the coordinator owes a `service_dials` attempt to.
///
/// `attempted` distinguishes a freshly-sieved entry (never yet processed by
/// `service_dials`) from one that has been processed at least once. Freshly-
/// sieved entries get an immediate-due wake out of `poll_timeout` so a caller
/// that advances solely by `poll_timeout` cannot orphan them: a caller that
/// drains `poll_event` (sieving `DialRequested` into `dial_pending`) and then
/// waits on `poll_timeout` alone would otherwise only see the intent's own
/// `deadline` and wake at `now + stream_timeout`, by which point
/// `service_dials` would discover the deadline elapsed and consume the intent
/// via `dial_failed`. Once `service_dials` attempts the entry, `attempted`
/// becomes `true` and stays `true`: future wake-ups are driven by the bridge's
/// own handshake `poll_timeout` and the intent's `deadline`. The TLS dial
/// always succeeds in surfacing a `Connect` action and inserting a
/// `Handshaking` bridge on its first attempt (there is no pooled-connection
/// credit to wait on), so an attempted TLS entry never requeues; the
/// `attempted` bit is retained verbatim from the QUIC coordinator for the
/// immediate-due wake discipline.
struct PendingDial<A> {
  id: StreamId,
  peer: A,
  deadline: Instant,
  attempted: bool,
}

/// How the coordinator mints the `Stream` for an exchange once its TLS
/// handshake completes. The membership `Endpoint` stays the sole `Stream`
/// factory; the bridge spends the whole handshake window with `stream = None`.
/// Newtype variants over the existing key types (no multi-field variants).
enum PendingMint<A> {
  /// Outbound dial: mint via `Endpoint::dial_succeeded(stream_id, now)` using
  /// the `StreamId` the inner endpoint allocated for the originating `start_*`.
  Outbound(StreamId),
  /// Inbound accept: mint via `Endpoint::accept_stream(peer, now)`.
  Inbound(A),
}

/// Per-exchange metadata the coordinator holds for the whole lifetime of one
/// reliable exchange (the bridge itself lives in [`TlsConns`], keyed by the
/// same [`ExchangeId`]). Accessor-only — private fields, read directly within
/// this module.
struct ExchangeMeta<A> {
  /// The peer `SocketAddr` every `poll_transport_transmit` for this exchange is
  /// tagged with so the driver writes the ciphertext on the right TCP
  /// connection.
  peer_socket: SocketAddr,
  /// `Some` until the handshake completes and the coordinator mints + promotes
  /// the `Stream`; `None` afterwards (an `Established` bridge needs no further
  /// minting decision).
  mint: Option<PendingMint<A>>,
  /// `true` once the coordinator has emitted this exchange's one
  /// [`TlsAction::Shutdown`] (graceful TCP write-side half-close after the
  /// bridge queued its `close_notify`). Guards a second emission.
  shutdown_emitted: bool,
}

/// Payload of [`TlsAction::Connect`]: dial a TCP connection for an exchange.
/// Accessor-only.
pub struct ConnectInfo {
  id: ExchangeId,
  peer: SocketAddr,
}

impl ConnectInfo {
  /// The exchange handle the coordinator keys this connection on. Every
  /// subsequent `handle_transport_data` / `poll_transport_transmit` for the
  /// connection carries this same handle.
  pub const fn id(&self) -> ExchangeId {
    self.id
  }

  /// The peer `SocketAddr` to TCP-connect to.
  pub const fn peer(&self) -> SocketAddr {
    self.peer
  }
}

/// Payload of [`TlsAction::Shutdown`] / [`TlsAction::Close`]: names one
/// exchange's TCP connection. Accessor-only.
pub struct ExchangeRef {
  id: ExchangeId,
}

impl ExchangeRef {
  /// The exchange handle whose TCP connection the action refers to.
  pub const fn id(&self) -> ExchangeId {
    self.id
  }
}

/// A transport directive the coordinator owes the driver for a per-exchange TCP
/// connection. Drained via [`TlsEndpoint::poll_action`].
///
/// Newtype variants over named accessor-only payload structs (the
/// no-multi-field-variant convention).
pub enum TlsAction {
  /// Open a TCP connection to the peer for a freshly-dialed outbound exchange.
  Connect(ConnectInfo),
  /// Half-close the TCP write side after the bridge queued its `close_notify`,
  /// so the peer reads a clean EOF once it has drained our buffered ciphertext.
  Shutdown(ExchangeRef),
  /// Tear down the TCP connection and forget the exchange — the bridge reached
  /// a terminal phase and has been reaped.
  Close(ExchangeRef),
}

impl TlsAction {
  /// Borrow the [`ConnectInfo`] iff this is a [`TlsAction::Connect`].
  pub const fn as_connect(&self) -> Option<&ConnectInfo> {
    match self {
      TlsAction::Connect(c) => Some(c),
      _ => None,
    }
  }

  /// Borrow the [`ExchangeRef`] iff this is a [`TlsAction::Shutdown`].
  pub const fn as_shutdown(&self) -> Option<&ExchangeRef> {
    match self {
      TlsAction::Shutdown(r) => Some(r),
      _ => None,
    }
  }

  /// Borrow the [`ExchangeRef`] iff this is a [`TlsAction::Close`].
  pub const fn as_close(&self) -> Option<&ExchangeRef> {
    match self {
      TlsAction::Close(r) => Some(r),
      _ => None,
    }
  }
}

/// Coordinator: `memberlist::Endpoint` (unreliable gossip + membership)
/// composed with per-exchange TLS-over-TCP (reliable). Pure Sans-I/O — inject
/// `now`.
///
/// `B` translates the membership address `A` to the transport `SocketAddr` and
/// supplies the rustls `server_name` (see [`AddrBridge`]); it is a marker type
/// parameter only — no value of `B` is stored.
pub struct TlsEndpoint<I, A, B>
where
  I: nodecraft::Id
    + memberlist_wire::Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  ep: Endpoint<I, A>,
  cfg: TlsConfig,
  /// In-flight reliable exchanges (one bridge each), keyed by [`ExchangeId`].
  /// Connection-per-exchange — no pool, slab, or drained-reap.
  conns: TlsConns<I, A>,
  /// Per-exchange coordinator metadata, keyed in lockstep with `conns`. An
  /// outbound exchange carries its originating `StreamId` in
  /// [`PendingMint::Outbound`] here, so the handshake-completion mint maps an
  /// exchange to its `dial_succeeded` `StreamId` by iterating these directly —
  /// no separate `StreamId -> ExchangeId` reverse index is needed.
  exchanges: HashMap<ExchangeId, ExchangeMeta<A>>,
  /// Outbound per-exchange ciphertext produced this tick (handshake flights,
  /// app records, `close_notify`), tagged with the exchange handle + peer so
  /// the driver writes it on the right TCP connection. Drained via
  /// [`Self::poll_transport_transmit`].
  out_transmit: std::collections::VecDeque<(ExchangeId, SocketAddr, Bytes)>,
  /// Outbound transport directives (`Connect` / `Shutdown` / `Close`). Drained
  /// via [`Self::poll_action`].
  out_actions: std::collections::VecDeque<TlsAction>,
  /// Raw inbound gossip datagrams. `memberlist-machine` has no umbrella `codec`
  /// dependency, so the coordinator cannot decode them in-crate and MUST NOT
  /// silently drop them (that would lose every UDP ping/ack/alive/suspect on
  /// the composed unit's public ingress). They are buffered here and surfaced
  /// via [`Self::poll_memberlist_ingress`] for the codec-owning layer to unwrap
  /// and feed back through [`Self::handle_packet`].
  mem_ingress: std::collections::VecDeque<(SocketAddr, Bytes)>,
  /// Private queue of pending dial intents. `memberlist::Endpoint::poll_event`
  /// emits `Event::DialRequested { id, peer, deadline }` for an external driver
  /// to dial — but in the composed design `TlsEndpoint` IS the driver:
  /// `service_dials` surfaces the `Connect` action and builds the bridge
  /// itself. If `DialRequested` leaked through [`Self::poll_event`] an external
  /// caller draining events between `handle_timeout` and the next
  /// `service_dials` would pop the intent and silently drop it — the bridge
  /// would never open and the exchange would never run. The coordinator
  /// therefore sieves `Event::DialRequested` into this private deque; external
  /// pollers only ever observe application-visible events. Each entry carries
  /// an `attempted` bit so a freshly-sieved intent surfaces in
  /// [`Self::poll_timeout`] as an immediate-due wake — see [`PendingDial`].
  dial_pending: std::collections::VecDeque<PendingDial<A>>,
  /// Most recent `now: Instant` injected by any `handle_*` / `start_*` wrapper.
  /// Used by [`Self::poll_timeout`] as the known-past anchor for the
  /// immediate-due wake of an unattempted `dial_pending` entry: the only way to
  /// signal "fire as soon as possible" out of an `Option<Instant>` Sans-I/O API
  /// is to return an `Instant <= caller's now`, and the only such anchor we may
  /// hold is one we observed from a prior `handle_*` call (Sans-I/O forbids
  /// `Instant::now()`). Stays `None` only before the very first `handle_*` /
  /// `start_*` call.
  last_now: Option<Instant>,
  _addr: core::marker::PhantomData<fn(B)>,
}

impl<I, A, B> TlsEndpoint<I, A, B>
where
  I: nodecraft::Id
    + memberlist_wire::Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  B: AddrBridge<A>,
{
  /// Build the coordinator from a membership [`Endpoint`] and a [`TlsConfig`]
  /// bundle.
  pub fn new(ep: Endpoint<I, A>, cfg: TlsConfig) -> Self {
    Self {
      ep,
      cfg,
      conns: TlsConns::new(),
      exchanges: HashMap::new(),
      out_transmit: std::collections::VecDeque::new(),
      out_actions: std::collections::VecDeque::new(),
      mem_ingress: std::collections::VecDeque::new(),
      dial_pending: std::collections::VecDeque::new(),
      last_now: None,
      _addr: core::marker::PhantomData,
    }
  }

  /// Borrow the inner membership endpoint (members / queue_user_broadcast / …).
  pub fn endpoint(&self) -> &Endpoint<I, A> {
    &self.ep
  }

  /// Number of active reliable-exchange bridges (one per in-flight push/pull,
  /// reliable-ping, or user-message exchange, plus any still-handshaking
  /// dial/accept). Observation-only, for a driver/test to assert no bridge
  /// leaked after an exchange completed or its connection dropped.
  pub fn live_bridge_count(&self) -> usize {
    self.conns.len()
  }

  /// Initiate one SWIM probe tick on the inner membership endpoint.
  ///
  /// Pass-through to [`Endpoint::start_probe`]; sets `last_now`. The probe
  /// itself rides the unreliable UDP path; only if it fails does the reliable
  /// TLS fallback kick in via the natural suspicion / failure-detection timing.
  pub fn start_probe(&mut self, now: Instant) -> bool {
    self.last_now = Some(now);
    self.ep.start_probe(now)
  }

  /// Seed an `Alive` state on the inner membership endpoint (bootstrap path).
  /// Pass-through to [`Endpoint::handle_alive`]; sets `last_now`.
  pub fn handle_alive(&mut self, from: A, alive: memberlist_wire::typed::Alive<I, A>, at: Instant) {
    self.last_now = Some(at);
    self.ep.handle_alive(from, alive, at);
  }

  /// Inject a `Suspect` event on the inner membership endpoint (test-harness
  /// path). Pass-through to [`Endpoint::handle_suspect`]; sets `last_now`.
  pub fn handle_suspect(
    &mut self,
    from: A,
    suspect: memberlist_wire::typed::Suspect<I>,
    at: Instant,
  ) {
    self.last_now = Some(at);
    self.ep.handle_suspect(from, suspect, at);
  }

  /// Re-queue an event for observation by a later [`Self::poll_event`].
  ///
  /// Anchors `last_now = Some(now)` unconditionally. `Event::DialRequested` is
  /// routed DIRECTLY into the private `dial_pending` deque (bypassing the inner
  /// queue) so a caller that calls [`Self::poll_timeout`] WITHOUT an
  /// intervening [`Self::poll_event`] sieve still sees the immediate-due rescue
  /// term; every other variant delegates to [`Endpoint::requeue_event`].
  pub fn requeue_event(&mut self, ev: Event<I, A>, now: Instant) {
    self.last_now = Some(now);
    match ev {
      Event::DialRequested { id, peer, deadline } => {
        self.dial_pending.push_back(PendingDial {
          id,
          peer,
          deadline,
          attempted: false,
        });
      }
      other => self.ep.requeue_event(other),
    }
  }

  /// Begin a graceful leave; delegates to the membership endpoint.
  pub fn leave(&mut self, now: Instant) -> Result<(), crate::error::Error> {
    self.last_now = Some(now);
    self.ep.leave(now)
  }

  /// Next membership/lifecycle event for the driver, if any.
  ///
  /// `Event::DialRequested` is sieved out of the inner endpoint's queue into
  /// the private [`dial_pending`](Self::dial_pending) deque and is NEVER
  /// returned to external callers: the coordinator IS the driver and dials
  /// itself (see [`Self::service_dials`]). External callers only observe
  /// application-visible events.
  pub fn poll_event(&mut self) -> Option<Event<I, A>> {
    loop {
      match self.ep.poll_event()? {
        Event::DialRequested { id, peer, deadline } => {
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
          continue;
        }
        other => return Some(other),
      }
    }
  }

  /// Unified next-deadline = `min` over the membership endpoint, every bridge
  /// (handshake or exchange deadline), AND every pending-dial intent's own
  /// deadline. Returns an immediate-due wake (an `Instant` already `<= caller's
  /// now`) whenever `dial_pending` holds an entry `service_dials` has not yet
  /// attempted.
  ///
  /// The pending-dial deadline term is correctness, not optimisation: a
  /// fully-stalled `dial_pending` queue must still be serviced no later than
  /// its intent's `deadline` so the `dial_failed` does not slip past the
  /// user-visible exchange timeout on a quiet cluster. The immediate-due term
  /// is defence-in-depth for callers that bypass the high-level `start_*`
  /// wrappers (which dial in-band) and queue a `DialRequested` directly. There
  /// are NO connection-pool terms — connection-per-exchange means the bridge's
  /// own `poll_timeout` already covers every per-connection timer.
  ///
  /// `last_now` is `None` only before the very first `handle_*` / `start_*`
  /// call: in that window the immediate-due wake degrades to the intent's
  /// `deadline` term.
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    let mut best = self.ep.poll_timeout();
    for id in self.conns.ids() {
      if let Some(b) = self.conns.get_mut(id) {
        if let Some(t) = b.poll_timeout() {
          best = Some(best.map_or(t, |b| b.min(t)));
        }
      }
    }
    let mut has_unattempted = false;
    for entry in &self.dial_pending {
      let t = entry.deadline;
      best = Some(best.map_or(t, |b| b.min(t)));
      if !entry.attempted {
        has_unattempted = true;
      }
    }
    if has_unattempted {
      if let Some(anchor) = self.last_now {
        best = Some(best.map_or(anchor, |b| b.min(anchor)));
      }
    }
    best
  }

  /// Next outbound per-exchange ciphertext `(exchange, peer, bytes)`, if any.
  /// The driver writes `bytes` on the TCP connection for `exchange` (to
  /// `peer`).
  pub fn poll_transport_transmit(&mut self) -> Option<(ExchangeId, SocketAddr, Bytes)> {
    self.out_transmit.pop_front()
  }

  /// Next outbound transport directive ([`TlsAction`]), if any.
  pub fn poll_action(&mut self) -> Option<TlsAction> {
    self.out_actions.pop_front()
  }

  /// Next typed unreliable memberlist [`Transmit`] for the driver to encode
  /// onto the unreliable (UDP) path, if any.
  ///
  /// Each call drains ONE `Transmit` straight out of the inner
  /// `Endpoint::poll_transmit`; nothing is prebuffered coordinator-internally,
  /// so the inner pop — which decrements `Endpoint`'s leave-completion counter
  /// and emits `Event::LeftCluster` after the last dead-self notice — happens
  /// at the SAME moment the datagram crosses to the external driver. A caller
  /// that `leave(now)`s, ticks, and then reads `poll_event` cannot observe
  /// `LeftCluster` until it has drained the dead-self tail through this
  /// accessor: tearing the socket down on `LeftCluster` therefore guarantees
  /// every dead-self broadcast has been handed to the driver, so peers see
  /// `Dead`/`Left` rather than wrongly Suspecting.
  pub fn poll_memberlist_transmit(&mut self) -> Option<Transmit<I, A>> {
    self.ep.poll_transmit()
  }

  /// Next raw inbound gossip datagram, if any. The codec-owning layer drains
  /// this, decodes each `Message`, and feeds it back through
  /// [`Self::handle_packet`].
  pub fn poll_memberlist_ingress(&mut self) -> Option<(SocketAddr, Bytes)> {
    self.mem_ingress.pop_front()
  }

  /// Feed one decoded unreliable memberlist
  /// [`Message`](memberlist_wire::typed::Message) into the inner membership
  /// endpoint. Pass-through to [`Endpoint::handle_packet`]; the composed unit's
  /// unreliable ingress is `handle_gossip` → `poll_memberlist_ingress` → (codec
  /// decode) → `handle_packet`, never a direct call into the inner `Endpoint`.
  pub fn handle_packet(
    &mut self,
    from: A,
    msg: memberlist_wire::typed::Message<I, A>,
    now: Instant,
  ) {
    self.ep.handle_packet(from, msg, now);
  }

  /// Inbound gossip datagram from the UDP socket.
  ///
  /// **Buffered only** — the codec-owning driver MUST drain via
  /// [`Self::poll_memberlist_ingress`], decode each frame, feed every typed
  /// message via [`Self::handle_packet`], and then call [`Self::handle_timeout`]
  /// to advance time. Running [`Self::handle_timeout`] before the buffered
  /// gossip is decoded and fed would risk same-instant probe / suspect timers
  /// firing before a just-arrived `Ack` / `Alive` is applied — a spurious
  /// fallback ping or false `Suspect` could fire even though the resolving
  /// message is already sitting in [`Self::poll_memberlist_ingress`]'s queue
  /// locally. (Mirror of the QUIC coordinator's `Class::Memberlist` ingress;
  /// here every UDP datagram is gossip, since reliable rides separate TCP.)
  pub fn handle_gossip(&mut self, from: A, datagram: &[u8], now: Instant) {
    self.last_now = Some(now);
    let socket = B::to_socket(&from);
    self
      .mem_ingress
      .push_back((socket, Bytes::copy_from_slice(datagram)));
  }

  /// Inbound ciphertext for one exchange's TCP connection.
  ///
  /// Routes the bytes into the owning bridge's
  /// [`TlsBridge::handle_transport_data`] (a zero-length slice is the TCP `read
  /// == 0` half-close anchor), then runs a coordinator tick. The bridge handles
  /// the handshake (shuttle) or the established byte pump (decrypt → plaintext →
  /// `Stream::handle_data`); a record-layer / mTLS / decode failure terminalizes
  /// the bridge, which the tick's `pump_bridges` then reaps. Mirrors the QUIC
  /// coordinator's `Class::Quic` path (`route_datagram_event` + `run_tick`).
  pub fn handle_transport_data(&mut self, id: ExchangeId, ciphertext: &[u8], now: Instant) {
    self.last_now = Some(now);
    if let Some(bridge) = self.conns.get_mut(id) {
      // Ignoring Err: an `Err` means the bridge terminalized (mTLS / decrypt /
      // record / decode failure); `run_tick`'s `pump_bridges` reaps it and
      // emits the `Close` action. There is no separate action to take here.
      let _ = bridge.handle_transport_data(ciphertext, now);
    }
    self.run_tick(now);
  }

  /// The driver accepted an inbound TCP connection from `from`. Allocates an
  /// [`ExchangeId`], builds a server-side `Handshaking` bridge bounded by
  /// [`ACCEPT_HANDSHAKE_DEADLINE`], and returns the handle the driver tags this
  /// connection's inbound ciphertext with. The `Stream` is minted later (at
  /// handshake completion, via `Endpoint::accept_stream`).
  ///
  /// A `TlsRecords::server` construction error (a misconfigured `ServerConfig`)
  /// is unrecoverable for this connection: no bridge is inserted and the
  /// returned handle has no exchange. The driver observes this as a connection
  /// that never produces ciphertext (and may close it on its own accept-side
  /// deadline); the membership layer is untouched because no `Stream` ever
  /// existed.
  pub fn accept_connection(&mut self, from: A, now: Instant) -> ExchangeId {
    let id = self.conns.allocate();
    let peer_socket = B::to_socket(&from);
    match TlsRecords::server(self.cfg.server().clone()) {
      Ok(records) => {
        let bridge = TlsBridge::new(records, now + ACCEPT_HANDSHAKE_DEADLINE);
        self.conns.insert(id, bridge);
        self.exchanges.insert(
          id,
          ExchangeMeta {
            peer_socket,
            mint: Some(PendingMint::Inbound(from)),
            shutdown_emitted: false,
          },
        );
      }
      Err(_) => {
        // No bridge for a config-rejected server connection. The handle is
        // still returned (monotonic; never reused) so the driver has a stable
        // key, but it maps to no exchange.
      }
    }
    id
  }

  /// Initiate an outbound push/pull state exchange with `peer` and attempt the
  /// dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_push_pull`] that ALSO drives
  /// `service_dials(now)` + `flush_outbound(now)` before returning, so the
  /// `DialRequested` the inner endpoint queues is sieved, attempted (the
  /// `Connect` action surfaced and the `Handshaking` bridge built), and the
  /// dial's first handshake flight emerges on the very next
  /// [`Self::poll_transport_transmit`] — a driver that uses only the public
  /// poll surface sees the exchange progress without a same-instant
  /// `handle_timeout` pre-pump.
  pub fn start_push_pull(&mut self, peer: A, kind: PushPullKind, now: Instant) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_push_pull(peer, kind, now);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a reliable-stream fallback ping for probe `probe_seq` and attempt
  /// the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_reliable_ping`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-flush
  /// semantics. The `deadline` is the owning probe's single cumulative deadline
  /// (NOT an independent stream-timeout), forwarded unchanged; `now` is taken
  /// separately because `service_dials` needs the real wall-clock instant and
  /// `last_now` must remain a known-past anchor.
  pub fn start_reliable_ping(
    &mut self,
    peer_id: I,
    peer_addr: A,
    probe_seq: u32,
    deadline: Instant,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self
      .ep
      .start_reliable_ping(peer_id, peer_addr, probe_seq, deadline);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a one-way reliable user-message delivery to `peer` and attempt
  /// the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_user_message`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-flush
  /// semantics.
  pub fn start_user_message(&mut self, peer: A, payload: Bytes, now: Instant) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_user_message(peer, payload, now);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Timer tick from the driver.
  pub fn handle_timeout(&mut self, now: Instant) {
    self.last_now = Some(now);
    self.run_tick(now);
  }

  // ---- internals --------------------------------------------------------

  /// The fixed per-tick step order (load-bearing — see module docs).
  ///
  /// Step (2) (pump every bridge + drain each non-terminal stream's
  /// endpoint-events into the `Endpoint`) MUST strictly precede step (3)
  /// (`ep.handle_timeout`): a reliable-fallback ping ack delivered on the same
  /// tick the probe cumulative deadline expires is carried by the stream's last
  /// `poll_endpoint_event`; draining it after the probe timeout would lose it
  /// and wrongly Suspect a live peer. Do not reorder.
  ///
  /// Step (4) (handshake-completion mint) mints the `Stream` for any bridge
  /// whose TLS handshake completed since the last tick and promotes it; a
  /// freshly-promoted OUTBOUND bridge carries its request bytes in the minted
  /// `Stream`'s output buffer. Step (5) (`service_dials`) inserts new
  /// `Handshaking` outbound bridges. Both happen AFTER step (2)'s
  /// `pump_bridges` already ran, so step (5.5)'s second `pump_bridges` pumps
  /// those just-promoted / just-inserted bridges in the same tick — the
  /// strict-poll self-sufficiency seam (without it the next coordinator wake
  /// under strict poll-surface driving comes only at the bridge's exchange
  /// deadline, by which point `Stream::handle_data` rejects the buffered
  /// request as timed out). `pump_bridges` is idempotent on already-pumped
  /// bridges, so the second call is a no-op on every bridge that already ran in
  /// step (2). There is NO connection drained-reap step (connection-per-
  /// exchange — a reaped bridge frees its own connection via the `Close`
  /// action).
  fn run_tick(&mut self, now: Instant) {
    // (1) inbound feed already done by the caller (`handle_transport_data`).
    // (2) pump bridges + drain stream endpoint-events into the Endpoint.
    self.pump_bridges(now);
    // (3) THEN membership timers (probe cumulative-deadline, suspicion).
    self.ep.handle_timeout(now);
    // (4) mint the Stream for any bridge whose handshake just completed.
    self.service_handshake_completions(now);
    // (5) dial requests emitted by (3).
    self.service_dials(now);
    // (5.5) pump bridges promoted/inserted by (4) and (5) this same tick.
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Step (2) of the per-tick order: pump every bridge's outbound half, drain
  /// each non-terminal stream's endpoint-events into the `Endpoint`, and
  /// D1-drain-then-reap any bridge that turned terminal.
  ///
  /// Extracted so [`Self::flush_outbound`] can re-use the same bridge step
  /// after `service_dials`. There is no inbound `pump_in`: inbound ciphertext
  /// is fed through [`Self::handle_transport_data`] → `TlsBridge::handle_transport_data`
  /// directly; this step only drives the outbound side and the endpoint-event
  /// drain.
  fn pump_bridges(&mut self, now: Instant) {
    for id in self.conns.ids() {
      // Split borrow: take the bridge out, operate, put back (or reap).
      let Some(mut br) = self.conns.remove(id) else {
        continue;
      };
      // Replay any pre-promotion ciphertext tail FIRST (inbound), before the
      // outbound pump. A peer that coalesced its first request with its final
      // handshake flight had that request's tail retained while the bridge was
      // still `Handshaking`; this is the post-mint pump (run AFTER
      // `service_handshake_completions` promoted the bridge), so the retained
      // tail reassembles into the just-minted `Stream` THIS tick rather than
      // waiting for the next transport read. A no-op on every bridge with no
      // retained tail (the steady state).
      // Ignoring Err: a replay failure terminalizes the bridge; the
      // `is_terminal()` reap path below handles it. There is no separate action.
      let _ = br.replay_pending(now);
      // `pump_out` sets the bridge `fatal` flag on a transport / FSM error, so
      // `is_terminal()` below drives the prompt reap; the `#[must_use]` Result
      // is consumed — terminality is the signal.
      // Ignoring Err: `pump_out` failing terminalizes the bridge; the
      // `is_terminal()` reap path below handles it. There is no separate action.
      let _ = br.pump_out(now);
      if br.is_terminal() {
        br.drain_then_reap(&mut self.ep, now);
        self.reap_bridge(id, &mut br, now);
        drop(br);
      } else {
        // Drain endpoint-events EVERY tick (non-terminal payload-only path).
        br.drain_payload_only(&mut self.ep, now);
        // `drain_payload_only` may flip the bridge to terminal (a
        // `StreamCommand::Close` from an admission-rejected join sets `fatal`);
        // re-check so the bridge D1-drains and reaps in this SAME tick rather
        // than holding the connection until its exchange deadline.
        if br.is_terminal() {
          br.drain_then_reap(&mut self.ep, now);
          self.reap_bridge(id, &mut br, now);
          drop(br);
        } else {
          // Graceful half-close signal: the bridge queued its `close_notify`
          // (clean exchange) and is awaiting the peer's, so tell the driver to
          // half-close the TCP write side after it has drained our buffered
          // ciphertext. Emitted at most once per exchange. The bridge's
          // outbound ciphertext is collected by `finalize_tick` while it is
          // still alive — only a reaped (dropped) bridge needs the inline
          // reap-time collection in `reap_bridge`.
          self.maybe_emit_shutdown(id, &br);
          self.conns.insert(id, br);
        }
      }
    }
  }

  /// Drain a terminal bridge's final ciphertext (its `close_notify`, plus any
  /// inbound-response bytes `drain_then_reap` just encrypted) into the outbound
  /// queue, emit the [`TlsAction::Close`] teardown, and forget the exchange.
  /// Called after `drain_then_reap`, before the bridge is dropped — otherwise
  /// the trailing `close_notify` produced by a failure-path `retire_halves`
  /// would be lost when the bridge drops.
  fn reap_bridge(&mut self, id: ExchangeId, br: &mut TlsBridge<I, A>, now: Instant) {
    self.collect_bridge_transmits(id, br);
    if let Some(PendingMint::Outbound(sid)) = self.exchanges.remove(&id).and_then(|m| m.mint) {
      // The bridge reaped before its handshake completed (e.g. an mTLS reject on
      // a dial): the `StreamId` was never minted into a `Stream`, but the inner
      // endpoint still holds the pending intent. Retire it so the exchange does
      // not strand (a reliable-ping fallback is released, etc.). A bridge whose
      // handshake completed has `mint == None` (taken at promotion), so a
      // promoted bridge's clean reap does not re-enter `dial_failed`.
      self.ep.dial_failed(
        sid,
        crate::error::StreamError::DialFailed("tls handshake aborted".into()),
        now,
      );
    }
    self
      .out_actions
      .push_back(TlsAction::Close(ExchangeRef { id }));
  }

  /// Emit the one [`TlsAction::Shutdown`] for an exchange once its bridge has
  /// queued `close_notify` (clean half-close while awaiting the peer's). A
  /// terminal bridge is reaped with `Close` instead and never reaches here.
  fn maybe_emit_shutdown(&mut self, id: ExchangeId, br: &TlsBridge<I, A>) {
    if !br.close_notify_sent() {
      return;
    }
    if let Some(meta) = self.exchanges.get_mut(&id) {
      if !meta.shutdown_emitted {
        meta.shutdown_emitted = true;
        self
          .out_actions
          .push_back(TlsAction::Shutdown(ExchangeRef { id }));
      }
    }
  }

  /// Drain one bridge's outbound ciphertext into [`Self::out_transmit`], tagged
  /// with its exchange handle + peer socket.
  fn collect_bridge_transmits(&mut self, id: ExchangeId, br: &mut TlsBridge<I, A>) {
    let Some(peer_socket) = self.exchanges.get(&id).map(|m| m.peer_socket) else {
      return;
    };
    let mut buf = Vec::new();
    br.poll_transport_transmit(&mut buf);
    if !buf.is_empty() {
      self
        .out_transmit
        .push_back((id, peer_socket, Bytes::from(buf)));
    }
  }

  /// Step (4): for every exchange still awaiting its `Stream`, mint it once the
  /// TLS handshake has completed.
  ///
  /// `is_handshaking()` is `false` once the record layer reports the handshake
  /// done (the mint window) AND once the bridge is `Established` (already
  /// minted) — so the `meta.mint.is_some()` guard distinguishes the mint window
  /// from an already-promoted bridge. A bridge that FAILED during the handshake
  /// is terminal (and not handshaking); it is skipped here and reaped by
  /// `pump_bridges`.
  fn service_handshake_completions(&mut self, now: Instant) {
    for id in self.conns.ids() {
      let needs_mint = matches!(
        self.exchanges.get(&id),
        Some(meta) if meta.mint.is_some()
      );
      if !needs_mint {
        continue;
      }
      let ready = match self.conns.get_mut(id) {
        Some(br) => !br.is_terminal() && !br.is_handshaking(),
        None => false,
      };
      if !ready {
        continue;
      }
      // Take the mint decision out of the meta (single-shot).
      let mint = self
        .exchanges
        .get_mut(&id)
        .and_then(|m| m.mint.take())
        .expect("needs_mint implies the mint decision is present");
      match mint {
        PendingMint::Outbound(stream_id) => match self.ep.dial_succeeded(stream_id, now) {
          Some(stream) => {
            if let Some(br) = self.conns.get_mut(id) {
              br.promote(stream);
            }
          }
          None => {
            // The dial deadline already elapsed: no `Stream` is minted. Tear
            // the bridge down — emit `Close` and forget the exchange. (The
            // inner endpoint already retired the intent inside `dial_succeeded`.)
            if let Some(mut br) = self.conns.remove(id) {
              self.collect_bridge_transmits(id, &mut br);
              drop(br);
            }
            self.exchanges.remove(&id);
            self
              .out_actions
              .push_back(TlsAction::Close(ExchangeRef { id }));
          }
        },
        PendingMint::Inbound(peer) => {
          let stream = self.ep.accept_stream(peer, now);
          if let Some(br) = self.conns.get_mut(id) {
            br.promote(stream);
          }
        }
      }
    }
  }

  /// Zero-time outbound flush invoked from the high-level `start_*` APIs AFTER
  /// `service_dials`. Runs the shared tick tail (bridge pump + handshake mint +
  /// `collect_transmits`) WITHOUT step (3) (`Endpoint::handle_timeout`).
  ///
  /// Step (3) is deliberately skipped: membership timers advance solely through
  /// the driver's explicit [`Self::handle_timeout`], which fires AFTER the
  /// driver has drained [`Self::poll_memberlist_ingress`], decoded each frame,
  /// and fed each typed message via [`Self::handle_packet`]. Advancing time
  /// inside a `start_*` call would fire same-instant probe / suspect / gossip /
  /// push-pull schedulers BEFORE a just-arrived (still-buffered) `Ack` /
  /// `Alive` is decoded and applied.
  ///
  /// `service_dials` is run BY THE CALLER (the `start_*` wrapper) before this
  /// method; this flush then mints any handshake that completed in-band and
  /// pumps the freshly-built bridge so its first handshake flight (fresh dial)
  /// or its request bytes (handshake already complete) emerge on the next
  /// [`Self::poll_transport_transmit`].
  fn flush_outbound(&mut self, now: Instant) {
    self.pump_bridges(now);
    self.service_handshake_completions(now);
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Shared tail of [`Self::run_tick`] and [`Self::flush_outbound`]: collect
  /// outbound ciphertext from every live bridge. (Terminal bridges already
  /// flushed their final ciphertext at reap time inside [`Self::reap_bridge`].)
  fn finalize_tick(&mut self, _now: Instant) {
    self.collect_transmits();
  }

  /// Move any `Event::DialRequested` currently in the inner endpoint's queue
  /// into the private [`dial_pending`](Self::dial_pending) deque, preserving
  /// FIFO order of every other event.
  fn sieve_dial_events(&mut self) {
    let mut others: Vec<Event<I, A>> = Vec::new();
    while let Some(ev) = self.ep.poll_event() {
      match ev {
        Event::DialRequested { id, peer, deadline } => {
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
        }
        other => others.push(other),
      }
    }
    for ev in others {
      self.ep.requeue_event(ev);
    }
  }

  /// Step (5): drain the private `dial_pending` deque, surfacing one
  /// [`TlsAction::Connect`] and building one `Handshaking` client bridge per
  /// intent. Does NOT call `dial_succeeded` — the `Stream` is minted at
  /// handshake completion (step 4) across a later tick.
  fn service_dials(&mut self, now: Instant) {
    // Sieve any DialRequested newly emitted by the inner endpoint into the
    // private deque, then drain that deque as the sole input. Non-DialRequested
    // events stay in the inner endpoint's queue for the public `poll_event`.
    self.sieve_dial_events();
    let pending = std::mem::take(&mut self.dial_pending);
    for entry in pending {
      let PendingDial {
        id,
        peer,
        deadline,
        attempted: _,
      } = entry;
      // Retire the intent without opening a connection if its own deadline has
      // already elapsed (mirrors the QUIC coordinator's expired-intent gate).
      if now >= deadline {
        self.ep.dial_failed(
          id,
          crate::error::StreamError::DialFailed("tls dial deadline elapsed".into()),
          now,
        );
        continue;
      }
      let peer_socket = B::to_socket(&peer);
      // Resolve the rustls verification identity for the peer. A
      // `ServerName::try_from` failure retires the intent — the dial cannot
      // proceed without a name to verify the peer's certificate against.
      let server_name = match ServerName::try_from(B::server_name(&peer).into_owned()) {
        Ok(name) => name,
        Err(e) => {
          self.ep.dial_failed(
            id,
            crate::error::StreamError::DialFailed(format!("tls server name: {e}")),
            now,
          );
          continue;
        }
      };
      let records = match TlsRecords::client(self.cfg.client().clone(), server_name) {
        Ok(records) => records,
        Err(e) => {
          self.ep.dial_failed(
            id,
            crate::error::StreamError::DialFailed(format!("tls client: {e}")),
            now,
          );
          continue;
        }
      };
      let exchange = self.conns.allocate();
      let bridge = TlsBridge::new(records, deadline);
      self.conns.insert(exchange, bridge);
      self.exchanges.insert(
        exchange,
        ExchangeMeta {
          peer_socket,
          mint: Some(PendingMint::Outbound(id)),
          shutdown_emitted: false,
        },
      );
      self.out_actions.push_back(TlsAction::Connect(ConnectInfo {
        id: exchange,
        peer: peer_socket,
      }));
    }
  }

  /// Collect outbound ciphertext from every live bridge into the outbound
  /// queue, tagged with the exchange handle + peer.
  fn collect_transmits(&mut self) {
    for id in self.conns.ids() {
      if let Some(mut br) = self.conns.remove(id) {
        self.collect_bridge_transmits(id, &mut br);
        self.conns.insert(id, br);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  #[test]
  fn tls_endpoint_type_is_constructible_signature() {
    // Behavioural coverage is tls_conformance (needs the sim clock + a peer +
    // the virtual TCP). This guards the public constructor signature only.
    fn _sig<I, A, B>()
    where
      I: nodecraft::Id
        + memberlist_wire::Data
        + nodecraft::CheapClone
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      A: memberlist_wire::Data
        + nodecraft::CheapClone
        + Eq
        + core::hash::Hash
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      B: super::AddrBridge<A>,
    {
      let _: fn(crate::endpoint::Endpoint<I, A>, super::TlsConfig) -> super::TlsEndpoint<I, A, B> =
        super::TlsEndpoint::<I, A, B>::new;
    }
  }
}
