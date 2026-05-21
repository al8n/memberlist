//! Composed plain-TCP + memberlist Sans-I/O coordinator.
//!
//! Carries reliable exchanges over a per-exchange plain TCP connection;
//! plain UDP carries unreliable gossip. The label is a one-time wire prefix
//! written once at stream start, identical in byte format to the frozen
//! `memberlist-proto` label frame (`[LABELED_TAG=12][len][label]`). There is
//! no transport-level encryption — the driver moves raw bytes both ways after
//! the label prefix is consumed.
//!
//! Mirrors [`crate::tls`] method-for-method, with the rustls record layer
//! replaced by the raw-passthrough [`RawRecords`](records::RawRecords) codec
//! and TLS's in-band `close_notify` half-close anchor replaced by the
//! out-of-band TCP FIN (`shutdown(write)`). Two ingress paths: gossip via
//! [`TcpEndpoint::handle_gossip`] (buffered raw for the codec-owning driver),
//! and per-exchange bytes via [`TcpEndpoint::handle_transport_data`] (routed
//! into the owning bridge, then a coordinator tick). Outbound, the coordinator
//! surfaces bytes keyed by exchange handle + peer
//! ([`TcpEndpoint::poll_transport_transmit`]), connect / half-close / teardown
//! signals ([`TcpEndpoint::poll_action`]), and the unreliable gossip
//! [`Transmit`] stream ([`TcpEndpoint::poll_memberlist_transmit`]).
//!
//! The fixed per-tick step order keeps the load-bearing invariant — the
//! stream-endpoint-event drain STRICTLY precedes `Endpoint::handle_timeout`
//! (else a reliable-fallback ping ack that lands the same tick the probe
//! cumulative deadline expires is lost and the peer is wrongly Suspected).
//!
//! # Driver output-drain ordering
//!
//! The coordinator self-orders its outputs so a driver doing the natural
//! "drain actions, drain transmits, drain memberlist, drain ingress, sleep
//! until `poll_timeout`, repeat" loop is correct:
//!
//! - [`TcpEndpoint::poll_action`] returns [`TcpAction::Connect`] first.
//! - Once all `Connect`s are drained, [`TcpAction::Shutdown`] /
//!   [`TcpAction::Close`] for an exchange is withheld while
//!   [`TcpEndpoint::poll_transport_transmit`] still holds bytes tagged with
//!   that exchange — a teardown applied before its exchange's last bytes
//!   are written would orphan them (TCP `shutdown(write)` closes the send
//!   half, so any later write fails).
//! - Once the matching bytes drain, the teardown surfaces.
//!
//! The driver does NOT need to partition actions itself; the natural drain
//! loop, repeated until no method makes progress, sequences
//! `Connect` → bytes → `Shutdown` / `Close` for each exchange correctly.
//!
//! [`RawRecords::send_close_notify`](records::RawRecords) emits no bytes (the
//! TCP send-half close is the out-of-band FIN, not an in-band record), so
//! [`TcpAction::Shutdown`] is the ONLY signal the driver gets that our send
//! half is owed; the gate above makes it land after the bytes already queued
//! for the exchange (the simulation harness in
//! [`memberlist-simulation/src/tcp_net.rs`](`drain_and_route`) models the
//! same shape by re-polling actions after the byte drain so released
//! teardowns arm the FIN in the same step they would have on the wire).

mod bridge;
mod config;
mod conn;
mod records;

use std::{
  collections::HashMap,
  net::SocketAddr,
  time::{Duration, Instant},
};

use bytes::Bytes;

use crate::{
  addr_bridge::AddrBridge,
  endpoint::Endpoint,
  event::{Event, PushPullKind, StreamId, Transmit},
};
use bridge::TcpBridge;
use records::RawRecords;

pub use config::TcpConfig;
pub use conn::ExchangeId;

use conn::TcpConns;

/// Handshake completion budget for an inbound (accepted) exchange.
///
/// The outbound dial deadline is the membership exchange deadline carried on
/// `Event::DialRequested`; an accepted connection has no such intent, so the
/// coordinator bounds its label-read with this fixed budget (matching the
/// default `EndpointConfig::stream_timeout`, which the membership `Endpoint`
/// applies to the accepted `Stream`'s exchange once it is minted). A label
/// exchange that has not settled by `now + ACCEPT_HANDSHAKE_DEADLINE` is
/// reaped by the bridge's `poll_timeout` / `pump_out` deadline path with no
/// `Stream` minted.
const ACCEPT_HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// One pending dial intent the coordinator owes a `service_dials` attempt to.
///
/// `attempted` distinguishes a freshly-sieved entry (never yet processed by
/// `service_dials`) from one that has been processed at least once. Freshly-
/// sieved entries get an immediate-due wake out of `poll_timeout` so a caller
/// that advances solely by `poll_timeout` cannot orphan them. Once
/// `service_dials` attempts the entry, `attempted` becomes `true` and stays
/// `true`. The plain-TCP dial always succeeds in surfacing a `Connect` action
/// and inserting a `Handshaking` bridge on its first attempt (there is no
/// pooled-connection credit to wait on), so an attempted entry never
/// requeues; the `attempted` bit is retained verbatim from the sibling TLS /
/// QUIC coordinators for the immediate-due wake discipline.
struct PendingDial<A> {
  id: StreamId,
  peer: A,
  deadline: Instant,
  attempted: bool,
}

/// How the coordinator mints the `Stream` for an exchange once its label step
/// settles. The membership `Endpoint` stays the sole `Stream` factory; the
/// bridge spends the whole label window with `stream = None`. Newtype variants
/// over the existing key types (no multi-field variants).
enum PendingMint<A> {
  /// Outbound dial: mint via `Endpoint::dial_succeeded(stream_id, now)` using
  /// the `StreamId` the inner endpoint allocated for the originating `start_*`.
  Outbound(StreamId),
  /// Inbound accept: mint via `Endpoint::accept_stream(peer, now)`.
  Inbound(A),
}

/// Per-exchange metadata the coordinator holds for the whole lifetime of one
/// reliable exchange (the bridge itself lives in [`TcpConns`], keyed by the
/// same [`ExchangeId`]). Accessor-only — private fields, read directly within
/// this module.
struct ExchangeMeta<A> {
  /// The peer `SocketAddr` every `poll_transport_transmit` for this exchange is
  /// tagged with so the driver writes the bytes on the right TCP connection.
  peer_socket: SocketAddr,
  /// `Some` until the label step settles and the coordinator mints + promotes
  /// the `Stream`; `None` afterwards (an `Established` bridge needs no further
  /// minting decision).
  mint: Option<PendingMint<A>>,
  /// `true` once the coordinator has emitted this exchange's one
  /// [`TcpAction::Shutdown`] (graceful TCP write-side half-close after the
  /// bridge retired its send half). Guards a second emission. The structural
  /// analog of the TLS coordinator's `shutdown_emitted`; the TLS half-close is
  /// the in-band `close_notify` alert, whereas the TCP half-close is the
  /// out-of-band `shutdown(write)` the driver issues, so the latch records
  /// that the FIN signal is owed to the driver rather than that an alert was
  /// queued.
  fin_emitted: bool,
}

/// Payload of [`TcpAction::Connect`]: dial a TCP connection for an exchange.
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

/// Payload of [`TcpAction::Shutdown`] / [`TcpAction::Close`]: names one
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
/// connection. Drained via [`TcpEndpoint::poll_action`].
///
/// Newtype variants over named accessor-only payload structs (the
/// no-multi-field-variant convention).
pub enum TcpAction {
  /// Open a TCP connection to the peer for a freshly-dialed outbound exchange.
  Connect(ConnectInfo),
  /// Half-close the TCP write side after the bridge retired its send half, so
  /// the peer reads a clean EOF (its `read == 0`) once it has drained our
  /// buffered bytes. The out-of-band analog of TLS's in-band `close_notify`:
  /// [`RawRecords::send_close_notify`](records::RawRecords) emits no bytes, so
  /// the FIN must be issued by the driver as a TCP `shutdown(write)`.
  Shutdown(ExchangeRef),
  /// Tear down the TCP connection and forget the exchange — the bridge
  /// reached a terminal phase and has been reaped.
  Close(ExchangeRef),
}

impl TcpAction {
  /// Borrow the [`ConnectInfo`] iff this is a [`TcpAction::Connect`].
  pub const fn as_connect(&self) -> Option<&ConnectInfo> {
    match self {
      TcpAction::Connect(c) => Some(c),
      _ => None,
    }
  }

  /// Borrow the [`ExchangeRef`] iff this is a [`TcpAction::Shutdown`].
  pub const fn as_shutdown(&self) -> Option<&ExchangeRef> {
    match self {
      TcpAction::Shutdown(r) => Some(r),
      _ => None,
    }
  }

  /// Borrow the [`ExchangeRef`] iff this is a [`TcpAction::Close`].
  pub const fn as_close(&self) -> Option<&ExchangeRef> {
    match self {
      TcpAction::Close(r) => Some(r),
      _ => None,
    }
  }
}

/// The [`ExchangeId`] a teardown directive ([`TcpAction::Shutdown`] /
/// [`TcpAction::Close`]) refers to. Panics on a `Connect` — the caller must
/// hand only teardown actions in.
fn teardown_exchange(action: &TcpAction) -> ExchangeId {
  match action {
    TcpAction::Shutdown(r) | TcpAction::Close(r) => r.id(),
    TcpAction::Connect(_) => unreachable!("teardown_exchange called on a Connect action"),
  }
}

/// Coordinator: `memberlist::Endpoint` (unreliable gossip + membership)
/// composed with per-exchange plain TCP (reliable). Pure Sans-I/O — inject
/// `now`.
///
/// `B` translates the membership address `A` to the transport `SocketAddr`
/// (see [`AddrBridge`]); it is a marker type parameter only — no value of `B`
/// is stored. The TCP path consults only [`AddrBridge::to_socket`]; the
/// rustls `server_name` accessor is unused (no record-layer certificate
/// verification on plain TCP).
pub struct TcpEndpoint<I, A, B>
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
  cfg: TcpConfig,
  /// In-flight reliable exchanges (one bridge each), keyed by [`ExchangeId`].
  /// Connection-per-exchange — no pool, slab, or drained-reap.
  conns: TcpConns<I, A>,
  /// Per-exchange coordinator metadata, keyed in lockstep with `conns`. An
  /// outbound exchange carries its originating `StreamId` in
  /// [`PendingMint::Outbound`] here, so the mint-on-label-settled step maps an
  /// exchange to its `dial_succeeded` `StreamId` by iterating these directly —
  /// no separate `StreamId -> ExchangeId` reverse index is needed.
  exchanges: HashMap<ExchangeId, ExchangeMeta<A>>,
  /// Outbound per-exchange bytes produced this tick (the one-time label
  /// prefix, then application bytes), tagged with the exchange handle + peer
  /// so the driver writes them on the right TCP connection. Drained via
  /// [`Self::poll_transport_transmit`].
  out_transmit: std::collections::VecDeque<(ExchangeId, SocketAddr, Bytes)>,
  /// Outbound [`TcpAction::Connect`] directives, in producer order. Drained
  /// first by [`Self::poll_action`] so a fresh dial's connection always opens
  /// before any same-tick `Shutdown` / `Close` targets an existing bridge's
  /// connection.
  pending_connects: std::collections::VecDeque<TcpAction>,
  /// Outbound [`TcpAction::Shutdown`] / [`TcpAction::Close`] directives, in
  /// producer order. Drained by [`Self::poll_action`] only after
  /// [`Self::pending_connects`] is exhausted AND the targeted exchange's
  /// bytes have left [`Self::out_transmit`] — withholding a teardown behind
  /// its own transmit prevents a TCP `shutdown(write)` from orphaning bytes
  /// the exchange still owes. Teardowns retain their producer order.
  pending_teardowns: std::collections::VecDeque<TcpAction>,
  /// Raw inbound gossip datagrams. `memberlist-machine` has no umbrella
  /// `codec` dependency, so the coordinator cannot decode them in-crate and
  /// MUST NOT silently drop them (that would lose every UDP
  /// ping/ack/alive/suspect on the composed unit's public ingress). They are
  /// buffered here and surfaced via [`Self::poll_memberlist_ingress`] for the
  /// codec-owning layer to unwrap and feed back through [`Self::handle_packet`].
  mem_ingress: std::collections::VecDeque<(SocketAddr, Bytes)>,
  /// Private queue of pending dial intents. `memberlist::Endpoint::poll_event`
  /// emits `Event::DialRequested { id, peer, deadline }` for an external
  /// driver to dial — but in the composed design `TcpEndpoint` IS the driver:
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
  /// immediate-due wake of an unattempted `dial_pending` entry: the only way
  /// to signal "fire as soon as possible" out of an `Option<Instant>`
  /// Sans-I/O API is to return an `Instant <= caller's now`, and the only
  /// such anchor we may hold is one we observed from a prior `handle_*` call
  /// (Sans-I/O forbids `Instant::now()`). Stays `None` only before the very
  /// first `handle_*` / `start_*` call.
  last_now: Option<Instant>,
  _addr: core::marker::PhantomData<fn(B)>,
}

impl<I, A, B> TcpEndpoint<I, A, B>
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
  /// Build the coordinator from a membership [`Endpoint`] and a [`TcpConfig`]
  /// bundle.
  pub fn new(ep: Endpoint<I, A>, cfg: TcpConfig) -> Self {
    Self {
      ep,
      cfg,
      conns: TcpConns::new(),
      exchanges: HashMap::new(),
      out_transmit: std::collections::VecDeque::new(),
      pending_connects: std::collections::VecDeque::new(),
      pending_teardowns: std::collections::VecDeque::new(),
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
  /// TCP fallback kick in via the natural suspicion / failure-detection timing.
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

  /// Next outbound per-exchange bytes `(exchange, peer, bytes)`, if any.
  /// The driver writes `bytes` on the TCP connection for `exchange` (to
  /// `peer`).
  pub fn poll_transport_transmit(&mut self) -> Option<(ExchangeId, SocketAddr, Bytes)> {
    self.out_transmit.pop_front()
  }

  /// Next outbound transport directive ([`TcpAction`]), if any.
  ///
  /// The coordinator self-orders its outputs so a driver doing the natural
  /// "drain actions, drain transmits, repeat until idle" loop is correct:
  ///
  /// - Every queued [`TcpAction::Connect`] surfaces before any queued
  ///   [`TcpAction::Shutdown`] / [`TcpAction::Close`], so a fresh dial's
  ///   connection opens before a same-tick `Shutdown` / `Close` targets an
  ///   existing bridge's connection.
  /// - A `Shutdown` / `Close` for an exchange is withheld while
  ///   [`Self::poll_transport_transmit`] still holds bytes tagged with that
  ///   exchange's [`ExchangeId`]. Applying the teardown before its last bytes
  ///   are written would orphan them — TCP `shutdown(write)` closes the send
  ///   half and subsequent writes fail — so the driver MUST drain the
  ///   transmit queue first. The gate makes the natural drain loop correct
  ///   without burdening the driver with an explicit phase contract.
  pub fn poll_action(&mut self) -> Option<TcpAction> {
    if let Some(connect) = self.pending_connects.pop_front() {
      return Some(connect);
    }
    // Find the first teardown whose exchange has no pending transmit bytes;
    // skip past (but retain in producer order) any whose bytes are still
    // queued. The retained teardowns will surface once the driver drains the
    // matching bytes via `poll_transport_transmit`.
    let idx = self
      .pending_teardowns
      .iter()
      .position(|action| !self.exchange_has_pending_bytes(teardown_exchange(action)))?;
    self.pending_teardowns.remove(idx)
  }

  /// `true` iff [`Self::out_transmit`] holds at least one chunk tagged with
  /// the given exchange handle. Used by [`Self::poll_action`] to withhold a
  /// teardown for an exchange whose last bytes have not yet been drained.
  fn exchange_has_pending_bytes(&self, id: ExchangeId) -> bool {
    self.out_transmit.iter().any(|(eid, _, _)| *eid == id)
  }

  /// Drop any [`Self::out_transmit`] chunks tagged with `exchange`. Called
  /// from the reap path BEFORE [`Self::collect_bridge_transmits`] so a
  /// Failed bridge does not leak stale bytes through
  /// [`Self::poll_transport_transmit`] after the per-exchange teardown gate
  /// releases its `Close`. A bridge can sit on queued `out_transmit` bytes
  /// (its label prefix, a request, …) from an earlier tick when its
  /// deadline elapses; without the purge a driver doing the natural
  /// "drain actions, drain transmits, repeat until idle" loop would write
  /// those stale bytes to the peer's socket between the gate's release and
  /// the `Close` — delivering membership state from an exchange the local
  /// node has already failed. The dropped bytes are safe to discard because
  /// the bridge is being torn down: any further send-half progress is
  /// forbidden by the `Failed` phase, and the bridge's remaining outbound
  /// buffer is dropped with the bridge itself.
  ///
  /// Clean (`BothClosed`) reaps have an empty pre-reap queue for the
  /// exchange — a server's response is encoded inside
  /// [`TcpBridge::drain_then_reap`]'s `SendPushPullResponse` branch and
  /// collected by [`Self::collect_bridge_transmits`] AFTER this purge runs,
  /// so the response chunk is preserved while pre-failure stragglers are
  /// dropped.
  fn purge_transmit_for(&mut self, exchange: ExchangeId) {
    self.out_transmit.retain(|(eid, _, _)| *eid != exchange);
  }

  /// Drop any pending [`TcpAction::Connect`] still queued for `exchange`.
  /// Symmetric to [`Self::purge_transmit_for`], but for the action queue
  /// instead of the transmit queue.
  ///
  /// Called from the dial-failure reap path
  /// ([`Self::service_handshake_completions`]'s `dial_succeeded(None)` branch)
  /// so a driver does not observe a `Connect` for an exchange the coordinator
  /// has already failed. Without this purge, a driver doing the natural
  /// "drain actions, drain transmits, repeat" loop would dequeue the queued
  /// `Connect` (Connects always surface before teardowns —
  /// see [`Self::poll_action`]'s ordering contract), open the TCP socket,
  /// then drain the same exchange's `Close` and tear it down — wasted work
  /// at best, and a vector for label disclosure (the bridge's
  /// `records.outbound` still holds the eager-queued local label until
  /// `bridge.fail_connection_lost()` clears it) if any path bypassed the
  /// clear.
  ///
  /// `pending_connects` only ever holds `TcpAction::Connect` (its discipline
  /// is enforced by the `debug_assert!` at each `push_back` site); the
  /// catch-all arm is defensive — variants other than `Connect` retain.
  fn purge_pending_connect_for(&mut self, exchange: ExchangeId) {
    self.pending_connects.retain(|action| match action {
      TcpAction::Connect(info) => info.id() != exchange,
      _ => true,
    });
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
  /// message via [`Self::handle_packet`], and then call
  /// [`Self::handle_timeout`] to advance time. Running [`Self::handle_timeout`]
  /// before the buffered gossip is decoded and fed would risk same-instant
  /// probe / suspect timers firing before a just-arrived `Ack` / `Alive` is
  /// applied — a spurious fallback ping or false `Suspect` could fire even
  /// though the resolving message is already sitting in
  /// [`Self::poll_memberlist_ingress`]'s queue locally. The TLS coordinator's
  /// analog mirrors this discipline; plain TCP carries every UDP datagram as
  /// gossip (reliable rides separate TCP connections).
  pub fn handle_gossip(&mut self, from: A, datagram: &[u8], now: Instant) {
    self.last_now = Some(now);
    let socket = B::to_socket(&from);
    self
      .mem_ingress
      .push_back((socket, Bytes::copy_from_slice(datagram)));
  }

  /// Inbound bytes for one exchange's TCP connection.
  ///
  /// Routes `bytes` into the owning bridge's
  /// [`TcpBridge::handle_transport_data`], then runs a coordinator tick.
  ///
  /// `eof = true` signals the TCP `read == 0` half-close anchor — the
  /// out-of-band peer-FIN that plain TCP delivers in place of TLS's in-band
  /// `close_notify` alert. The TLS coordinator infers its close anchor from
  /// [`crate::tls`]'s `peer_has_closed()` (latched on the in-band alert), but
  /// the plain-TCP record layer
  /// [`RawRecords::peer_has_closed`](records::RawRecords) is permanently
  /// `false` — there is no in-band close signal — so the driver MUST surface
  /// the FIN via this parameter. The bridge's byte pump derives the same
  /// close-anchor truth value (`eof || records.peer_has_closed()`) it shares
  /// with the TLS bridge; the explicit flag carries the missing transport
  /// signal.
  ///
  /// A `(bytes.len() > 0, eof = true)` delivery — bytes followed by an
  /// observed `read == 0` on the same wake — is fed in two steps: the bytes
  /// first (one full pump), then an empty-slice EOF (the recv-half retirement
  /// anchor). The single coordinator tick at the end advances time once.
  pub fn handle_transport_data(&mut self, id: ExchangeId, bytes: &[u8], eof: bool, now: Instant) {
    self.last_now = Some(now);
    if let Some(bridge) = self.conns.get_mut(id) {
      if !bytes.is_empty() {
        // Ignoring Err: an `Err` means the bridge terminalized (label /
        // decode / transport failure); `run_tick`'s `pump_bridges` reaps it
        // and emits the `Close` action. There is no separate action here.
        let _ = bridge.handle_transport_data(bytes, now);
      }
      if eof {
        // Empty-slice feed = TCP `read == 0` EOF anchor. Drives the recv-half
        // retirement (`observe_recv_fin`) or the truncation-decode-fail path
        // depending on the bridge's current state. Run even when the bytes
        // step terminalized the bridge — the bridge ignores subsequent feeds
        // in a terminal phase.
        // Ignoring Err: same as the bytes feed above — terminality is the
        // reap signal, not the return value.
        let _ = bridge.handle_transport_data(&[], now);
      }
    }
    self.run_tick(now);
  }

  /// The driver accepted an inbound TCP connection from `from`. Allocates an
  /// [`ExchangeId`], builds a server-side `Handshaking` bridge bounded by
  /// [`ACCEPT_HANDSHAKE_DEADLINE`], and returns the handle the driver tags this
  /// connection's inbound bytes with. The `Stream` is minted later (at
  /// label-step settled, via `Endpoint::accept_stream`).
  pub fn accept_connection(&mut self, from: A, now: Instant) -> ExchangeId {
    let id = self.conns.allocate();
    let peer_socket = B::to_socket(&from);
    let records = RawRecords::acceptor(
      self.cfg.label().map(|s| s.to_vec()),
      self.cfg.skip_inbound_label_check(),
    );
    let bridge = TcpBridge::new(records, now + ACCEPT_HANDSHAKE_DEADLINE);
    self.conns.insert(id, bridge);
    self.exchanges.insert(
      id,
      ExchangeMeta {
        peer_socket,
        mint: Some(PendingMint::Inbound(from)),
        fin_emitted: false,
      },
    );
    id
  }

  /// Initiate an outbound push/pull state exchange with `peer` and attempt the
  /// dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_push_pull`] that ALSO drives
  /// `service_dials(now)` + `flush_outbound(now)` before returning, so the
  /// `DialRequested` the inner endpoint queues is sieved, attempted (the
  /// `Connect` action surfaced and the `Handshaking` bridge built), and the
  /// dial's first label prefix emerges on the very next
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
  /// semantics. The `deadline` is the owning probe's single cumulative
  /// deadline (NOT an independent stream-timeout), forwarded unchanged; `now`
  /// is taken separately because `service_dials` needs the real wall-clock
  /// instant and `last_now` must remain a known-past anchor.
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
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time
  /// outbound-flush semantics.
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
  /// tick the probe cumulative deadline expires is carried by the stream's
  /// last `poll_endpoint_event`; draining it after the probe timeout would
  /// lose it and wrongly Suspect a live peer. Do not reorder.
  ///
  /// Step (4) (label-settled mint) mints the `Stream` for any bridge whose
  /// label step settled since the last tick and promotes it; a freshly-promoted
  /// OUTBOUND bridge carries its request bytes in the minted `Stream`'s output
  /// buffer. Step (5) (`service_dials`) inserts new `Handshaking` outbound
  /// bridges. The dialer's [`RawRecords`](records::RawRecords) is never
  /// handshaking (its inbound label is validated in-line on the established
  /// intake), so step (5.5) — a second `service_handshake_completions` —
  /// promotes those freshly-inserted dial bridges in the SAME tick, before
  /// step (5.6)'s `pump_bridges` pumps their request bytes out. Without the
  /// step (5.5) extra promote, a reliable-fallback ping bridge created by
  /// step (3) would have its `Stream` minted only on the NEXT
  /// coordinator wake — under a strict-poll driver that wakes only at
  /// [`Self::poll_timeout`], that next wake is the bridge's exchange deadline
  /// itself, at which point [`crate::stream::Stream::handle_data`] would
  /// reject the buffered request as timed out. `pump_bridges` and
  /// `service_handshake_completions` are both idempotent on already-handled
  /// bridges, so the duplicated calls are no-ops on bridges already serviced
  /// upstream. There is NO connection drained-reap step
  /// (connection-per-exchange — a reaped bridge frees its own connection via
  /// the `Close` action).
  fn run_tick(&mut self, now: Instant) {
    // (1) inbound feed already done by the caller (`handle_transport_data`).
    // (2) pump bridges + drain stream endpoint-events into the Endpoint.
    self.pump_bridges(now);
    // (3) THEN membership timers (probe cumulative-deadline, suspicion).
    self.ep.handle_timeout(now);
    // (4) mint the Stream for any bridge whose label step just settled.
    self.service_handshake_completions(now);
    // (5) dial requests emitted by (3).
    self.service_dials(now);
    // (5.5) promote any dial bridge whose records are not handshaking from
    // the moment of construction (the dialer's role) so step (5.6)'s pump
    // can transmit the request bytes this same tick. Idempotent on bridges
    // already promoted by step (4).
    self.service_handshake_completions(now);
    // (5.6) pump bridges promoted/inserted by (4), (5), and (5.5) this same
    // tick.
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Step (2) of the per-tick order: pump every bridge's outbound half, drain
  /// each non-terminal stream's endpoint-events into the `Endpoint`, and
  /// D1-drain-then-reap any bridge that turned terminal.
  ///
  /// Extracted so [`Self::flush_outbound`] can re-use the same bridge step
  /// after `service_dials`. There is no inbound `pump_in`: inbound bytes are
  /// fed through [`Self::handle_transport_data`] →
  /// `TcpBridge::handle_transport_data` directly; this step only drives the
  /// outbound side and the endpoint-event drain.
  fn pump_bridges(&mut self, now: Instant) {
    for id in self.conns.ids() {
      // Split borrow: take the bridge out, operate, put back (or reap).
      let Some(mut br) = self.conns.remove(id) else {
        continue;
      };
      // Replay any pre-promotion buffered plaintext FIRST (inbound), before
      // the outbound pump. A peer that coalesced its first request with its
      // label prefix had that request stripped of its label and buffered as
      // inbound plaintext while the bridge was still `Handshaking`; this is
      // the post-mint pump (run AFTER `service_handshake_completions`
      // promoted the bridge), so the buffered plaintext reassembles into the
      // just-minted `Stream` THIS tick rather than waiting for the next
      // transport read. A no-op on every bridge with no buffered tail (the
      // steady state).
      // Ignoring Err: a replay failure terminalizes the bridge; the
      // `is_terminal()` reap path below handles it. There is no separate
      // action.
      let _ = br.replay_pending(now);
      // `pump_out` sets the bridge `fatal` flag on a transport / FSM error, so
      // `is_terminal()` below drives the prompt reap; the `#[must_use]` Result
      // is consumed — terminality is the signal.
      // Ignoring Err: `pump_out` failing terminalizes the bridge; the
      // `is_terminal()` reap path below handles it. There is no separate
      // action.
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
          // Graceful half-close signal: the bridge retired its send half
          // (clean exchange) and is awaiting the peer's FIN, so tell the
          // driver to half-close the TCP write side after it has drained our
          // buffered bytes. Emitted at most once per exchange. The bridge's
          // outbound bytes are collected by `finalize_tick` while it is still
          // alive — only a reaped (dropped) bridge needs the inline
          // reap-time collection in `reap_bridge`.
          self.maybe_emit_shutdown(id, &br);
          self.conns.insert(id, br);
        }
      }
    }
  }

  /// Drain a terminal bridge's final outbound bytes (anything `drain_then_reap`
  /// just encoded into its reply) into the outbound queue, emit the
  /// [`TcpAction::Close`] teardown, and forget the exchange. Called after
  /// `drain_then_reap`, before the bridge is dropped — otherwise any trailing
  /// bytes a failure-path `retire_halves` produced would be lost when the
  /// bridge drops.
  fn reap_bridge(&mut self, id: ExchangeId, br: &mut TcpBridge<I, A>, now: Instant) {
    // Drop any pre-reap outbound chunks still tagged with this exchange
    // BEFORE collecting the bridge's final bytes and queueing its `Close` —
    // but ONLY for a failed reap.
    //
    // On a Failed reap, `out_transmit` can hold the bridge's pre-failure
    // label / request bytes from earlier ticks (a dialer that was waiting
    // for the response when its exchange deadline elapsed never drained the
    // queued bytes); without this purge the [`Self::poll_action`] teardown
    // gate would withhold the `Close` behind those stale bytes, and a driver
    // doing the natural "drain actions, drain transmits, repeat" loop would
    // emit them on the wire after the local exchange has already failed —
    // leaking membership state from a failed push/pull, or sending a probe
    // to a peer the local node has given up on. The dropped bytes are safe
    // to discard because the bridge is being torn down: the failed phase
    // forbids further send progress, and the bridge's remaining outbound
    // buffer is dropped with the bridge itself.
    //
    // On a clean (`BothClosed`) reap, `out_transmit` may already hold
    // legitimate response chunks queued by an EARLIER pump. The acceptor's
    // lazy outbound label prefix fires the instant its inbound label
    // validates; if the dialer's `[label]` and `[request][FIN]` arrive in
    // two separate transport reads, the next `pump_bridges` drains the
    // acceptor's label prefix into `out_transmit` BEFORE the request bytes
    // arrive — and the response chunk added by `collect_bridge_transmits`
    // below joins the queued label as the second tagged chunk for the
    // exchange. Purging would drop the label prefix and leave the peer with
    // a response that the dialer's inbound-label check then rejects as
    // unlabeled. Skip the purge for clean reaps so the bytes split across
    // multiple `out_transmit` entries reach the wire intact.
    if br.is_failed() {
      self.purge_transmit_for(id);
    }
    self.collect_bridge_transmits(id, br);
    if let Some(PendingMint::Outbound(sid)) = self.exchanges.remove(&id).and_then(|m| m.mint) {
      // The bridge reaped before its label step settled (e.g. a label
      // mismatch on a dial): the `StreamId` was never minted into a `Stream`,
      // but the inner endpoint still holds the pending intent. Retire it so
      // the exchange does not strand (a reliable-ping fallback is released,
      // etc.). A bridge whose label step settled has `mint == None` (taken at
      // promotion), so a promoted bridge's clean reap does not re-enter
      // `dial_failed`.
      self.ep.dial_failed(
        sid,
        crate::error::StreamError::DialFailed("tcp label exchange aborted".into()),
        now,
      );
    }
    let action = TcpAction::Close(ExchangeRef { id });
    debug_assert!(
      matches!(action, TcpAction::Shutdown(_) | TcpAction::Close(_)),
      "pending_teardowns holds only Shutdown / Close actions",
    );
    self.pending_teardowns.push_back(action);
  }

  /// Emit the one [`TcpAction::Shutdown`] for an exchange once its bridge has
  /// retired its send half (clean half-close while awaiting the peer's FIN).
  /// A terminal bridge is reaped with `Close` instead and never reaches here.
  ///
  /// The structural analog of the TLS coordinator's `maybe_emit_shutdown`,
  /// gated by the per-exchange `fin_emitted` latch. The TLS half-close is the
  /// in-band `close_notify` alert (already encoded into the outbound queue by
  /// the record layer); the TCP half-close is the out-of-band TCP FIN, so
  /// this action tells the driver to issue `shutdown(write)` on the connection
  /// after it has drained our buffered bytes.
  fn maybe_emit_shutdown(&mut self, id: ExchangeId, br: &TcpBridge<I, A>) {
    if !br.fin_owed() {
      return;
    }
    if let Some(meta) = self.exchanges.get_mut(&id) {
      if !meta.fin_emitted {
        meta.fin_emitted = true;
        let action = TcpAction::Shutdown(ExchangeRef { id });
        debug_assert!(
          matches!(action, TcpAction::Shutdown(_) | TcpAction::Close(_)),
          "pending_teardowns holds only Shutdown / Close actions",
        );
        self.pending_teardowns.push_back(action);
      }
    }
  }

  /// Drain one bridge's outbound bytes into [`Self::out_transmit`], tagged
  /// with its exchange handle + peer socket.
  fn collect_bridge_transmits(&mut self, id: ExchangeId, br: &mut TcpBridge<I, A>) {
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

  /// Step (4): for every exchange still awaiting its `Stream`, mint it once
  /// the label step has settled.
  ///
  /// `is_handshaking()` is `false` once the record layer reports the label
  /// step done (the mint window) AND once the bridge is `Established` (already
  /// minted) — so the `meta.mint.is_some()` guard distinguishes the mint
  /// window from an already-promoted bridge. A bridge that FAILED during the
  /// label step is terminal (and not handshaking); it is skipped here and
  /// reaped by `pump_bridges`.
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
            // The dial intent was retired by the inner endpoint (deadline
            // elapsed in `dial_succeeded`'s `now >= intent.deadline` check, or
            // an external `dial_failed` cleared the intent) before a `Stream`
            // could be minted. The bridge is in dial-failed state.
            //
            // (1) Fail the bridge BEFORE removing it so the failure transition
            //     runs its `records.clear_outbound()` — without this the
            //     dialer's eager-queued local label
            //     (`RawRecords::dialer` queues `[12][len][local_label]` at
            //     construction) survives in `records.outbound` and
            //     `collect_bridge_transmits` would drain it into
            //     `out_transmit`. `fail_connection_lost` is the public
            //     entry point that triggers `records.clear_outbound`: a
            //     retired-but-unopened socket and a lost-mid-exchange socket
            //     share the same correctness invariant — no FIN can be
            //     delivered, so the bridge must transition without retiring
            //     halves while clearing the outbound buffer.
            if let Some(br) = self.conns.get_mut(id) {
              br.fail_connection_lost();
            }
            // (2) Purge any already-drained chunks tagged with this exchange
            //     from `out_transmit` (defensive — handles bytes an earlier
            //     pump may have drained before the dial intent was retired).
            self.purge_transmit_for(id);
            // (3) Purge any still-queued `Connect` for this exchange so a
            //     driver doing the natural drain loop does not open a TCP
            //     socket for an exchange the coordinator has already failed.
            //     If the driver had already drained `Connect` from a prior
            //     tick this is a no-op; the `Close` enqueued below still
            //     fires so the driver can clean up the open socket.
            self.purge_pending_connect_for(id);
            // (4) Remove the bridge WITHOUT calling
            //     `collect_bridge_transmits`. The bridge is failed and being
            //     dropped; its `records.outbound` has just been cleared by
            //     step (1)'s `fail_connection_lost`. Collecting from a
            //     failed bridge is the leak vector this fix closes — bytes
            //     from a failed bridge must never reach `out_transmit`.
            if let Some(br) = self.conns.remove(id) {
              drop(br);
            }
            self.exchanges.remove(&id);
            // (5) Emit `Close` so a driver that had already drained
            //     `Connect` from an earlier tick can tear the open socket
            //     down. If `Connect` was still in `pending_connects`,
            //     step (3) dropped it and `Close` is a no-op for that
            //     exchange.
            let action = TcpAction::Close(ExchangeRef { id });
            debug_assert!(
              matches!(action, TcpAction::Shutdown(_) | TcpAction::Close(_)),
              "pending_teardowns holds only Shutdown / Close actions",
            );
            self.pending_teardowns.push_back(action);
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
  /// `service_dials`. Runs the shared tick tail (bridge pump + label-settled
  /// mint + `collect_transmits`) WITHOUT step (3) (`Endpoint::handle_timeout`).
  ///
  /// Step (3) is deliberately skipped: membership timers advance solely
  /// through the driver's explicit [`Self::handle_timeout`], which fires AFTER
  /// the driver has drained [`Self::poll_memberlist_ingress`], decoded each
  /// frame, and fed each typed message via [`Self::handle_packet`]. Advancing
  /// time inside a `start_*` call would fire same-instant probe / suspect /
  /// gossip / push-pull schedulers BEFORE a just-arrived (still-buffered)
  /// `Ack` / `Alive` is decoded and applied.
  ///
  /// `service_dials` is run BY THE CALLER (the `start_*` wrapper) before this
  /// method; this flush then mints any label step that settled in-band and
  /// pumps the freshly-built bridge so its first label prefix (fresh dial)
  /// or its request bytes (label step already settled) emerge on the next
  /// [`Self::poll_transport_transmit`].
  fn flush_outbound(&mut self, now: Instant) {
    self.pump_bridges(now);
    self.service_handshake_completions(now);
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Shared tail of [`Self::run_tick`] and [`Self::flush_outbound`]: collect
  /// outbound bytes from every live bridge. (Terminal bridges already flushed
  /// their final bytes at reap time inside [`Self::reap_bridge`].)
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
  /// [`TcpAction::Connect`] and building one `Handshaking` client bridge per
  /// intent. Does NOT call `dial_succeeded` — the `Stream` is minted at the
  /// label-settled step (step 4) across a later tick (or this same tick when
  /// invoked from a `start_*` flush, since the dialer's label step settles
  /// at construction).
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
      // Retire the intent without opening a connection if its own deadline
      // has already elapsed (mirrors the sibling coordinators'
      // expired-intent gate).
      if now >= deadline {
        self.ep.dial_failed(
          id,
          crate::error::StreamError::DialFailed("tcp dial deadline elapsed".into()),
          now,
        );
        continue;
      }
      let peer_socket = B::to_socket(&peer);
      let records = RawRecords::dialer(
        self.cfg.label().map(|s| s.to_vec()),
        self.cfg.skip_inbound_label_check(),
      );
      let exchange = self.conns.allocate();
      let bridge = TcpBridge::new(records, deadline);
      self.conns.insert(exchange, bridge);
      self.exchanges.insert(
        exchange,
        ExchangeMeta {
          peer_socket,
          mint: Some(PendingMint::Outbound(id)),
          fin_emitted: false,
        },
      );
      let action = TcpAction::Connect(ConnectInfo {
        id: exchange,
        peer: peer_socket,
      });
      debug_assert!(
        matches!(action, TcpAction::Connect(_)),
        "pending_connects holds only Connect actions",
      );
      self.pending_connects.push_back(action);
    }
  }

  /// Collect outbound bytes from every live bridge into the outbound queue,
  /// tagged with the exchange handle + peer.
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
  use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
  };

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::{ConnectInfo, ExchangeId, ExchangeRef, TcpAction, TcpConfig, TcpEndpoint};
  use crate::{
    addr_bridge::AddrBridge,
    config::EndpointConfig,
    endpoint::Endpoint,
    event::{Event, PushPullKind},
  };

  /// Identity `AddrBridge` for `A = SocketAddr`, matching the sim harness's
  /// shape. The `server_name` accessor is unused on the plain-TCP path (no
  /// certificate verification) but must be supplied for the trait.
  struct IdentityBridge;

  impl AddrBridge<SocketAddr> for IdentityBridge {
    fn to_socket(addr: &SocketAddr) -> SocketAddr {
      *addr
    }
    fn from_socket(socket: SocketAddr) -> SocketAddr {
      socket
    }
    fn server_name(_addr: &SocketAddr) -> Cow<'_, str> {
      Cow::Borrowed("localhost")
    }
  }

  fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
  }

  fn endpoint(local_port: u16) -> Endpoint<SmolStr, SocketAddr> {
    Endpoint::new(EndpointConfig::new(
      SmolStr::new(format!("n-{local_port}")),
      addr(local_port),
    ))
  }

  /// Public-constructor signature check. Mirrors
  /// `tls::tests::tls_endpoint_type_is_constructible_signature`; behavioural
  /// coverage lives in the sim harness.
  #[test]
  fn tcp_endpoint_type_is_constructible_signature() {
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);
    assert_eq!(coord.live_bridge_count(), 0);
  }

  /// `TcpAction` accessor parity: each variant's `as_*` returns `Some` only
  /// for itself. The accessors are the only seam the driver can pattern-match
  /// against without owning the enum's privacy.
  #[test]
  fn tcp_action_accessors_match_only_their_variant() {
    let id = super::conn::ExchangeId::new(0);
    let connect = TcpAction::Connect(ConnectInfo {
      id,
      peer: addr(7000),
    });
    assert!(connect.as_connect().is_some());
    assert!(connect.as_shutdown().is_none());
    assert!(connect.as_close().is_none());

    let shutdown = TcpAction::Shutdown(ExchangeRef { id });
    assert!(shutdown.as_shutdown().is_some());
    assert!(shutdown.as_connect().is_none());
    assert!(shutdown.as_close().is_none());

    let close = TcpAction::Close(ExchangeRef { id });
    assert!(close.as_close().is_some());
    assert!(close.as_connect().is_none());
    assert!(close.as_shutdown().is_none());
  }

  /// `start_push_pull` sieves the `Event::DialRequested` it emits into the
  /// private dial queue (it must NEVER leak through `poll_event`) and the
  /// in-band `service_dials` + `flush_outbound` surfaces the `Connect` action
  /// + the dialer's label prefix in the SAME call. The strict-poll
  /// self-sufficiency seam: a driver using only the public poll surface sees
  /// the dial proceed without a separate `handle_timeout` pre-pump.
  #[test]
  fn start_push_pull_dials_in_band_and_does_not_leak_dial_requested() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    let connect = coord
      .poll_action()
      .expect("the first Connect action surfaced in-band");
    let exchange_id = match &connect {
      TcpAction::Connect(c) => {
        assert_eq!(c.peer(), addr(7000));
        c.id()
      }
      other => panic!("expected Connect, got {:?}", action_kind(other)),
    };

    // No DialRequested leaks through poll_event.
    while let Some(ev) = coord.poll_event() {
      assert!(
        !matches!(ev, Event::DialRequested { .. }),
        "DialRequested must NEVER leak through poll_event",
      );
    }

    // The dialer's label prefix is surfaced on the same tick.
    let (id, peer, bytes) = coord
      .poll_transport_transmit()
      .expect("the dialer queued its one-time label prefix in-band");
    assert_eq!(id, exchange_id);
    assert_eq!(peer, addr(7000));
    assert!(
      bytes.starts_with(&[12u8]),
      "label prefix begins with LABELED_TAG=12",
    );
    assert!(coord.live_bridge_count() >= 1);
  }

  /// FIN-emit gating: `fin_owed()` flips a single [`TcpAction::Shutdown`] for
  /// the exchange, latched by `ExchangeMeta::fin_emitted` against a re-emit.
  /// Mirrors the TLS coordinator's `maybe_emit_shutdown` analog, with
  /// `fin_owed()` (out-of-band TCP FIN) replacing `close_notify_sent()`
  /// (in-band TLS alert) as the gate.
  ///
  /// Drives a real push/pull exchange to the half-close anchor: the dialer's
  /// `Stream::poll_transmit` yields its request bytes; once exhausted with
  /// `sent_any = true` the bridge calls `send_close_notify()` (a no-op on
  /// TCP), latches `fin_sent`, and `fin_owed()` flips true. The coordinator's
  /// `maybe_emit_shutdown` then pushes one Shutdown action. A second tick
  /// pumping the bridge MUST NOT emit a duplicate (the `fin_emitted` latch).
  #[test]
  fn fin_owed_emits_one_shutdown_per_exchange() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // Start an outbound user-message exchange. The dialer label step settles
    // at construction (no inbound to read), the Stream is minted in the
    // same in-band flush, and the request bytes are queued — but the send
    // half is NOT yet retired (peer hasn't been heard from). On the next
    // `handle_timeout` the bridge pump observes `sent_any = true` with no
    // further yields and retires the send half, flipping `fin_owed()` true.
    let payload = Bytes::from_static(b"hello-tcp");
    let _sid = coord.start_user_message(addr(7000), payload, now);

    // Drain the in-band Connect, the dialer's label prefix, and the first
    // request bytes (still no Shutdown — `pump_out` has not seen poll_transmit
    // exhausted yet in this same tick because the request is the first yield).
    let connect = coord
      .poll_action()
      .expect("Connect surfaced for the outbound user-message dial");
    let exchange_id = match connect {
      TcpAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };

    // Drain everything queued in-band by the start_* call.
    while coord.poll_transport_transmit().is_some() {}

    // One subsequent tick: `pump_out` re-enters with the Stream's
    // poll_transmit exhausted and `sent_any = true`, calls
    // `records.send_close_notify()` (no-op on TCP) and `observe_send_fin()`,
    // latching `fin_sent`. The coordinator observes `fin_owed()` and pushes
    // one Shutdown.
    coord.handle_timeout(now + Duration::from_millis(1));

    let shutdown = coord
      .poll_action()
      .expect("fin_owed flips true → one Shutdown emitted");
    let shutdown_ref = match shutdown {
      TcpAction::Shutdown(r) => r,
      other => panic!("expected Shutdown, got {:?}", action_kind(&other)),
    };
    assert_eq!(
      shutdown_ref.id(),
      exchange_id,
      "Shutdown names the same exchange we dialed",
    );

    // A second tick MUST NOT re-emit Shutdown for the same exchange. The
    // bridge's `fin_owed()` stays `true` (the latch is sticky), so the
    // ExchangeMeta::fin_emitted gate is what prevents the duplicate. A
    // mutation that removes the latch check would emit a second Shutdown
    // here — the duplicate-Shutdown assertion below is the mutation gate.
    coord.handle_timeout(now + Duration::from_millis(2));

    while let Some(act) = coord.poll_action() {
      assert!(
        !matches!(act, TcpAction::Shutdown(_)),
        "fin_emitted latch must suppress a duplicate Shutdown for the same exchange (got {:?})",
        action_kind(&act),
      );
    }
  }

  /// Tick step (2) (bridge pump + endpoint-event drain) MUST run strictly
  /// before step (3) (`Endpoint::handle_timeout`): an inbound reply that lands
  /// the same tick a deadline elapses on must reach the `Endpoint` before the
  /// timer-driven cleanup fires, or a live exchange is wrongly closed. This
  /// test exercises the ordering directly: a configured `handle_timeout(now)`
  /// runs the full tick without panic (the inner ordering is preserved by
  /// `run_tick` regardless of whether a bridge is present). Behavioural
  /// coverage of the ordering on a live exchange is the sim harness's job.
  #[test]
  fn handle_timeout_drives_pump_before_endpoint_timeout() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // Repeatedly ticking with no bridges is safe and a no-op on the bridge
    // pump — the ordering invariant means the inner endpoint advances cleanly.
    coord.handle_timeout(now);
    coord.handle_timeout(now + Duration::from_millis(1));
    assert_eq!(coord.live_bridge_count(), 0);
  }

  /// `accept_connection` allocates a fresh `ExchangeId`, installs an acceptor
  /// bridge bounded by `ACCEPT_HANDSHAKE_DEADLINE`, and does NOT emit a
  /// Connect/Shutdown/Close action (the driver already has the inbound
  /// socket).
  #[test]
  fn accept_connection_installs_acceptor_bridge_without_emitting_actions() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    let id = coord.accept_connection(addr(7000), now);
    assert_eq!(coord.live_bridge_count(), 1);
    let _ = id; // monotonic, exercised in TcpConns tests

    assert!(
      coord.poll_action().is_none(),
      "accept_connection MUST NOT enqueue Connect/Shutdown/Close",
    );
    assert!(coord.poll_transport_transmit().is_none());
  }

  /// A `handle_timeout` whose membership-timer step (step 3) emits a fresh
  /// `Event::DialRequested` — the canonical case is the reliable-fallback
  /// ping the SWIM probe arms on direct timeout — MUST surface the
  /// dialer's `Connect` action AND its first request bytes on the SAME
  /// tick. The dialer's [`RawRecords`](records::RawRecords) is not
  /// handshaking from construction, so step (4)'s
  /// `service_handshake_completions` (which runs BEFORE step (5)'s
  /// `service_dials`) never sees the freshly-inserted bridge; step (5.5)'s
  /// extra `service_handshake_completions` promotes it before step (5.6)'s
  /// `pump_bridges` transmits the request.
  ///
  /// Without step (5.5), a driver that wakes solely on [`Self::poll_timeout`]
  /// — the strict-poll discipline — would not see the request bytes appear
  /// until the bridge's exchange deadline (the only timer remaining once
  /// `Endpoint::poll_timeout` and the bridge's own `poll_timeout` are
  /// drained), by which point [`crate::stream::Stream::handle_data`] would
  /// reject them as timed out and the fallback ping would never reach the
  /// peer.
  ///
  /// Mutation gate: deleting step (5.5) from `run_tick` fails the
  /// `poll_transport_transmit().is_some()` assertion below — the first
  /// post-tick `poll_transport_transmit` would yield `None` because the
  /// bridge is still unpromoted at `pump_bridges` time and has no Stream to
  /// drive `poll_transmit` for.
  #[test]
  fn timer_emitted_dial_request_bytes_appear_same_tick() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // Inject a DialRequested directly into the private dial queue — the
    // shape `Endpoint::handle_timeout` would produce when SWIM arms the
    // reliable-fallback ping inside step (3). Use `start_reliable_ping`
    // on the inner endpoint and then drive the run_tick path WITHOUT the
    // flush_outbound that `start_*` wrappers normally run, so only the
    // `handle_timeout` → run_tick path is exercised.
    let sid = coord.ep.start_reliable_ping(
      SmolStr::new("srv"),
      addr(7000),
      7,
      now + Duration::from_secs(5),
    );
    let _ = sid;

    // ONE handle_timeout(now): step (3) sieves the DialRequested → dial_pending,
    // step (5) `service_dials` builds the dialer bridge, step (5.5)
    // `service_handshake_completions` promotes it (the dialer is not
    // handshaking from construction), step (5.6) `pump_bridges` drives
    // `Stream::poll_transmit` and queues the request bytes.
    coord.handle_timeout(now);

    let mut connect_exchange = None;
    while let Some(act) = coord.poll_action() {
      if let TcpAction::Connect(c) = act {
        connect_exchange = Some(c.id());
        break;
      }
    }
    let connect_exchange = connect_exchange.expect("Connect action surfaced this tick");
    let (id, _peer, bytes) = coord
      .poll_transport_transmit()
      .expect("the dialer's [label||request] bytes are on the wire this tick");
    assert_eq!(id, connect_exchange);
    assert!(
      bytes.starts_with(&[12u8]),
      "first byte is LABELED_TAG (the one-time label prefix)"
    );
    assert!(
      bytes.len() > 2 + "cluster-x".len(),
      "bytes carry both the label prefix AND a non-empty request tail (got {} bytes)",
      bytes.len()
    );
  }

  /// A driver may deliver a peer's `[label||first request]` plus the same-tick
  /// out-of-band TCP FIN as ONE coordinator call
  /// (`handle_transport_data(id, bytes, eof=true, now)`). The coordinator splits
  /// that into a bytes feed followed by an empty-slice EOF anchor; the bridge
  /// is still `Handshaking` for BOTH feeds (the `Stream` is minted by step (4)
  /// `service_handshake_completions`, not by the bytes feed itself). The
  /// pre-promote FIN must therefore be latched on the bridge so the post-mint
  /// [`crate::tcp::bridge::TcpBridge::replay_pending`] honors it; otherwise
  /// the bridge promotes, drains the buffered request without an EOF, never
  /// retires its recv half, stays in `Established(SendClosed)` (the one-way
  /// user-message reaches FSM `Done` and the bridge half-closes its send
  /// side), and is held until the accept deadline elapses — a single coalesced
  /// exchange stalls 10 s on a peer that half-closes after the request.
  ///
  /// The structural analog of the TLS coordinator's coalesced
  /// `[handshake_final||first_record]||close_notify` round trip, except TCP's
  /// FIN is out of band so the latch lives on the bridge (not on the records
  /// layer — `RawRecords::peer_has_closed()` is permanently `false`).
  #[test]
  fn coalesced_label_request_with_eof_terminalizes_same_tick_or_soon() {
    let now = Instant::now();
    let server_addr = addr(7400);
    let dialer_addr = addr(7401);

    // Server coordinator. The acceptor bridge is installed by
    // `accept_connection`; the dialer's coalesced bytes arrive via
    // `handle_transport_data` and the same-tick EOF rides the same call.
    let server_ep = endpoint(server_addr.port());
    let cfg_s = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut server: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> =
      TcpEndpoint::new(server_ep, cfg_s);
    let server_exchange = server.accept_connection(dialer_addr, now);
    assert_eq!(server.live_bridge_count(), 1);

    // Dialer coordinator. Starting a one-way user-message produces the
    // `[label||frame]` blob on the very first `poll_transport_transmit` (the
    // dialer's label step settles at construction and `start_user_message`
    // flushes outbound in-band).
    let dialer_ep = endpoint(dialer_addr.port());
    let cfg_d = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut dialer: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> =
      TcpEndpoint::new(dialer_ep, cfg_d);
    let payload = Bytes::from_static(b"coalesced-with-eof");
    let _sid = dialer.start_user_message(server_addr, payload.clone(), now);
    // The user message reaches Done on the dialer this tick, so a second
    // `handle_timeout` retires its send half and `Shutdown` is emitted; we
    // only need the bytes for this assertion.
    dialer.handle_timeout(now);

    // Coalesce every byte the dialer produced this tick (the label prefix
    // and the user-message frame) into one buffer.
    let mut coalesced = Vec::new();
    while let Some((_id, _peer, bytes)) = dialer.poll_transport_transmit() {
      coalesced.extend_from_slice(&bytes);
    }
    assert!(
      coalesced.len() > 2,
      "the dialer produced both the label prefix and the user-message frame, \
       got {} bytes",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced blob with the same-tick out-of-band FIN in
    // ONE coordinator call — the exact shape the bug is about.
    server.handle_transport_data(server_exchange, &coalesced, true, now);
    // One follow-up tick at the SAME `now` so any post-mint pump that ran in
    // step (5.5) has flushed and reaping has had a tick to surface `Close`.
    server.handle_timeout(now);

    // The buffered user-message frame surfaced on the server `Endpoint` (the
    // recv-half FIN authorized the dispatched event; without the latch the
    // event would still surface, but only after the 10 s accept deadline).
    let mut got = None;
    while let Some(ev) = server.poll_event() {
      if let Event::UserPacket { data, .. } = ev {
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the coalesced user-message frame surfaced on the server Endpoint",
    );

    // The bridge reached `BothClosed` and was reaped this same tick: the
    // coordinator emitted exactly one `Close` for this exchange (preceded by
    // a `Shutdown`, since the bridge retired its send half on the same tick
    // it observed the recv FIN), and `live_bridge_count()` is back to zero.
    // The teardown for this exchange is withheld behind its bytes — drain the
    // transmit queue first, then collect the actions.
    let mut saw_close_for_exchange = false;
    let mut close_count = 0usize;
    loop {
      let mut made_progress = false;
      while let Some(act) = server.poll_action() {
        if let TcpAction::Close(r) = act {
          if r.id() == server_exchange {
            saw_close_for_exchange = true;
          }
          close_count += 1;
        }
        made_progress = true;
      }
      while server.poll_transport_transmit().is_some() {
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }
    assert!(
      saw_close_for_exchange,
      "the coordinator reaped the bridge and emitted Close for the exchange",
    );
    assert_eq!(close_count, 1, "exactly one Close per exchange");
    assert_eq!(
      server.live_bridge_count(),
      0,
      "the bridge was reaped — no leaked exchange held to the accept deadline",
    );
  }

  /// Within-tick action ordering: a `Shutdown` enqueued for an existing
  /// bridge BEFORE a `Connect` enqueued for a brand-new dial in the same tick
  /// MUST surface from [`TcpEndpoint::poll_action`] as `[Connect, Shutdown]`,
  /// not the FIFO insertion order. A naive driver that drains actions in
  /// poll-order, writes bytes, and stops on the first non-`Connect` would
  /// otherwise pop the `Shutdown`, drain transmit (which carries bytes for
  /// the NEW exchange under same-tick promote), and orphan-write to a peer
  /// the `Connect` had not yet opened.
  ///
  /// Mutation gate: reverting [`TcpEndpoint::poll_action`] to a single-queue
  /// FIFO pop fails the first assertion below — the first action returned
  /// would be the `Shutdown` (insertion order) rather than the `Connect`.
  #[test]
  fn same_tick_shutdown_old_and_connect_new_emit_connect_first() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // Simulate step (2) `pump_bridges` enqueuing a `Shutdown` for an
    // existing bridge by pushing one directly to the teardown queue (the
    // same producer site `maybe_emit_shutdown` uses). The `ExchangeId` value
    // is irrelevant for the ordering assertion — only the variant ordering
    // is.
    let existing = super::conn::ExchangeId::new(42);
    coord
      .pending_teardowns
      .push_back(TcpAction::Shutdown(ExchangeRef { id: existing }));

    // Now drive a brand-new dial through the same tick the `Shutdown` is
    // queued in: `start_push_pull` runs `service_dials` in-band, which
    // pushes a `Connect` to the connect queue. After this call the
    // coordinator has BOTH a queued `Shutdown` (for the existing exchange)
    // AND a queued `Connect` (for the brand-new exchange).
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    let first = coord
      .poll_action()
      .expect("a Connect was enqueued by the in-band service_dials");
    let new_exchange = match first {
      TcpAction::Connect(c) => {
        assert_eq!(
          c.peer(),
          addr(7000),
          "the in-band dial's Connect names the new peer",
        );
        c.id()
      }
      other => panic!(
        "first poll_action MUST surface the Connect before any Shutdown / \
         Close enqueued the same tick (got {:?})",
        action_kind(&other),
      ),
    };
    assert_ne!(
      new_exchange, existing,
      "the new dial's exchange id is distinct from the existing one",
    );

    // The very next poll_action surfaces the Shutdown for the existing
    // exchange (teardowns drain only after every queued Connect has been
    // drained).
    let second = coord
      .poll_action()
      .expect("the queued Shutdown surfaces after the Connect");
    match second {
      TcpAction::Shutdown(r) => assert_eq!(
        r.id(),
        existing,
        "the trailing Shutdown still names the existing exchange",
      ),
      other => panic!(
        "second poll_action MUST be the queued Shutdown (got {:?})",
        action_kind(&other),
      ),
    }
  }

  /// Within-tick action ordering, `Close` variant: a `Close` enqueued for a
  /// terminal-reap of an old bridge BEFORE a `Connect` enqueued for a
  /// brand-new dial in the same tick MUST surface as `[Connect, Close]`.
  /// Mirrors [`same_tick_shutdown_old_and_connect_new_emit_connect_first`]
  /// for the `Close` producer site
  /// ([`TcpEndpoint::reap_bridge`] and the dial-deadline-elapsed branch of
  /// [`TcpEndpoint::service_handshake_completions`]).
  ///
  /// Mutation gate: reverting [`TcpEndpoint::poll_action`] to a single-queue
  /// FIFO pop fails the first assertion below — the first action returned
  /// would be the `Close` (insertion order).
  #[test]
  fn same_tick_close_and_new_connect_emit_connect_first() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    let reaped = super::conn::ExchangeId::new(99);
    coord
      .pending_teardowns
      .push_back(TcpAction::Close(ExchangeRef { id: reaped }));

    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    let first = coord
      .poll_action()
      .expect("a Connect was enqueued by the in-band service_dials");
    let new_exchange = match first {
      TcpAction::Connect(c) => {
        assert_eq!(c.peer(), addr(7000));
        c.id()
      }
      other => panic!(
        "first poll_action MUST surface the Connect before any Shutdown / \
         Close enqueued the same tick (got {:?})",
        action_kind(&other),
      ),
    };
    assert_ne!(
      new_exchange, reaped,
      "the new dial's exchange id is distinct from the reaped one",
    );

    let second = coord
      .poll_action()
      .expect("the queued Close surfaces after the Connect");
    match second {
      TcpAction::Close(r) => assert_eq!(
        r.id(),
        reaped,
        "the trailing Close still names the reaped exchange",
      ),
      other => panic!(
        "second poll_action MUST be the queued Close (got {:?})",
        action_kind(&other),
      ),
    }
  }

  /// A naive driver — one that calls [`TcpEndpoint::poll_action`] and
  /// [`TcpEndpoint::poll_transport_transmit`] in a single loop, with no
  /// explicit phase partitioning — MUST observe every byte tagged with an
  /// exchange's [`ExchangeId`] BEFORE it observes that exchange's
  /// [`TcpAction::Shutdown`] / [`TcpAction::Close`]. Applying the teardown
  /// first would issue TCP `shutdown(write)` on a socket whose last bytes
  /// have not yet been written; the send half closes and the trailing write
  /// fails, orphaning the response.
  ///
  /// Setup: a one-way outbound `start_user_message` — the dialer's request
  /// bytes and its half-close anchor both land in the SAME tick. The
  /// request is drained from the stream's `poll_transmit` (sent_any flips
  /// true) and the next iteration exhausts it; `pump_out` then calls
  /// `send_close_notify` + `observe_send_fin`, flipping `fin_owed` true,
  /// and `maybe_emit_shutdown` enqueues the `Shutdown` — all before
  /// `finalize_tick` collects the bytes into `out_transmit`.
  ///
  /// Mutation gate: dropping the per-exchange transmit-pending guard from
  /// [`TcpEndpoint::poll_action`] makes a naive drain emit the `Shutdown`
  /// in the same iteration as the bytes (the `Shutdown` surfaces from the
  /// first `poll_action` sweep BEFORE the bytes are drained), failing the
  /// "last byte observed before Shutdown" assertion below.
  #[test]
  fn naive_drain_loop_writes_last_bytes_before_shutdown() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // One-way user-message: the dialer's request and its half-close anchor
    // both land in the in-band `flush_outbound` tick.
    let payload = Bytes::from_static(b"orphan-canary");
    let _sid = coord.start_user_message(addr(7000), payload, now);

    // Naive driver loop: drain actions, drain transmits, repeat until idle.
    // Record what was observed per iteration so we can prove the LAST byte
    // for the exchange was observed BEFORE the exchange's `Shutdown`.
    let mut bytes_observed: Vec<(usize, ExchangeId)> = Vec::new();
    let mut actions_observed: Vec<(usize, TcpAction)> = Vec::new();
    let mut iter = 0usize;
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push((iter, action));
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        assert!(!bytes.is_empty(), "transmit chunks are never empty");
        bytes_observed.push((iter, id));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
      iter += 1;
    }

    // The Connect was emitted first, in iteration 0.
    let (connect_iter, connect_id) = actions_observed
      .iter()
      .find_map(|(i, a)| a.as_connect().map(|c| (*i, c.id())))
      .expect("Connect was emitted for the dialed user-message exchange");
    assert_eq!(
      connect_iter, 0,
      "Connect surfaces in the first poll_action sweep",
    );

    // The Shutdown was emitted for the same exchange.
    let (shutdown_iter, shutdown_id) = actions_observed
      .iter()
      .find_map(|(i, a)| a.as_shutdown().map(|r| (*i, r.id())))
      .expect("Shutdown was emitted once the send half retired");
    assert_eq!(
      shutdown_id, connect_id,
      "Shutdown names the same exchange the Connect opened",
    );

    // The LAST byte tagged with the exchange was observed in some iteration.
    let last_byte_iter = bytes_observed
      .iter()
      .rfind(|(_, id)| *id == connect_id)
      .map(|(i, _)| *i)
      .expect("the dialer queued bytes for the exchange");

    // The load-bearing assertion: the LAST byte for the exchange was
    // observed in an iteration STRICTLY BEFORE the Shutdown for that
    // exchange. With the per-exchange transmit-pending gate this holds by
    // construction; without it, the Shutdown surfaces in the same iteration
    // as the bytes (or earlier), and a naive `apply(action)` loop would
    // shutdown(write) the socket before writing the trailing bytes.
    assert!(
      last_byte_iter < shutdown_iter,
      "last byte for exchange {:?} was observed in iter {} but Shutdown \
       surfaced in iter {} — a naive driver would orphan the bytes",
      connect_id,
      last_byte_iter,
      shutdown_iter,
    );
  }

  /// An exchange whose deadline elapses BEFORE the driver has drained its
  /// outbound bytes must not leak those bytes after the bridge has been
  /// reaped. The transmit queue is eagerly populated by
  /// [`TcpEndpoint::pump_bridges`] one tick (the bridge's pre-failure label /
  /// request chunk lands in `out_transmit`); a later tick fires
  /// [`TcpEndpoint::handle_timeout`] past the bridge's deadline, the
  /// `pump_out` deadline path fails the bridge, and
  /// [`TcpEndpoint::reap_bridge`] enqueues `Close`. Without the
  /// [`TcpEndpoint::purge_transmit_for`] step, a driver doing the natural
  /// "drain actions, drain transmits, repeat" loop would emit the stale
  /// pre-failure bytes (the teardown gate withholds `Close` while
  /// `out_transmit` still has the exchange's chunk, so the bytes surface
  /// first) — delivering membership state from an exchange the local node
  /// has already failed, or sending a probe to a peer the local node has
  /// already given up on.
  ///
  /// Setup: dial an outbound push/pull at `now`, drain its `Connect` (so
  /// the natural drain loop below sees only the post-failure ordering of
  /// transmit vs. teardown), and advance the clock past the bridge's
  /// exchange deadline. The bridge's deadline is the exchange deadline
  /// carried on `Event::DialRequested` (`now + stream_timeout`, default
  /// 10s), so `now + 11s` is strictly past it.
  ///
  /// Mutation gate: reverting the `purge_transmit_for(id)` call in
  /// [`TcpEndpoint::reap_bridge`] fails the "no stale bytes" assertion
  /// below — the dialer's `[label, request]` chunk surfaces from the
  /// natural drain loop ahead of the released `Close`.
  #[test]
  fn timed_out_exchange_purges_stale_transmit_before_close() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // (1) Dial an outbound push/pull. The in-band `service_dials` +
    // `flush_outbound` builds and promotes the bridge in the SAME tick and
    // queues `[label, request]` into `out_transmit` (see `run_tick` step
    // (5.5)).
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    // (2) Drain the `Connect` to keep the natural drain loop below from
    // racing it against the post-failure bytes / `Close` ordering. The
    // gate has no Connects-vs-bytes interaction; pulling the `Connect` out
    // here isolates the assertion to the stale-byte / `Close` ordering.
    let connect = coord.poll_action();
    let exchange = match connect {
      Some(TcpAction::Connect(c)) => c.id(),
      other => panic!(
        "first poll_action MUST surface the dial's Connect (got {:?})",
        other.as_ref().map(action_kind),
      ),
    };
    // Do NOT drain `poll_transport_transmit` here — the queued
    // `[label, request]` chunk for the soon-to-fail exchange is exactly
    // the stale byte payload the purge must drop.
    assert!(
      coord.exchange_has_pending_bytes(exchange),
      "pre-failure invariant: the dialer queued bytes for the exchange",
    );

    // (3) Advance past the bridge's exchange deadline. The default
    // `stream_timeout` is 10s and the dial deadline is `now + 10s`;
    // pumping at `now + 11s` is strictly past it.
    let deadline_elapsed = now + Duration::from_secs(11);
    coord.handle_timeout(deadline_elapsed);

    // (4) Natural drain loop. With the purge in place, the gate releases
    // the `Close` immediately (`out_transmit` for the exchange is empty
    // after the reap), and no stale bytes for the failed exchange surface
    // from `poll_transport_transmit`.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<TcpAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (5) Load-bearing assertion: NO bytes for the failed exchange
    // surfaced. Mutation gate: dropping `purge_transmit_for` from
    // `reap_bridge` lets the dialer's pre-failure `[label, request]` chunk
    // surface here.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "stale bytes for the timed-out exchange {:?} must not be emitted; \
       got {:?}",
      exchange,
      stale,
    );

    // (6) Load-bearing assertion: `Close` for the failed exchange did
    // surface — the gate must release the teardown once the purge empties
    // the exchange's queue, else the bridge would leak (no `Close` ever
    // fires).
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the timed-out exchange {:?} must surface from the natural \
       drain loop after the purge releases the gate; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// An acceptor bridge whose inbound label is REJECTED (a wrong-cluster
  /// peer dials this node) must not leak its local label prefix to the peer
  /// before the coordinator emits `Close`. The acceptor's `RawRecords`
  /// queues `[12][len][local_label]` in its outbound buffer at
  /// construction; without the bridge's failure-retirement clearing that
  /// buffer, the reap path's `collect_bridge_transmits` drains it into
  /// `out_transmit` AFTER `purge_transmit_for` runs — and a driver doing
  /// the natural "drain actions, drain transmits, repeat" loop emits the
  /// local cluster label on the wire to a peer whose dial was just
  /// rejected.
  ///
  /// Setup: `accept_connection` builds the acceptor; one
  /// `handle_transport_data` delivers a wrong-cluster `[12][len][other]`
  /// header that `intake_handshaking` rejects (`self.fail(Transport)`); the
  /// natural drain loop then observes the resulting `Close` with NO bytes
  /// for the exchange.
  ///
  /// Mutation gate: removing `self.records.clear_outbound()` from
  /// `TcpBridge::fail` fails the "no stale bytes" assertion below — the
  /// leaked chunk is the local label `[12][len][cluster-x]` queued at
  /// `accept_connection` time.
  #[test]
  fn failed_acceptor_label_mismatch_no_bytes_before_close() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // (1) Driver accepts an inbound TCP connection from a wrong-cluster
    // peer. The acceptor's `RawRecords::outbound` is populated with the
    // local label prefix `[12][9]["cluster-x"]` at construction. The
    // acceptor stays Handshaking until its inbound label is validated.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) Wrong-cluster peer sends its own labeled header. The acceptor's
    // `classify_inbound_label` returns Rejected (label mismatch), the
    // bridge enters `Failed(Transport)` via `intake_handshaking`'s
    // `self.fail(...)`, and the driver-facing `run_tick` runs through to
    // the reap.
    let mut wrong = vec![12u8, 7];
    wrong.extend_from_slice(b"other-x");
    coord.handle_transport_data(exchange, &wrong, false, now);

    // (3) Natural drain loop: drain actions, drain transmits, repeat
    // until idle. Record everything observed for this exchange.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<TcpAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (4) Load-bearing assertion: NO bytes for the failed exchange
    // surfaced — the local label `[12][len][cluster-x]` queued at
    // `accept_connection` was cleared by `TcpBridge::fail`'s
    // `records.clear_outbound()` before the reap could drain it.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "stale outbound bytes for the rejected acceptor {:?} must not be \
       emitted to the wrong-cluster peer; got {:?}",
      exchange,
      stale,
    );

    // (5) Load-bearing assertion: `Close` for the failed exchange did
    // surface — the bridge is retired, but the coordinator MUST signal
    // the driver to close the socket.
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the rejected acceptor {:?} must surface; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// An acceptor whose `ACCEPT_HANDSHAKE_DEADLINE` elapses with NO inbound
  /// bytes (the slow-loris that connects but never sends a label) must not
  /// leak its local label prefix to that peer on the reap. The
  /// handshake-deadline guard in `TcpBridge::pump_out` fires
  /// `self.fail(Timeout)`, and the bridge's `records.clear_outbound()`
  /// drops the queued `[12][len][local_label]` before the coordinator's
  /// reap path can collect it.
  ///
  /// Setup: `accept_connection` builds the acceptor; NO
  /// `handle_transport_data` is delivered (the peer connects then stalls).
  /// One `handle_timeout(now + ACCEPT_HANDSHAKE_DEADLINE + 1s)` drives the
  /// handshake-deadline reap.
  ///
  /// Mutation gate: removing `self.records.clear_outbound()` from
  /// `TcpBridge::fail` fails the "no stale bytes" assertion — the local
  /// label leaks to the stalled peer at reap.
  #[test]
  fn failed_accept_handshake_timeout_no_bytes_before_close() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // (1) Driver accepts an inbound connection; the acceptor queues
    // `[12][9]["cluster-x"]` in `RawRecords::outbound` at construction.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) Slow-loris peer: NO `handle_transport_data` is delivered. The
    // bridge's deadline is `now + ACCEPT_HANDSHAKE_DEADLINE` (10s by
    // default); ticking at `now + 11s` is strictly past it.
    let deadline_elapsed = now + Duration::from_secs(11);
    coord.handle_timeout(deadline_elapsed);

    // (3) Natural drain loop. The handshake-deadline guard fired
    // (`self.fail(Timeout)`), `clear_outbound` ran, and the reap's
    // `collect_bridge_transmits` drained an empty buffer.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<TcpAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (4) Load-bearing assertion: NO bytes for the stalled exchange
    // surfaced.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "stale outbound bytes for the slow-loris acceptor {:?} must not be \
       emitted on the handshake-deadline reap; got {:?}",
      exchange,
      stale,
    );

    // (5) Load-bearing assertion: `Close` for the failed exchange did
    // surface.
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the slow-loris acceptor {:?} must surface; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// A driver that runs one `handle_timeout` tick on a freshly-accepted
  /// connection — BEFORE any inbound bytes arrive — must observe NO
  /// outbound bytes for that exchange. The acceptor's outbound label prefix
  /// is queued LAZILY at inbound-label validation, so the bridge's
  /// `records.outbound` is empty across the pump and `finalize_tick`'s
  /// `collect_bridge_transmits` drains nothing for this exchange. Faithful
  /// to `memberlist-core/src/network.rs::handle_conn`'s
  /// `read_message`-before-`send_message` ordering: an acceptor reveals
  /// cluster identity only after the dialer has proven its own.
  ///
  /// Mutation gate: restoring the at-construction eager queue in
  /// `RawRecords::acceptor` (calling `queue_outbound_label_if_needed` from
  /// the constructor) makes this test fail with the leaked
  /// `[12][len][cluster-x]` bytes surfacing on the first pump.
  #[test]
  fn accept_connection_then_handle_timeout_no_bytes_before_inbound_validation() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // (1) Driver accepts an inbound TCP connection; the acceptor is
    // Handshaking with an EMPTY `records.outbound` (lazy queue not yet
    // fired). No same-tick transmits surface from `accept_connection`.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) One driver tick with NO inbound bytes — models a scanner /
    // wrong-cluster / slow-loris peer that completes only the TCP
    // handshake. The bridge stays Handshaking, the lazy queue stays
    // dormant, `pump_bridges` finds an empty outbound buffer, and
    // `finalize_tick` collects nothing for the exchange.
    coord.handle_timeout(now);

    // (3) Drain everything the driver can observe.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<TcpAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (4) Load-bearing assertion: NO bytes for the still-handshaking
    // acceptor surfaced — the local label `[12][9][cluster-x]` is still
    // unqueued because inbound validation has not happened.
    let leaked: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      leaked.is_empty(),
      "handshaking acceptor {:?} must not leak the local cluster label to a \
       peer that has not validated its inbound label; got {:?}",
      exchange,
      leaked,
    );

    // (5) Sanity: no `Close` either — the bridge is still alive, waiting
    // for the dialer's labeled request (bounded by ACCEPT_HANDSHAKE_DEADLINE).
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      !close_for_exchange,
      "the bridge is still Handshaking; no Close should fire yet",
    );
  }

  /// Once a valid inbound label arrives, the acceptor's lazy outbound
  /// label queue fires and the next pump surfaces the `[12][len][label]`
  /// prefix on the wire — proving the asymmetric-queue redesign still
  /// emits the labeled reply when cluster identity is confirmed. Paired
  /// with `accept_connection_then_handle_timeout_no_bytes_before_inbound_validation`
  /// for the full "silent until validated, then labeled" invariant.
  #[test]
  fn accept_connection_then_valid_inbound_then_outbound_label_emitted() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // (1) Accept the connection; nothing on the wire yet.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(coord.poll_transport_transmit().is_none());

    // (2) Deliver a matching labeled header from the peer; the acceptor's
    // `classify_inbound_label` returns `Accepted`, the lazy queue fires
    // inside `handle_transport_data`, and the bridge's
    // `records.outbound` now holds the `[12][9][cluster-x]` prefix.
    let mut inbound = vec![12u8, 9];
    inbound.extend_from_slice(b"cluster-x");
    coord.handle_transport_data(exchange, &inbound, false, now);

    // (3) One tick to run the pump + collect bytes.
    coord.handle_timeout(now);

    // (4) Drain the queue and confirm the acceptor's outbound label
    // prefix surfaced for the exchange.
    let mut bytes_for_exchange: Vec<Bytes> = Vec::new();
    while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
      if id == exchange {
        bytes_for_exchange.push(bytes);
      }
    }
    assert!(
      !bytes_for_exchange.is_empty(),
      "lazy queue must fire on inbound validation; expected the acceptor's \
       labeled reply prefix to surface on the wire",
    );
    let first = &bytes_for_exchange[0];
    assert!(
      first.len() >= 2 + 9,
      "first chunk must contain at least the full label prefix, got len={}",
      first.len(),
    );
    assert_eq!(first[0], 12, "leading byte is LABELED_TAG");
    assert_eq!(
      first[1] as usize,
      "cluster-x".len(),
      "declared label length"
    );
    assert_eq!(
      &first[2..2 + 9],
      b"cluster-x",
      "label bytes match the configured cluster label",
    );
  }

  /// A clean acceptor exchange whose inbound `[label]` and `[request][FIN]`
  /// arrive in TWO separate transport reads must deliver BOTH its outbound
  /// label prefix AND its push/pull response to the wire — the
  /// [`TcpEndpoint::reap_bridge`] purge must not drop the legitimately-queued
  /// label.
  ///
  /// Setup: a dialer/acceptor pair on shared in-process coordinators. The
  /// dialer produces a real `[label]||[push/pull request]` blob via
  /// `start_push_pull`. We feed the LABEL portion to the acceptor with
  /// `eof=false` (first read), tick to drain the acceptor's lazy label into
  /// `out_transmit`, then feed the REQUEST tail with `eof=true` (second
  /// read), and tick once more to let the acceptor's response reach
  /// `out_transmit` and the bridge reach `BothClosed` for the clean reap.
  ///
  /// With the `is_failed()` gate on [`TcpEndpoint::reap_bridge`]'s
  /// [`TcpEndpoint::purge_transmit_for`] step, the clean reap preserves
  /// BOTH the label chunk (queued during the first tick) and the response
  /// chunk (added by the reap's `collect_bridge_transmits`). Without the
  /// gate the purge drops the label chunk and the peer sees a labelless
  /// response — its own inbound-label check then rejects the reply.
  ///
  /// Mutation gate: dropping the `if br.is_failed()` gate in
  /// `reap_bridge` (unconditional purge) fails the
  /// `response_bytes_after_reap_present` assertion below — `second_chunks`
  /// is empty because the purge dropped the label that earlier pumps had
  /// queued, leaving the reap with the response chunk only, and the queued
  /// label is gone.
  #[test]
  fn clean_acceptor_split_inbound_label_then_request_preserves_full_response_on_wire() {
    let now = Instant::now();
    let server_addr = addr(7500);
    let dialer_addr = addr(7501);

    // (1) Build a real dialer coordinator and ask it to start a push/pull.
    // The in-band `service_dials` + `flush_outbound` builds the dialer's
    // bridge, surfaces `Connect`, and queues `[label||push_pull_request]`
    // into `out_transmit`.
    let dialer_ep = endpoint(dialer_addr.port());
    let cfg_d = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut dialer: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> =
      TcpEndpoint::new(dialer_ep, cfg_d);
    let _sid = dialer.start_push_pull(server_addr, PushPullKind::Join, now);
    // Drain the `Connect` so the natural drain loop below sees only bytes.
    let _ = dialer
      .poll_action()
      .expect("dialer's first poll_action is the Connect");
    let mut dialer_bytes = Vec::new();
    while let Some((_id, _peer, bytes)) = dialer.poll_transport_transmit() {
      dialer_bytes.extend_from_slice(&bytes);
    }
    // The dialer produced `[12][9][cluster-x]` followed by an encoded
    // push/pull request frame. The label prefix is exactly 11 bytes; the
    // request tail is what remains.
    assert!(
      dialer_bytes.len() > 11,
      "dialer produced label + request, got {} bytes",
      dialer_bytes.len(),
    );
    assert_eq!(dialer_bytes[0], 12, "LABELED_TAG byte");
    assert_eq!(dialer_bytes[1] as usize, "cluster-x".len());
    let label_only = &dialer_bytes[..11];
    let request_tail = &dialer_bytes[11..];

    // (2) Build the acceptor coordinator and accept the connection. The
    // bridge is Handshaking; the lazy outbound label has NOT fired yet.
    let server_ep = endpoint(server_addr.port());
    let cfg_s = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut server: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> =
      TcpEndpoint::new(server_ep, cfg_s);
    let server_exchange = server.accept_connection(dialer_addr, now);

    // (3) First transport read: ONLY the dialer's label prefix, no FIN.
    // The acceptor's inbound label validates (`classify_inbound_label`
    // returns `Accepted`), the lazy outbound label fires inside
    // `RawRecords::handle_transport_data`, and the bridge's
    // `records.outbound` now holds `[12][9][cluster-x]`. The Stream is
    // not minted yet because the bytes-only feed leaves the bridge
    // Handshaking until the next `run_tick`'s
    // `service_handshake_completions`.
    server.handle_transport_data(server_exchange, label_only, false, now);

    // (4) One tick to promote and drain the acceptor's lazy label into
    // `out_transmit`. The bridge transitions Handshaking → Active and the
    // pump drains the queued label prefix into the coordinator's transmit
    // queue. The acceptor's lazy-queued label NOW sits in `out_transmit`
    // tagged with `server_exchange` — and we do NOT drain it yet (the
    // load-bearing invariant: a driver that ticks the coordinator multiple
    // times before reaching its `poll_transport_transmit` loop must still
    // receive the full reply).
    server.handle_timeout(now);
    assert!(
      server.exchange_has_pending_bytes(server_exchange),
      "the acceptor's lazy-queued label must be sitting in out_transmit \
       between ticks (the pre-condition for a clean reap with already-queued \
       bytes)",
    );

    // (5) Second transport read: the request tail + the same-tick FIN
    // anchor. The Stream FSM decodes the push/pull request, the endpoint
    // generates a response (the `SendPushPullResponse` command, encoded
    // and loaded by `drain_then_reap`), the bridge observes the recv-half
    // FIN → `RecvClosed`, then `pump_out` half-closes the send half →
    // `BothClosed`. The next `run_tick` reaps the bridge.
    //
    // Crucially, we do this BEFORE draining `out_transmit` — the label
    // chunk from step (4) is still in the queue. The reap that follows
    // will trigger `purge_transmit_for(server_exchange)` — gated on
    // `is_failed()` so an unconditional purge does not drop the legitimate
    // label chunk queued in step (4); the `is_failed` gate is the mutation
    // gate.
    server.handle_transport_data(server_exchange, request_tail, true, now);

    // (6) One tick at the same `now` so the post-mint pump flushes and the
    // reaper runs. The `BothClosed` reap is where `purge_transmit_for` fires
    // — its `is_failed` gate must preserve the legitimate label chunk.
    server.handle_timeout(now);

    // (7) Drain transmit AFTER the reap. Two chunks must surface in this
    // exchange — the label prefix queued in step (4) AND the response
    // body added by the reap's `collect_bridge_transmits` — so the
    // dialer reassembles a well-formed labeled reply.
    let mut all_chunks: Vec<Bytes> = Vec::new();
    while let Some((id, _peer, bytes)) = server.poll_transport_transmit() {
      if id == server_exchange {
        all_chunks.push(bytes);
      }
    }

    // Load-bearing assertions:
    //
    // (a) The label chunk from step (4) survived the clean reap — the
    //     first chunk must lead with `LABELED_TAG=12`. The mutation gate
    //     (`reap_bridge` purges unconditionally) fails this assertion: the
    //     purge dropped the label chunk, so the first surviving chunk
    //     starts with the response body's leading byte (the push/pull
    //     framing header), NOT the label tag.
    //
    // (b) At least two chunks surfaced — the label and the response are
    //     two separate `out_transmit` entries (label queued in tick (4),
    //     response added by the reap in tick (6)) and the natural drain
    //     loop pops them in queue order. The mutation gate drops the
    //     label, leaving exactly one chunk (the response) and failing
    //     this assertion.
    assert!(
      all_chunks.len() >= 2,
      "the full wire reply spans TWO out_transmit entries (label from \
       tick (4), response from the reap in tick (6)); the unconditional \
       purge mutation drops the label, leaving only the response. got \
       {} chunks: {:?}",
      all_chunks.len(),
      all_chunks.iter().map(Bytes::len).collect::<Vec<_>>(),
    );
    assert_eq!(
      all_chunks[0][0], 12,
      "the first chunk must lead with LABELED_TAG (the acceptor's outbound \
       label prefix); the mutation gate causes this to fail because the \
       purge dropped the label and the first surviving chunk is the \
       push/pull response body's framing byte",
    );
    assert_eq!(
      all_chunks[0][1] as usize,
      "cluster-x".len(),
      "the label chunk's declared length matches the configured label",
    );

    // (8) The clean reap must emit `Close` for the exchange. The teardown
    // gate releases `Close` once `out_transmit` for the exchange has been
    // drained (step (7) drained the queue, so the gate is unblocked).
    let mut actions: Vec<TcpAction> = Vec::new();
    while let Some(a) = server.poll_action() {
      actions.push(a);
    }
    let close_for_exchange = actions
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == server_exchange);
    assert!(
      close_for_exchange,
      "the clean reap must emit Close for the exchange; got {:?}",
      actions.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (9) The bridge was reaped — no stragglers held to the accept
    // deadline.
    assert_eq!(
      server.live_bridge_count(),
      0,
      "the bridge was reaped on the clean exchange completion",
    );
  }

  /// A dialer whose stream intent is retired by the inner endpoint
  /// (deadline elapsed inside `dial_succeeded`, or an explicit external
  /// `dial_failed`) BEFORE the coordinator could mint a `Stream` MUST NOT
  /// leak the eager-queued local cluster label, AND its still-queued
  /// `Connect` action MUST NOT surface to the driver. The
  /// [`TcpEndpoint::service_handshake_completions`] `dial_succeeded`-returns-
  /// `None` branch is the reap site for this scenario: without the four-step
  /// fail-and-purge — bridge `fail_connection_lost` (clears
  /// `records.outbound`), `purge_transmit_for`,
  /// `purge_pending_connect_for`, then drop the bridge with NO
  /// `collect_bridge_transmits` — a driver doing the natural drain loop would
  /// dequeue `Connect` (Connects surface before teardowns), open the
  /// socket, write the label, and then close it: a wrong-cluster peer
  /// observes the local label even though the dial intent was never
  /// supposed to reach it.
  ///
  /// Setup constructs the failed state directly. We invoke the inner
  /// `start_push_pull` so a `DialRequested` event + a `pending_stream_intent`
  /// exist, then drive `service_dials` so the dialer bridge is inserted
  /// (mint = `Some(Outbound)`, `records.outbound` carries `[12][len][label]`,
  /// `Connect` is queued in `pending_connects`). We then call
  /// `ep.dial_failed` to retire the intent — the natural API does not
  /// reach this branch through `start_push_pull`'s in-band flush, which
  /// promotes the dialer bridge in the SAME tick before any intent
  /// retirement is possible, so the direct injection is the only way to
  /// exercise the branch deterministically. A subsequent `flush_outbound`
  /// drives `service_handshake_completions`, which calls
  /// `dial_succeeded(stream_id, now)`, observes `None`, and runs the
  /// fail-and-purge reap.
  ///
  /// Mutation gate: removing the `bridge.fail_connection_lost()` call from
  /// the `dial_succeeded(None)` branch lets the dialer's eager-queued
  /// `[12][len][cluster-x]` survive in `records.outbound` (`clear_outbound`
  /// never runs). Removing the `purge_pending_connect_for(id)` call lets the
  /// queued `Connect` surface, so the natural drain loop opens the socket.
  /// Restoring the `collect_bridge_transmits(id, &mut br)` call
  /// unconditionally routes the just-cleared-or-pre-leak label into
  /// `out_transmit`. Any of those mutations fails the assertions below.
  #[test]
  fn failed_dial_no_label_leak_no_connect_emitted() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    let mut coord: TcpEndpoint<SmolStr, SocketAddr, IdentityBridge> = TcpEndpoint::new(ep, cfg);

    // (1) Create the dial intent in the inner endpoint. This queues a
    // `DialRequested` event in `coord.ep.pending_events` and inserts the
    // intent into `pending_stream_intents`. We deliberately invoke the
    // inner endpoint directly rather than `coord.start_push_pull` so the
    // coordinator's same-tick in-band flush (which would promote the
    // dialer bridge and clear its `mint`) does not run yet.
    let stream_id = coord
      .ep
      .start_push_pull(addr(7000), PushPullKind::Refresh, now);

    // (2) Sieve the `DialRequested` into `dial_pending`, then drive
    // `service_dials` to insert the dialer bridge with
    // `mint = Some(Outbound(stream_id))` and eager-queue
    // `[12][9]["cluster-x"]` into `records.outbound`. A `Connect` for
    // the exchange lands in `pending_connects`. The bridge is NOT
    // promoted yet — that happens in
    // `service_handshake_completions`, which we drive in step (4).
    coord.sieve_dial_events();
    coord.service_dials(now);

    // Discover the exchange handle of the just-inserted dialer bridge.
    // Exactly one bridge exists at this point.
    let ids = coord.conns.ids();
    assert_eq!(ids.len(), 1, "service_dials inserted exactly one bridge");
    let exchange = ids[0];

    // (3) Retire the dial intent directly through the inner endpoint.
    // After this, `dial_succeeded(stream_id, now)` returns `None` (the
    // intent is gone), which is the trigger for the `dial_succeeded(None)`
    // branch in `service_handshake_completions`.
    coord.ep.dial_failed(
      stream_id,
      crate::error::StreamError::DialFailed("test injection".into()),
      now,
    );

    // (4) Drive the same in-band flush `start_push_pull`'s wrapper would
    // have run: `pump_bridges` → `service_handshake_completions` →
    // `pump_bridges` → `finalize_tick`. The Handshaking bridge's
    // `pump_out` does NOT fail it (the bridge deadline is still in the
    // future), so the bridge survives `pump_bridges` and the SHC step
    // calls `dial_succeeded(stream_id, now)` → `None`, entering the
    // fail-and-purge reap path.
    coord.flush_outbound(now);

    // (5) Natural drain loop: drain actions and transmits until idle.
    // Record everything for assertion.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<TcpAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (6) Load-bearing assertion: NO bytes for the failed exchange
    // surfaced. The leaked chunk a missing fix would emit is the
    // dialer's eager `[12][9]["cluster-x"]` prefix.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "failed dial must not emit any bytes for the exchange; got {:?}",
      stale,
    );

    // (7) Load-bearing assertion: NO `Connect` for the failed exchange
    // surfaced. The Connect was queued by `service_dials` and must be
    // purged by `purge_pending_connect_for` so the driver does not open
    // a socket for an exchange the coordinator has already failed.
    let connect_for_exchange = actions_observed.iter().any(|a| match a {
      TcpAction::Connect(info) => info.id() == exchange,
      _ => false,
    });
    assert!(
      !connect_for_exchange,
      "failed dial must not emit Connect for the exchange; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (8) `Close` for the failed exchange surfaces — the contract per the
    // fix is to emit `Close` so a driver that had already drained
    // `Connect` from a prior tick can clean up the open socket. In this
    // test the Connect was purged so a driver's `Close` is a no-op for
    // an unopened socket, but the action presence is the documented
    // behaviour.
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the failed exchange must surface; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (9) The bridge was reaped and the exchange forgotten.
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the bridge was reaped on the dial-failure path",
    );
  }

  // ---- helpers ----------------------------------------------------------

  fn action_kind(a: &TcpAction) -> &'static str {
    match a {
      TcpAction::Connect(_) => "Connect",
      TcpAction::Shutdown(_) => "Shutdown",
      TcpAction::Close(_) => "Close",
    }
  }
}
