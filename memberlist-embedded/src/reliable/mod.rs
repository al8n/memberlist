//! The pooled-stream reliable plane: a fixed set of connection slots (the
//! driver's stream sockets, keyed by an opaque [`StreamIo::Conn`](crate::StreamIo::Conn)
//! handle) multiplexing all concurrent reliable exchanges, with no task
//! spawning.
//!
//! At construction the driver pre-creates a fixed number of reliable-stream
//! sockets and stores their handles in the free-list. One handle is immediately
//! removed from the pool and placed in `listen` state as the passive-open
//! server socket. The remaining handles in the pool are available for outbound
//! dials and inbound connections that arrive on the listener.
//!
//! Each reliable exchange's full lifecycle — its connection slot, the bytes
//! parked while the socket opens or backpressures, a deferred graceful FIN, the
//! drain deadline of a graceful close still flushing buffered bytes, and whether
//! the peer's EOF was delivered — lives in ONE [`Connection`] keyed by
//! [`ExchangeId`] in [`ReliablePlane::connections`]. Modelling the exchange as a
//! single state machine keeps every transition a single mutation and makes the
//! invariants structural: tearing an exchange down is removing its `Connection`,
//! which drops all of its per-exchange state at once. A graceful close that
//! still has outbound bytes to deliver is the one teardown that does NOT remove
//! the `Connection` on the spot — it parks in [`ConnState::Closing`] so the
//! egress pump can finish flushing those bytes, then removes it once they are
//! delivered (or its drain deadline forces an abort), preserving the same
//! whole-`Connection` removal at completion.
//!
//! This module owns only the MAPPING and LIFECYCLE BOOKKEEPING; it performs no
//! socket I/O. Every transition that would touch a socket is driven by the
//! [`Engine`](crate::Engine), which calls the matching [`StreamIo`](crate::StreamIo)
//! method on the driver's socket pool. `C` is the driver's opaque connection
//! handle (`StreamIo::Conn`).

use bytes::Bytes;
use core::net::SocketAddr;
use hashbrown::HashMap;
use memberlist_proto::{Instant, streams::ExchangeId};
use std::{collections::VecDeque, vec::Vec};

use crate::stream_io::SlotGen;

/// Free-list of pre-created reliable-stream connection handles.
///
/// Handles are added at construction via [`Pool::push`]. The dial/accept paths
/// call [`Pool::take`] to borrow a handle for one exchange and [`Pool::give`] to
/// return it when the exchange completes or fails.
pub struct Pool<C> {
  free: Vec<C>,
}

impl<C> Pool<C> {
  /// Create an empty free-list.
  pub fn new() -> Self {
    Self { free: Vec::new() }
  }

  /// Append a handle to the free-list. Called once per connection slot at
  /// construction.
  pub fn push(&mut self, c: C) {
    self.free.push(c);
  }

  /// Remove and return a free connection handle, or `None` if all slots are in
  /// use (pool exhausted).
  ///
  /// The free-list holds ONLY reusable slots by construction: a slot mid-teardown
  /// lives in [`ReliablePlane::retiring`], not here, and is returned to the pool
  /// only once its teardown is acknowledged
  /// ([`StreamIo::teardown_done`](crate::StreamIo::teardown_done)). So the plain
  /// LIFO pop always yields a slot the engine may immediately `listen` / `connect`.
  pub fn take(&mut self) -> Option<C> {
    self.free.pop()
  }

  /// Return a handle to the free-list once its occupancy's teardown has been
  /// acknowledged. The engine returns a slot here only after
  /// [`StreamIo::teardown_done`](crate::StreamIo::teardown_done) reports the
  /// socket is reset and reusable, so every pooled slot is reusable by
  /// construction.
  pub fn give(&mut self, c: C) {
    self.free.push(c);
  }

  /// Whether the free-list is empty (all slots assigned to active exchanges, the
  /// listener, or a pending teardown in [`ReliablePlane::retiring`]).
  pub fn is_empty(&self) -> bool {
    self.free.is_empty()
  }

  /// Number of slots currently in the free-list. A diagnostic surfaced by the
  /// driver to witness pool recovery.
  pub fn free_len(&self) -> usize {
    self.free.len()
  }
}

impl<C> Default for Pool<C> {
  fn default() -> Self {
    Self::new()
  }
}

/// The lifecycle stage of one reliable exchange's connection.
///
/// The progression is `PendingDial`/`Dialing → Established → HalfClosed`, with
/// `Established → Closing` as a terminal drain branch. An outbound exchange
/// starts in `Dialing` (slot assigned) or `PendingDial` (pool exhausted, no slot
/// yet); an inbound exchange is accepted straight into `Established`.
/// `HalfClosed` is entered when our deferred graceful FIN is finally emitted.
/// `Closing` is entered when a graceful `StreamAction::Close` arrives while the
/// connection still has outbound bytes the peer has not received: the
/// `Connection` stays mapped so the egress pump keeps flushing those bytes, and
/// the terminal FIN + slot reclaim is deferred until the bytes are delivered (or
/// the close deadline forces an abort). Teardown (`StreamAction::Close`)
/// otherwise removes the whole [`Connection`] from any state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
  /// Dial requested but no slot was free (pool exhausted). `out` / `fin_pending`
  /// accumulate here; when a slot frees, the engine assigns one and transitions
  /// the connection to `Dialing`. The dial intent is never dropped on
  /// exhaustion.
  PendingDial,
  /// Slot assigned, the TCP three-way handshake is in flight (SynSent →
  /// Established). Outbound bytes stay parked in `out` until the socket is
  /// writable.
  Dialing,
  /// Connected with the write half open. Pumps both directions; when
  /// `fin_pending` is set and `out` is drained and acknowledged, the graceful
  /// FIN is emitted and the connection transitions to `HalfClosed`. A graceful
  /// `StreamAction::Close` arriving here with outbound bytes still undelivered
  /// transitions instead to `Closing` to drain them before the terminal FIN.
  Established,
  /// Our write-half FIN has been sent; the connection is read-only. It keeps
  /// draining the peer's reply and EOF until the machine issues
  /// `StreamAction::Close`. The link layer's `close()` shuts only the transmit
  /// half, so a FinWait socket still receives.
  HalfClosed,
  /// A graceful `StreamAction::Close` arrived while the send-capable socket
  /// (Established / CloseWait) still held outbound bytes the peer has not
  /// received — either parked in `out` (partial-write backpressure) or in the tx
  /// ring awaiting ACK. The `Connection` stays mapped and the egress pump keeps
  /// flushing `out` into the tx ring; only once `out` is empty AND the tx ring is
  /// fully acknowledged does the engine emit the terminal FIN (`close()`) and
  /// detach the slot into `closing`. `close_deadline` is the backstop: a peer
  /// that never drains the tx ring is force-aborted and reclaimed at the deadline
  /// so the pool cannot wedge. This is the drain-before-close guarantee that
  /// keeps an oversized push/pull reply from being truncated by the FIN.
  Closing,
}

/// One reliable exchange's connection lifecycle and buffered state.
///
/// This is the whole per-exchange state. Tearing the exchange down is removing
/// the `Connection` from [`ReliablePlane::connections`], which drops the
/// connection handle, the parked `out` bytes, the deferred-FIN flag, the close
/// drain deadline, and the EOF-delivered flag together — no separate per-field
/// purges to keep in sync. The only teardown that defers that removal is a
/// graceful close with bytes still to deliver: it parks in [`ConnState::Closing`]
/// until the egress pump has flushed them, then removes the whole `Connection`.
pub struct Connection<C> {
  /// The peer's socket address (the dial target, or the accepted remote).
  pub peer: SocketAddr,
  /// The assigned pooled connection handle. `None` only in
  /// [`ConnState::PendingDial`], where the pool was exhausted and no slot has
  /// been assigned yet.
  pub socket: Option<C>,
  /// The connection's lifecycle stage.
  pub state: ConnState,
  /// Outbound bytes not yet fully written to the socket's tx ring. Holds
  /// partial-write remainders (backpressure) and bytes parked while the socket
  /// is still opening or the dial is deferred. Ordered oldest-first so
  /// per-exchange byte order is preserved across ticks.
  pub out: VecDeque<Bytes>,
  /// A graceful write-half FIN (`StreamAction::Shutdown`) was requested but not
  /// yet emitted. Deferred until the socket is `Established` and `out` is fully
  /// drained and acknowledged, then emitted via `close()` exactly once (see
  /// [`ConnState::Established`]).
  pub fin_pending: bool,
  /// The peer's FIN/EOF has been delivered to the machine. Gates the
  /// exactly-once empty-EOF `handle_transport_data` call.
  pub eof_delivered: bool,
  /// Backstop deadline for the [`ConnState::Closing`] drain. `Some` only while
  /// the connection is `Closing`: it is the instant by which the buffered
  /// outbound bytes must have been delivered and the terminal FIN emitted. If the
  /// peer never drains the tx ring (permanent backpressure / vanished peer) the
  /// drain check force-aborts the socket and reclaims it at this instant so the
  /// pool cannot wedge. Folded into the engine's returned wakeup alongside the
  /// `closing`-map deadlines so a deadline-driven caller honors it.
  pub close_deadline: Option<Instant>,
  /// The undelivered byte count (`out` bytes + the socket's tx `send_queue`) at
  /// the last drain-progress observation, meaningful only while `Closing`. The
  /// drain check re-arms `close_deadline` whenever the count shrinks, making
  /// `close_timeout` a NO-PROGRESS (idle) bound rather than a total-duration cap:
  /// a slow-but-progressing peer (reading the response over more than
  /// `close_timeout`) is never force-aborted, while a stalled / vanished peer
  /// (no progress for the full `close_timeout`) still is.
  pub close_drain_mark: usize,
}

impl<C> Connection<C> {
  /// A connection whose slot was assigned and is dialing the peer.
  pub fn dialing(peer: SocketAddr, socket: C) -> Self {
    Self {
      peer,
      socket: Some(socket),
      state: ConnState::Dialing,
      out: VecDeque::new(),
      fin_pending: false,
      eof_delivered: false,
      close_deadline: None,
      close_drain_mark: 0,
    }
  }

  /// A connection whose dial is deferred because the pool was exhausted: no slot
  /// yet, parked in [`ConnState::PendingDial`]. Outbound bytes and a graceful FIN
  /// may still accumulate until a slot is assigned.
  pub fn pending_dial(peer: SocketAddr) -> Self {
    Self {
      peer,
      socket: None,
      state: ConnState::PendingDial,
      out: VecDeque::new(),
      fin_pending: false,
      eof_delivered: false,
      close_deadline: None,
      close_drain_mark: 0,
    }
  }

  /// A connection for an inbound exchange accepted on the listener: the socket
  /// is already `Established` (the handshake completed before accept).
  pub fn accepted(peer: SocketAddr, socket: C) -> Self {
    Self {
      peer,
      socket: Some(socket),
      state: ConnState::Established,
      out: VecDeque::new(),
      fin_pending: false,
      eof_delivered: false,
      close_deadline: None,
      close_drain_mark: 0,
    }
  }

  /// Assign a freed slot to a connection that was waiting in `PendingDial`,
  /// transitioning it to `Dialing`. Any `out` bytes and a pending FIN parked
  /// while it waited are retained and flush once the socket is Established.
  pub fn assign_socket(&mut self, socket: C) {
    self.socket = Some(socket);
    self.state = ConnState::Dialing;
  }

  /// Whether all outbound bytes have been written and acknowledged at the
  /// `out`-queue level (no parked remainder). A precondition — together with the
  /// socket's own `send_queue() == 0` — for emitting a deferred graceful FIN.
  pub fn out_is_empty(&self) -> bool {
    self.out.is_empty()
  }

  /// Total bytes still parked in `out` (not yet written to the socket's tx ring).
  pub fn out_bytes(&self) -> usize {
    self.out.iter().map(|b| b.len()).sum()
  }
}

/// The phase of a retired slot occupancy in [`ReliablePlane::retiring`].
///
/// A retire begins as `Draining` after a graceful `close` (our FIN is flushing;
/// the socket works through the TCP FIN states before it is reusable) or as
/// `Aborting` after an `abort` (the RST is egressing). The reap escalates a
/// `Draining` entry past its deadline to `Aborting`; an `Aborting` entry never
/// regresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetirePhase {
  /// A graceful close is in flight (subsumes the pre-ledger "closing" state):
  /// our FIN was emitted and the socket is draining toward a reusable state, or
  /// the peer's FIN is still pending. Reclaimed once the driver acknowledges the
  /// teardown; escalated to [`RetirePhase::Aborting`] if it stalls past its
  /// deadline.
  Draining,
  /// An abort (RST) has been issued and the slot is awaiting the driver's
  /// acknowledgement that the reset has egressed and the socket is reusable.
  Aborting,
}

/// One retired slot occupancy awaiting its teardown acknowledgement in
/// [`ReliablePlane::retiring`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retiring {
  /// The generation of the occupancy being torn down. Matched against the
  /// driver's [`StreamIo::teardown_done`](crate::StreamIo::teardown_done) so an
  /// acknowledgement frees only the occupancy it actually completed.
  pub generation: SlotGen,
  /// The instant at which the reap escalates this teardown: a `Draining` entry
  /// switches to `Aborting` (issuing the RST); an `Aborting` entry re-issues the
  /// abort and counts a `teardown_overruns`.
  pub deadline: Instant,
  /// Whether this retire is a graceful drain or an abort.
  pub phase: RetirePhase,
}

/// Maps in-flight exchanges to their [`Connection`], plus the pool and listener.
///
/// `connections` is keyed by [`ExchangeId`] (the machine's correlation token);
/// each entry is the complete lifecycle of one reliable exchange. `listener`
/// holds the dedicated passive-open slot created at construction; it is `None`
/// only when the pool was configured with zero slots (`tcp_pool_size == 0`), an
/// unusual but valid degenerate case, or transiently after an accept consumed it
/// and the pool could not yet replenish.
///
/// The fields are populated by the engine's connection-management (dial / accept
/// / pump) paths.
pub struct ReliablePlane<C> {
  /// Free slots available for new exchanges.
  pub pool: Pool<C>,
  /// Active reliable exchanges, each modelled as one [`Connection`] state
  /// machine.
  pub connections: HashMap<ExchangeId, Connection<C>>,
  /// The dedicated passive-open slot.
  pub listener: Option<C>,
  /// Slots whose occupancy has been retired (a graceful `close` or an `abort`
  /// issued) but whose teardown the driver has not yet acknowledged
  /// ([`StreamIo::teardown_done`](crate::StreamIo::teardown_done)). Keyed by the
  /// connection handle, each entry records the retired occupancy's generation,
  /// the deadline by which the reap pass escalates a stalled teardown, and its
  /// [`RetirePhase`]. A slot is returned to [`Pool`] ONLY once its teardown is
  /// acknowledged, so a handle is never reused while a pending RST/FIN could be
  /// clobbered or suppressed.
  ///
  /// Populated by every engine teardown path — a graceful `close` whose peer has
  /// not finished the close (parked `Draining`), an `abort` of a failed or
  /// never-established exchange (`Aborting`), and the force-abort of a stalled
  /// drain. Without this ledger a detached handle would be unreachable (absent
  /// from `connections`, `pool`, and `listener`) and the slot would leak.
  ///
  /// The deadline bounds the wait: a link layer such as smoltcp sets no TCP
  /// timeout by default, so a peer that vanishes mid-FIN would keep the socket
  /// `is_open()` forever. When a `Draining` entry passes its deadline the reap
  /// escalates it to `Aborting` (issuing the RST); an `Aborting` entry past its
  /// deadline re-issues the abort and counts a [`teardown_overruns`](Self::teardown_overruns).
  /// The slot is freed only when the driver finally acknowledges the teardown —
  /// never blindly, since a driver whose socket is owned by an async worker
  /// cannot be dispossessed by the engine.
  pub retiring: HashMap<C, Retiring>,
  /// The current (or last) occupancy generation of each connection handle.
  ///
  /// The engine advances a slot's generation each time it takes the slot out of
  /// the pool for a fresh `listen` / `connect`, and stamps that generation onto
  /// the retire (`retiring`) so a teardown acknowledgement is matched to the
  /// exact occupancy it completes. A slot with no entry has never been occupied;
  /// its first occupancy uses [`SlotGen::START`]. Bounded by the pool size.
  pub slot_gen: HashMap<C, SlotGen>,
  /// Diagnostic count of `Aborting`-phase deadline expiries — a retired occupancy
  /// whose teardown the driver has still not acknowledged a full `close_timeout`
  /// after the abort was issued. A non-zero value witnesses a residual socket pin
  /// (e.g. an embassy worker future the engine cannot dispossess); it never
  /// causes the engine to free a slot whose teardown is unacknowledged.
  pub teardown_overruns: u64,
  /// Monotonic count of inbound reliable connections accepted on the listener.
  ///
  /// Incremented once per passive open handed to the machine. It is a diagnostic
  /// for the listener self-healing invariant: after the pool is momentarily
  /// exhausted (the listener slot becomes the exchange and no free slot is
  /// available to replenish it), a later free slot must be re-established as the
  /// listener so a SECOND inbound connection is still accepted. Membership/event
  /// observation cannot witness that invariant because gossip can converge a peer
  /// with no TCP accept at all; this counter measures the accept directly.
  pub accepted_inbound: u64,
}

impl<C> ReliablePlane<C> {
  /// Create an empty reliable plane: no pool entries, no connections, no
  /// listener, no retiring slots. Slots are added by the driver immediately
  /// after.
  pub fn new() -> Self {
    Self {
      pool: Pool::new(),
      connections: HashMap::new(),
      listener: None,
      retiring: HashMap::new(),
      slot_gen: HashMap::new(),
      teardown_overruns: 0,
      accepted_inbound: 0,
    }
  }

  /// Number of retired occupancies still in the graceful-close ([`RetirePhase::Draining`])
  /// phase — the analog of the pre-ledger "closing" count. Slots aborted (or
  /// escalated to [`RetirePhase::Aborting`]) are excluded: a graceful close parks
  /// here awaiting the peer, whereas an abort reclaims as soon as its RST egress
  /// is acknowledged.
  pub fn draining_count(&self) -> usize {
    self
      .retiring
      .values()
      .filter(|r| r.phase == RetirePhase::Draining)
      .count()
  }

  /// Number of reliable exchanges currently in [`ConnState::HalfClosed`]: their
  /// graceful write-half FIN has been emitted but the `Connection` is still
  /// mapped, awaiting the peer's reply and/or FIN.
  pub fn half_closed_count(&self) -> usize {
    self
      .connections
      .values()
      .filter(|c| c.state == ConnState::HalfClosed)
      .count()
  }

  /// Number of reliable exchanges still in [`ConnState::PendingDial`]: a dial was
  /// requested but the pool was exhausted, so no slot is assigned yet.
  pub fn pending_dial_count(&self) -> usize {
    self
      .connections
      .values()
      .filter(|c| c.state == ConnState::PendingDial)
      .count()
  }
}

impl<C> Default for ReliablePlane<C> {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests;
