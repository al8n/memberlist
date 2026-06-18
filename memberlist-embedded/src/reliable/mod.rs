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
use core::{hash::Hash, net::SocketAddr};
use hashbrown::HashMap;
use memberlist_proto::{Instant, streams::ExchangeId};
use std::{collections::VecDeque, vec::Vec};

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
  pub fn take(&mut self) -> Option<C> {
    self.free.pop()
  }

  /// Remove and return the most-recently-freed handle for which `ready` is
  /// `true`, leaving every other handle (those still tearing down) in the pool.
  ///
  /// A driver whose teardown is asynchronous returns a freed slot to the pool
  /// before its worker has reset the underlying socket; reusing such a slot for a
  /// fresh `listen` / `connect` would clobber the pending reset and leak the prior
  /// connection's state. This lets the engine pick a slot that is actually reset
  /// (`StreamIo::reuse_ready`) and skip the rest until a later tick, without
  /// reordering the survivors (so the still-tearing-down slots stay available the
  /// instant they become ready). For a synchronous-teardown driver every slot is
  /// ready, so this is exactly `take`.
  pub fn take_where(&mut self, mut ready: impl FnMut(&C) -> bool) -> Option<C> {
    // Scan newest-first (the end of the Vec) to match `take`'s LIFO reuse, and
    // remove the first ready handle in place, preserving the order of the rest.
    let pos = (0..self.free.len()).rev().find(|&i| ready(&self.free[i]))?;
    Some(self.free.remove(pos))
  }

  /// Return a handle to the free-list once its exchange is complete. The caller
  /// is responsible for resetting / aborting the socket first so it is ready for
  /// reuse.
  pub fn give(&mut self, c: C) {
    self.free.push(c);
  }

  /// Whether the free-list is empty (all slots assigned to active exchanges or
  /// the listener).
  pub fn is_empty(&self) -> bool {
    self.free.is_empty()
  }

  /// Whether any free handle currently satisfies `ready` — i.e. at least one
  /// pooled slot the engine could reuse THIS tick. Used by the end-of-tick
  /// invariant so a pool holding only still-resetting (not reuse-ready) slots does
  /// not count as "a free slot the rebalance should have spent".
  pub fn any_where(&self, ready: impl FnMut(&C) -> bool) -> bool {
    self.free.iter().any(ready)
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
  /// Slots parked mid-close (our FIN sent, the peer's not yet completed), each
  /// paired with the deadline by which the reap pass force-aborts it; reclaimed
  /// to the free-list once they reach `Closed` (the peer's FIN completed the
  /// close) OR the deadline elapses.
  ///
  /// Populated by the engine's `teardown` (or `flush_closing`) when a graceful
  /// close emits the terminal FIN on a socket whose peer has not finished the
  /// close: the exchange already half-closed (its graceful FIN went out earlier
  /// while the `Connection` stayed mapped so the peer's reply still pumped); an
  /// acceptor torn down in `CloseWait` whose reply was already fully delivered;
  /// or a connection that finished draining its buffered reply in
  /// [`ConnState::Closing`] and only then FIN-ed. In each case the `Connection`
  /// is removed and the detached handle is parked here (with deadline
  /// `now + close_timeout`) so it stays reachable. Without this map the handle
  /// would be unreachable (absent from `connections`, `pool`, and `listener`)
  /// and the slot would leak, permanently shrinking the pool after enough closes
  /// whose peer lingered.
  ///
  /// The deadline bounds the close: a link layer such as smoltcp sets no TCP
  /// timeout by default, so a peer that vanishes mid-FIN (stuck in
  /// FinWait/LastAck) would keep the socket `is_open()` forever and the handle
  /// would never return to the pool. The reap pass force-`abort()`s any handle
  /// parked past its deadline, guaranteeing the pool (and the listener
  /// replenished from it) always recover. A `Close` whose socket is already
  /// `!is_open()` (the clean both-FIN case) bypasses this map and returns
  /// straight to the pool.
  pub closing: HashMap<C, Instant>,
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

impl<C> ReliablePlane<C>
where
  C: Hash + Eq,
{
  /// Create an empty reliable plane: no pool entries, no connections, no
  /// listener, no closing slots. Slots are added by the driver immediately
  /// after.
  pub fn new() -> Self {
    Self {
      pool: Pool::new(),
      connections: HashMap::new(),
      listener: None,
      closing: HashMap::new(),
      accepted_inbound: 0,
    }
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

impl<C> Default for ReliablePlane<C>
where
  C: Hash + Eq,
{
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests;
