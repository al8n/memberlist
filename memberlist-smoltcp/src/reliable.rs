//! The pooled-TCP reliable plane: a fixed set of smoltcp `tcp::Socket`s
//! multiplexing all concurrent reliable exchanges, with no task spawning.
//!
//! At construction [`crate::Memberlist::new`] pre-creates `cfg.tcp_pool_size`
//! TCP sockets and stores their [`SocketHandle`]s in the free-list. One handle
//! is immediately removed from the pool and placed in `listen` state as the
//! passive-open server socket. The remaining handles in the pool are available
//! for outbound dials and inbound connections that arrive on the listener.
//!
//! Each reliable exchange's full lifecycle — its socket, the bytes parked while
//! the socket opens or backpressures, a deferred graceful FIN, the drain deadline
//! of a graceful close still flushing buffered bytes, and whether the peer's EOF
//! was delivered — lives in ONE [`Connection`] keyed by [`ExchangeId`] in
//! [`ReliablePlane::connections`]. Modelling the exchange as a single state
//! machine keeps every transition a single mutation and makes the invariants
//! structural: tearing an exchange down is removing its `Connection`, which drops
//! all of its per-exchange state at once. A graceful close that still has
//! outbound bytes to deliver is the one teardown that does NOT remove the
//! `Connection` on the spot — it parks in [`ConnState::Closing`] so the egress
//! pump can finish flushing those bytes, then removes it once they are delivered
//! (or its drain deadline forces an abort), preserving the same whole-`Connection`
//! removal at completion.

use bytes::Bytes;
use core::net::SocketAddr;
use hashbrown::HashMap;
use memberlist_machine::{Instant, streams::ExchangeId};
use smoltcp::iface::SocketHandle;
use std::{collections::VecDeque, vec::Vec};

/// Free-list of pre-created TCP socket handles.
///
/// Sockets are added at construction via [`Pool::push`]. The dial/accept paths
/// call [`Pool::take`] to borrow a socket for one exchange and [`Pool::give`]
/// to return it when the exchange completes or fails.
pub(crate) struct Pool {
  free: Vec<SocketHandle>,
}

impl Pool {
  /// Create an empty free-list.
  pub(crate) fn new() -> Self {
    Self { free: Vec::new() }
  }

  /// Append a handle to the free-list. Called once per socket at construction.
  pub(crate) fn push(&mut self, h: SocketHandle) {
    self.free.push(h);
  }

  /// Remove and return a free socket handle, or `None` if all sockets are in
  /// use (pool exhausted).
  pub(crate) fn take(&mut self) -> Option<SocketHandle> {
    self.free.pop()
  }

  /// Return a socket to the free-list once its exchange is complete. The
  /// caller is responsible for resetting / aborting the socket first so it
  /// is ready for reuse.
  pub(crate) fn give(&mut self, h: SocketHandle) {
    self.free.push(h);
  }

  /// Whether the free-list is empty (all sockets assigned to active exchanges
  /// or the listener).
  #[allow(dead_code)] // not yet wired: used by the pool-exhaustion detection path
  pub(crate) fn is_empty(&self) -> bool {
    self.free.is_empty()
  }

  /// Number of sockets currently in the free-list. A diagnostic surfaced by
  /// [`crate::Memberlist::pool_free_count`] to witness pool recovery.
  pub(crate) fn free_len(&self) -> usize {
    self.free.len()
  }
}

/// The lifecycle stage of one reliable exchange's TCP connection.
///
/// The progression is `PendingDial`/`Dialing → Established → HalfClosed`, with
/// `Established → Closing` as a terminal drain branch. An outbound exchange
/// starts in `Dialing` (socket assigned) or `PendingDial` (pool exhausted, no
/// socket yet); an inbound exchange is accepted straight into `Established`.
/// `HalfClosed` is entered when our deferred graceful FIN is finally emitted.
/// `Closing` is entered when a graceful `StreamAction::Close` arrives while the
/// connection still has outbound bytes the peer has not received: the
/// `Connection` stays mapped so the egress pump keeps flushing those bytes, and
/// the terminal FIN + socket reclaim is deferred until the bytes are delivered
/// (or the close deadline forces an abort). Teardown (`StreamAction::Close`)
/// otherwise removes the whole [`Connection`] from any state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnState {
  /// Dial requested but no socket was free (pool exhausted). `out` /
  /// `fin_pending` accumulate here; when a socket frees,
  /// [`crate::Memberlist::drain_pending_dials`] assigns one and transitions the
  /// connection to `Dialing`. The dial intent is never dropped on exhaustion.
  PendingDial,
  /// Socket assigned, the TCP three-way handshake is in flight (SynSent →
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
  /// `StreamAction::Close`. smoltcp's `close()` shuts only the transmit half, so
  /// a FinWait socket still receives.
  HalfClosed,
  /// A graceful `StreamAction::Close` arrived while the send-capable socket
  /// (Established / CloseWait) still held outbound bytes the peer has not
  /// received — either parked in `out` (partial-write backpressure) or in the tx
  /// ring awaiting ACK. The `Connection` stays mapped and the egress pump keeps
  /// flushing `out` into the tx ring; only once `out` is empty AND the tx ring is
  /// fully acknowledged does the driver emit the terminal FIN (`close()`) and
  /// detach the socket into `closing`. `close_deadline` is the backstop: a peer
  /// that never drains the tx ring is force-aborted and reclaimed at the deadline
  /// so the pool cannot wedge. This is the drain-before-close guarantee that
  /// keeps an oversized push/pull reply from being truncated by the FIN.
  Closing,
}

/// One reliable exchange's connection lifecycle and buffered state.
///
/// This is the whole per-exchange state. Tearing the exchange down is removing
/// the `Connection` from [`ReliablePlane::connections`], which drops the socket
/// handle reference, the parked `out` bytes, the deferred-FIN flag, the close
/// drain deadline, and the EOF-delivered flag together — no separate per-field
/// purges to keep in sync. The only teardown that defers that removal is a
/// graceful close with bytes still to deliver: it parks in [`ConnState::Closing`]
/// until the egress pump has flushed them, then removes the whole `Connection`.
pub(crate) struct Connection {
  /// The peer's socket address (the dial target, or the accepted remote).
  pub(crate) peer: SocketAddr,
  /// The assigned pooled TCP socket. `None` only in [`ConnState::PendingDial`],
  /// where the pool was exhausted and no socket has been assigned yet.
  pub(crate) socket: Option<SocketHandle>,
  /// The connection's lifecycle stage.
  pub(crate) state: ConnState,
  /// Outbound bytes not yet fully written to the socket's tx ring. Holds
  /// partial-write remainders (backpressure) and bytes parked while the socket
  /// is still opening or the dial is deferred. Ordered oldest-first so
  /// per-exchange byte order is preserved across ticks.
  pub(crate) out: VecDeque<Bytes>,
  /// A graceful write-half FIN (`StreamAction::Shutdown`) was requested but not
  /// yet emitted. Deferred until the socket is `Established` and `out` is fully
  /// drained and acknowledged, then emitted via `close()` exactly once (see
  /// [`ConnState::Established`]).
  pub(crate) fin_pending: bool,
  /// The peer's FIN/EOF has been delivered to the machine. Gates the
  /// exactly-once empty-EOF `handle_transport_data` call.
  pub(crate) eof_delivered: bool,
  /// Backstop deadline for the [`ConnState::Closing`] drain. `Some` only while
  /// the connection is `Closing`: it is the instant by which the buffered
  /// outbound bytes must have been delivered and the terminal FIN emitted. If the
  /// peer never drains the tx ring (permanent backpressure / vanished peer) the
  /// drain check force-aborts the socket and reclaims it at this instant so the
  /// pool cannot wedge. Folded into [`crate::Memberlist::poll`]'s returned wakeup
  /// alongside the `closing`-map deadlines so a deadline-driven caller honors it.
  pub(crate) close_deadline: Option<Instant>,
  /// The undelivered byte count (`out` bytes + the socket's tx `send_queue`) at
  /// the last drain-progress observation, meaningful only while `Closing`. The
  /// drain check re-arms `close_deadline` whenever the count shrinks, making
  /// `close_timeout` a NO-PROGRESS (idle) bound rather than a total-duration cap:
  /// a slow-but-progressing peer (reading the response over more than
  /// `close_timeout`) is never force-aborted, while a stalled / vanished peer
  /// (no progress for the full `close_timeout`) still is.
  pub(crate) close_drain_mark: usize,
}

impl Connection {
  /// A connection whose socket was assigned and is dialing the peer.
  pub(crate) fn dialing(peer: SocketAddr, socket: SocketHandle) -> Self {
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

  /// A connection whose dial is deferred because the pool was exhausted: no
  /// socket yet, parked in [`ConnState::PendingDial`]. Outbound bytes and a
  /// graceful FIN may still accumulate until a socket is assigned.
  pub(crate) fn pending_dial(peer: SocketAddr) -> Self {
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
  pub(crate) fn accepted(peer: SocketAddr, socket: SocketHandle) -> Self {
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

  /// Assign a freed socket to a connection that was waiting in `PendingDial`,
  /// transitioning it to `Dialing`. Any `out` bytes and a pending FIN parked
  /// while it waited are retained and flush once the socket is Established.
  pub(crate) fn assign_socket(&mut self, socket: SocketHandle) {
    self.socket = Some(socket);
    self.state = ConnState::Dialing;
  }

  /// Whether all outbound bytes have been written and acknowledged at the
  /// `out`-queue level (no parked remainder). A precondition — together with the
  /// socket's own `send_queue() == 0` — for emitting a deferred graceful FIN.
  pub(crate) fn out_is_empty(&self) -> bool {
    self.out.is_empty()
  }

  /// Total bytes still parked in `out` (not yet written to the socket's tx ring).
  pub(crate) fn out_bytes(&self) -> usize {
    self.out.iter().map(|b| b.len()).sum()
  }
}

/// Maps in-flight exchanges to their [`Connection`], plus the pool and listener.
///
/// `connections` is keyed by [`ExchangeId`] (the machine's correlation token);
/// each entry is the complete lifecycle of one reliable exchange. `listener`
/// holds the dedicated passive-open socket created at construction; it is `None`
/// only when the pool was configured with zero sockets (`tcp_pool_size == 0`),
/// an unusual but valid degenerate case, or transiently after an accept consumed
/// it and the pool could not yet replenish.
///
/// The fields are populated by the connection-management (dial / accept / pump)
/// paths.
pub(crate) struct ReliablePlane {
  /// Free sockets available for new exchanges.
  pub(crate) pool: Pool,
  /// Active reliable exchanges, each modelled as one [`Connection`] state
  /// machine.
  pub(crate) connections: HashMap<ExchangeId, Connection>,
  /// The dedicated passive-open socket.
  pub(crate) listener: Option<SocketHandle>,
  /// Sockets parked mid-close (our FIN sent, the peer's not yet completed), each
  /// paired with the deadline by which the reap pass force-aborts it; reclaimed
  /// to the free-list once they reach `Closed` (the peer's FIN completed the
  /// close) OR the deadline elapses.
  ///
  /// Populated by `teardown` (or `flush_closing`) when a graceful close emits the
  /// terminal FIN on a socket whose peer has not finished the close: the exchange
  /// already half-closed (its graceful FIN went out earlier while the `Connection`
  /// stayed mapped so the peer's reply still pumped); an acceptor torn down in
  /// `CloseWait` whose reply was already fully delivered; or a connection that
  /// finished draining its buffered reply in [`ConnState::Closing`] and only then
  /// FIN-ed. In each case the `Connection` is removed and the detached handle is
  /// parked here (with deadline `now + close_timeout`) so it stays reachable.
  /// Without this map the handle would be unreachable (absent from `connections`,
  /// `pool`, and `listener`) and the socket would leak, permanently shrinking the
  /// pool after enough closes whose peer lingered.
  ///
  /// The deadline bounds the close: smoltcp sets no TCP timeout by default, so a
  /// peer that vanishes mid-FIN (stuck in FinWait/LastAck) would keep the socket
  /// `is_open()` forever and the handle would never return to the pool. The reap
  /// pass force-`abort()`s any handle parked past its deadline, guaranteeing the
  /// pool (and the listener replenished from it) always recover. A `Close` whose
  /// socket is already `!is_open()` (the clean both-FIN case) bypasses this map
  /// and returns straight to the pool.
  pub(crate) closing: HashMap<SocketHandle, Instant>,
  /// Monotonic count of inbound reliable connections accepted on the listener.
  ///
  /// Incremented once per passive open handed to the machine. It is a
  /// diagnostic for the listener self-healing invariant: after the pool is
  /// momentarily exhausted (the listener socket becomes the exchange and no
  /// free socket is available to replenish it), a later free socket must be
  /// re-established as the listener so a SECOND inbound connection is still
  /// accepted. Membership/event observation cannot witness that invariant
  /// because gossip can converge a peer with no TCP accept at all; this counter
  /// measures the accept directly.
  pub(crate) accepted_inbound: u64,
}

impl ReliablePlane {
  /// Create an empty reliable plane: no pool entries, no connections, no
  /// listener, no closing sockets. Sockets are added by `Memberlist::new`
  /// immediately after.
  pub(crate) fn new() -> Self {
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
  /// mapped, awaiting the peer's reply and/or FIN. Surfaced by
  /// [`crate::Memberlist::half_closed_count`].
  pub(crate) fn half_closed_count(&self) -> usize {
    self
      .connections
      .values()
      .filter(|c| c.state == ConnState::HalfClosed)
      .count()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::net::{IpAddr, Ipv4Addr};
  use smoltcp::{iface::SocketSet, socket::tcp};

  fn peer(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
  }

  /// A throwaway `SocketSet` is enough to mint real `SocketHandle`s without a
  /// device or interface; the unit tests below never drive the socket, they only
  /// exercise the `Connection` state machine that wraps the handle.
  fn fresh_handle(set: &mut SocketSet<'static>) -> SocketHandle {
    let rx = tcp::SocketBuffer::new(std::vec![0u8; 64]);
    let tx = tcp::SocketBuffer::new(std::vec![0u8; 64]);
    set.add(tcp::Socket::new(rx, tx))
  }

  #[test]
  fn dialing_connection_has_socket_and_dialing_state() {
    let mut set = SocketSet::new(std::vec::Vec::new());
    let h = fresh_handle(&mut set);
    let c = Connection::dialing(peer(7946), h);
    assert_eq!(c.state, ConnState::Dialing);
    assert_eq!(c.socket, Some(h));
    assert!(c.out_is_empty());
    assert!(!c.fin_pending);
    assert!(!c.eof_delivered);
  }

  #[test]
  fn accepted_connection_starts_established() {
    let mut set = SocketSet::new(std::vec::Vec::new());
    let h = fresh_handle(&mut set);
    let c = Connection::accepted(peer(7946), h);
    // The listener completes its handshake before accept, so an accepted
    // connection is immediately Established (write half open).
    assert_eq!(c.state, ConnState::Established);
    assert_eq!(c.socket, Some(h));
  }

  #[test]
  fn pending_dial_has_no_socket() {
    let c = Connection::pending_dial(peer(7946));
    assert_eq!(c.state, ConnState::PendingDial);
    assert_eq!(c.socket, None);
  }

  #[test]
  fn pending_dial_accumulates_out_and_fin_then_transitions_on_socket_assignment() {
    // The dial, the request bytes, and the graceful FIN can all arrive in the
    // SAME tick before any socket exists (a pool-exhausted push/pull). They must
    // survive on the PendingDial connection until a socket is assigned.
    let mut c = Connection::pending_dial(peer(7946));
    c.out.push_back(Bytes::from_static(b"request-part-1"));
    c.out.push_back(Bytes::from_static(b"request-part-2"));
    c.fin_pending = true;
    assert_eq!(c.state, ConnState::PendingDial);
    assert!(c.socket.is_none());

    // A socket frees: assign it and transition PendingDial -> Dialing. The parked
    // bytes (in order) and the deferred FIN must be retained for the egress pump
    // and the deferred-FIN flush to consume once the socket is Established.
    let mut set = SocketSet::new(std::vec::Vec::new());
    let h = fresh_handle(&mut set);
    c.assign_socket(h);
    assert_eq!(c.state, ConnState::Dialing);
    assert_eq!(c.socket, Some(h));
    assert!(c.fin_pending, "deferred FIN must survive socket assignment");
    assert_eq!(
      c.out.len(),
      2,
      "parked bytes must survive socket assignment"
    );
    assert_eq!(c.out[0], Bytes::from_static(b"request-part-1"));
    assert_eq!(c.out[1], Bytes::from_static(b"request-part-2"));
  }

  #[test]
  fn out_is_empty_tracks_parked_bytes() {
    let c0 = Connection::pending_dial(peer(7946));
    assert!(c0.out_is_empty());
    let mut c1 = Connection::pending_dial(peer(7946));
    c1.out.push_back(Bytes::from_static(b"x"));
    assert!(!c1.out_is_empty());
    c1.out.pop_front();
    assert!(c1.out_is_empty());
  }

  #[test]
  fn eof_delivered_flag_models_deliver_once() {
    // The inbound pump delivers the peer EOF exactly once by flipping this flag
    // on first observation; a second `Finished` must find it already set.
    let mut set = SocketSet::new(std::vec::Vec::new());
    let h = fresh_handle(&mut set);
    let mut c = Connection::accepted(peer(7946), h);
    assert!(!c.eof_delivered);
    // First observation: not yet delivered, so deliver and latch.
    let first = !c.eof_delivered;
    c.eof_delivered = true;
    assert!(first, "first EOF observation must deliver");
    // Second observation: already latched, so suppress.
    let second = !c.eof_delivered;
    assert!(!second, "second EOF observation must be suppressed");
  }

  #[test]
  fn established_to_half_closed_on_fin_emission() {
    // Modelled transition: an Established connection whose deferred FIN is
    // emitted clears `fin_pending` and moves to HalfClosed (still mapped so the
    // peer's reply keeps pumping). Emitting again is impossible — HalfClosed is
    // no longer selected by the deferred-FIN flush.
    let mut set = SocketSet::new(std::vec::Vec::new());
    let h = fresh_handle(&mut set);
    let mut c = Connection::accepted(peer(7946), h);
    c.fin_pending = true;
    assert_eq!(c.state, ConnState::Established);

    // The flush emits the FIN and records the transition.
    c.fin_pending = false;
    c.state = ConnState::HalfClosed;
    assert_eq!(c.state, ConnState::HalfClosed);
    assert!(!c.fin_pending, "an emitted FIN is no longer pending");
  }

  #[test]
  fn graceful_close_with_unsent_bytes_enters_closing_with_a_deadline() {
    // Modelled transition: a send-capable connection that still holds outbound
    // bytes when its graceful `Close` arrives enters `Closing` (KEPT mapped) with
    // a drain deadline, so the egress pump can finish flushing `out` before the
    // terminal FIN. A still-pending Shutdown FIN is subsumed by the drain's own
    // terminal FIN, so `fin_pending` is cleared.
    let mut set = SocketSet::new(std::vec::Vec::new());
    let h = fresh_handle(&mut set);
    let mut c = Connection::accepted(peer(7946), h);
    c.out
      .push_back(Bytes::from_static(b"oversized-reply-remainder"));
    c.fin_pending = true;
    assert_eq!(c.state, ConnState::Established);
    assert!(c.close_deadline.is_none());

    // teardown records the drain transition.
    let deadline = Instant::from_origin(core::time::Duration::from_secs(99));
    c.state = ConnState::Closing;
    c.close_deadline = Some(deadline);
    c.fin_pending = false;

    assert_eq!(c.state, ConnState::Closing);
    assert_eq!(
      c.close_deadline,
      Some(deadline),
      "the drain deadline is the abort backstop and the poll() wakeup source"
    );
    assert!(
      !c.out_is_empty(),
      "the buffered remainder is retained for the egress pump to flush"
    );
    assert!(
      !c.fin_pending,
      "the Closing drain owns the terminal FIN, so a pending Shutdown is cleared"
    );
  }
}
