use super::*;

fn peer(port: u16) -> SocketAddr {
  use core::net::{IpAddr, Ipv4Addr};
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
}

/// The unit tests below never drive a socket; they exercise only the
/// `Connection` state machine that wraps the opaque handle. A `u32` is a valid
/// `Copy + Eq` stand-in for a driver's `StreamIo::Conn`.
type Conn = u32;

#[test]
fn take_where_skips_not_ready_slots_without_reordering() {
  // Model an async-teardown driver: only some pooled slots are reset (ready).
  // `take_where` must return a ready slot and leave the not-ready ones in the
  // pool, in order, so they become takeable the instant they reset.
  let mut pool = Pool::<Conn>::new();
  pool.push(10); // ready
  pool.push(11); // NOT ready (still resetting)
  pool.push(12); // ready

  // Newest-first scan: 12 is ready, take it.
  let ready = |c: &Conn| *c != 11;
  assert_eq!(pool.take_where(ready), Some(12));
  // 10 is the next ready one (11 is skipped, not removed).
  assert_eq!(pool.take_where(ready), Some(10));
  // Only the not-ready slot remains; `take_where` now finds nothing ready even
  // though the pool is non-empty.
  assert_eq!(pool.free_len(), 1);
  assert!(!pool.is_empty());
  assert_eq!(pool.take_where(ready), None);
  // Once it resets, it becomes takeable.
  assert_eq!(pool.take_where(|_| true), Some(11));
  assert!(pool.is_empty());
}

#[test]
fn any_where_reports_only_ready_slots() {
  let mut pool = Pool::<Conn>::new();
  assert!(!pool.any_where(|_| true), "empty pool has no ready slot");
  pool.push(1);
  pool.push(2);
  // No slot is ready (all still resetting): the pool is non-empty but
  // `any_where` is false — the end-of-tick invariant must not flag this.
  assert!(!pool.any_where(|_| false));
  // At least one ready slot flips it true.
  assert!(pool.any_where(|c| *c == 2));
}

#[test]
fn dialing_connection_has_socket_and_dialing_state() {
  let c = Connection::<Conn>::dialing(peer(7946), 1);
  assert_eq!(c.state, ConnState::Dialing);
  assert_eq!(c.socket, Some(1));
  assert!(c.out_is_empty());
  assert!(!c.fin_pending);
  assert!(!c.eof_delivered);
}

#[test]
fn accepted_connection_starts_established() {
  let c = Connection::<Conn>::accepted(peer(7946), 2);
  // The listener completes its handshake before accept, so an accepted
  // connection is immediately Established (write half open).
  assert_eq!(c.state, ConnState::Established);
  assert_eq!(c.socket, Some(2));
}

#[test]
fn pending_dial_has_no_socket() {
  let c = Connection::<Conn>::pending_dial(peer(7946));
  assert_eq!(c.state, ConnState::PendingDial);
  assert_eq!(c.socket, None);
}

#[test]
fn pending_dial_accumulates_out_and_fin_then_transitions_on_socket_assignment() {
  // The dial, the request bytes, and the graceful FIN can all arrive in the
  // SAME tick before any slot exists (a pool-exhausted push/pull). They must
  // survive on the PendingDial connection until a slot is assigned.
  let mut c = Connection::<Conn>::pending_dial(peer(7946));
  c.out.push_back(Bytes::from_static(b"request-part-1"));
  c.out.push_back(Bytes::from_static(b"request-part-2"));
  c.fin_pending = true;
  assert_eq!(c.state, ConnState::PendingDial);
  assert!(c.socket.is_none());

  // A slot frees: assign it and transition PendingDial -> Dialing. The parked
  // bytes (in order) and the deferred FIN must be retained for the egress pump
  // and the deferred-FIN flush to consume once the socket is Established.
  c.assign_socket(9);
  assert_eq!(c.state, ConnState::Dialing);
  assert_eq!(c.socket, Some(9));
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
  let c0 = Connection::<Conn>::pending_dial(peer(7946));
  assert!(c0.out_is_empty());
  let mut c1 = Connection::<Conn>::pending_dial(peer(7946));
  c1.out.push_back(Bytes::from_static(b"x"));
  assert!(!c1.out_is_empty());
  c1.out.pop_front();
  assert!(c1.out_is_empty());
}

#[test]
fn eof_delivered_flag_models_deliver_once() {
  // The inbound pump delivers the peer EOF exactly once by flipping this flag
  // on first observation; a second `Finished` must find it already set.
  let mut c = Connection::<Conn>::accepted(peer(7946), 3);
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
  let mut c = Connection::<Conn>::accepted(peer(7946), 4);
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
  let mut c = Connection::<Conn>::accepted(peer(7946), 5);
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
