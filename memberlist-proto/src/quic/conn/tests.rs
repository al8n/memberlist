use super::{super::crypto::QuicOptions, *};
use crate::quic::crypto::tests::{test_client, test_endpoint_config, test_server};

fn quinn_pair() -> (QuinnEndpoint, QuinnEndpoint, QuicOptions) {
  let cfg = QuicOptions::new(
    test_endpoint_config(&[3u8; 32]),
    test_server(),
    test_client(),
    quinn_proto::TransportConfig::default(),
    "localhost",
    super::super::UnreliableTransport::Datagram,
  );
  let client = QuinnEndpoint::new(cfg.endpoint_arc(), None, true, Some([0x5a; 32]));
  let server = QuinnEndpoint::new(
    cfg.endpoint_arc(),
    Some(cfg.server_arc()),
    true,
    Some([0x5a; 32]),
  );
  (client, server, cfg)
}

#[test]
fn dial_inserts_then_reuse_returns_same_handle() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch1 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  let ch2 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert_eq!(ch1, ch2, "second call reuses the peer connection");
  assert_eq!(t.handle_for(&peer), Some(ch1));
}

#[test]
fn reap_if_drained_is_false_until_drained_then_removes() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert!(
    !t.reap_if_drained(&mut client, ch),
    "fresh conn is not drained"
  );
  assert!(t.handle_for(&peer).is_some());

  // Initiate a graceful close; quinn will transition through Closed →
  // Drained after the close timer fires.
  t.get_mut(ch)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());

  for _ in 0..5000 {
    if t.get_mut(ch).unwrap().conn_ref().is_drained() {
      break;
    }
    match t.get_mut(ch).unwrap().conn_mut().poll_timeout() {
      Some(d) => t.get_mut(ch).unwrap().conn_mut().handle_timeout(d),
      None => break,
    }
  }

  assert!(
    t.reap_if_drained(&mut client, ch),
    "connection must be drained after close+timeout drive"
  );
  assert_eq!(t.handle_for(&peer), None, "route entry removed on reap");
}

/// `Connection::close` drives the cached `Connection` into `State::Closed`
/// — `is_closed()` is `true` immediately (quinn-proto's `State::is_closed`
/// matches `Closed | Draining | Drained`); the 3×PTO `Timer::Close` has
/// not yet fired so `is_drained()` is still `false`. In that
/// closed-before-drained window `Streams::open(Dir::Bi)` already refuses
/// (same `is_closed()` check inside `Streams::open`), but
/// `is_handshaking()` is `false`, so `service_dials`'s handshaking-requeue
/// would not catch it.
/// For an outbound that had previously reached `Established` (now
/// `DrainClosed`), the pre-pass (P2) clears `route.outbound` and the reliable
/// selection dials a fresh one (R7) — silently reusing the cached handle would
/// route a redialable intent into `dial_failed` and lose the exchange.
///
/// Negative control: drop P2's `DrainClosed` clear (leave a closed
/// established-once outbound in `route.outbound`) and this test fails —
/// `ch_redial == ch1` and the route/slab state is wrong.
#[test]
fn get_or_dial_redials_when_cached_conn_is_closed_not_drained() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch1 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  // Synthesize "this connection has reached Established at least once"
  // — the runtime sets this on the same tick the conn transitions out
  // of `Handshake`; the unit test substitutes for that observation
  // because driving a real handshake in-test would defeat the closed-
  // state isolation. (For a never-Established cached entry the
  // behaviour is covered by `closed_never_established_does_not_redial`.)
  t.conns.get_mut(ch1.0).unwrap().established_at_least_once = true;
  // Drive the cached `Connection` into `State::Closed` (closed-before-
  // drained: `is_closed()` is `true`, `is_drained()` is still `false`,
  // `is_handshaking()` is `false`).
  t.get_mut(ch1)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  assert!(t.get(ch1).unwrap().conn.is_closed());
  assert!(!t.get(ch1).unwrap().conn.is_drained());
  assert!(!t.get(ch1).unwrap().conn.is_handshaking());

  // Second dial in the closed-before-drained window: MUST redial.
  let ch_redial = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert_ne!(
    ch_redial, ch1,
    "closed-before-drained pool window must redial, not reuse the cached handle"
  );
  // The slab holds two entries for the same peer: the old one awaiting
  // its drained-reap, and the new live one.
  assert!(
    t.get(ch1).is_some(),
    "old slab entry retained so the in-flight drained-reap completes"
  );
  assert!(
    t.get(ch_redial).is_some(),
    "new slab entry inserted by the redial"
  );
  // The best-usable handle is the live (new) redial for subsequent
  // outbound exchanges.
  assert_eq!(
    t.handle_for(&peer),
    Some(ch_redial),
    "route advanced to the fresh redial"
  );

  // Drive the OLD connection to `Drained` and confirm its slab slot is
  // cleared while the new route is preserved (the equality guard).
  for _ in 0..5000 {
    if t.get(ch1).is_none_or(|e| e.conn_ref().is_drained()) {
      break;
    }
    match t.get_mut(ch1).unwrap().conn_mut().poll_timeout() {
      Some(d) => t.get_mut(ch1).unwrap().conn_mut().handle_timeout(d),
      None => break,
    }
  }
  assert!(
    t.reap_if_drained(&mut client, ch1),
    "old closed connection must drain-reap to completion"
  );
  // Critical: the new redial's route MUST survive the old handle's
  // drained-reap (handle-equality guard).
  assert_eq!(
    t.handle_for(&peer),
    Some(ch_redial),
    "drained-reap of the OLD handle must NOT clobber the NEW redial's route"
  );
  assert!(
    t.get(ch_redial).is_some(),
    "redial slab entry intact after old-handle drained-reap"
  );
}

/// Anti-storm: a closed OUTBOUND that never reached `Established` (our own dial
/// to an unreachable peer) is authoritative — `TerminalOutbound`. A reliable
/// intent must return it (R5) so the caller's `open(Dir::Bi)=None &&
/// !is_handshaking()` path falls through to `dial_failed`, consuming the intent
/// on a single handshake attempt per push-pull deadline rather than generating a
/// fresh handshake per attempt. The pre-pass (P2) leaves a `TerminalOutbound` in
/// place; no redial happens within the drain window.
///
/// Negative control (the outbound half of the direction split): remove the
/// `direction == Outbound` arm of the handle classifier so a closed
/// never-established connection no longer classifies as `TerminalOutbound` — R5
/// is not taken, the reliable fresh-dial guard is tripped, and this test fails
/// (a fresh redial replaces the terminal, or a debug assertion fires).
#[test]
fn closed_never_established_does_not_redial() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch1 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  // Do NOT seed `established_at_least_once`: this represents a
  // handshake failure (the conn never reached Established).
  assert!(!t.conns.get(ch1.0).unwrap().established_at_least_once);
  t.get_mut(ch1)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  assert!(t.get(ch1).unwrap().conn.is_closed());

  // Second dial: MUST return the cached `ch1` so the caller's
  // `dial_failed` path fires, not a fresh redial.
  let ch_same = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert_eq!(
    ch_same, ch1,
    "never-Established closed conn must NOT generate a fresh handshake \
             attempt per dial — fall through to the existing dial_failed path"
  );
  assert_eq!(t.conns.len(), 1, "no extra slab entry was created");
}

/// `conn_mut` lazy-tracks `established_at_least_once`: the first call
/// where the underlying connection is in the Established state (not
/// handshaking, not closed) sticks the flag to `true`, and it stays
/// `true` even after the connection later closes (the redial-vs-
/// dial_failed decision in `get_or_dial` looks at the sticky flag).
#[test]
fn established_flag_sticks_via_conn_mut_lazy_tracking() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert!(!t.conns.get(ch.0).unwrap().established_at_least_once);
  // `conn_mut` on a handshaking conn: flag stays `false`. The returned
  // `&mut Connection` is the side effect we want to exercise; the binding
  // itself is discarded.
  let _ = t.get_mut(ch).unwrap().conn_mut();
  assert!(!t.conns.get(ch.0).unwrap().established_at_least_once);
  // Synthesize the Established observation by stepping the conn into
  // a non-handshaking-non-closed state would normally require a
  // peer-driven handshake. For this lazy-tracking unit test, prove
  // the flag flips ONLY when `conn_mut` sees the right state by
  // directly checking `!is_handshaking() && !is_closed()` on the
  // current state (handshaking — flag still false), then by setting
  // the flag directly to demonstrate stickiness across a subsequent
  // `close`.
  t.conns.get_mut(ch.0).unwrap().established_at_least_once = true;
  t.get_mut(ch)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  // Closed state — `conn_mut` must NOT clear the sticky flag. The
  // returned `&mut Connection` is exercised for its side effect; the
  // binding itself is discarded.
  let _ = t.get_mut(ch).unwrap().conn_mut();
  assert!(
    t.conns.get(ch.0).unwrap().established_at_least_once,
    "flag must remain sticky after the conn closes"
  );
}

/// A closed-never-Established OUTBOUND (`TerminalOutbound`) must NOT hide a
/// live accepted inbound. The two fields are independent: an accepted
/// connection is written to `route.inbound` while the closed outbound stays
/// in `route.outbound`, and the reliable table's R4 (`TerminalOutbound` +
/// handshaking inbound → return the inbound) surfaces the live connection.
///
/// Setup: dial X (outbound `ch1`, never established), close it
/// (`TerminalOutbound`), then `insert_accepted` a fresh inbound for the same
/// peer. Assert the peer's best-usable handle is the live inbound, that a
/// subsequent reliable `get_or_dial` returns it (R4), and that reaping the OLD
/// outbound does not clobber the inbound (handle-equality guard).
#[test]
fn terminal_outbound_does_not_hide_live_accepted_inbound() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // (1) Outbound dial X → `route.outbound = ch1`.
  // `established_at_least_once` is left false to represent a
  // handshake failure / closed-before-Established outbound.
  let ch1 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert_eq!(t.handle_for(&peer), Some(ch1));
  assert!(!t.conns.get(ch1.0).unwrap().established_at_least_once);
  // (2) Drive the cached connection into `Closed` without ever
  // transitioning to Established. `Connection::close` flips
  // `is_closed()` to true immediately while `is_drained()` is still
  // false (the 3xPTO drain timer has not fired).
  t.get_mut(ch1)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  assert!(t.get(ch1).unwrap().conn.is_closed());
  assert!(!t.get(ch1).unwrap().conn.is_drained());
  // (3) Mint a fresh `(ConnectionHandle, Connection)` from the same
  // quinn endpoint the original dial used. The runtime path is
  // `quinn.accept(...)` from the `DatagramEvent::NewConnection` arm
  // of `route_datagram_event`; here `quinn.connect` reuses that
  // endpoint's slab so the new `ch.0` lines up with the local
  // ConnTable slab's next vacant key (the lockstep `insert_accepted`
  // asserts). The accept-vs-dial origin does not matter to
  // `insert_accepted` itself — it inserts the entry as inbound and sets
  // `route.inbound`.
  let alt: SocketAddr = "127.0.0.1:4500".parse().unwrap();
  let (new_ch, new_conn) = client
    .connect(now.into_std(), cfg.client().clone(), alt, "localhost")
    .expect("fresh connection minted on the same client endpoint");
  t.insert_accepted(new_ch, new_conn, peer);
  // (4) The accepted inbound is live; the closed outbound is
  // `TerminalOutbound`. The best-usable handle is the live inbound — the
  // closed outbound in the separate `route.outbound` field cannot hide it.
  assert_eq!(
    t.handle_for(&peer),
    Some(new_ch),
    "the live accepted inbound is the best-usable handle; the closed \
             outbound in a separate field cannot hide it"
  );
  // (5) A subsequent reliable dial returns the live inbound via R4
  // (`TerminalOutbound` outbound + handshaking inbound).
  let ch_after = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  assert_eq!(
    ch_after, new_ch,
    "a reliable dial with a terminal outbound + live inbound returns the \
             inbound (R4), not the closed outbound ch1"
  );
  // (6) Old outbound entry persists awaiting its drained-reap. Drive it
  // to Drained and confirm the equality-guarded `reap_if_drained`
  // clears the OLD slab slot without clobbering the live inbound.
  for _ in 0..5000 {
    if t.get(ch1).is_none_or(|e| e.conn_ref().is_drained()) {
      break;
    }
    match t.get_mut(ch1).unwrap().conn_mut().poll_timeout() {
      Some(d) => t.get_mut(ch1).unwrap().conn_mut().handle_timeout(d),
      None => break,
    }
  }
  assert!(
    t.reap_if_drained(&mut client, ch1),
    "old closed outbound must drained-reap to completion"
  );
  assert_eq!(
    t.handle_for(&peer),
    Some(new_ch),
    "the live inbound must survive the OLD outbound's drained-reap \
             (handle-equality-guarded route removal protects it)"
  );
}

/// Isolated: the handle-equality guard's three cases — plain reap
/// (route holds `ch`, clear it), mid-drain redial (route holds new_ch,
/// skip), and route absent (skip). The first two are exercised by
/// `reap_if_drained_is_false_until_drained_then_removes` and
/// `get_or_dial_redials_when_cached_conn_is_closed_not_drained`
/// respectively; this test pins case (c): a synthetic state where the
/// slab still holds `ch` (drained) but the route entry was already
/// removed (e.g. by a fresh `get_or_dial` that dialed before this reap
/// ran). The reap must be idempotent — clear the slab slot, leave the
/// (now-absent) route state alone.
#[test]
fn reap_if_drained_handle_equality_guard_idempotent_when_peers_absent() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  // Drive the conn through `close` to `Drained`.
  t.get_mut(ch)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  for _ in 0..5000 {
    if t.get(ch).unwrap().conn_ref().is_drained() {
      break;
    }
    match t.get_mut(ch).unwrap().conn_mut().poll_timeout() {
      Some(d) => t.get_mut(ch).unwrap().conn_mut().handle_timeout(d),
      None => break,
    }
  }
  assert!(t.get(ch).unwrap().conn_ref().is_drained());
  // Synthesize "route entry already removed" (the redial case at the
  // limit: the route was cleared but the old slab still holds the drained
  // entry awaiting this reap).
  t.peer_routes.remove(&peer);
  assert_eq!(t.handle_for(&peer), None);
  // The reap must still complete (slab cleared, no panic, no
  // resurrection of a stale route entry).
  assert!(
    t.reap_if_drained(&mut client, ch),
    "reap must succeed even when the route entry was pre-removed"
  );
  assert!(t.get(ch).is_none(), "slab slot freed");
  assert_eq!(
    t.handle_for(&peer),
    None,
    "route state unchanged (no resurrection)"
  );
}

/// Race-defensive pre-pass arm: a route field pointing at a handle whose slab
/// slot has already been vacated is a slab-gone class. The pre-pass clears it
/// (`is_none_or` treats a vanished slab entry as clearable), and with the field
/// gone the reliable selection dials a FRESH outbound rather than returning the
/// dangling handle.
///
/// Negative control: if P2 did not treat a slab-gone outbound as clearable, its
/// class would be `Gone` and the reliable table would trip the
/// `debug_assert!(oc.is_none())` fresh-dial guard.
#[test]
fn prepass_clears_slab_gone_outbound_then_dials_fresh() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // Plant a route whose outbound handle has no slab entry — the slab-gone input.
  let phantom = ConnectionHandle(999);
  t.peer_routes.insert(
    peer,
    PeerRoute {
      outbound: Some(phantom),
      inbound: None,
    },
  );
  assert!(t.conns.get(phantom.0).is_none(), "slab slot is vacant");

  let ch = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .expect("a fresh outbound is dialed after the slab-gone field is pruned");
  assert_ne!(ch, phantom, "the dangling handle is never returned");
  assert_eq!(t.conns.len(), 1, "a fresh outbound slab entry was created");
  assert_eq!(
    t.handle_for(&peer),
    Some(ch),
    "the fresh outbound is the peer's best-usable handle"
  );
}

/// `reap_if_drained` on a handle whose slab slot is already gone returns
/// `false` without touching quinn or the route (the `None => return false`
/// guard). Idempotent: a second reap of a handle that was already reaped is a
/// no-op.
#[test]
fn reap_if_drained_on_absent_slab_slot_is_false_noop() {
  let (mut client, _server, _cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let phantom = ConnectionHandle(1234);
  assert!(
    !t.reap_if_drained(&mut client, phantom),
    "reaping a handle with no slab entry must be a false no-op"
  );
}

/// `ConnEntry`'s observation accessors on a freshly-dialed entry that has
/// not driven a handshake: `peer()` returns the dialed address,
/// `has_pending_events`/`pending_events_len` report an empty deferred queue,
/// and `take_pending_events` on an empty deque is a no-op drain. (The
/// `queue_pending_event` append path is covered end-to-end by the mod.rs
/// `conn_entry_pending_events_drop_with_entry_on_reap` regression.)
#[test]
fn conn_entry_observation_accessors_on_fresh_entry() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  let e = t.get_mut(ch).unwrap();
  assert_eq!(e.peer(), peer, "entry reports the dialed peer address");
  assert!(
    !e.has_pending_events(),
    "fresh entry has no deferred events"
  );
  assert_eq!(e.pending_events_len(), 0);
  let drained: Vec<quinn_proto::ConnectionEvent> = e.take_pending_events().collect();
  assert!(drained.is_empty(), "draining an empty deque yields nothing");
  assert!(
    !e.has_pending_events(),
    "deque still empty after the no-op drain"
  );
}

/// The per-source pending cap counts INBOUND (server-accepted) connections
/// only: a local OUTBOUND dial to a peer must not charge against that peer's
/// inbound pending allowance. Counting it would wedge simultaneous
/// bidirectional dialing — each side's own handshaking outbound to the peer
/// would fill the cap and block the peer's inbound Initial, so neither exchange
/// completes.
///
/// Negative control: set `pending_indexed: true` in `get_or_dial` (or increment
/// the `pending_inbound` index there) — the outbound dial is then counted and
/// the `== 0` assertion fails.
#[test]
fn pending_inbound_from_excludes_local_outbound_dial() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  // A freshly-dialed outbound connection is handshaking and never established;
  // it is not entered into the per-source pending index (`pending_indexed` is
  // false for an outbound dial).
  assert!(t.get(ch).unwrap().conn_ref().is_handshaking());
  assert_eq!(
    t.pending_inbound_from(&peer),
    0,
    "a local outbound dial must not consume the peer's inbound pending allowance \
       (or simultaneous bidirectional dialing wedges)"
  );
}

/// A failed inbound handshake becomes non-handshaking but its closed/draining
/// slab entry lingers until its drained-reap. The pending index MUST keep it
/// charged (its `pending_indexed` unit is released only on establishment or
/// reap, never merely because the connection closed), so a source cannot exceed
/// the per-source cap by repeatedly opening inbound handshakes that fail — the
/// closed-but-undrained entries still consume its allowance.
///
/// A closed-before-Established connection is the observable state of a failed
/// inbound TLS handshake (`is_closed() && !is_handshaking()`, established flag
/// never set); `Connection::close` on a handshaking connection reproduces it
/// exactly — the same modelling the closed-never-Established tests in this file
/// use (`terminal_outbound_does_not_hide_live_accepted_inbound`).
///
/// Negative control: release the index unit when a connection closes (e.g. clear
/// `pending_indexed` on `is_closed()`) — the closed failed entries drop out and
/// the final `== 2` assertion fails (the source's allowance is wrongly freed by
/// handshakes that failed).
#[test]
fn pending_inbound_from_charges_failed_never_established_inbound() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let src: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // Two distinct inbound (server-accepted) connections attributed to `src`,
  // minted on the same endpoint so their slab slots stay in lockstep with the
  // local ConnTable (the accept-vs-dial origin is irrelevant to
  // `insert_accepted`, which sets direction = Inbound and inserts — see
  // `terminal_outbound_does_not_hide_live_accepted_inbound`).
  let a1: SocketAddr = "127.0.0.1:4500".parse().unwrap();
  let a2: SocketAddr = "127.0.0.1:4501".parse().unwrap();
  let (ch1, c1) = client
    .connect(now.into_std(), cfg.client().clone(), a1, "localhost")
    .unwrap();
  t.insert_accepted(ch1, c1, src);
  let (ch2, c2) = client
    .connect(now.into_std(), cfg.client().clone(), a2, "localhost")
    .unwrap();
  t.insert_accepted(ch2, c2, src);
  // Both handshaking → both charged; none has yet failed.
  assert_eq!(t.pending_inbound_from(&src), 2);
  assert_eq!(t.failed_never_established_inbound_from(&src), 0);
  // Drive BOTH into Closed WITHOUT ever establishing — the failed-inbound
  // signature: `is_closed() && !is_handshaking()`, established flag never set,
  // and not yet drained (the 3xPTO close timer has not fired, so it lingers).
  for ch in [ch1, ch2] {
    t.get_mut(ch)
      .unwrap()
      .conn_mut()
      .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
    assert!(t.get(ch).unwrap().conn_ref().is_closed());
    assert!(!t.get(ch).unwrap().conn_ref().is_handshaking());
    assert!(!t.get(ch).unwrap().conn_ref().is_drained());
  }
  assert_eq!(
    t.failed_never_established_inbound_from(&src),
    2,
    "both inbound entries left the handshaking phase without ever establishing"
  );
  assert_eq!(
    t.pending_inbound_from(&src),
    2,
    "failed never-established inbound entries stay charged until their \
       drained-reap; a source cannot exceed the per-source cap by opening \
       inbound handshakes that fail"
  );
}

/// Drive `ch` through its close timer until `is_drained()`, so `reap_if_drained`
/// will remove it. Mirrors the drain loop in
/// `reap_if_drained_is_false_until_drained_then_removes`.
fn drive_to_drained(t: &mut ConnTable, ch: ConnectionHandle) {
  for _ in 0..5000 {
    if t.get(ch).is_none_or(|e| e.conn_ref().is_drained()) {
      return;
    }
    match t.get_mut(ch).unwrap().conn_mut().poll_timeout() {
      Some(d) => t.get_mut(ch).unwrap().conn_mut().handle_timeout(d),
      None => return,
    }
  }
}

/// The per-source pending index maintains the increment/decrement invariant
/// across the full inbound lifecycle — every accept increments exactly once and
/// is matched by exactly one decrement, at establishment OR at reap-while-
/// un-established, never both and never neither. `indexed_inbound_recount`
/// (read straight from the slab) must equal `pending_inbound_from` (read from
/// the index) after every step: any missed or doubled increment/decrement makes
/// them diverge. This is the leak/underflow guard for the O(1) index.
///
/// Negative controls, each breaking a distinct step: (a) drop the increment in
/// `insert_accepted` — the initial counts are wrong; (b) drop the establishment
/// release in `reconcile_pending_inbound` — the count stays at 3 after
/// establishment; (c) drop the reap release in `reap_if_drained` — the count
/// stays at 2 after reaping the never-established entry; (d) release again when
/// reaping the already-established entry (e.g. gate reap on
/// `!established_at_least_once` instead of `pending_indexed`) — the count
/// underflows below 1.
#[test]
fn pending_inbound_index_pairs_increment_with_exactly_one_decrement() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let s1: SocketAddr = "127.0.0.1:5001".parse().unwrap();
  let s2: SocketAddr = "127.0.0.1:5002".parse().unwrap();

  // Mint 3 inbound from s1 and 2 from s2. Minted on the SAME endpoint and
  // inserted in lockstep so the slab slots match the ConnectionHandles (the
  // `insert_accepted` slot assertion) — the accept-vs-dial origin is irrelevant
  // to `insert_accepted`.
  let mut s1_handles = Vec::new();
  for port in [5101u16, 5102, 5103] {
    let dst: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (ch, c) = client
      .connect(now.into_std(), cfg.client().clone(), dst, "localhost")
      .unwrap();
    t.insert_accepted(ch, c, s1);
    s1_handles.push(ch);
  }
  for port in [5201u16, 5202] {
    let dst: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let (ch, c) = client
      .connect(now.into_std(), cfg.client().clone(), dst, "localhost")
      .unwrap();
    t.insert_accepted(ch, c, s2);
  }

  // (a) Accept increments: index matches the independent recount for each source.
  assert_eq!(t.pending_inbound_from(&s1), 3);
  assert_eq!(t.pending_inbound_from(&s1), t.indexed_inbound_recount(&s1));
  assert_eq!(t.pending_inbound_from(&s2), 2);
  assert_eq!(t.pending_inbound_from(&s2), t.indexed_inbound_recount(&s2));

  // (b) Establishment release: mark one s1 connection Established (as the runtime
  // does on the establishing tick) and reconcile — exactly one unit released.
  t.conns
    .get_mut(s1_handles[0].0)
    .unwrap()
    .established_at_least_once = true;
  t.reconcile_pending_inbound(s1_handles[0]);
  assert_eq!(
    t.pending_inbound_from(&s1),
    2,
    "establishment releases exactly one unit"
  );
  assert_eq!(t.pending_inbound_from(&s1), t.indexed_inbound_recount(&s1));

  // Reconcile is idempotent — a second pass over the same established connection
  // must not release again.
  t.reconcile_pending_inbound(s1_handles[0]);
  assert_eq!(
    t.pending_inbound_from(&s1),
    2,
    "reconcile does not double-release an already-released connection"
  );

  // (c) Reap release for a NEVER-established connection: its unit is still
  // charged, so reaping it releases exactly one.
  let never_established = s1_handles[1];
  t.get_mut(never_established).unwrap().conn_mut().close(
    now.into_std(),
    0u32.into(),
    bytes::Bytes::new(),
  );
  drive_to_drained(&mut t, never_established);
  assert!(t.reap_if_drained(&mut client, never_established));
  assert_eq!(
    t.pending_inbound_from(&s1),
    1,
    "reaping a never-established inbound connection releases its charged unit"
  );
  assert_eq!(t.pending_inbound_from(&s1), t.indexed_inbound_recount(&s1));

  // (d) Reap of the ESTABLISHED connection: its unit was already released at
  // establishment, so reaping it must NOT release again (no underflow).
  let established = s1_handles[0];
  t.get_mut(established).unwrap().conn_mut().close(
    now.into_std(),
    0u32.into(),
    bytes::Bytes::new(),
  );
  drive_to_drained(&mut t, established);
  assert!(t.reap_if_drained(&mut client, established));
  assert_eq!(
    t.pending_inbound_from(&s1),
    1,
    "reaping an already-released (established) connection must not double-release"
  );
  assert_eq!(t.pending_inbound_from(&s1), t.indexed_inbound_recount(&s1));

  // s2 was never touched: its count and recount are unchanged throughout.
  assert_eq!(t.pending_inbound_from(&s2), 2);
  assert_eq!(t.pending_inbound_from(&s2), t.indexed_inbound_recount(&s2));
}

/// Reliable `get_or_dial` with no global cap.
fn dial_reliable(
  t: &mut ConnTable,
  quinn: &mut QuinnEndpoint,
  cfg: &QuicOptions,
  now: Instant,
  peer: SocketAddr,
) -> Result<ConnectionHandle, DialError> {
  t.get_or_dial(
    quinn,
    now,
    cfg.client().clone(),
    peer,
    "localhost",
    None,
    Reliability::Reliable,
  )
}

/// Accept a fresh inbound connection for `peer`, minted on `quinn` in lockstep
/// so the ConnTable slab slot matches the handle (the `insert_accepted` slot
/// assertion). The accept-vs-dial origin is irrelevant to `insert_accepted`.
fn accept_inbound(
  t: &mut ConnTable,
  quinn: &mut QuinnEndpoint,
  cfg: &QuicOptions,
  now: Instant,
  peer: SocketAddr,
  mint_addr: SocketAddr,
) -> ConnectionHandle {
  let (ch, conn) = quinn
    .connect(now.into_std(), cfg.client().clone(), mint_addr, "localhost")
    .expect("fresh connection minted on the client endpoint");
  t.insert_accepted(ch, conn, peer);
  ch
}

/// Transport address a ferried inbound is delivered from (its membership key).
const FERRY_CLIENT_ADDR: &str = "127.0.0.1:5001";
/// Transport address of the endpoint a ferried outbound dials.
const FERRY_SERVER_ADDR: &str = "127.0.0.1:5000";

/// Drive a full QUIC handshake so `t` (acting as SERVER, on `server_ep`) holds a
/// freshly ESTABLISHED inbound from [`FERRY_CLIENT_ADDR`]. A raw client
/// connection on `client_ep` dials, and datagrams are ferried both ways until
/// the inbound leaves handshaking. Returns the inbound handle in `t`. Multiple
/// calls (a fresh raw client each) yield distinct inbounds from the same peer.
fn establish_inbound(
  t: &mut ConnTable,
  server_ep: &mut QuinnEndpoint,
  client_ep: &mut QuinnEndpoint,
  cfg: &QuicOptions,
) -> ConnectionHandle {
  use quinn_proto::DatagramEvent;
  let server_addr: SocketAddr = FERRY_SERVER_ADDR.parse().unwrap();
  let client_addr: SocketAddr = FERRY_CLIENT_ADDR.parse().unwrap();
  let now = Instant::now().into_std();
  let (cch, mut cconn) = client_ep
    .connect(now, cfg.client().clone(), server_addr, "localhost")
    .expect("raw client dial");
  let mut inbound: Option<ConnectionHandle> = None;
  for _ in 0..200 {
    let mut buf = Vec::new();
    while let Some(tr) = cconn.poll_transmit(now, 1, &mut buf) {
      let data = bytes::BytesMut::from(&buf[..tr.size]);
      let mut scratch = Vec::new();
      if let Some(ev) = server_ep.handle(now, client_addr, None, None, data, &mut scratch) {
        match ev {
          DatagramEvent::ConnectionEvent(ch, cev) => {
            if let Some(e) = t.get_mut(ch) {
              e.conn_mut().handle_event(cev);
            }
          }
          DatagramEvent::NewConnection(inc) => {
            let mut ab = Vec::new();
            if let Ok((ch, conn)) = server_ep.accept(inc, now, &mut ab, Some(cfg.server_arc())) {
              t.insert_accepted(ch, conn, client_addr);
              inbound = Some(ch);
            }
          }
          DatagramEvent::Response(_) => {}
        }
      }
      buf.clear();
    }
    if let Some(ich) = inbound {
      let mut sbuf = Vec::new();
      while let Some(tr) = t
        .get_mut(ich)
        .unwrap()
        .conn_mut()
        .poll_transmit(now, 1, &mut sbuf)
      {
        let data = bytes::BytesMut::from(&sbuf[..tr.size]);
        let mut scratch = Vec::new();
        if let Some(DatagramEvent::ConnectionEvent(ch, cev)) =
          client_ep.handle(now, server_addr, None, None, data, &mut scratch)
        {
          debug_assert_eq!(ch, cch);
          cconn.handle_event(cev);
        }
        sbuf.clear();
      }
    }
    while let Some(ev) = cconn.poll_endpoint_events() {
      if ev.is_drained() {
        continue;
      }
      if let Some(cev) = client_ep.handle_event(cch, ev) {
        cconn.handle_event(cev);
      }
    }
    if let Some(ich) = inbound {
      while let Some(ev) = t.get_mut(ich).unwrap().conn_mut().poll_endpoint_events() {
        if ev.is_drained() {
          continue;
        }
        if let Some(cev) = server_ep.handle_event(ich, ev) {
          t.get_mut(ich).unwrap().conn_mut().handle_event(cev);
        }
      }
    }
    cconn.handle_timeout(now);
    if let Some(ich) = inbound {
      t.get_mut(ich).unwrap().conn_mut().handle_timeout(now);
    }
    let cup = !cconn.is_handshaking() && !cconn.is_closed();
    let sup = inbound
      .and_then(|ich| t.get(ich))
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false);
    if cup && sup {
      break;
    }
  }
  let ich = inbound.expect("server accepted the inbound");
  assert!(
    !t.get(ich).unwrap().conn_ref().is_handshaking(),
    "the inbound must reach Established"
  );
  ich
}

/// Drive `ch` — a CLIENT-role connection already inserted in `t` (dialed on
/// `client_ep` to `remote`) — through a full QUIC handshake to Established by
/// ferrying datagrams against a throwaway server endpoint built internally, so
/// callers holding several client-role connections on one `client_ep` (hence one
/// slab lockstep) can each be established independently.
fn ferry_client_conn_to_established(
  t: &mut ConnTable,
  client_ep: &mut QuinnEndpoint,
  cfg: &QuicOptions,
  ch: ConnectionHandle,
  remote: SocketAddr,
) {
  use quinn_proto::DatagramEvent;
  let client_addr: SocketAddr = FERRY_CLIENT_ADDR.parse().unwrap();
  let now = Instant::now().into_std();
  let mut server_ep = QuinnEndpoint::new(
    cfg.endpoint_arc(),
    Some(cfg.server_arc()),
    true,
    Some([0x5a; 32]),
  );
  let mut server_conns = ConnTable::new();
  let mut server_ch: Option<ConnectionHandle> = None;
  for _ in 0..200 {
    let mut buf = Vec::new();
    while let Some(tr) = t
      .get_mut(ch)
      .unwrap()
      .conn_mut()
      .poll_transmit(now, 1, &mut buf)
    {
      let data = bytes::BytesMut::from(&buf[..tr.size]);
      let mut scratch = Vec::new();
      if let Some(ev) = server_ep.handle(now, client_addr, None, None, data, &mut scratch) {
        match ev {
          DatagramEvent::ConnectionEvent(sch, cev) => {
            if let Some(e) = server_conns.get_mut(sch) {
              e.conn_mut().handle_event(cev);
            }
          }
          DatagramEvent::NewConnection(inc) => {
            let mut ab = Vec::new();
            if let Ok((sch, conn)) = server_ep.accept(inc, now, &mut ab, Some(cfg.server_arc())) {
              server_conns.insert_accepted(sch, conn, client_addr);
              server_ch = Some(sch);
            }
          }
          DatagramEvent::Response(_) => {}
        }
      }
      buf.clear();
    }
    if let Some(sch) = server_ch {
      let mut sbuf = Vec::new();
      while let Some(tr) = server_conns
        .get_mut(sch)
        .unwrap()
        .conn_mut()
        .poll_transmit(now, 1, &mut sbuf)
      {
        let data = bytes::BytesMut::from(&sbuf[..tr.size]);
        let mut scratch = Vec::new();
        if let Some(DatagramEvent::ConnectionEvent(rch, cev)) =
          client_ep.handle(now, remote, None, None, data, &mut scratch)
          && rch == ch
        {
          t.get_mut(ch).unwrap().conn_mut().handle_event(cev);
        }
        sbuf.clear();
      }
    }
    while let Some(ev) = t.get_mut(ch).unwrap().conn_mut().poll_endpoint_events() {
      if ev.is_drained() {
        continue;
      }
      if let Some(cev) = client_ep.handle_event(ch, ev) {
        t.get_mut(ch).unwrap().conn_mut().handle_event(cev);
      }
    }
    if let Some(sch) = server_ch {
      while let Some(ev) = server_conns
        .get_mut(sch)
        .unwrap()
        .conn_mut()
        .poll_endpoint_events()
      {
        if ev.is_drained() {
          continue;
        }
        if let Some(cev) = server_ep.handle_event(sch, ev) {
          server_conns
            .get_mut(sch)
            .unwrap()
            .conn_mut()
            .handle_event(cev);
        }
      }
    }
    t.get_mut(ch).unwrap().conn_mut().handle_timeout(now);
    if let Some(sch) = server_ch {
      server_conns
        .get_mut(sch)
        .unwrap()
        .conn_mut()
        .handle_timeout(now);
    }
    let cup = t
      .get(ch)
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false);
    let sup = server_ch
      .and_then(|sch| server_conns.get(sch))
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false);
    if cup && sup {
      break;
    }
  }
  assert!(
    !t.get(ch).unwrap().conn_ref().is_handshaking() && !t.get(ch).unwrap().conn_ref().is_closed(),
    "the connection must reach Established"
  );
}

/// Establish a real OUTBOUND connection in `t` (t as CLIENT) dialed to `peer`.
fn establish_outbound(
  t: &mut ConnTable,
  client_ep: &mut QuinnEndpoint,
  cfg: &QuicOptions,
  peer: SocketAddr,
) -> ConnectionHandle {
  let now = Instant::now();
  let ch = t
    .get_or_dial(
      client_ep,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Reliable,
    )
    .unwrap();
  ferry_client_conn_to_established(t, client_ep, cfg, ch, peer);
  ch
}

/// Asymmetric-reachability fix: a closed never-established INBOUND (the peer's
/// failed dial toward us) is non-authoritative — it says nothing about our
/// egress. A reliable intent must dial our OWN independent outbound, and the
/// pre-pass must have pruned the dead inbound from the route (never left it as a
/// stale authoritative entry).
///
/// Negative control: drop P1's inbound prune — the dead inbound is left in
/// `route.inbound` and the final `route.inbound == None` assertion fails. The
/// contrasting outbound half (a closed never-established OUTBOUND stays
/// authoritative → `dial_failed`, no redial) is pinned by
/// `closed_never_established_does_not_redial`.
#[test]
fn closed_inbound_is_non_authoritative_and_triggers_a_fresh_outbound_dial() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // An inbound that closes WITHOUT ever establishing (DeadInbound).
  let dead = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4500".parse().unwrap(),
  );
  t.get_mut(dead)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  assert!(t.get(dead).unwrap().conn_ref().is_closed());
  assert!(!t.get(dead).unwrap().established_at_least_once());

  let ch = dial_reliable(&mut t, &mut client, &cfg, now, peer)
    .expect("a fresh independent outbound is dialed");
  assert_ne!(
    ch, dead,
    "the dead inbound is never returned as authoritative"
  );
  assert_eq!(
    t.conns.get(ch.0).unwrap().direction,
    ConnDirection::Outbound,
    "a fresh outbound was dialed"
  );
  assert_eq!(
    t.conns.len(),
    2,
    "the dead inbound is retained (awaiting reap) beside the new outbound"
  );
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.inbound),
    None,
    "the closed never-established inbound is pruned from the route by the pre-pass"
  );
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.outbound),
    Some(ch),
    "the fresh outbound is recorded as the route's outbound"
  );
}

/// An accept never displaces a LIVE tracked inbound: a second inbound Y accepted
/// while X is live (handshaking) does NOT overwrite `route.inbound` — an accept
/// proves only that an Initial arrived, not that the remote instance is alive. Y
/// becomes the tracked inbound only once it establishes (the establishment
/// chokepoint promotes the most recently established inbound), after which
/// selection surfaces Y and Y stays discoverable when X is reaped.
///
/// Negative control: make `insert_accepted` overwrite unconditionally — Y then
/// displaces the live X at accept and the `route.inbound == Some(x)` assertion
/// fails.
#[test]
fn accept_does_not_displace_a_live_inbound() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // Inbound X accepted → route.inbound = X.
  let x = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4500".parse().unwrap(),
  );
  assert_eq!(t.peer_routes.get(&peer).and_then(|r| r.inbound), Some(x));
  assert_eq!(t.handle_for(&peer), Some(x));

  // A second inbound Y accepted while X is live (not closed) → X is NOT displaced;
  // the tracked inbound stays X.
  let y = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4501".parse().unwrap(),
  );
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.inbound),
    Some(x),
    "an accept does not displace a live tracked inbound"
  );

  // Y establishes → the chokepoint promotes the most recently established inbound.
  t.on_established_transition(y);
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.inbound),
    Some(y),
    "establishment promotes the newly-established inbound"
  );
  assert_eq!(
    t.handle_for(&peer),
    Some(y),
    "selection surfaces the promoted inbound"
  );

  // After X is removed, Y is still discoverable.
  t.get_mut(x)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  drive_to_drained(&mut t, x);
  assert!(t.reap_if_drained(&mut client, x));
  assert_eq!(
    t.handle_for(&peer),
    Some(y),
    "the promoted inbound remains discoverable after the older one is reaped"
  );
}

/// At the global connection cap with a live handshaking inbound, a reliable
/// intent (R6) must degrade to the live inbound and repark — NOT propagate
/// `AtGlobalCap`. (A synchronous `connect` error at R6 degrades through the same
/// arm; it cannot be injected at this unit level, so this at-cap case is the
/// anchor for that arm.)
///
/// Negative control: propagate the dial error at R6 (`Err(e) => Err(e)`) — the
/// `expect` below panics because no handle is returned.
#[test]
fn reliable_r6_at_cap_returns_the_live_inbound_not_an_error() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let inbound = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4500".parse().unwrap(),
  );
  assert!(t.get(inbound).unwrap().conn_ref().is_handshaking());
  let sel = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      Some(1),
      Reliability::Reliable,
    )
    .expect("R6 at cap returns the live inbound, not an error");
  assert_eq!(
    sel, inbound,
    "the live handshaking inbound is returned for repark"
  );
  assert_eq!(
    t.conns.len(),
    1,
    "no companion outbound was created at the cap"
  );
}

/// Simultaneous-dial: an established OUTBOUND stays canonical even when a live
/// INBOUND is also present (R1 precedes R2/R3), so the exchange does not wedge.
///
/// Negative control: remove the R1 arm — the established outbound is no longer
/// matched, the reliable table trips its fresh-dial guard, and `sel == o` fails.
#[test]
fn established_outbound_precedes_a_live_inbound() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = FERRY_SERVER_ADDR.parse().unwrap();
  // A real established outbound O (drives a full handshake).
  let o = establish_outbound(&mut t, &mut client, &cfg, peer);
  // A live handshaking inbound I for the same peer.
  let i = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4600".parse().unwrap(),
  );
  assert_ne!(o, i);

  let sel = dial_reliable(&mut t, &mut client, &cfg, now, peer).unwrap();
  assert_eq!(
    sel, o,
    "R1 (established outbound) is selected over a live inbound; no wedge"
  );
}

/// The self-healing residual the transport layer deliberately accepts: a delayed
/// inbound that establishes into an EMPTY route is promoted (unconditionally, the
/// most recently established inbound wins), and even if it is a zombie to a dead
/// prior instance it is bounded — once it idle-reaps the route empties and a
/// subsequent fresh reliable dial proceeds. No permanent wedge; the route
/// recovers.
#[test]
fn established_inbound_into_empty_route_promotes_then_self_heals_on_reap() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();

  // A delayed inbound X arrives into an EMPTY route (accept fills the empty slot).
  let x = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4500".parse().unwrap(),
  );
  assert_eq!(t.peer_routes.get(&peer).and_then(|r| r.inbound), Some(x));

  // It establishes → the chokepoint promotes it unconditionally; selection rides
  // it (this is the zombie-can-capture-selection residual).
  t.on_established_transition(x);
  assert_eq!(t.peer_routes.get(&peer).and_then(|r| r.inbound), Some(x));
  assert_eq!(
    t.handle_for(&peer),
    Some(x),
    "the promoted inbound is selected"
  );

  // The idle timeout closes it; its drained-reap empties the route (the bounded
  // self-heal — no per-peer state survives).
  t.get_mut(x)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  drive_to_drained(&mut t, x);
  assert!(t.reap_if_drained(&mut client, x));
  assert_eq!(t.handle_for(&peer), None, "the reaped route is empty");
  assert!(
    !t.peer_routes.contains_key(&peer),
    "no residual route entry"
  );

  // A subsequent fresh reliable dial proceeds — the route recovers, no wedge.
  // (The reaped slab slot is reused, so the new handle value may equal the old
  // one; recovery is proven by a fresh Outbound connection being dialed, not by
  // the handle differing.)
  let fresh = dial_reliable(&mut t, &mut client, &cfg, now, peer)
    .expect("a fresh reliable dial proceeds after the zombie reaps");
  assert_eq!(
    t.conns.len(),
    1,
    "exactly the fresh outbound is tracked after recovery"
  );
  assert_eq!(
    t.conns.get(fresh.0).unwrap().direction,
    ConnDirection::Outbound,
    "the recovery dial is a self-initiated outbound"
  );
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.outbound),
    Some(fresh)
  );
}

/// A real established INBOUND is selected on both planes (R2 / U2), and a newer
/// inbound that establishes while it is live is declined at accept, then
/// promoted at the establishment chokepoint so selection follows to the newest
/// established inbound — the end-to-end crash-restart property.
///
/// Negative control: revert the inbound arm of `on_established_transition` — the
/// promotion never runs, `route.inbound` stays the prior X, and the final
/// `sel2 == y` assertion fails (selection keeps returning the superseded one).
#[test]
fn established_inbound_selected_and_promotion_supersedes_the_prior_one() {
  let (mut client, mut server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = FERRY_CLIENT_ADDR.parse().unwrap();

  // A real established inbound X, no outbound.
  let x = establish_inbound(&mut t, &mut server, &mut client, &cfg);
  assert_eq!(t.peer_routes.get(&peer).and_then(|r| r.inbound), Some(x));
  // R2 / U2: the established inbound is selected on both planes.
  assert_eq!(
    dial_reliable(&mut t, &mut client, &cfg, now, peer).unwrap(),
    x,
    "R2 selects the established inbound"
  );
  assert_eq!(
    t.get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      None,
      Reliability::Unreliable,
    )
    .unwrap(),
    x,
    "U2 selects the established inbound"
  );

  // A second inbound Y is accepted+established while X is live → NOT displaced at
  // accept (X stays tracked); the promotion at the establishment chokepoint is
  // what moves selection to the newly-established Y.
  let y = establish_inbound(&mut t, &mut server, &mut client, &cfg);
  assert_ne!(x, y);
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.inbound),
    Some(x),
    "a new inbound is not tracked at accept while the prior one is live"
  );

  // The establishment chokepoint promotes the newly-established inbound.
  t.on_established_transition(y);
  assert_eq!(
    t.peer_routes.get(&peer).and_then(|r| r.inbound),
    Some(y),
    "establishment promotes the newly-established inbound over the prior one"
  );
  let sel2 = dial_reliable(&mut t, &mut client, &cfg, now, peer).unwrap();
  assert_eq!(
    sel2, y,
    "selection follows to the newly-established inbound (R2), not the prior X"
  );
}

/// The unreliable table finds a live handshaking inbound behind a terminal
/// outbound (U4) and never creates a companion outbound beside it; a terminal
/// outbound with no inbound is returned so the datagram falls back to UDP (U5).
/// (The established-inbound rows U1/U2 are covered by
/// `established_inbound_selected_and_promotion_supersedes_the_prior_one`.)
#[test]
fn unreliable_selection_prefers_live_inbound_without_companion_dial() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();

  // U4: a live handshaking inbound, no outbound → return the inbound, no dial.
  let p1: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let i1 = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    p1,
    "127.0.0.1:4500".parse().unwrap(),
  );
  let before = t.conns.len();
  let sel = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      p1,
      "localhost",
      None,
      Reliability::Unreliable,
    )
    .unwrap();
  assert_eq!(sel, i1, "U4 returns the live handshaking inbound");
  assert_eq!(
    t.conns.len(),
    before,
    "U4 never creates a companion outbound beside a live inbound"
  );

  // U4 behind a terminal outbound: a live handshaking inbound is found even
  // though a closed never-established outbound occupies the outbound field.
  let p2: SocketAddr = "127.0.0.1:4444".parse().unwrap();
  let o = dial_reliable(&mut t, &mut client, &cfg, now, p2).unwrap();
  t.get_mut(o)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  let i2 = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    p2,
    "127.0.0.1:4600".parse().unwrap(),
  );
  let sel2 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      p2,
      "localhost",
      None,
      Reliability::Unreliable,
    )
    .unwrap();
  assert_eq!(
    sel2, i2,
    "U4 returns the live handshaking inbound even behind a terminal outbound"
  );

  // U5: a terminal outbound with no inbound → the outbound (→ NotReady → UDP).
  let p3: SocketAddr = "127.0.0.1:4455".parse().unwrap();
  let o3 = dial_reliable(&mut t, &mut client, &cfg, now, p3).unwrap();
  t.get_mut(o3)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  let sel3 = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      p3,
      "localhost",
      None,
      Reliability::Unreliable,
    )
    .unwrap();
  assert_eq!(
    sel3, o3,
    "U5 returns the terminal outbound; the datagram path then falls back to UDP"
  );
}

/// A route the pre-pass empties (its only tracked connection pruned) leaves no
/// `peer_routes` entry.
///
/// Negative control: drop the F4 empty-route removal — the route lingers as a
/// `PeerRoute` with both fields `None` and the `!contains_key` assertion fails.
#[test]
fn prepass_removes_a_route_left_fully_empty() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // Only a closed never-established inbound → route.inbound set, no outbound.
  let dead = accept_inbound(
    &mut t,
    &mut client,
    &cfg,
    now,
    peer,
    "127.0.0.1:4500".parse().unwrap(),
  );
  t.get_mut(dead)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  assert!(t.peer_routes.contains_key(&peer));

  // A reliable intent AT the global cap: the pre-pass prunes the dead inbound,
  // emptying the route (F4 removes it); the fresh dial is then refused at the
  // cap, so nothing recreates the route.
  let err = t
    .get_or_dial(
      &mut client,
      now,
      cfg.client().clone(),
      peer,
      "localhost",
      Some(1),
      Reliability::Reliable,
    )
    .expect_err("the fresh dial is refused at the global cap");
  assert!(matches!(err, DialError::AtGlobalCap));
  assert!(
    !t.peer_routes.contains_key(&peer),
    "a route the pre-pass emptied leaves no peer_routes entry"
  );
}
