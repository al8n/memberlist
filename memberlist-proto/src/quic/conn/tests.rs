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
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
    .unwrap();
  let ch2 = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
  assert_eq!(t.handle_for(&peer), None, "peers entry removed on reap");
}

/// `Connection::close` drives the cached `Connection` into `State::Closed`
/// — `is_closed()` is `true` immediately (quinn-proto's `State::is_closed`
/// matches `Closed | Draining | Drained`); the 3×PTO `Timer::Close` has
/// not yet fired so `is_drained()` is still `false`. In that
/// closed-before-drained window `Streams::open(Dir::Bi)` already refuses
/// (same `is_closed()` check inside `Streams::open`), but
/// `is_handshaking()` is `false`, so `service_dials`'s handshaking-requeue
/// would not catch it.
/// For a connection that had previously reached `Established`,
/// `get_or_dial` MUST detect the un-reusable cached connection and dial
/// a fresh one — silently returning the cached handle would route a
/// redialable intent into `dial_failed` and lose the exchange.
///
/// Negative control: revert the `is_closed()` check in `get_or_dial`
/// (return the cached `ch` whenever the slab still holds it) and this
/// test fails — `ch_redial == ch1` and the slab/peers state is wrong.
#[test]
fn get_or_dial_redials_when_cached_conn_is_closed_not_drained() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch1 = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
  // `peers[peer]` points at the live (new) handle for subsequent
  // outbound exchanges.
  assert_eq!(
    t.handle_for(&peer),
    Some(ch_redial),
    "peers mapping advanced to the fresh redial"
  );

  // Drive the OLD connection to `Drained` and confirm its slab slot is
  // cleared while the new mapping is preserved (Part B equality guard).
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
  // Critical: the new redial's mapping MUST survive the old handle's
  // drained-reap (handle-equality guard).
  assert_eq!(
    t.handle_for(&peer),
    Some(ch_redial),
    "drained-reap of the OLD handle must NOT clobber the NEW redial's peers mapping"
  );
  assert!(
    t.get(ch_redial).is_some(),
    "redial slab entry intact after old-handle drained-reap"
  );
}

/// A cached connection that is `is_closed()` but has NEVER reached
/// `Established` (handshake failure for an unreachable peer) must NOT be
/// redialed by `get_or_dial`: a fresh dial to the same unreachable peer
/// would just fail the same way, and a SWIM periodic push-pull schedule
/// would generate one fresh handshake per attempt for the lifetime of
/// the deadline. Return the cached handle so
/// `service_dials::open(Dir::Bi)=None && !is_handshaking()` falls through
/// to `dial_failed`, consuming the intent on a single handshake attempt
/// per push-pull deadline.
#[test]
fn closed_never_established_does_not_redial() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch1 = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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

/// Closed-never-Established cached entry must NOT hide a live accepted
/// connection. `insert_accepted` uses the SAME `!Connection::is_closed()`
/// usability predicate as `get_or_dial`'s outbound reuse check, so when
/// the cached canonical for a peer is closed (regardless of whether it
/// ever reached Established), the newly-accepted connection becomes
/// canonical (`peers[peer] = new_ch`). Symmetric with `get_or_dial`.
///
/// Setup: dial X (the cache is seeded with `ch1`, `was_established`
/// left false — the handshake-failed-or-closed-before-Established
/// signature). Close X. Now `insert_accepted` a new connection for X.
/// Assert that `peers[X]` advances to the new handle (the live
/// accepted connection becomes the canonical), and that subsequent
/// `get_or_dial(X)` returns the new handle so future outbound
/// exchanges ride the live connection instead of the closed cached
/// one. Finally, drive the OLD handle through drained-reap and
/// confirm the equality-guarded `reap_if_drained` does NOT clobber
/// the new canonical mapping.
///
/// Negative control: if `insert_accepted` checked only
/// `self.conns.contains(existing.0)` (slab presence — true while the
/// closed handle is still awaiting its drained-reap), the cached
/// closed handle would stay canonical. The assertion
/// `peers[X] == Some(new_ch)` below would fail, every subsequent
/// outbound dial would hit the closed cached `ch1`, fall through to
/// the `service_dials::is_closed → dial_failed` branch, and the live
/// accepted connection would be permanently unused.
#[test]
fn closed_never_established_cached_does_not_hide_live_accepted_connection() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // (1) Outbound dial X. `ch1` is now the canonical handle for peer.
  // `established_at_least_once` is left false to represent a
  // handshake failure / closed-before-Established cache.
  let ch1 = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
  // `insert_accepted` itself — it only inserts the entry into the
  // slab and updates `peers` per the usability predicate.
  let alt: SocketAddr = "127.0.0.1:4500".parse().unwrap();
  let (new_ch, new_conn) = client
    .connect(now.into_std(), cfg.client().clone(), alt, "localhost")
    .expect("fresh connection minted on the same client endpoint");
  t.insert_accepted(new_ch, new_conn, peer);
  // (4) The accepted connection IS live (`!is_closed()`), and the
  // cached canonical was closed (`is_closed()`), so the `peers`
  // canonical advances to `new_ch`. The slab-presence-only predicate
  // (`self.conns.contains(ch1.0)`) would instead leave the closed
  // handle canonical because the drained-pending slab entry is still
  // present.
  assert_eq!(
    t.handle_for(&peer),
    Some(new_ch),
    "the live accepted connection must replace the closed cached \
             canonical (the closed cached handle was hiding the live \
             accepted connection from subsequent outbound dials)"
  );
  // (5) Subsequent outbound dial returns the NEW canonical: outbound
  // exchanges ride the live accepted connection.
  let ch_after = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
    .unwrap();
  assert_eq!(
    ch_after, new_ch,
    "outbound dial after the accept must reuse the new live canonical, \
             not the closed cached ch1"
  );
  // (6) Old slab entry persists awaiting its drained-reap. Drive it
  // to Drained and confirm the equality-guarded `reap_if_drained`
  // clears the OLD slab slot without clobbering the NEW canonical.
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
    "old closed cached handle must drained-reap to completion"
  );
  assert_eq!(
    t.handle_for(&peer),
    Some(new_ch),
    "the new canonical must survive the OLD handle's drained-reap \
             (handle-equality-guarded `peers` removal protects it)"
  );
}

/// Part B isolated: the handle-equality guard's three cases — plain reap
/// (peers→ch holds, remove), mid-drain redial (peers→new_ch holds,
/// skip), and peers absent (None, skip). The first two are exercised by
/// `reap_if_drained_is_false_until_drained_then_removes` and
/// `get_or_dial_redials_when_cached_conn_is_closed_not_drained`
/// respectively; this test pins case (c): a synthetic state where the
/// slab still holds `ch` (drained) but `peers[e.peer]` was already
/// removed (e.g. by a fresh `get_or_dial` that dialed before this reap
/// ran). The reap must be idempotent — clear the slab slot, leave the
/// (now-absent) peers state alone.
#[test]
fn reap_if_drained_handle_equality_guard_idempotent_when_peers_absent() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
  // Synthesize "peers entry already removed" (the redial case at the
  // limit: peers cleared but the old slab still holds the drained
  // entry awaiting this reap).
  t.peers.remove(&peer);
  assert_eq!(t.handle_for(&peer), None);
  // The reap must still complete (slab cleared, no panic, no
  // resurrection of a stale peers entry).
  assert!(
    t.reap_if_drained(&mut client, ch),
    "reap must succeed even when peers entry was pre-removed"
  );
  assert!(t.get(ch).is_none(), "slab slot freed");
  assert_eq!(
    t.handle_for(&peer),
    None,
    "peers state unchanged (no resurrection)"
  );
}

/// Race-defensive arm of `get_or_dial`: the `peers` map holds a handle whose
/// slab slot has already been vacated (the `None` arm of the `match
/// self.conns.get(ch.0)` → `(reusable=false, was_established=false)`). A
/// never-Established cached handle is authoritative — the call returns the
/// cached handle so `service_dials`'s `open(Bi)=None && !is_handshaking()`
/// path fires `dial_failed` rather than spawning a fresh handshake storm. No
/// new slab entry is created.
#[test]
fn get_or_dial_returns_cached_handle_when_peers_points_at_vacated_slab_slot() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  // Plant a `peers` mapping to a handle index that has no slab entry — the
  // race-defensive `None` arm's input. `was_established` resolves to false,
  // so this is the never-Established branch.
  let phantom = ConnectionHandle(999);
  t.peers.insert(peer, phantom);
  assert!(t.conns.get(phantom.0).is_none(), "slab slot is vacant");

  let ch = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
    .expect("get_or_dial returns the cached handle");
  assert_eq!(
    ch, phantom,
    "the stale never-Established handle is authoritative — returned so \
       service_dials fires dial_failed, not a fresh handshake"
  );
  assert_eq!(
    t.conns.len(),
    0,
    "no new slab entry was created for a phantom"
  );
}

/// `reap_if_drained` on a handle whose slab slot is already gone returns
/// `false` without touching quinn or `peers` (the `None => return false`
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
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
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
/// Negative control: drop the `direction == ConnDirection::Inbound` filter in
/// `pending_inbound_from` — the outbound dial is then counted and the `== 0`
/// assertion fails.
#[test]
fn pending_inbound_from_excludes_local_outbound_dial() {
  let (mut client, _server, cfg) = quinn_pair();
  let mut t = ConnTable::new();
  let now = Instant::now();
  let peer: SocketAddr = "127.0.0.1:4433".parse().unwrap();
  let ch = t
    .get_or_dial(&mut client, now, cfg.client().clone(), peer, "localhost")
    .unwrap();
  // A freshly-dialed outbound connection is handshaking and never established —
  // exactly the state the pre-fix `is_handshaking()` count would have charged.
  assert!(t.get(ch).unwrap().conn_ref().is_handshaking());
  assert_eq!(
    t.pending_inbound_from(&peer),
    0,
    "a local outbound dial must not consume the peer's inbound pending allowance \
       (or simultaneous bidirectional dialing wedges)"
  );
}

/// A failed inbound handshake becomes non-handshaking but its closed/draining
/// slab entry lingers until its drained-reap. `pending_inbound_from` MUST keep
/// it charged (`!established_at_least_once`), so a source cannot exceed the
/// per-source cap by repeatedly opening inbound handshakes that fail — the
/// closed-but-undrained entries still consume its allowance.
///
/// A closed-before-Established connection is the observable state of a failed
/// inbound TLS handshake (`is_closed() && !is_handshaking()`, established flag
/// never set); `Connection::close` on a handshaking connection reproduces it
/// exactly — the same modelling the closed-never-Established tests in this file
/// use (`closed_never_established_cached_does_not_hide_live_accepted_connection`).
///
/// Negative control: count `is_handshaking()` instead of
/// `!established_at_least_once` in `pending_inbound_from` — the closed failed
/// entries drop out and the final `== 2` assertion fails (the source's
/// allowance is wrongly freed by handshakes that failed).
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
  // `closed_never_established_cached_does_not_hide_live_accepted_connection`).
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
