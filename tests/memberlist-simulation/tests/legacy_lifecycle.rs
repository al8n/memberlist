//! Top-level Memberlist lifecycle scenarios ported from
//! `memberlist-core/src/base/tests.rs` into deterministic simulation.
//!
//! Each test replaces async ceremony, real I/O, and delegate subscribers
//! with the `Cluster` API.
//!
//! ## Skipped scenarios
//!
//! - `memberlist_join_with_labels` (line 218) — codec/label handling is a
//!   driver concern, not a state-machine concern.
//! - `memberlist_join_cancel` (line 363) — tests async task cancellation,
//!   which has no simulation equivalent.
//! - `memberlist_join_cancel_passive` (line 457) — same; async cancellation.
//! - `memberlist_conflict_delegate` (line 977) — covered by `alive_node_conflict`
//!   in `tests/legacy_alive.rs` (Task C).
//! - `memberlist_ping_delegate` (line 1065) — covered by
//!   `ping_emits_ping_completed_with_rtt` in `tests/legacy_push_pull_ping_reset.rs`
//!   (Task E).

use bytes::Bytes;
use memberlist_simulation::{Cluster, EndpointOptions, Event, Meta, Reliability, State};
use smol_str::SmolStr;
use std::{net::SocketAddr, time::Duration};

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

// ── 1. `memberlist_create` (line 77) ─────────────────────────────────────────

/// Single-node Cluster: assert it knows itself.
///
/// Ported from `memberlist-core/src/base/tests.rs:77 memberlist_create`.
#[test]
fn memberlist_create() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34001));

  assert_eq!(c.num_members(a1), 1, "single node should see only itself");
  assert_eq!(
    c.local_id(a1),
    Some(SmolStr::new("m1")),
    "local_id should match"
  );
  assert_eq!(
    c.get_node_state(a1, &SmolStr::new("m1")),
    Some(State::Alive),
    "local node should be Alive"
  );
}

// ── 2. `memberlist_create_shutdown` (line 107) ────────────────────────────────

/// Create then leave: verify the node transitions to Left.
///
/// The legacy test calls `m.shutdown()`; in simulation we call `Cluster::leave`
/// (maps to `Endpoint::leave`), then step to propagate the Dead broadcast.
///
/// Ported from `memberlist-core/src/base/tests.rs:107 memberlist_create_shutdown`.
#[test]
fn memberlist_create_shutdown() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34002));

  assert_eq!(c.num_members(a1), 1);

  // Leave (simulation equivalent of shutdown).
  c.leave(a1).expect("leave should succeed");

  // Drain events; the local node should now appear Left.
  for _ in 0..50 {
    c.step();
  }

  // The node marks itself Left (via Dead self-message).
  assert_eq!(
    c.get_node_state(a1, &SmolStr::new("m1")),
    Some(State::Left),
    "local node should be Left after leave"
  );
}

// ── 3. `memberlist_shutdown_cleanup` (line 137) ────────────────────────────────
//
// This scenario shuts down, drops the Memberlist, and re-creates it at the
// same address.  In simulation we do not actually "drop" or "reuse" an
// endpoint because `Cluster` owns all endpoints — there is no resource
// reclamation to test.  Skip with a note.
//
// Skipped: tests transport-level port reuse after shutdown, which is a driver
// concern (bindaddr reuse), not a state-machine concern.

// ── 4. `memberlist_shutdown_cleanup2` (line 159) ───────────────────────────────
//
// Two nodes join, the first shuts down, then re-registers at the same address.
// Same driver-level concern as above.
//
// Skipped: port reuse / address rebind is a driver concern.

// ── 5. `memberlist_join` (line 192) — HIGH-VALUE ──────────────────────────────

/// Two-node bootstrap: m2 joins m1's cluster.
///
/// Legacy: `m2.join(m1.advertise_address())` sends a push/pull with
/// `PushPullKind::Join`.  After settling, m2 has 2 members.
///
/// Ported from `memberlist-core/src/base/tests.rs:192 memberlist_join`.
#[test]
fn memberlist_join() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34010));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34011));

  // m2 joins m1 via a join push/pull.
  c.join(a2, a1);

  // Step until the push/pull stream exchange completes.
  for _ in 0..200 {
    c.step();
  }

  assert_eq!(c.num_members(a2), 2, "m2 should see 2 members after join");
  assert_eq!(c.num_members(a1), 2, "m1 should see 2 members after join");
}

// ── 6. `memberlist_join_shutdown` (line 516) ──────────────────────────────────

/// Join two nodes, then have m1 leave.  m2 should drop to 1 online member.
///
/// Legacy uses `m1.shutdown()` + `wait_for_condition`.  In simulation we step
/// until the Dead broadcast from m1 reaches m2 and m2 processes it.
///
/// Ported from `memberlist-core/src/base/tests.rs:516 memberlist_join_shutdown`.
#[test]
fn memberlist_join_shutdown() {
  let mut c = Cluster::new();
  let a1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(34020),
    |cfg: EndpointOptions<_, _>| cfg.with_gossip_interval(Duration::from_millis(10)),
  );
  let a2 = c.add_node_with(
    SmolStr::new("m2"),
    addr(34021),
    |cfg: EndpointOptions<_, _>| cfg.with_gossip_interval(Duration::from_millis(10)),
  );

  // Bootstrap membership.
  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }
  assert_eq!(c.num_members(a2), 2, "both nodes should see each other");

  // m1 leaves.
  c.leave(a1).expect("m1 leave should succeed");

  // Step enough for m1's Dead broadcast to reach m2.
  for _ in 0..500 {
    c.step();
  }

  // m2 should see m1 as Left (or Dead — both indicate not-online).
  let m1_state = c.get_node_state(a2, &SmolStr::new("m1"));
  assert!(
    matches!(m1_state, Some(State::Left) | Some(State::Dead)),
    "m2 should see m1 as Left or Dead after m1.leave(), got {m1_state:?}"
  );
}

// ── 7. `memberlist_node_delegate_meta` (line 549) ─────────────────────────────

/// Meta byte attached at construction propagates to peer via push/pull.
///
/// Legacy: each node is constructed with a `NodeDelegate` returning static
/// metadata.  In simulation we use `EndpointOptions::with_initial_meta`.
///
/// Ported from `memberlist-core/src/base/tests.rs:549 memberlist_node_delegate_meta`.
#[test]
fn memberlist_node_delegate_meta() {
  let meta_web = Meta::try_from("web").unwrap();
  let meta_lb = Meta::try_from("lb").unwrap();

  let mut c = Cluster::new();
  let a1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(34030),
    |cfg: EndpointOptions<_, _>| cfg.with_initial_meta(meta_web.clone()),
  );
  let a2 = c.add_node_with(
    SmolStr::new("m2"),
    addr(34031),
    |cfg: EndpointOptions<_, _>| cfg.with_initial_meta(meta_lb.clone()),
  );

  // m1 joins m2.
  c.join(a1, a2);
  for _ in 0..300 {
    c.step();
  }

  // Both nodes should know each other.
  assert_eq!(c.num_members(a1), 2, "m1 should see 2 members");
  assert_eq!(c.num_members(a2), 2, "m2 should see 2 members");

  // m1's view: m1 has meta "web", m2 has meta "lb".
  let m1_meta_at_a1 = c
    .member(a1, &SmolStr::new("m1"))
    .map(|ns| ns.meta_ref().clone());
  let m2_meta_at_a1 = c
    .member(a1, &SmolStr::new("m2"))
    .map(|ns| ns.meta_ref().clone());
  assert_eq!(
    m1_meta_at_a1.as_ref().map(|m| m.as_bytes()),
    Some(b"web".as_ref()),
    "m1 meta at a1 should be 'web'"
  );
  assert_eq!(
    m2_meta_at_a1.as_ref().map(|m| m.as_bytes()),
    Some(b"lb".as_ref()),
    "m2 meta at a1 should be 'lb'"
  );

  // m2's view: same expectations.
  let m1_meta_at_a2 = c
    .member(a2, &SmolStr::new("m1"))
    .map(|ns| ns.meta_ref().clone());
  let m2_meta_at_a2 = c
    .member(a2, &SmolStr::new("m2"))
    .map(|ns| ns.meta_ref().clone());
  assert_eq!(
    m1_meta_at_a2.as_ref().map(|m| m.as_bytes()),
    Some(b"web".as_ref()),
    "m1 meta at a2 should be 'web'"
  );
  assert_eq!(
    m2_meta_at_a2.as_ref().map(|m| m.as_bytes()),
    Some(b"lb".as_ref()),
    "m2 meta at a2 should be 'lb'"
  );
}

// ── 8. `memberlist_node_delegate_meta_update` (line 632) ──────────────────────

/// After construction, change meta; verify peers see the update via gossip.
///
/// Legacy: calls `update_node(Duration::ZERO)` on each side.  In simulation
/// we call `Cluster::set_meta` (maps to `Endpoint::update_meta`) and step.
///
/// Ported from `memberlist-core/src/base/tests.rs:632 memberlist_node_delegate_meta_update`.
#[test]
fn memberlist_node_delegate_meta_update() {
  let meta_web = Meta::try_from("web").unwrap();
  let meta_lb = Meta::try_from("lb").unwrap();

  let mut c = Cluster::new();
  let a1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(34040),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_initial_meta(meta_web)
        .with_gossip_interval(Duration::from_millis(10))
    },
  );
  let a2 = c.add_node_with(
    SmolStr::new("m2"),
    addr(34041),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_initial_meta(meta_lb)
        .with_gossip_interval(Duration::from_millis(10))
    },
  );

  // Bootstrap.
  c.join(a1, a2);
  for _ in 0..300 {
    c.step();
  }
  assert_eq!(c.num_members(a1), 2, "both nodes should be joined");

  // Update meta on both sides.
  let meta_api = Meta::try_from("api").unwrap();
  let meta_db = Meta::try_from("db").unwrap();
  c.set_meta(a1, meta_api).expect("set_meta m1 → api");
  c.set_meta(a2, meta_db).expect("set_meta m2 → db");

  // Let gossip carry the new Alive messages.
  for _ in 0..500 {
    c.step();
  }

  // m1's view: m1 → "api", m2 → "db".
  let m1_meta_at_a1 = c
    .member(a1, &SmolStr::new("m1"))
    .map(|ns| ns.meta_ref().clone());
  let m2_meta_at_a1 = c
    .member(a1, &SmolStr::new("m2"))
    .map(|ns| ns.meta_ref().clone());
  assert_eq!(
    m1_meta_at_a1.as_ref().map(|m| m.as_bytes()),
    Some(b"api".as_ref()),
    "m1 meta at a1 should be 'api' after update"
  );
  assert_eq!(
    m2_meta_at_a1.as_ref().map(|m| m.as_bytes()),
    Some(b"db".as_ref()),
    "m2 meta at a1 should be 'db' after update"
  );

  // m2's view: same.
  let m1_meta_at_a2 = c
    .member(a2, &SmolStr::new("m1"))
    .map(|ns| ns.meta_ref().clone());
  let m2_meta_at_a2 = c
    .member(a2, &SmolStr::new("m2"))
    .map(|ns| ns.meta_ref().clone());
  assert_eq!(
    m1_meta_at_a2.as_ref().map(|m| m.as_bytes()),
    Some(b"api".as_ref()),
    "m1 meta at a2 should be 'api' after update"
  );
  assert_eq!(
    m2_meta_at_a2.as_ref().map(|m| m.as_bytes()),
    Some(b"db".as_ref()),
    "m2 meta at a2 should be 'db' after update"
  );
}

// ── 9. `memberlist_send` (line 826) ───────────────────────────────────────────

/// Best-effort UDP delivery: m1 sends "ping" to m2 and m2 sends "pong" to m1.
/// Each side observes `Event::UserPacket` with the expected payload.
///
/// Legacy uses `m.send(addr, bytes)` which queues a UDP user-data packet.
/// In simulation `Cluster::send` enqueues a `Message::UserData` datagram.
///
/// Ported from `memberlist-core/src/base/tests.rs:826 memberlist_send`.
#[test]
fn memberlist_send() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34050));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34051));

  // Bootstrap: join so both endpoints are aware of each other
  // (not strictly required for UDP delivery, but matches legacy setup).
  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }

  // m1 sends "ping" to m2; m2 sends "pong" to m1.
  c.send(a1, a2, Bytes::from_static(b"ping"));
  c.send(a2, a1, Bytes::from_static(b"pong"));

  // Step to deliver the datagrams.
  for _ in 0..50 {
    c.step();
  }

  // Collect UserPacket events from m2 — should contain "ping".
  let mut m2_got_ping = false;
  while let Some(ev) = c.poll_event(a2) {
    if let Event::UserPacket(p) = ev
      && p.data_ref().as_ref() == b"ping"
      && p.reliability() == Reliability::Unreliable
    {
      m2_got_ping = true;
    }
  }

  // Collect UserPacket events from m1 — should contain "pong".
  let mut m1_got_pong = false;
  while let Some(ev) = c.poll_event(a1) {
    if let Event::UserPacket(p) = ev
      && p.data_ref().as_ref() == b"pong"
      && p.reliability() == Reliability::Unreliable
    {
      m1_got_pong = true;
    }
  }

  assert!(m2_got_ping, "m2 should have received 'ping' as UserPacket");
  assert!(m1_got_pong, "m1 should have received 'pong' as UserPacket");
}

// ── 10. `memberlist_user_data` (line 734) ─────────────────────────────────────

/// Variant of `memberlist_send`: verify the UserPacket event arrives with the
/// correct `from` address.
///
/// The legacy test exercises broadcast queuing and remote-state exchange; here
/// we focus on the User-data delivery path that `Cluster::send` exercises.
///
/// Ported from `memberlist-core/src/base/tests.rs:734 memberlist_user_data`
/// (simplified — broadcast ordering and remote-state bytes are a delegate
/// concern not present in the simulation harness).
#[test]
fn memberlist_user_data() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34060));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34061));

  // Join so both endpoints are running.
  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }

  let payload = Bytes::from_static(b"hello-from-m1");
  c.send(a1, a2, payload.clone());

  for _ in 0..50 {
    c.step();
  }

  // m2 should observe the UserPacket with `from == a1`.
  let mut received = false;
  while let Some(ev) = c.poll_event(a2) {
    if let Event::UserPacket(p) = ev
      && *p.from_ref() == a1
      && *p.data_ref() == payload
    {
      received = true;
    }
  }
  assert!(received, "m2 should receive the user-data payload from m1");
}

// ── 11. `memberlist_send_reliable` (line 1128) ────────────────────────────────

/// Reliable (stream) delivery: m2 sends a payload to m1 via
/// `Cluster::send_reliable`.  m1 observes `Event::UserPacket { reliability:
/// Reliable }`.
///
/// Ported from `memberlist-core/src/base/tests.rs:1128 memberlist_send_reliable`.
#[test]
fn memberlist_send_reliable() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34070));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34071));

  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }
  assert_eq!(c.num_members(a2), 2, "both nodes joined");

  // m2 sends a reliable stream message to m1.
  c.send_reliable(a2, a1, Bytes::from_static(b"send_reliable"));

  // Step to let the stream exchange complete.
  for _ in 0..300 {
    c.step();
  }

  // m1 should see a Reliable UserPacket.
  let mut received = false;
  while let Some(ev) = c.poll_event(a1) {
    if let Event::UserPacket(p) = ev
      && p.data_ref().as_ref() == b"send_reliable"
      && p.reliability() == Reliability::Reliable
    {
      received = true;
    }
  }
  assert!(
    received,
    "m1 should receive reliable user-data from m2 via stream"
  );
}

// ── 12. `memberlist_leave` (line 897) — HIGH-VALUE ────────────────────────────

/// m1 leaves; m2 should see m1 transition to Left (or Dead).
///
/// Legacy: `m1.leave(1s)` + sleep 10ms.  In simulation we call
/// `Cluster::leave` and step for gossip delivery.
///
/// Ported from `memberlist-core/src/base/tests.rs:897 memberlist_leave`.
#[test]
fn memberlist_leave() {
  let mut c = Cluster::new();
  let a1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(34080),
    |cfg: EndpointOptions<_, _>| cfg.with_gossip_interval(Duration::from_millis(5)),
  );
  let a2 = c.add_node_with(
    SmolStr::new("m2"),
    addr(34081),
    |cfg: EndpointOptions<_, _>| cfg.with_gossip_interval(Duration::from_millis(5)),
  );

  // Both nodes join.
  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }
  assert_eq!(c.num_members(a2), 2, "both nodes should see each other");
  assert_eq!(c.num_members(a1), 2, "both nodes should see each other");

  // m1 leaves.
  c.leave(a1).expect("m1 leave should succeed");

  // Step to propagate m1's Dead/Left broadcast to m2.
  for _ in 0..1000 {
    c.step();
  }

  // m1 sees itself as Left.
  assert_eq!(
    c.get_node_state(a1, &SmolStr::new("m1")),
    Some(State::Left),
    "m1 should see itself as Left"
  );

  // m2 should see m1 as Left (or Dead — the Dead broadcast carries the Left
  // transition; memberlist-core maps Dead-self to Left on receipt).
  let m1_state_at_m2 = c.get_node_state(a2, &SmolStr::new("m1"));
  assert!(
    matches!(m1_state_at_m2, Some(State::Left) | Some(State::Dead)),
    "m2 should see m1 as Left or Dead, got {m1_state_at_m2:?}"
  );
}
