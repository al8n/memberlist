//! Dead-family legacy unit tests ported from
//! `memberlist-core/src/state/tests.rs` into deterministic simulation.
//!
//! Each test mirrors its legacy counterpart's state-machine assertions;
//! async ceremony, mutex locks, and delegate subscribers are replaced by
//! the `Cluster` API.
//!
//! **Broadcast-count note**: The simulation `Endpoint` pre-queues one Alive
//! broadcast for the local node at creation time (legacy does not do this at
//! the equivalent stage). All `broadcast_queue_len` assertions therefore use
//! `>= 1` or drain-based checks rather than `== 1` where the legacy asserts
//! exactly 1.

use memberlist_proto::typed::Message;
use memberlist_simulation::{Alive, Cluster, Dead, Node, State};
use smol_str::SmolStr;
use std::{net::SocketAddr, time::Duration};

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

// ── 1. `dead_node_no_node` (legacy line 1705) ────────────────────────────────

/// Inject a Dead for a peer ID that has never been seen.
///
/// Asserts:
/// - State map remains at 1 (local node only; no phantom node inserted).
///
/// Ported from `memberlist-core/src/state/tests.rs:1705 dead_node_no_node`.
#[test]
fn dead_node_no_node() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(23001));

  let unknown_id = SmolStr::new("unknown_peer");
  let local_id = c.local_id(m1).unwrap();

  let d = Dead::new(1, unknown_id, local_id);
  c.dead_node(m1, d);

  // Only the local node; the unknown peer must not have been inserted.
  assert_eq!(
    c.num_members(m1),
    1,
    "unknown peer must not be inserted on Dead"
  );
}

// ── 2. `dead_node_left` (legacy line 1734) ────────────────────────────────────

/// A remote self-marked Dead (`node == from`) records reclaim-protected
/// `State::Dead`, never `State::Left`.
///
/// A self-leave is unattributable in plaintext gossip: SWIM relays the
/// departing node's own `Dead{X, from: X}` through arbitrary peers, so a peer
/// cannot tell a genuine self-leave from a forged one. Granting `State::Left` —
/// which is immediately address-reclaimable and exempt from the
/// incarnation-staleness guard — off such a payload would let a forger redirect
/// a live id to an attacker-chosen address. So the target is recorded as
/// `State::Dead`, reclaim-protected by `dead_node_reclaim_time`; `State::Left`
/// is reserved for the local node's view of ITSELF leaving.
///
/// A departed node still returns legitimately: once `dead_node_reclaim_time`
/// has elapsed, a fresh Alive at a new address is adopted. The address-change
/// reclaim is a new membership instance, exempt from the incarnation gate.
///
/// Asserts:
/// - After Dead(node==from): `State::Dead` (not Left).
/// - Dead broadcast is queued.
/// - Past the reclaim window, an Alive at a new address is adopted:
///   `State::Alive`, with the address and meta updated.
///
/// Ported from `memberlist-core/src/state/tests.rs:1734 dead_node_left`,
/// realigned to the self-leave-takeover hardening.
#[test]
fn dead_node_self_marked_marks_dead_not_left() {
  let reclaim = Duration::from_millis(10);
  let mut c = Cluster::new();
  let m1 = c.add_node_with(SmolStr::new("m1"), addr(23011), |cfg| {
    cfg.with_dead_node_reclaim_time(reclaim)
  });

  let peer_id = SmolStr::new("test_node");
  let peer_addr1 = addr(23091);
  let peer_addr2: SocketAddr = "127.0.0.2:9000".parse().unwrap();

  // Join peer.
  c.alive_node(
    m1,
    Alive::new(1, Node::new(peer_id.clone(), peer_addr1)),
    false,
  );
  // Drain join event.
  while c.poll_event(m1).is_some() {}

  // Dead with node == from → reclaim-protected Dead (NOT Left).
  let d = Dead::new(1, peer_id.clone(), peer_id.clone());
  c.dead_node(m1, d);

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "peer with Dead(node==from) must record reclaim-protected Dead, never Left"
  );

  // Dead broadcast must be queued.
  let broadcasts = c.drain_broadcasts(m1);
  assert!(
    broadcasts.iter().any(|m| matches!(m, Message::Dead(_))),
    "Dead(node==from) must queue a Dead broadcast; got: {broadcasts:?}"
  );

  // Drain leave event.
  while c.poll_event(m1).is_some() {}

  // Age the Dead entry past `dead_node_reclaim_time` so its address becomes
  // reclaimable, then rejoin at a NEW address with a higher incarnation. The
  // address-change reclaim adopts the new address and is exempt from the
  // incarnation gate, so the fresh Alive re-alives the peer — the legitimate
  // return path a plain reclaim-protected Dead still allows.
  c.age_node(m1, &peer_id, reclaim + Duration::from_millis(1));
  use memberlist_simulation::Meta;
  let foo_meta: Meta = "foo".try_into().unwrap();
  c.alive_node(
    m1,
    Alive::new(3, Node::new(peer_id.clone(), peer_addr2)).with_meta(foo_meta),
    false,
  );

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Alive),
    "peer should be Alive again after a reclaim-window rejoin at a new address"
  );
  assert_eq!(
    c.get_node_incarnation(m1, &peer_id),
    Some(3),
    "incarnation should be 3 after rejoin"
  );
  assert_eq!(
    c.member(m1, &peer_id).map(|ns| *ns.address_ref()),
    Some(peer_addr2),
    "address must be updated to the rejoin address after reclaim"
  );
  assert_eq!(
    c.member_meta(m1, &peer_id).map(|(meta, _, _)| meta),
    Some(b"foo".to_vec()),
    "meta must be updated after reclaim"
  );
}

// ── 3. `dead_node` (legacy line 1806) ────────────────────────────────────────

/// An Alive peer transitions to Dead when a Dead message arrives.
///
/// Steps:
/// 1. Join peer as Alive (inc 1).
/// 2. Age the peer's state_change by 3600 s.
/// 3. Inject Dead → state becomes Dead.
/// 4. Assert Dead broadcast queued.
/// 5. Assert a NodeLeft event is emitted.
///
/// Ported from `memberlist-core/src/state/tests.rs:1806 dead_node`.
#[test]
fn dead_node_alive_to_dead() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(23021));

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(23090);
  let local_id = c.local_id(m1).unwrap();

  // 1. Join peer.
  c.alive_node(
    m1,
    Alive::new(1, Node::new(peer_id.clone(), peer_addr)),
    false,
  );
  // Drain join event.
  while c.poll_event(m1).is_some() {}

  // 2. Age peer.
  c.age_node(m1, &peer_id, Duration::from_secs(3600));

  // 3. Inject Dead.
  c.dead_node(m1, Dead::new(1, peer_id.clone(), local_id));

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "peer should be Dead after dead_node"
  );

  // 4. Dead broadcast must be queued.
  let broadcasts = c.drain_broadcasts(m1);
  assert!(
    broadcasts.iter().any(|m| matches!(m, Message::Dead(_))),
    "dead_node must queue a Dead broadcast; got: {broadcasts:?}"
  );

  // 5. NodeLeft event must be emitted.
  use memberlist_simulation::Event;
  let mut found_leave = false;
  while let Some(ev) = c.poll_event(m1) {
    if let Event::NodeLeft(node) = ev {
      if node.id_ref() == &peer_id {
        found_leave = true;
      }
    }
  }
  assert!(found_leave, "dead_node must emit a NodeLeft event");
}

// ── 4. `dead_node_double` (legacy line 1873) ─────────────────────────────────

/// Killing an already-Dead peer a second time is a no-op.
///
/// Steps:
/// 1. Join peer, age it, kill it (first Dead).
/// 2. Kill it again with a higher incarnation.
/// 3. Assert: state unchanged (still Dead), no new broadcast, no new event.
///
/// Ported from `memberlist-core/src/state/tests.rs:1873 dead_node_double`.
#[test]
fn dead_node_double_noop() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(23031));

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(23089);
  let local_id = c.local_id(m1).unwrap();

  // 1. Join peer, age, kill.
  c.alive_node(
    m1,
    Alive::new(1, Node::new(peer_id.clone(), peer_addr)),
    false,
  );
  c.age_node(m1, &peer_id, Duration::from_secs(3600));
  c.dead_node(m1, Dead::new(1, peer_id.clone(), local_id.clone()));

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "peer should be Dead"
  );
  // Drain events from first kill.
  while c.poll_event(m1).is_some() {}

  // Record queue length before second Dead.
  let bq_before_second = c.broadcast_queue_len(m1);

  // 2. Second Dead (higher incarnation) must be ignored because state is Dead.
  c.dead_node(m1, Dead::new(2, peer_id.clone(), local_id));

  // 3. No new broadcast (queue must not have grown).
  let bq_after_second = c.broadcast_queue_len(m1);
  assert_eq!(
    bq_before_second, bq_after_second,
    "second Dead on already-Dead peer must not enqueue a broadcast: before={bq_before_second}, after={bq_after_second}"
  );
  // No new leave event.
  use memberlist_simulation::Event;
  let mut found_leave = false;
  while let Some(ev) = c.poll_event(m1) {
    if let Event::NodeLeft(node) = ev {
      if node.id_ref() == &peer_id {
        found_leave = true;
      }
    }
  }
  assert!(!found_leave, "second Dead must not emit another NodeLeft");

  // State still Dead.
  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "state must remain Dead"
  );
}

// ── 5. `dead_node_old_dead` (legacy line 1943) ───────────────────────────────

/// A Dead with a lower incarnation than the tracked peer is ignored.
///
/// Steps:
/// 1. Join peer at incarnation 10.
/// 2. Inject Dead at incarnation 1 (old).
/// 3. Assert: state remains Alive.
///
/// Ported from `memberlist-core/src/state/tests.rs:1943 dead_node_old_dead`.
#[test]
fn dead_node_old_dead_ignored() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(23041));

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(23088);
  let local_id = c.local_id(m1).unwrap();

  // 1. Join peer at incarnation 10.
  c.alive_node(
    m1,
    Alive::new(10, Node::new(peer_id.clone(), peer_addr)),
    false,
  );
  c.age_node(m1, &peer_id, Duration::from_secs(3600));

  // 2. Dead at old incarnation.
  c.dead_node(m1, Dead::new(1, peer_id.clone(), local_id));

  // 3. State must remain Alive.
  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Alive),
    "old-incarnation Dead must not change state"
  );
}

// ── 6. `dead_node_alive_replay` (legacy line 1985) ───────────────────────────

/// Replaying an Alive at the same incarnation after a Dead is ignored.
///
/// Steps:
/// 1. Join peer at incarnation 10.
/// 2. Kill peer at incarnation 10.
/// 3. Replay Alive at incarnation 10.
/// 4. Assert: state remains Dead.
///
/// The simulation's `process_alive_decided` guard:
///   `alive_incarnation <= local_incarnation` returns early for non-local nodes.
///
/// Ported from `memberlist-core/src/state/tests.rs:1985 dead_node_alive_replay`.
#[test]
fn dead_node_alive_replay_ignored() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(23051));

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(23087);
  let local_id = c.local_id(m1).unwrap();

  // 1. Join peer at incarnation 10.
  let alive = Alive::new(10, Node::new(peer_id.clone(), peer_addr));
  c.alive_node(m1, alive.clone(), false);

  // 2. Kill peer.
  c.dead_node(m1, Dead::new(10, peer_id.clone(), local_id));
  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "peer should be Dead"
  );

  // 3. Replay the same Alive (same incarnation) → must be a no-op.
  c.alive_node(m1, alive, false);

  // 4. State remains Dead.
  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "Alive replay at same incarnation must not resurrect a Dead node"
  );
}

// ── 7. `dead_node_refute` (legacy line 2023) ─────────────────────────────────

/// A Dead about the LOCAL node (while not leaving) causes a refute:
/// incarnation bumped, Alive broadcast queued, state remains Alive.
///
/// Asserts:
/// - `local_incarnation` increases after the Dead.
/// - At least one `Message::Alive` appears in the broadcast queue.
/// - Node state remains Alive.
///
/// Ported from `memberlist-core/src/state/tests.rs:2023 dead_node_refute`.
#[test]
fn dead_node_refute() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(23061));

  let local_id = c.local_id(m1).unwrap();
  let inc_before = c.local_incarnation(m1).unwrap();

  // Dead about ourselves (node == local, from == local).
  // Since the endpoint is not in Leaving state, it must refute.
  let d = Dead::new(inc_before, local_id.clone(), local_id.clone());
  c.dead_node(m1, d);

  // State must remain Alive.
  assert_eq!(
    c.get_node_state(m1, &local_id),
    Some(State::Alive),
    "local node should remain Alive after refute"
  );

  // Incarnation must have been bumped.
  let inc_after = c.local_incarnation(m1).unwrap();
  assert!(
    inc_after > inc_before,
    "refute must bump local incarnation: before={inc_before}, after={inc_after}"
  );

  // The broadcast queue must contain an Alive refute.
  let broadcasts = c.drain_broadcasts(m1);
  assert!(
    broadcasts.iter().any(|m| matches!(m, Message::Alive(_))),
    "refute must queue an Alive broadcast; got: {broadcasts:?}"
  );
}

// ── Skipped tests ──────────────────────────────────────────────────────────────
//
// All seven dead-family scenarios map cleanly onto the public `Cluster` API.
// No tests were skipped.
//
// **dead_node_double invariant note**:
//   The legacy test uses `d.set_incarnation(2)` to re-send the Dead with a
//   higher incarnation; the simulation passes `Dead::new(2, ...)` directly.
//   The end invariant is the same: `State::Dead` guards prevent re-transition.
//
// **dead_node_self_marked — a self-marked Dead records reclaim-protected Dead**:
//   `Dead::new(inc, peer_id, peer_id)` (node == from) → `State::Dead`,
//   reclaim-protected by `dead_node_reclaim_time`.
//   `Dead::new(inc, peer_id, other_id)` (node != from) → `State::Dead`.
//   A remote self-marked departure is unattributable in plaintext gossip, so it
//   is never granted the immediately-reclaimable `State::Left`; `Left` is
//   reserved for the local node's view of itself leaving.
