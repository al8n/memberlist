//! Suspect-family legacy unit tests ported from
//! `memberlist-core/src/state/tests.rs` into deterministic simulation.
//!
//! Each test mirrors its legacy counterpart's state-machine assertions;
//! async ceremony, mutex locks, and delegate subscribers are replaced by
//! the `Cluster` API.
//!
//! **Broadcast-count note**: The simulation `Endpoint` pre-queues one Alive
//! broadcast for the local node at creation time (legacy does not do this at
//! the equivalent stage). All `broadcast_queue_len` assertions therefore use
//! `>= 1` rather than `== 1` where the legacy asserts exactly 1.

use memberlist_proto::typed::Message;
use memberlist_simulation::{Alive, Cluster, EndpointOptions, Node, State, Suspect};
use smol_str::SmolStr;
use std::{net::SocketAddr, time::Duration};

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

// ── 1. `suspect_node_no_node` (legacy line 1438) ──────────────────────────────

/// Inject a Suspect for a peer ID that has never been seen.
///
/// Asserts:
/// - State map still has 0 non-local entries (no phantom node inserted).
///
/// Ported from `memberlist-core/src/state/tests.rs:1438 suspect_node_no_node`.
#[test]
fn suspect_node_no_node() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(22001));

  let unknown_id = SmolStr::new("unknown_peer");
  let local_id = c.local_id(m1).unwrap();

  let s = Suspect::new(1, unknown_id, local_id);
  c.suspect_node(m1, s);

  // Only the local node exists; the unknown peer must not have been inserted.
  assert_eq!(
    c.num_members(m1),
    1,
    "unknown peer must not be inserted on Suspect"
  );
}

// ── 2. `suspect_node` (legacy line 1464) ─────────────────────────────────────

/// Alive peer transitions Alive → Suspect → Dead after suspicion timeout.
///
/// Steps:
/// 1. Join peer as Alive (inc 1).
/// 2. Age peer's state_change by 3600 s (mirror of legacy `change_node`).
/// 3. Inject Suspect → state becomes Suspect, Suspect broadcast queued.
/// 4. Advance time past suspicion timeout → state becomes Dead, Dead broadcast queued.
///
/// Ported from `memberlist-core/src/state/tests.rs:1464 suspect_node`.
#[test]
fn suspect_node_alive_to_suspect_to_dead() {
  let mut c = Cluster::new();
  let m1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(22011),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_suspicion_mult(1)
        .with_probe_interval(Duration::from_millis(1))
    },
  );

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(22099);
  let local_id = c.local_id(m1).unwrap();

  // 1. Join peer.
  c.alive_node(
    m1,
    Alive::new(1, Node::new(peer_id.clone(), peer_addr)),
    false,
  );

  // 2. Age the peer so the state_change is well in the past (mirrors legacy
  //    `change_node(|s| s.state_change -= 3600s)`).
  c.age_node(m1, &peer_id, Duration::from_secs(3600));

  // 3. Inject Suspect.
  c.suspect_node(m1, Suspect::new(1, peer_id.clone(), local_id));

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Suspect),
    "peer should be Suspect after suspect_node"
  );

  // A Suspect broadcast must be queued.
  let broadcasts = c.drain_broadcasts(m1);
  assert!(
    broadcasts.iter().any(|m| matches!(m, Message::Suspect(_))),
    "expected a Suspect broadcast after suspect_node; got: {broadcasts:?}"
  );

  // 4. Advance past suspicion timeout so the timer fires.
  c.advance(Duration::from_secs(10));

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Dead),
    "peer should be Dead after suspicion timeout"
  );

  // A Dead broadcast must be queued.
  let broadcasts = c.drain_broadcasts(m1);
  assert!(
    broadcasts.iter().any(|m| matches!(m, Message::Dead(_))),
    "expected a Dead broadcast after suspicion timeout; got: {broadcasts:?}"
  );
}

// ── 3. `suspect_node_double_suspect` (legacy line 1548) ───────────────────────

/// Suspecting a peer twice does not restart the timer or emit a second broadcast.
///
/// Steps:
/// 1. Join peer as Alive (inc 1), age it, inject first Suspect → Suspect state.
/// 2. Inject the same Suspect again.
/// 3. Assert: state_change unchanged, no new broadcast (queue grows by at most
///    one confirm broadcast, which only fires if the confirmer is different).
///
/// Ported from `memberlist-core/src/state/tests.rs:1548 suspect_node_double_suspect`.
#[test]
fn suspect_node_double_suspect() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(22021));

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(22098);
  let local_id = c.local_id(m1).unwrap();

  // 1. Join peer and age it.
  c.alive_node(
    m1,
    Alive::new(1, Node::new(peer_id.clone(), peer_addr)),
    false,
  );
  c.age_node(m1, &peer_id, Duration::from_secs(3600));

  // First Suspect → Suspect state.
  c.suspect_node(m1, Suspect::new(1, peer_id.clone(), local_id.clone()));
  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Suspect),
    "peer should be Suspect after first suspect"
  );

  let change_before = c
    .get_node_state_change(m1, &peer_id)
    .expect("state_change should exist");

  // Record queue length before second Suspect.
  let bq_before_second = c.broadcast_queue_len(m1);

  // 2. Second Suspect from the same from-id → confirm path, Confirmation::Ignored
  //    (same from; no new broadcast). state_change must be unchanged.
  c.suspect_node(m1, Suspect::new(1, peer_id.clone(), local_id));
  let change_after = c
    .get_node_state_change(m1, &peer_id)
    .expect("state_change should exist");
  assert_eq!(
    change_before, change_after,
    "state_change must not advance on double-suspect"
  );

  // Queue length must not have grown (Ignored confirmation does not re-broadcast).
  let bq_after_second = c.broadcast_queue_len(m1);
  assert_eq!(
    bq_before_second, bq_after_second,
    "double-suspect from same from-id must not enqueue a new broadcast: before={bq_before_second}, after={bq_after_second}"
  );
}

// ── 4. `suspect_node_old_suspect` (legacy line 1608) ─────────────────────────

/// Suspect with strictly lower incarnation than the tracked peer is ignored.
///
/// **Legacy-vs-simulation note**: The original `suspect_node_old_suspect` test
/// sets up a peer at incarnation 1 and sends a Suspect also at incarnation 1,
/// but it manipulates `state_change` to simulate the node being "old".  The
/// core suspicion logic in both legacy and simulation guards on
/// `suspect_incarnation < peer_incarnation` (strict less-than), not on
/// state_change age.  A Suspect at the *same* incarnation still causes
/// transition.  The true "old incarnation ignored" invariant is:
///   peer at inc 5, Suspect at inc 1 → still Alive, no broadcast.
/// This port exercises that invariant directly.
///
/// Ported from `memberlist-core/src/state/tests.rs:1608 suspect_node_old_suspect`.
#[test]
fn suspect_node_old_suspect_ignored() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(22031));

  let peer_id = SmolStr::new("test_node");
  let peer_addr = addr(22097);
  let local_id = c.local_id(m1).unwrap();

  // Join peer at incarnation 5.
  c.alive_node(
    m1,
    Alive::new(5, Node::new(peer_id.clone(), peer_addr)),
    false,
  );
  // Age the peer so it is not protected by any recency guard.
  c.age_node(m1, &peer_id, Duration::from_secs(3600));

  // Record queue length before old-incarnation Suspect.
  let bq_before = c.broadcast_queue_len(m1);

  // Suspect with old incarnation (1 < 5) → must be ignored.
  c.suspect_node(m1, Suspect::new(1, peer_id.clone(), local_id));

  assert_eq!(
    c.get_node_state(m1, &peer_id),
    Some(State::Alive),
    "old-incarnation Suspect must not change state"
  );
  let bq_after = c.broadcast_queue_len(m1);
  assert_eq!(
    bq_before, bq_after,
    "old-incarnation Suspect must not enqueue a new broadcast: before={bq_before}, after={bq_after}"
  );
}

// ── 5. `suspect_node_refute` (legacy line 1657) ───────────────────────────────

/// A Suspect about the LOCAL node causes the node to refute (bump incarnation)
/// and queue an Alive broadcast.  The node remains Alive throughout.
///
/// Asserts:
/// - `local_incarnation` increases after the Suspect.
/// - At least one `Message::Alive` appears in the broadcast queue.
/// - Node state remains Alive.
///
/// Ported from `memberlist-core/src/state/tests.rs:1657 suspect_node_refute`.
#[test]
fn suspect_node_refute() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(22041));

  let local_id = c.local_id(m1).unwrap();
  let inc_before = c.local_incarnation(m1).unwrap();

  // Suspect about ourselves.
  let s = Suspect::new(inc_before, local_id.clone(), local_id.clone());
  c.suspect_node(m1, s);

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
// No suspect-family tests were fully skipped.  See the note on
// `suspect_node_old_suspect` above for the one case where the port exercises
// a semantically equivalent invariant rather than the exact legacy setup.
//
// **Broadcast-count relaxation** (noted for future hardening):
//   Legacy tests assert `broadcast_queue_len == 1` because the legacy
//   memberlist does not pre-queue a self-Alive at construction time.
//   The simulation `Endpoint::new` pre-queues one self-Alive broadcast.
//   `suspect_node_refute` therefore asserts via `drain_broadcasts` rather
//   than an exact queue count.
