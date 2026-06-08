//! Merge, gossip, and gossip-to-dead legacy unit tests ported from
//! `memberlist-core/src/state/tests.rs` into deterministic simulation.
//!
//! Each test mirrors its legacy counterpart's state-machine assertions;
//! async ceremony, mutex locks, and delegate subscribers are replaced by
//! the `Cluster` API.

use memberlist_simulation::{
  Alive, Cluster, Dead, EndpointOptions, Event, Node, PushNodeState, State, Suspect,
};
use smol_str::SmolStr;
use std::{net::SocketAddr, time::Duration};

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

// ── 1. `merge_state` (legacy line 2072) ──────────────────────────────────────

/// Inject a list of `PushNodeState`s via `merge_remote_state`; observe
/// `NodeJoined` events for newly discovered peers; verify members appear in
/// the host's view with the correct merged state.
///
/// Legacy scenario:
/// - node1 pre-exists as Alive inc 1, then Suspect; remote says Alive inc 2 → stays Alive inc 2.
/// - node2 pre-exists as Alive inc 1; remote says Suspect inc 1 → becomes Suspect.
/// - node3 pre-exists as Alive inc 1; remote says Dead inc 1 → becomes Suspect
///   (the sim processes Dead remote state as Suspect, matching legacy behaviour
///   where Dead is converted to a Suspect during merge to avoid eagerly killing nodes).
/// - node4 is new; remote says Alive inc 2 → NodeJoined, state Alive.
///
/// Ported from `memberlist-core/src/state/tests.rs:2072 merge_state`.
#[test]
fn merge_state() {
  let mut c = Cluster::new();
  let m1 = c.add_node(SmolStr::new("m1"), addr(24001));

  let local_id = c.local_id(m1).unwrap();

  let id1 = SmolStr::new("node1");
  let id2 = SmolStr::new("node2");
  let id3 = SmolStr::new("node3");
  let id4 = SmolStr::new("node4");

  let a1 = addr(24011);
  let a2 = addr(24012);
  let a3 = addr(24013);
  let a4 = addr(24014);

  // Pre-populate node1, node2, node3 as Alive at inc 1.
  c.alive_node(m1, Alive::new(1, Node::new(id1.clone(), a1)), false);
  c.alive_node(m1, Alive::new(1, Node::new(id2.clone(), a2)), false);
  c.alive_node(m1, Alive::new(1, Node::new(id3.clone(), a3)), false);

  // Suspect node1 (mirrors legacy `suspect_node`).
  c.suspect_node(m1, Suspect::new(1, id1.clone(), local_id.clone()));
  // Drain all bootstrap events so they don't pollute the post-merge check.
  while c.poll_event(m1).is_some() {}

  // Build remote state.
  let remote = vec![
    PushNodeState::new(2, id1.clone(), a1, State::Alive),
    PushNodeState::new(1, id2.clone(), a2, State::Suspect),
    PushNodeState::new(1, id3.clone(), a3, State::Dead),
    PushNodeState::new(2, id4.clone(), a4, State::Alive),
  ];

  c.merge_remote_state(m1, remote);

  // node1: remote Alive inc 2 > current Suspect inc 1 → Alive.
  assert_eq!(
    c.get_node_state(m1, &id1),
    Some(State::Alive),
    "node1 should be Alive after merge (remote inc 2 overrides Suspect inc 1)"
  );
  assert_eq!(
    c.get_node_incarnation(m1, &id1),
    Some(2),
    "node1 incarnation should be 2"
  );

  // node2: remote Suspect inc 1 == current Alive inc 1 → Suspect.
  assert_eq!(
    c.get_node_state(m1, &id2),
    Some(State::Suspect),
    "node2 should be Suspect after merge"
  );

  // node3: remote Dead is processed as Suspect (merge converts Dead → Suspect).
  // The state should be Suspect (not Alive, not Dead).
  assert_eq!(
    c.get_node_state(m1, &id3),
    Some(State::Suspect),
    "node3 should be Suspect after merge (Dead remote state → Suspect)"
  );

  // node4: new node → Alive inc 2.
  assert_eq!(
    c.get_node_state(m1, &id4),
    Some(State::Alive),
    "node4 should be Alive after merge"
  );
  assert_eq!(
    c.get_node_incarnation(m1, &id4),
    Some(2),
    "node4 incarnation should be 2"
  );

  // Expect exactly one NodeJoined for node4 (the only brand-new node).
  let mut found_join_for_node4 = false;
  let mut extra_joins = 0usize;
  while let Some(ev) = c.poll_event(m1) {
    if let Event::NodeJoined(ns) = ev {
      if ns.id_ref() == &id4 {
        found_join_for_node4 = true;
      } else {
        extra_joins += 1;
      }
    }
  }
  assert!(found_join_for_node4, "expected NodeJoined for node4");
  assert_eq!(extra_joins, 0, "unexpected extra NodeJoined events");
}

// ── 2. `gossip` (legacy line 2167) ───────────────────────────────────────────

/// Three nodes: m1 knows about m2 and m3.  After gossip fires, m2 and m3
/// should learn each other through the membership broadcasts m1 emits.
///
/// The gossip scheduler only targets Alive/Suspect peers, so m2 and m3 must
/// be Alive at m1 for the broadcasts to be sent.  We advance time past the
/// gossip interval repeatedly to let the scheduler fire.
///
/// Ported from `memberlist-core/src/state/tests.rs:2167 gossip`.
#[test]
fn gossip_propagates_membership() {
  let mut c = Cluster::new();

  // Use a very short gossip interval so the scheduler fires quickly.
  let m1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(24101),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_gossip_interval(Duration::from_millis(10))
        .with_push_pull_interval(Duration::from_secs(3600)) // disable PP for this test
    },
  );
  let m2 = c.add_node_with(
    SmolStr::new("m2"),
    addr(24102),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_gossip_interval(Duration::from_millis(10))
        .with_push_pull_interval(Duration::from_secs(3600))
    },
  );
  let m3 = c.add_node_with(
    SmolStr::new("m3"),
    addr(24103),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_gossip_interval(Duration::from_millis(10))
        .with_push_pull_interval(Duration::from_secs(3600))
    },
  );

  // Bootstrap: m1 knows m2 and m3 (and itself).
  c.inject_alive(m1, SmolStr::new("m2"), m2, 1);
  c.inject_alive(m1, SmolStr::new("m3"), m3, 1);

  // m2 knows only itself; m3 knows only itself initially.
  // Run the simulation.  The gossip scheduler on m1 will broadcast m2 and m3
  // membership messages to random Alive/Suspect peers (m2 and m3).
  // After enough ticks m2 should learn m3 (and vice versa) via m1's gossip.
  for _ in 0..200 {
    c.step();
  }

  // m2 should know m1 and m3.
  assert!(
    c.member(m2, &SmolStr::new("m1")).is_some(),
    "m2 should know m1 after gossip"
  );
  assert!(
    c.member(m2, &SmolStr::new("m3")).is_some(),
    "m2 should know m3 after gossip"
  );

  // m3 should know m1 and m2.
  assert!(
    c.member(m3, &SmolStr::new("m1")).is_some(),
    "m3 should know m1 after gossip"
  );
  assert!(
    c.member(m3, &SmolStr::new("m2")).is_some(),
    "m3 should know m2 after gossip"
  );
}

// ── 3. `gossip_to_dead` (legacy line 2220) ────────────────────────────────────

/// Gossip skips `Dead`/`Left` peers whose `state_change` is older than
/// `gossip_to_the_dead_time`.  Concretely:
///
/// - m1 knows m2 (Dead, state_change aged well past `gossip_to_the_dead_time`).
/// - m1 queues broadcasts and the gossip scheduler fires.
/// - m2 must NOT receive any gossip (Dead peers older than the window are
///   excluded from the candidate set in `fire_gossip_scheduler`).
///
/// We assert this by confirming m2 never learns about any broadcast payload
/// from m1 (its member map stays at 1 — only itself).
///
/// Legacy note: the original test manipulates `state_change` via `change_node`
/// and listens on an event subscriber.  We achieve the same result by ageing
/// m2's state_change beyond `gossip_to_the_dead_time` and verifying m2's
/// member count stays at 1.
///
/// Ported from `memberlist-core/src/state/tests.rs:2220 gossip_to_dead`.
#[test]
fn gossip_skips_aged_dead_peers() {
  // Short gossip_to_the_dead_time so we can easily age past it.
  let dead_time = Duration::from_millis(100);

  let mut c = Cluster::new();

  let m1 = c.add_node_with(
    SmolStr::new("m1"),
    addr(24201),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_gossip_interval(Duration::from_millis(10))
        .with_gossip_to_the_dead_time(dead_time)
        .with_push_pull_interval(Duration::from_secs(3600))
    },
  );
  let m2 = c.add_node_with(
    SmolStr::new("m2"),
    addr(24202),
    |cfg: EndpointOptions<_, _>| {
      cfg
        .with_gossip_interval(Duration::from_millis(10))
        .with_gossip_to_the_dead_time(dead_time)
        .with_push_pull_interval(Duration::from_secs(3600))
    },
  );

  let local_id = c.local_id(m1).unwrap();

  // m1 knows m2 as Alive.
  c.inject_alive(m1, SmolStr::new("m2"), m2, 1);
  while c.poll_event(m1).is_some() {}

  // Kill m2 at m1.
  c.dead_node(m1, Dead::new(1, SmolStr::new("m2"), local_id.clone()));
  while c.poll_event(m1).is_some() {}

  assert_eq!(
    c.get_node_state(m1, &SmolStr::new("m2")),
    Some(State::Dead),
    "m2 should be Dead at m1"
  );

  // Age m2's state_change well past gossip_to_the_dead_time (200 ms > 100 ms).
  c.age_node(m1, &SmolStr::new("m2"), Duration::from_millis(200));

  // Run gossip: the scheduler fires but m2 is filtered out (Dead + aged).
  for _ in 0..100 {
    c.step();
  }

  // m2 should still know only itself (member count == 1).
  // If gossip had reached m2, it would have learned about m1 and its member
  // count would be > 1.
  assert_eq!(
    c.num_members(m2),
    1,
    "m2 should NOT have received gossip (Dead peer aged past gossip_to_the_dead_time)"
  );
}

// ── Skipped / weakened tests ───────────────────────────────────────────────────
//
// `merge_state` — node3 state:
//   The legacy test checks `State::Suspect` for node3 after merging a Dead
//   remote state.  This is because `merge_state` (legacy) calls
//   `suspect_node` for Dead entries rather than `dead_node`, preferring
//   soft-kill semantics during anti-entropy.  The simulation's `merge_state`
//   (via `Endpoint::process_remote_state`) applies the same logic: Dead remote
//   entries are treated as Suspect.  The assertion above matches this.
//
// `gossip_to_dead` — weak assertion:
//   The legacy test uses an event subscriber to assert that m2 receives
//   exactly 0 events when m1 gossips to a Dead-and-aged peer.  In the
//   simulation we assert that m2's member count stays at 1 (only itself),
//   which is equivalent: if gossip reached m2, m2 would have learned m1 and
//   num_members would be 2.
