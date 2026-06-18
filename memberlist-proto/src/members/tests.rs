use super::*;
use crate::typed::{Meta, NodeState, State};
use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};
use rand::{SeedableRng, rngs::SmallRng};
use smol_str::SmolStr;
use std::panic::AssertUnwindSafe;

fn make_node(id: &str, port: u16, st: State) -> NodeState<SmolStr, SocketAddr> {
  NodeState::new(
    SmolStr::new(id),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
    st,
  )
  .with_meta(Meta::empty())
}

fn member(id: &str, port: u16, st: State) -> Member<SmolStr, SocketAddr> {
  let now = Instant::now();
  Member::new(LocalNodeState::new(make_node(id, port, st), now))
}

fn local_node() -> Node<SmolStr, SocketAddr> {
  Node::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6999),
  )
}

#[test]
fn local_node_state_state_change_updates_on_set_state() {
  let now = Instant::now();
  let mut s = LocalNodeState::new(make_node("a", 7000, State::Alive), now);
  assert_eq!(s.state(), State::Alive);
  let later = now + Duration::from_secs(1);
  s.set_state(State::Suspect, later);
  assert_eq!(s.state(), State::Suspect);
  assert_eq!(s.state_change(), later);
}

#[test]
fn local_node_state_dead_or_left() {
  let now = Instant::now();
  let alive = LocalNodeState::new(make_node("a", 7000, State::Alive), now);
  assert!(!alive.dead_or_left());

  let mut node = LocalNodeState::new(make_node("b", 7001, State::Alive), now);
  node.set_state(State::Dead, now);
  assert!(node.dead_or_left());
  node.set_state(State::Left, now);
  assert!(node.dead_or_left());
  node.set_state(State::Suspect, now);
  assert!(!node.dead_or_left());
}

#[test]
fn member_starts_with_no_suspicion() {
  let now = Instant::now();
  let s = LocalNodeState::new(make_node("a", 7000, State::Suspect), now);
  let m = Member::new(s);
  assert!(m.suspicion().is_none());
}

#[test]
fn members_starts_empty() {
  let m = Members::<SmolStr, SocketAddr>::new(local_node());
  assert_eq!(m.len(), 0);
  assert!(m.is_empty());
  assert!(!m.any_alive());
}

#[test]
fn insert_then_get_and_contains() {
  let mut m = Members::new(local_node());
  m.insert(member("alice", 7000, State::Alive));
  assert_eq!(m.len(), 1);
  assert!(m.contains(&SmolStr::new("alice")));
  assert!(m.get(&SmolStr::new("alice")).is_some());
  assert!(m.get(&SmolStr::new("missing")).is_none());
}

#[test]
fn insert_duplicate_returns_old() {
  let mut m = Members::new(local_node());
  let alive = member("alice", 7000, State::Alive);
  let suspect = member("alice", 7000, State::Suspect);
  assert!(m.insert(alive).is_none());
  let old = m.insert(suspect).expect("expected old member returned");
  assert_eq!(old.state_ref().state(), State::Alive);
  assert_eq!(m.len(), 1);
  assert_eq!(
    m.get(&SmolStr::new("alice")).unwrap().state_ref().state(),
    State::Suspect,
  );
}

#[test]
fn insert_at_random_keeps_node_map_consistent() {
  let mut m = Members::new(local_node());
  let mut rng = SmallRng::seed_from_u64(42);
  for i in 0..20u16 {
    let id = format!("node{i}");
    m.insert_at_random(member(&id, 7000 + i, State::Alive), &mut rng);
  }
  for (id, idx) in m.node_map.iter() {
    assert_eq!(m.nodes[*idx].state_ref().id_ref(), id, "invariant violated");
  }
  assert_eq!(m.len(), 20);
}

#[test]
fn insert_at_random_at_specific_offset_works() {
  let mut m = Members::new(local_node());
  m.insert(member("alice", 7000, State::Alive));
  m.insert(member("bob", 7001, State::Alive));
  m.insert(member("carol", 7002, State::Alive));
  // Insert "dan" at offset 1 — should displace "bob" to position 3.
  m.insert_at_random_at(member("dan", 7003, State::Alive), 1);
  assert_eq!(m.len(), 4);
  // Verify the invariant.
  for (id, idx) in m.node_map.iter() {
    assert_eq!(m.nodes[*idx].state_ref().id_ref(), id, "invariant violated");
  }
  // dan should be at slot 1.
  assert_eq!(*m.node_map.get(&SmolStr::new("dan")).unwrap(), 1);
}

#[test]
fn remove_returns_member_and_shifts_indices() {
  let mut m = Members::new(local_node());
  m.insert(member("a", 7000, State::Alive));
  m.insert(member("b", 7001, State::Alive));
  m.insert(member("c", 7002, State::Alive));
  let removed = m.remove(&SmolStr::new("b")).expect("b present");
  assert_eq!(removed.state_ref().id_ref(), &SmolStr::new("b"));
  assert_eq!(m.len(), 2);
  let c_idx = *m.node_map.get(&SmolStr::new("c")).unwrap();
  assert_eq!(c_idx, 1);
  assert_eq!(m.nodes[c_idx].state_ref().id_ref(), &SmolStr::new("c"));
}

#[test]
fn shuffle_keeps_node_map_consistent() {
  let mut m = Members::new(local_node());
  let mut rng = SmallRng::seed_from_u64(99);
  for i in 0..50u16 {
    let id = format!("node{i}");
    m.insert(member(&id, 7000 + i, State::Alive));
  }
  m.shuffle(&mut rng);
  for (id, idx) in m.node_map.iter() {
    assert_eq!(m.nodes[*idx].state_ref().id_ref(), id);
  }
  assert_eq!(m.len(), 50);
}

#[test]
fn insert_at_random_at_offset_zero_works() {
  let mut m = Members::new(local_node());
  m.insert(member("alice", 7000, State::Alive));
  m.insert(member("bob", 7001, State::Alive));
  m.insert_at_random_at(member("zach", 7777, State::Alive), 0);
  assert_eq!(*m.node_map.get(&SmolStr::new("zach")).unwrap(), 0);
  for (id, idx) in m.node_map.iter() {
    assert_eq!(m.nodes[*idx].state_ref().id_ref(), id);
  }
}

#[test]
fn insert_at_random_at_offset_len_works() {
  let mut m = Members::new(local_node());
  m.insert(member("alice", 7000, State::Alive));
  m.insert(member("bob", 7001, State::Alive));
  // n == 2, offset == 2 ⇒ no swap, lands at end.
  m.insert_at_random_at(member("zach", 7777, State::Alive), 2);
  assert_eq!(*m.node_map.get(&SmolStr::new("zach")).unwrap(), 2);
  for (id, idx) in m.node_map.iter() {
    assert_eq!(m.nodes[*idx].state_ref().id_ref(), id);
  }
}

#[test]
fn insert_at_random_at_into_empty_works() {
  let mut m = Members::new(local_node());
  m.insert_at_random_at(member("alice", 7000, State::Alive), 0);
  assert_eq!(m.len(), 1);
  assert_eq!(*m.node_map.get(&SmolStr::new("alice")).unwrap(), 0);
}

#[test]
fn any_alive_excludes_local_and_dead() {
  let mut m = Members::new(local_node());
  m.insert(member("local", 6999, State::Alive));
  assert!(!m.any_alive(), "local-only cluster reports any_alive");

  let mut dead = member("alice", 7000, State::Alive);
  dead.state_mut().set_state(State::Dead, Instant::now());
  m.insert(dead);
  assert!(!m.any_alive());

  m.insert(member("bob", 7001, State::Alive));
  assert!(m.any_alive());
}

#[test]
fn local_node_state_server_accessors_and_replace() {
  let now = Instant::now();
  let mut s = LocalNodeState::new(make_node("a", 7000, State::Alive), now);
  assert_eq!(s.server_ref().id_ref(), &SmolStr::new("a"));
  assert_eq!(s.id_ref(), &SmolStr::new("a"));
  assert_eq!(
    s.address_ref(),
    &SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000)
  );
  let arc = s.server_arc();
  assert!(Arc::ptr_eq(&arc, &s.server_arc()));

  // Replacing the shared NodeState swaps the visible identity.
  let replacement = Arc::new(make_node("a2", 7100, State::Alive));
  s.set_server(replacement.clone());
  assert!(Arc::ptr_eq(&s.server_arc(), &replacement));
  assert_eq!(s.id_ref(), &SmolStr::new("a2"));
}

#[test]
fn local_node_state_incarnation_and_state_change_mutators() {
  let now = Instant::now();
  let mut s = LocalNodeState::new(make_node("a", 7000, State::Alive), now);
  assert_eq!(s.incarnation(), 0);
  s.set_incarnation(42);
  assert_eq!(s.incarnation(), 42);

  assert_eq!(s.state_change(), now);
  let later = now + Duration::from_secs(3);
  s.set_state_change(later);
  assert_eq!(s.state_change(), later);
  // set_state_change must not disturb the liveness state itself.
  assert_eq!(s.state(), State::Alive);
}

fn suspicion(from: &str) -> Suspicion<SmolStr> {
  Suspicion::new(
    SmolStr::new(from),
    3,
    Duration::from_millis(100),
    Duration::from_millis(500),
    Instant::now(),
  )
}

#[test]
fn member_suspicion_lifecycle() {
  let now = Instant::now();
  let s = LocalNodeState::new(make_node("a", 7000, State::Suspect), now);

  // with_suspicion attaches at construction.
  let mut m = Member::new(s).with_suspicion(suspicion("watcher"));
  assert!(m.suspicion().is_some());
  assert_eq!(m.suspicion().unwrap().k(), 3);

  // suspicion_mut allows mutation in place.
  assert!(m.suspicion_mut().is_some());

  // set_suspicion(None) clears it.
  m.set_suspicion(None);
  assert!(m.suspicion().is_none());

  // set_suspicion(Some) restores, take_suspicion removes and returns it.
  m.set_suspicion(Some(suspicion("watcher2")));
  let taken = m.take_suspicion().expect("suspicion present");
  assert_eq!(taken.k(), 3);
  assert!(m.suspicion().is_none(), "take must clear");
}

#[test]
fn member_state_ref_and_state_mut() {
  let now = Instant::now();
  let mut m = Member::new(LocalNodeState::new(make_node("a", 7000, State::Alive), now));
  assert_eq!(m.state_ref().state(), State::Alive);
  m.state_mut().set_state(State::Suspect, now);
  assert_eq!(m.state_ref().state(), State::Suspect);
}

#[test]
fn members_local_ref_and_get_mut() {
  let mut m = Members::new(local_node());
  assert_eq!(m.local_ref().id_ref(), &SmolStr::new("local"));
  m.insert(member("alice", 7000, State::Alive));
  let alice = m.get_mut(&SmolStr::new("alice")).expect("alice present");
  alice.state_mut().set_state(State::Dead, Instant::now());
  assert_eq!(
    m.get(&SmolStr::new("alice")).unwrap().state_ref().state(),
    State::Dead
  );
  assert!(m.get_mut(&SmolStr::new("missing")).is_none());
}

#[test]
fn members_iter_and_iter_mut() {
  let mut m = Members::new(local_node());
  m.insert(member("a", 7000, State::Alive));
  m.insert(member("b", 7001, State::Alive));
  assert_eq!(m.iter().count(), 2);
  for member in m.iter_mut() {
    member.state_mut().set_state(State::Suspect, Instant::now());
  }
  assert!(
    m.iter()
      .all(|mem| mem.state_ref().state() == State::Suspect)
  );
}

#[test]
fn insert_at_random_at_panics_on_duplicate_id() {
  let mut m = Members::new(local_node());
  m.insert(member("alice", 7000, State::Alive));
  let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
    m.insert_at_random_at(member("alice", 7000, State::Alive), 0);
  }));
  assert!(result.is_err(), "duplicate id must panic");
}

#[test]
fn insert_at_random_at_panics_on_out_of_range_target() {
  let mut m = Members::new(local_node());
  m.insert(member("alice", 7000, State::Alive));
  // len is 1; target 5 is out of `0..=len`.
  let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
    m.insert_at_random_at(member("bob", 7001, State::Alive), 5);
  }));
  assert!(result.is_err(), "out-of-range target must panic");
}

#[test]
fn insert_at_random_into_empty_lands_at_zero() {
  let mut m = Members::new(local_node());
  let mut rng = SmallRng::seed_from_u64(7);
  m.insert_at_random(member("solo", 7000, State::Alive), &mut rng);
  assert_eq!(m.len(), 1);
  assert_eq!(*m.node_map.get(&SmolStr::new("solo")).unwrap(), 0);
}

#[test]
fn remove_absent_id_returns_none() {
  let mut m = Members::new(local_node());
  m.insert(member("a", 7000, State::Alive));
  assert!(m.remove(&SmolStr::new("ghost")).is_none());
  assert_eq!(m.len(), 1);
}
