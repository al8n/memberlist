//! Cluster membership state.

use crate::Instant;
use std::sync::Arc;

use crate::{
  Id, Node,
  typed::{NodeState, State},
};
use indexmap::IndexMap;
use rand::{Rng, RngExt};
use smallvec::SmallVec;

use crate::suspicion::Suspicion;

/// Local extension of a peer's wire-protocol [`NodeState`]: adds the bookkeeping
/// fields the gossip state machine mutates over time (incarnation, current
/// liveness state, last state-change timestamp).
///
/// `server` is `Arc`-wrapped so it can be cheaply cloned into outgoing
/// `Event`s without copying the underlying NodeState bytes.
#[derive(Debug, Clone)]
pub struct LocalNodeState<I, A> {
  server: Arc<NodeState<I, A>>,
  incarnation: u32,
  state_change: Instant,
  state: State,
}

impl<I, A> LocalNodeState<I, A> {
  /// Construct from a fresh `NodeState`. The initial liveness state is
  /// taken from `node.state()`.
  pub fn new(node: NodeState<I, A>, now: Instant) -> Self {
    let state = node.state();
    Self {
      server: Arc::new(node),
      incarnation: 0,
      state_change: now,
      state,
    }
  }

  /// The shared NodeState (id, address, meta, versions, etc.).
  #[inline(always)]
  pub fn server_ref(&self) -> &NodeState<I, A> {
    &self.server
  }

  /// Return a cheap clone of the shared NodeState arc.
  #[inline(always)]
  pub fn server_arc(&self) -> Arc<NodeState<I, A>> {
    self.server.clone()
  }

  /// Replace the underlying NodeState (e.g. on metadata update).
  #[inline(always)]
  pub fn set_server(&mut self, server: Arc<NodeState<I, A>>) {
    self.server = server;
  }

  /// Current monotonic incarnation number.
  #[inline(always)]
  pub const fn incarnation(&self) -> u32 {
    self.incarnation
  }

  /// Set the incarnation. Caller is responsible for ensuring the new value
  /// is monotonically greater than or equal to the old.
  #[inline(always)]
  pub const fn set_incarnation(&mut self, inc: u32) {
    self.incarnation = inc;
  }

  /// Current liveness state.
  #[inline(always)]
  pub const fn state(&self) -> State {
    self.state
  }

  /// When the liveness state last changed.
  #[inline(always)]
  pub const fn state_change(&self) -> Instant {
    self.state_change
  }

  /// Transition to a new liveness state. The state-change timestamp is updated.
  #[inline(always)]
  pub const fn set_state(&mut self, state: State, now: Instant) {
    self.state = state;
    self.state_change = now;
  }

  /// Overwrite the state-change timestamp directly.
  ///
  /// Only intended for test helpers that age a node's state without sleeping
  /// (e.g. `Cluster::age_node`). Normal gossip code should use [`set_state`].
  #[inline(always)]
  pub const fn set_state_change(&mut self, t: Instant) {
    self.state_change = t;
  }

  /// True iff the node is `Dead` or `Left`.
  #[inline(always)]
  pub const fn dead_or_left(&self) -> bool {
    matches!(self.state, State::Dead | State::Left)
  }

  /// Convenience: peer id (forwarded from the underlying `NodeState`).
  /// Not `const fn` because it goes through `Arc::deref`, which is not const.
  #[inline(always)]
  pub fn id_ref(&self) -> &I {
    self.server.id_ref()
  }

  /// Convenience: peer address (forwarded from the underlying `NodeState`).
  /// Not `const fn` because it goes through `Arc::deref`, which is not const.
  #[inline(always)]
  pub fn address_ref(&self) -> &A {
    self.server.address_ref()
  }
}

/// One entry in the [`Members`] container: the local view of a peer's state
/// plus the optional active suspicion timer for that peer.
#[derive(Debug)]
pub struct Member<I, A> {
  state: LocalNodeState<I, A>,
  suspicion: Option<Suspicion<I>>,
}

impl<I, A> Member<I, A> {
  /// Construct a member with no active suspicion.
  pub const fn new(state: LocalNodeState<I, A>) -> Self {
    Self {
      state,
      suspicion: None,
    }
  }

  /// Local view of the peer (id, address, incarnation, liveness, …).
  #[inline(always)]
  pub const fn state_ref(&self) -> &LocalNodeState<I, A> {
    &self.state
  }

  /// Mutable view of the local peer state — used by the gossip state machine
  /// to apply incarnation bumps and liveness transitions.
  #[inline(always)]
  pub const fn state_mut(&mut self) -> &mut LocalNodeState<I, A> {
    &mut self.state
  }

  /// Active suspicion timer, if any.
  #[inline(always)]
  pub const fn suspicion(&self) -> Option<&Suspicion<I>> {
    self.suspicion.as_ref()
  }

  /// Mutable view of the active suspicion timer, if any.
  #[inline(always)]
  pub const fn suspicion_mut(&mut self) -> Option<&mut Suspicion<I>> {
    self.suspicion.as_mut()
  }

  /// Replace the suspicion timer (`Some` to start one, `None` to clear).
  #[inline(always)]
  pub fn set_suspicion(&mut self, s: Option<Suspicion<I>>) {
    self.suspicion = s;
  }

  /// Take ownership of the active suspicion timer, leaving `None`.
  #[inline(always)]
  pub fn take_suspicion(&mut self) -> Option<Suspicion<I>> {
    self.suspicion.take()
  }

  /// Builder: attach a suspicion timer at construction time.
  #[must_use]
  #[inline(always)]
  pub fn with_suspicion(mut self, s: Suspicion<I>) -> Self {
    self.suspicion = Some(s);
    self
  }
}

/// Cluster membership: every peer the local node has ever heard of, plus the
/// local node itself. Backed by a `SmallVec<Member>` for cache-friendly iteration
/// and an `IndexMap<I, usize>` for O(1) lookup-by-id; the `usize` value is the
/// index of the member in the vector.
///
/// **Invariant:** for every `(id, idx)` in `node_map`, `nodes[idx].state.id() == id`.
/// Methods that mutate `nodes` (insert/remove/swap/shuffle) update `node_map`
/// to maintain this invariant.
#[derive(Debug)]
pub struct Members<I, A> {
  local: Node<I, A>,
  nodes: SmallVec<[Member<I, A>; 16]>,
  node_map: IndexMap<I, usize, rustc_hash::FxBuildHasher>,
}

impl<I, A> Members<I, A>
where
  I: Id,
{
  /// Construct an empty container. `local` is the local node's identity
  /// (id + advertise address); it is stored but NOT inserted into `nodes`.
  /// The local node is added like any other peer via `insert`.
  pub fn new(local: Node<I, A>) -> Self {
    Self {
      local,
      nodes: SmallVec::new(),
      node_map: IndexMap::default(),
    }
  }

  /// The local node's identity, as supplied at construction.
  #[inline(always)]
  pub const fn local_ref(&self) -> &Node<I, A> {
    &self.local
  }

  /// Number of tracked members.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.nodes.len()
  }

  /// True iff there are no tracked members.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.nodes.is_empty()
  }

  /// Whether the given id is tracked.
  #[inline(always)]
  pub fn contains(&self, id: &I) -> bool {
    self.node_map.contains_key(id)
  }

  /// Lookup by id.
  #[inline(always)]
  pub fn get(&self, id: &I) -> Option<&Member<I, A>> {
    let idx = *self.node_map.get(id)?;
    self.nodes.get(idx)
  }

  /// Mutable lookup by id.
  #[inline(always)]
  pub fn get_mut(&mut self, id: &I) -> Option<&mut Member<I, A>> {
    let idx = *self.node_map.get(id)?;
    self.nodes.get_mut(idx)
  }

  /// Iterate over all members in vector order.
  #[inline(always)]
  pub fn iter(&self) -> impl Iterator<Item = &Member<I, A>> {
    self.nodes.iter()
  }

  /// Mutable iterate over all members in vector order.
  #[inline(always)]
  pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Member<I, A>> {
    self.nodes.iter_mut()
  }

  /// Insert a new member at the end. If a member with the same id already
  /// exists, replaces it and returns the old one (so the caller can decide
  /// whether to ignore or merge).
  pub fn insert(&mut self, m: Member<I, A>) -> Option<Member<I, A>> {
    let id = m.state_ref().id_ref().cheap_clone();
    if let Some(&existing_idx) = self.node_map.get(&id) {
      let old = core::mem::replace(&mut self.nodes[existing_idx], m);
      return Some(old);
    }
    let idx = self.nodes.len();
    self.nodes.push(m);
    self.node_map.insert(id, idx);
    None
  }

  /// Insert a new member at a uniformly random position.
  ///
  /// Convenience wrapper around `insert_at_random_at` that picks the offset
  /// using the supplied RNG. See `insert_at_random_at` for the full doc.
  pub fn insert_at_random(&mut self, m: Member<I, A>, rng: &mut impl Rng) {
    let n = self.nodes.len();
    let target = if n == 0 { 0 } else { rng.random_range(0..=n) };
    self.insert_at_random_at(m, target);
  }

  /// Insert a new member at the given position (Fisher-Yates style:
  /// push at end, then swap with the supplied target index). `target` must
  /// be in `0..=self.len()`.
  ///
  /// Panics if a member with the same id already exists, or if `target`
  /// exceeds `self.len()`.
  pub fn insert_at_random_at(&mut self, m: Member<I, A>, target: usize) {
    let id = m.state_ref().id_ref().cheap_clone();
    assert!(
      !self.node_map.contains_key(&id),
      "Members::insert_at_random_at: id already present",
    );
    let n = self.nodes.len();
    assert!(
      target <= n,
      "Members::insert_at_random_at: target out of range"
    );
    self.nodes.push(m);
    self.node_map.insert(id.cheap_clone(), n);
    if target != n {
      self.nodes.swap(n, target);
      let displaced_id = self.nodes[n].state_ref().id_ref().cheap_clone();
      *self.node_map.get_mut(&id).unwrap() = target;
      *self.node_map.get_mut(&displaced_id).unwrap() = n;
    }
  }

  /// Remove the member with the given id, returning it if present.
  /// O(n) because it preserves order of remaining members (uses `Vec::remove`).
  pub fn remove(&mut self, id: &I) -> Option<Member<I, A>> {
    let idx = self.node_map.shift_remove(id)?;
    let m = self.nodes.remove(idx);
    for (_, slot) in self.node_map.iter_mut() {
      if *slot > idx {
        *slot -= 1;
      }
    }
    Some(m)
  }

  /// Shuffle the member list using Fisher-Yates with the supplied RNG,
  /// keeping `node_map` consistent.
  pub fn shuffle(&mut self, rng: &mut impl Rng) {
    let n = self.nodes.len();
    for i in (1..n).rev() {
      let j = rng.random_range(0..=i);
      if i != j {
        let id_i = self.nodes[i].state_ref().id_ref().cheap_clone();
        let id_j = self.nodes[j].state_ref().id_ref().cheap_clone();
        self.nodes.swap(i, j);
        *self.node_map.get_mut(&id_i).unwrap() = j;
        *self.node_map.get_mut(&id_j).unwrap() = i;
      }
    }
  }

  /// True iff at least one member other than the local node is not dead/left.
  pub fn any_alive(&self) -> bool {
    self
      .nodes
      .iter()
      .any(|m| !m.state_ref().dead_or_left() && m.state_ref().id_ref() != self.local.id_ref())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::typed::{Meta, NodeState, State};
  use core::net::{IpAddr, Ipv4Addr, SocketAddr};
  use rand::{SeedableRng, rngs::SmallRng};
  use smol_str::SmolStr;

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
    let later = now + core::time::Duration::from_secs(1);
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
    let later = now + core::time::Duration::from_secs(3);
    s.set_state_change(later);
    assert_eq!(s.state_change(), later);
    // set_state_change must not disturb the liveness state itself.
    assert_eq!(s.state(), State::Alive);
  }

  fn suspicion(from: &str) -> Suspicion<SmolStr> {
    Suspicion::new(
      SmolStr::new(from),
      3,
      core::time::Duration::from_millis(100),
      core::time::Duration::from_millis(500),
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
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
      m.insert_at_random_at(member("alice", 7000, State::Alive), 0);
    }));
    assert!(result.is_err(), "duplicate id must panic");
  }

  #[test]
  fn insert_at_random_at_panics_on_out_of_range_target() {
    let mut m = Members::new(local_node());
    m.insert(member("alice", 7000, State::Alive));
    // len is 1; target 5 is out of `0..=len`.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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
}
