//! Cluster membership state.

use core::hash::Hash;
use std::sync::Arc;

use crate::{
  Node,
  typed::{NodeState, State},
};

use cheap_clone::CheapClone;
use indexmap::IndexMap;
use rand::{Rng, RngExt};
use smallvec_wrapper::LargeVec;

use crate::{Instant, suspicion::Suspicion};

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
  /// Monotonic membership-instance token. **Invariant:** a generation
  /// identifies ONE membership instance across its whole lifetime
  /// (admission → removal); it stays fixed through every in-place update of the
  /// same record — an incarnation bump (refutation), a meta update, an
  /// Alive/Suspect/Dead transition — and changes ONLY when the record is
  /// REPLACED by a different instance: a brand-new id, or a Left/Dead id
  /// reclaimed / re-admitted at a new-or-same address and any incarnation. The
  /// gossip machine assigns it from `Endpoint::next_member_generation` at each
  /// such replacement, so `(id, address, incarnation)` collisions between an old
  /// and a fresh instance (which a `reset_nodes` reclaim + equal-incarnation
  /// rejoin produces) still carry distinct generations. `0` is reserved as
  /// "unset" and is never assigned to a tracked instance.
  generation: u64,
}

impl<I, A> Member<I, A> {
  /// Construct a member with no active suspicion and an unset (`0`) generation.
  /// The gossip machine assigns the real generation at the admission /
  /// replacement site via [`with_generation`](Self::with_generation) or
  /// [`set_generation`](Self::set_generation).
  pub const fn new(state: LocalNodeState<I, A>) -> Self {
    Self {
      state,
      suspicion: None,
      generation: 0,
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

  /// This member instance's generation token. See the
  /// [`generation`](Self::generation) field for the invariant. `0` means unset.
  #[inline(always)]
  pub const fn generation(&self) -> u64 {
    self.generation
  }

  /// Set the generation token. Called at the admission / replacement site to
  /// stamp a new membership instance (including an in-place reclaim that adopts
  /// a new address for a Left/Dead id). See the [`generation`](Self::generation)
  /// field invariant.
  #[inline(always)]
  pub const fn set_generation(&mut self, generation: u64) {
    self.generation = generation;
  }

  /// Builder: stamp the generation token at construction time. See the
  /// [`generation`](Self::generation) field invariant.
  #[must_use]
  #[inline(always)]
  pub const fn with_generation(mut self, generation: u64) -> Self {
    self.generation = generation;
    self
  }
}

/// Cluster membership: every peer the local node has ever heard of, plus the
/// local node itself. Backed by a `LargeVec<Member>` for cache-friendly iteration
/// and an `IndexMap<I, usize>` for O(1) lookup-by-id; the `usize` value is the
/// index of the member in the vector.
///
/// **Invariant:** for every `(id, idx)` in `node_map`, `nodes[idx].state.id() == id`.
/// Methods that mutate `nodes` (insert/remove/swap/shuffle) update `node_map`
/// to maintain this invariant.
#[derive(Debug)]
pub struct Members<I, A> {
  local: Node<I, A>,
  nodes: LargeVec<Member<I, A>>,
  node_map: IndexMap<I, usize, rustc_hash::FxBuildHasher>,
}

impl<I, A> Members<I, A> {
  /// Construct an empty container. `local` is the local node's identity
  /// (id + advertise address); it is stored but NOT inserted into `nodes`.
  /// The local node is added like any other peer via `insert`.
  pub fn new(local: Node<I, A>) -> Self {
    Self {
      local,
      nodes: LargeVec::new(),
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
}

impl<I, A> Members<I, A>
where
  I: Eq,
{
  /// True iff at least one member other than the local node is not dead/left.
  pub fn any_alive(&self) -> bool {
    self
      .nodes
      .iter()
      .any(|m| !m.state_ref().dead_or_left() && m.state_ref().id_ref() != self.local.id_ref())
  }
}

impl<I, A> Members<I, A>
where
  I: Eq + Hash,
{
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
}

impl<I, A> Members<I, A>
where
  I: Eq + Hash + CheapClone,
{
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

  /// Insert a new member at a uniformly random position.
  ///
  /// Convenience wrapper around `insert_at_random_at` that picks the offset
  /// using the supplied RNG. See `insert_at_random_at` for the full doc.
  pub fn insert_at_random(&mut self, m: Member<I, A>, rng: &mut impl Rng) {
    let n = self.nodes.len();
    let target = if n == 0 { 0 } else { rng.random_range(0..=n) };
    self.insert_at_random_at(m, target);
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
}

#[cfg(test)]
mod tests;
