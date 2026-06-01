//! [`MemberlistSnapshot`] — a lock-free view of observable membership, published
//! by the driver through `ArcSwap` and read by `Memberlist` handles.

use std::{net::SocketAddr, sync::Arc};

use memberlist_proto::{Node, typed::NodeState};

/// An immutable snapshot of the cluster's observable membership at one instant.
///
/// The driver republishes this (via `ArcSwap`) after every membership change;
/// `Memberlist` handles read the latest with no lock contention. It carries only
/// membership identity/liveness — application payloads (`UserPacket` /
/// `RemoteStateReceived`) are delivered to the `Delegate`, not retained here.
#[derive(Debug, Clone)]
pub struct MemberlistSnapshot<I, A = SocketAddr> {
  members: Vec<Arc<NodeState<I, A>>>,
  local: Arc<NodeState<I, A>>,
  alive_count: usize,
  member_count: usize,
  health_score: usize,
}

impl<I, A> MemberlistSnapshot<I, A> {
  /// Builds a snapshot from the membership view. Called by the driver each time
  /// it republishes.
  #[must_use]
  pub fn new(
    members: Vec<Arc<NodeState<I, A>>>,
    local: Arc<NodeState<I, A>>,
    alive_count: usize,
    member_count: usize,
    health_score: usize,
  ) -> Self {
    Self {
      members,
      local,
      alive_count,
      member_count,
      health_score,
    }
  }

  /// All known members (full [`NodeState`], carrying liveness + incarnation) —
  /// alive, suspect, and recently dead within the reclaim window.
  #[must_use]
  pub fn members(&self) -> &[Arc<NodeState<I, A>>] {
    self.members.as_slice()
  }

  /// This node's own identity and advertised address, as a [`Node`] pair.
  ///
  /// Kept for backward compatibility with existing callers that use
  /// `.local().id_ref()` / `.local().addr_ref()`. For full state (incarnation,
  /// meta, liveness) use [`Self::local_ref`].
  #[must_use]
  pub fn local(&self) -> Node<I, A>
  where
    I: Clone,
    A: Clone,
  {
    Node::new(
      self.local.id_ref().clone(),
      self.local.address_ref().clone(),
    )
  }

  /// Borrow the local node's full state.
  #[must_use]
  pub fn local_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.local
  }

  /// The count of members currently in the alive state.
  #[must_use]
  pub const fn alive_count(&self) -> usize {
    self.alive_count
  }

  /// The count of all known members.
  #[must_use]
  pub const fn num_members(&self) -> usize {
    self.member_count
  }

  /// The local node's Lifeguard health score (`0` = healthy; higher = worse).
  /// Mirrors `memberlist-core`'s `health_score`.
  #[must_use]
  pub const fn health_score(&self) -> usize {
    self.health_score
  }

  /// Look up a member by id. Returns `None` if the id is not in the snapshot.
  #[must_use]
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<&Arc<NodeState<I, A>>>
  where
    I: PartialEq,
  {
    self.members.iter().find(|ns| ns.id_ref() == id)
  }

  /// Iterate members currently in the alive state.
  #[inline]
  pub fn online_members(&self) -> impl Iterator<Item = &Arc<NodeState<I, A>>> {
    self
      .members
      .iter()
      .filter(|ns| matches!(ns.state(), memberlist_proto::typed::State::Alive))
  }

  /// Iterate members matching `pred`.
  #[inline]
  pub fn members_by<'a>(
    &'a self,
    mut pred: impl FnMut(&NodeState<I, A>) -> bool + 'a,
  ) -> impl Iterator<Item = &'a Arc<NodeState<I, A>>> {
    self.members.iter().filter(move |ns| pred(ns))
  }

  /// Count members matching `pred`.
  #[inline]
  pub fn num_members_by(&self, mut pred: impl FnMut(&NodeState<I, A>) -> bool) -> usize {
    self.members.iter().filter(|ns| pred(ns)).count()
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  #[inline]
  pub fn members_map_by<O>(&self, mut f: impl FnMut(&NodeState<I, A>) -> Option<O>) -> Vec<O> {
    self.members.iter().filter_map(|ns| f(ns)).collect()
  }
}

/// Builds a membership snapshot from the machine's current view (shared by a
/// backend constructor's initial publish and a driver's per-change republish).
///
/// Each member's [`NodeState`] is stamped with the FSM-tracked liveness state
/// (`member_liveness`) rather than the wire-protocol state (`ns.state()`, which
/// is frozen at the last Alive broadcast). The local node is taken directly from
/// the membership map so it carries the real meta, incarnation, and protocol
/// versions.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub(crate) fn snapshot_of<I: crate::NodeId>(
  ep: &memberlist_proto::Endpoint<I, SocketAddr>,
) -> MemberlistSnapshot<I, SocketAddr> {
  let mut alive_count = 0usize;
  let members: Vec<Arc<NodeState<I, SocketAddr>>> = ep
    .members()
    .map(|ns| {
      let fsm = ep
        .member_liveness(ns.id_ref())
        .unwrap_or(memberlist_proto::typed::State::Unknown(0));
      if matches!(fsm, memberlist_proto::typed::State::Alive) {
        alive_count += 1;
      }
      Arc::new((*ns).clone().with_state(fsm))
    })
    .collect();
  let local = ep
    .member(ep.local_id_ref())
    .expect("local node is always in the membership map");
  MemberlistSnapshot::new(
    members,
    local,
    alive_count,
    ep.num_members(),
    ep.health_score(),
  )
}

#[cfg(test)]
mod tests {
  use std::{net::SocketAddr, sync::Arc};

  use memberlist_proto::typed::{NodeState, State};
  use smol_str::SmolStr;

  use super::MemberlistSnapshot;

  fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  #[test]
  fn snapshot_carries_node_state_and_health() {
    let a = Arc::new(NodeState::new(SmolStr::new("a"), sock(1), State::Alive));
    let local = Arc::new(NodeState::new(SmolStr::new("me"), sock(9), State::Alive));
    let snap = MemberlistSnapshot::new(vec![a.clone()], local.clone(), 1, 1, 7);
    assert_eq!(snap.members().len(), 1);
    assert_eq!(snap.health_score(), 7);
    assert_eq!(snap.local_ref().id_ref(), local.id_ref());
  }

  #[test]
  fn snapshot_queries() {
    let alive = Arc::new(NodeState::new(SmolStr::new("a"), sock(1), State::Alive));
    let dead = Arc::new(NodeState::new(SmolStr::new("d"), sock(2), State::Dead));
    let local = Arc::new(NodeState::new(SmolStr::new("me"), sock(9), State::Alive));
    let snap = MemberlistSnapshot::new(vec![alive.clone(), dead.clone()], local, 1, 2, 0);
    assert!(snap.by_id(&SmolStr::new("a")).is_some());
    assert!(snap.by_id(&SmolStr::new("zzz")).is_none());
    assert_eq!(snap.online_members().count(), 1);
    assert_eq!(snap.num_members_by(|ns| ns.address_ref().port() == 2), 1);
  }
}
