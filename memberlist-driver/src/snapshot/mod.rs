//! [`MemberlistSnapshot`] — the observable membership view both driver crates publish.

use core::net::SocketAddr;
use std::sync::Arc;

use memberlist_proto::{
  Node,
  typed::{NodeState, State},
};

/// An immutable snapshot of the cluster's observable membership at one instant.
///
/// A driver republishes this after every membership change; `Memberlist` handles read the latest
/// one with no coordination. It carries only membership identity and liveness — application
/// payloads (`UserPacket` / `RemoteStateReceived`) are delivered to the `Delegate`, not retained
/// here.
///
/// Generic over the wire id / address types `<I, A>`, mirroring the underlying [`NodeState<I, A>`];
/// a handle instantiates it as `MemberlistSnapshot<I, SocketAddr>`.
#[derive(Debug, Clone)]
pub struct MemberlistSnapshot<I, A = SocketAddr> {
  members: Vec<Arc<NodeState<I, A>>>,
  local: Arc<NodeState<I, A>>,
  alive_count: usize,
  member_count: usize,
  health_score: usize,
}

impl<I, A> MemberlistSnapshot<I, A> {
  /// Builds a snapshot from the membership view. Called by a driver each time it republishes.
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

  /// All known members (full [`NodeState`], carrying liveness + incarnation) — alive, suspect, and
  /// recently dead within the reclaim window.
  #[must_use]
  pub const fn members(&self) -> &[Arc<NodeState<I, A>>] {
    self.members.as_slice()
  }

  /// All known members. Alias for [`Self::members`].
  #[must_use]
  pub const fn members_slice(&self) -> &[Arc<NodeState<I, A>>] {
    self.members.as_slice()
  }

  /// This node's own identity and advertised address, as a [`Node`] pair. For full state
  /// (incarnation, meta, liveness) use [`Self::local_ref`].
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
  pub const fn local_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.local
  }

  /// The count of members currently in the alive state.
  #[must_use]
  pub const fn alive_count(&self) -> usize {
    self.alive_count
  }

  /// The count of all known members (alive + suspect + dead/left).
  #[must_use]
  pub const fn member_count(&self) -> usize {
    self.member_count
  }

  /// The count of all known members. Alias for [`Self::member_count`].
  #[must_use]
  pub const fn num_members(&self) -> usize {
    self.member_count
  }

  /// The local node's Lifeguard health score (`0` = healthy; higher = worse). Mirrors
  /// `memberlist-core`'s `health_score`.
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
      .filter(|ns| matches!(ns.state(), State::Alive))
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

#[cfg(test)]
mod tests;
