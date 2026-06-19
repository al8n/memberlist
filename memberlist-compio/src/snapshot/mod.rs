//! Snapshot of memberlist state, published into a single-owner cell.

use core::net::SocketAddr;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use memberlist_proto::typed::NodeState;

/// The published-snapshot cell shared by the handle and the driver. Single-owner
/// per thread (thread-per-core): the driver swaps in a fresh
/// `Rc<MemberlistSnapshot>` after each state-affecting tick, and the handle (and
/// the driver) read the current one. The `RefCell` is a borrow check, not a lock
/// — handle and driver share one thread and never borrow it at overlapping times.
pub(crate) type SnapshotCell<I, A = SocketAddr> = Rc<RefCell<Rc<MemberlistSnapshot<I, A>>>>;

/// Snapshot of the memberlist's current observable state. Read via
/// [`Memberlist::snapshot`](crate::Memberlist::snapshot). Snapshots are
/// immutable; the driver publishes a new one (single-owner cell swap) after
/// each mutation that affects the observable state.
///
/// Generic over the wire id / address types `<I, A>`, mirroring the
/// underlying [`NodeState<I, A>`]. The [`Memberlist`](crate::Memberlist)
/// handle instantiates this as `MemberlistSnapshot<I, SocketAddr>` for the
/// id type its backend carries.
#[derive(Debug, Clone)]
pub struct MemberlistSnapshot<I, A> {
  members: Vec<Arc<NodeState<I, A>>>,
  local: Arc<NodeState<I, A>>,
  alive_count: usize,
  member_count: usize,
  health_score: usize,
}

impl<I, A> MemberlistSnapshot<I, A> {
  /// Construct a new snapshot.
  #[inline(always)]
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

  /// Borrow all known members (full [`NodeState`], carrying liveness + incarnation).
  #[inline(always)]
  pub fn members(&self) -> &[Arc<NodeState<I, A>>] {
    self.members.as_slice()
  }

  /// Borrow all known members. Alias for [`Self::members`].
  #[inline(always)]
  pub fn members_slice(&self) -> &[Arc<NodeState<I, A>>] {
    self.members.as_slice()
  }

  /// Borrow the local node's state.
  #[inline(always)]
  pub fn local_ref(&self) -> &Arc<NodeState<I, A>> {
    &self.local
  }

  /// Number of alive members.
  #[inline(always)]
  pub const fn alive_count(&self) -> usize {
    self.alive_count
  }

  /// Total member count (alive + suspect + dead/left).
  #[inline(always)]
  pub const fn member_count(&self) -> usize {
    self.member_count
  }

  /// The local node's Lifeguard health score (`0` = healthy; higher = worse).
  /// Mirrors `memberlist-core`'s `health_score`.
  #[inline(always)]
  pub const fn health_score(&self) -> usize {
    self.health_score
  }

  /// Look up a member by id.
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

#[cfg(test)]
mod tests;
