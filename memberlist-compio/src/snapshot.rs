//! Lock-free snapshot of memberlist state, published via arc-swap.

use memberlist_wire::Node;
use smol_str::SmolStr;
use std::net::SocketAddr;

/// Snapshot of the memberlist's current observable state. Read via
/// [`Memberlist::snapshot`](crate::Memberlist::snapshot). Snapshots are
/// immutable; the driver publishes a new one (lock-free swap) after
/// each mutation that affects the observable state.
#[derive(Debug, Clone)]
pub struct MemberlistSnapshot {
  members: Vec<Node<SmolStr, SocketAddr>>,
  local: Node<SmolStr, SocketAddr>,
  alive_count: usize,
  member_count: usize,
}

impl MemberlistSnapshot {
  /// Construct a new snapshot.
  #[inline(always)]
  pub const fn new(
    members: Vec<Node<SmolStr, SocketAddr>>,
    local: Node<SmolStr, SocketAddr>,
    alive_count: usize,
    member_count: usize,
  ) -> Self {
    Self {
      members,
      local,
      alive_count,
      member_count,
    }
  }

  /// Borrow the members slice.
  #[inline(always)]
  pub fn members_slice(&self) -> &[Node<SmolStr, SocketAddr>] {
    self.members.as_slice()
  }

  /// Borrow the local node.
  #[inline(always)]
  pub const fn local_ref(&self) -> &Node<SmolStr, SocketAddr> {
    &self.local
  }

  /// Number of alive members.
  #[inline(always)]
  pub const fn alive_count(&self) -> usize {
    self.alive_count
  }

  /// Total member count (alive + dead + suspect).
  #[inline(always)]
  pub const fn member_count(&self) -> usize {
    self.member_count
  }
}
