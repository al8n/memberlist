//! Lock-free snapshot of memberlist state, published via arc-swap.

use memberlist_proto::Node;

/// Snapshot of the memberlist's current observable state. Read via
/// [`Memberlist::snapshot`](crate::Memberlist::snapshot). Snapshots are
/// immutable; the driver publishes a new one (lock-free swap) after
/// each mutation that affects the observable state.
///
/// Generic over the wire id / address types `<I, A>`, mirroring the
/// underlying [`Node<I, A>`]. The pinned aliases
/// [`TcpMemberlist`](crate::TcpMemberlist) /
/// [`TlsMemberlist`](crate::TlsMemberlist) /
/// [`QuicMemberlist`](crate::QuicMemberlist) all instantiate this as
/// `MemberlistSnapshot<SmolStr, SocketAddr>`; power users that
/// construct [`Memberlist`](crate::Memberlist) directly pick their own.
#[derive(Debug, Clone)]
pub struct MemberlistSnapshot<I, A> {
  members: Vec<Node<I, A>>,
  local: Node<I, A>,
  alive_count: usize,
  member_count: usize,
}

impl<I, A> MemberlistSnapshot<I, A> {
  /// Construct a new snapshot.
  #[inline(always)]
  pub const fn new(
    members: Vec<Node<I, A>>,
    local: Node<I, A>,
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
  pub fn members_slice(&self) -> &[Node<I, A>] {
    self.members.as_slice()
  }

  /// Borrow the local node.
  #[inline(always)]
  pub const fn local_ref(&self) -> &Node<I, A> {
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
