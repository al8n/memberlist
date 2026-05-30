//! [`MemberlistSnapshot`] — a lock-free view of observable membership, published
//! by the driver through `ArcSwap` and read by `Memberlist` handles.

use std::net::SocketAddr;

use memberlist_wire::Node;

/// An immutable snapshot of the cluster's observable membership at one instant.
///
/// The driver republishes this (via `ArcSwap`) after every membership change;
/// `Memberlist` handles read the latest with no lock contention. It carries only
/// membership identity/liveness — application payloads (`UserPacket` /
/// `RemoteStateReceived`) are delivered to the `Delegate`, not retained here.
#[derive(Debug, Clone)]
pub struct MemberlistSnapshot<I, A = SocketAddr> {
  members: Vec<Node<I, A>>,
  local: Node<I, A>,
  alive_count: usize,
  member_count: usize,
}

impl<I, A> MemberlistSnapshot<I, A> {
  /// Builds a snapshot from the membership view. Called by the driver each time
  /// it republishes; the four parts are read straight off the machine's
  /// `Endpoint`.
  #[must_use]
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

  /// All known members — alive, suspect, and recently dead within the reclaim
  /// window.
  #[must_use]
  pub fn members(&self) -> &[Node<I, A>] {
    &self.members
  }

  /// This node's own identity and advertised address.
  #[must_use]
  pub const fn local(&self) -> &Node<I, A> {
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
}

/// Builds a membership snapshot from the machine's current view (shared by a
/// backend constructor's initial publish and a driver's per-change republish).
#[cfg(any(feature = "quic", feature = "tcp"))]
pub(crate) fn snapshot_of<I: crate::NodeId>(
  ep: &memberlist_machine::Endpoint<I, SocketAddr>,
) -> MemberlistSnapshot<I, SocketAddr> {
  use memberlist_wire::CheapClone;
  let mut members = Vec::new();
  let mut alive_count = 0usize;
  for ns in ep.members() {
    members.push(Node::new(
      ns.id_ref().cheap_clone(),
      ns.address_ref().cheap_clone(),
    ));
    if let Some(memberlist_wire::typed::State::Alive) = ep.member_liveness(ns.id_ref()) {
      alive_count += 1;
    }
  }
  let local = Node::new(
    ep.local_id_ref().cheap_clone(),
    ep.advertise_ref().cheap_clone(),
  );
  MemberlistSnapshot::new(members, local, alive_count, ep.num_members())
}
