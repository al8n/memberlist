//! [`MemberlistSnapshot`] — a lock-free view of observable membership, published
//! by the driver through `ArcSwap` and read by `Memberlist` handles.

use std::{net::SocketAddr, sync::Arc};

use memberlist_proto::typed::{NodeState, State};

pub use memberlist_driver::MemberlistSnapshot;

/// Builds a membership snapshot from the machine's current view (shared by a
/// backend constructor's initial publish and a driver's per-change republish).
///
/// Each member's [`NodeState`] is stamped with the FSM-tracked liveness state
/// (`member_liveness`) rather than the wire-protocol state (`ns.state()`, which
/// is frozen at the last Alive broadcast). The local node is taken directly from
/// the membership map so it carries the real meta, incarnation, and protocol
/// versions.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub(crate) fn snapshot_of<I, R>(
  ep: &memberlist_proto::Endpoint<I, SocketAddr, R>,
) -> MemberlistSnapshot<I, SocketAddr>
where
  I: crate::NodeId,
  R: rand::Rng,
{
  let mut alive_count = 0usize;
  let members: Vec<Arc<NodeState<I, SocketAddr>>> = ep
    .members()
    .map(|ns| {
      let fsm = ep.member_liveness(ns.id_ref()).unwrap_or(State::Unknown(0));
      if matches!(fsm, State::Alive) {
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
