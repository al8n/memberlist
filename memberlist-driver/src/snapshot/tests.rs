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
  assert_eq!(snap.members_slice().len(), 1);
  assert_eq!(snap.health_score(), 7);
  assert_eq!(snap.alive_count(), 1);
  assert_eq!(snap.member_count(), 1);
  assert_eq!(snap.num_members(), 1);
  assert_eq!(snap.local_ref().id_ref(), local.id_ref());
  assert_eq!(snap.local().id_ref(), local.id_ref());
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
  assert_eq!(
    snap
      .members_by(|ns| matches!(ns.state(), State::Dead))
      .count(),
    1
  );
  assert_eq!(
    snap
      .members_map_by(|ns| Some(ns.address_ref().port()))
      .len(),
    2
  );
}
