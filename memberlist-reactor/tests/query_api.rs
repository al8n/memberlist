//! Tests for `Memberlist` handle membership query API — `by_id`, `online_members`,
//! `num_online_members`, `members_by`, `num_members_by`, `members_map_by`,
//! `local_state`, `local_id`, `advertise_address`, `members`, `num_members`,
//! and `health_score`.

#![cfg(feature = "tcp")]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{MaybeResolved, Memberlist, Options, SocketAddrResolver, VoidDelegate};
use smol_str::SmolStr;

async fn make(id: &str) -> Memberlist<SmolStr> {
  Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handle_query_api_two_node_cluster() {
  let a = make("node-a").await;
  let b = make("node-b").await;
  let a_addr = *a.local().addr_ref();

  // --- single-node accessors before join ---
  assert_eq!(a.local_id().as_str(), "node-a");
  assert_eq!(a.advertise_address(), a_addr);
  assert_eq!(a.health_score(), 0, "fresh node is fully healthy");
  assert_eq!(a.num_members(), 1);
  assert_eq!(a.num_online_members(), 1);
  assert_eq!(a.members().len(), 1);
  assert_eq!(a.online_members().len(), 1);
  assert!(a.local_state().id_ref().as_str() == "node-a");

  // --- join ---
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  // Wait for both nodes to see each other.
  let converged = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "cluster did not converge within 8s");

  // --- two-node query API ---
  let b_id = b.local_id();
  let found = a.by_id(&b_id);
  assert!(found.is_some(), "node-a should know node-b after join");

  assert_eq!(
    a.num_online_members(),
    2,
    "both nodes alive after convergence"
  );
  assert_eq!(
    a.online_members().len(),
    2,
    "both nodes alive after convergence"
  );

  // members_by: find node-b by id
  let matched = a.members_by(|ns| ns.id_ref() == &b_id);
  assert_eq!(matched.len(), 1);

  // num_members_by: count nodes with the b_id
  let cnt = a.num_members_by(|ns| ns.id_ref() == &b_id);
  assert_eq!(cnt, 1);

  // members_map_by: extract ids of all members
  let ids: Vec<SmolStr> = a.members_map_by(|ns| Some(ns.id_ref().clone()));
  assert!(ids.len() >= 2);

  // members() total count
  assert_eq!(a.num_members(), a.members().len());

  a.shutdown().await.ok();
  b.shutdown().await.ok();
}
