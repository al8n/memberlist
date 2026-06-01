//! Integration tests for membership query API on the Memberlist handle.
//!
//! Exercises every membership query method on the `Memberlist` handle
//! against a live cluster.

#![cfg(feature = "tcp")]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::{Duration, Instant},
};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Options, SocketAddrResolver, TcpMemberlist,
  TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::TcpOptions;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

async fn make_tcp(id: &str, addr: SocketAddr) -> TcpMemberlist<SmolStr, SocketAddr> {
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_tcp_options(TcpOptions::new(Some(b"queries-test".to_vec()))),
  );
  TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist")
}

async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

#[compio::test]
async fn handle_query_api_after_join() {
  let seed_addr = loopback_addr(7700);
  let joiner_addr = loopback_addr(7701);

  let seed = make_tcp("q-seed", seed_addr).await;
  let joiner = make_tcp("q-joiner", joiner_addr).await;

  let _count = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join");

  // Wait for both sides to see each other.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(converged, "cluster did not converge");

  // --- local_id / advertise_address ---
  assert_eq!(joiner.local_id(), SmolStr::new("q-joiner"));
  assert_eq!(joiner.advertise_address(), joiner_addr);
  assert_eq!(seed.local_id(), SmolStr::new("q-seed"));

  // --- local_state ---
  let ls = joiner.local_state();
  assert_eq!(ls.id_ref(), &SmolStr::new("q-joiner"));

  // --- health_score ---
  assert_eq!(joiner.health_score(), 0, "fresh node is fully healthy");

  // --- by_id ---
  assert!(
    joiner.by_id(&SmolStr::new("q-seed")).is_some(),
    "joiner should know q-seed"
  );
  assert!(
    joiner.by_id(&SmolStr::new("nobody")).is_none(),
    "unknown id should return None"
  );

  // --- online_members / num_online_members ---
  let online = joiner.online_members();
  assert_eq!(online.len(), 2, "both nodes alive after convergence");
  assert_eq!(joiner.num_online_members(), 2);

  // --- members / num_members ---
  let all = joiner.members();
  assert_eq!(all.len(), joiner.num_members());
  assert!(all.len() >= 2);

  // --- members_by / num_members_by ---
  let by_port = joiner.members_by(|ns| ns.address_ref().port() == seed_addr.port());
  assert_eq!(by_port.len(), 1);
  let count_by = joiner.num_members_by(|ns| ns.address_ref().port() == seed_addr.port());
  assert_eq!(count_by, 1);

  // --- members_map_by ---
  let ids: Vec<SmolStr> = joiner.members_map_by(|ns| {
    if ns.address_ref().port() == seed_addr.port() {
      Some(ns.id_ref().clone())
    } else {
      None
    }
  });
  assert_eq!(ids, vec![SmolStr::new("q-seed")]);

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}
