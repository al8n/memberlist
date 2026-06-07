//! CIDR peer-admission tests over TCP (reactor driver).
//!
//! A [`CidrPolicy`] installed as the alive delegate filters inbound Alive
//! messages by the peer's advertised IP: a peer outside the allow-list never
//! enters the membership table, even though the join exchange itself succeeds.

#![cfg(all(feature = "tcp", feature = "cidr"))]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{
  CidrPolicy, MaybeResolved, Memberlist, Options, SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

/// Build a reactor TCP node on an OS-allocated loopback port, optionally with a
/// CIDR admission policy installed as the alive delegate.
async fn make(id: &str, policy: Option<CidrPolicy>) -> Memberlist<SmolStr> {
  let mut opts = Options::<SmolStr>::new();
  if let Some(policy) = policy {
    opts = opts.with_alive_delegate(policy);
  }
  Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    opts,
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  tokio::time::timeout(deadline, async {
    loop {
      if predicate() {
        return;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await
  .is_ok()
}

/// A node whose CIDR allow-list excludes the peer's network ignores the peer's
/// Alive, so the peer never enters its membership table — even though the join
/// exchange itself completes. B reaching two members proves the exchange worked,
/// so A staying at one is the CIDR block, not a failed join.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_blocks_peer_outside_allowlist() {
  // A admits only 10.0.0.0/8; the loopback peer (127.0.0.1) is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make("cidr-a", Some(policy)).await;
  let b = make("cidr-b", None).await;
  let a_addr = *a.local().addr_ref();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let b_converged = wait_until(|| b.num_members() == 2, Duration::from_secs(8)).await;
  assert!(
    b_converged,
    "B should see both nodes (join worked): b={}",
    b.num_members()
  );
  assert_eq!(
    a.num_members(),
    1,
    "A must reject the out-of-policy peer, saw {}",
    a.num_members()
  );

  a.shutdown().await.ok();
  b.shutdown().await.ok();
}

/// Control / non-vacuity: a CIDR allow-list that DOES include the peer's network
/// admits it. The policy gates by IP — it does not block unconditionally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_admits_peer_inside_allowlist() {
  // A admits 127.0.0.0/8; the loopback peer IS in policy.
  let policy = CidrPolicy::try_from(["127.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make("cidr-allow-a", Some(policy)).await;
  let b = make("cidr-allow-b", None).await;
  let a_addr = *a.local().addr_ref();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let converged = wait_until(
    || a.num_members() == 2 && b.num_members() == 2,
    Duration::from_secs(8),
  )
  .await;
  assert!(
    converged,
    "in-policy peer must be admitted: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  a.shutdown().await.ok();
  b.shutdown().await.ok();
}

/// A joiner whose OWN policy excludes the seed still completes the join
/// EXCHANGE (the seed is contacted), but the seed's Alive is dropped, so the
/// seed never enters the joiner's membership. Join accounting counts contacted
/// exchanges, not admitted peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_on_joiner_blocks_seed_from_membership() {
  // The joiner admits only 10.0.0.0/8; its loopback seed is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let seed = make("cidr-seed", None).await;
  let joiner = make("cidr-joiner", Some(policy)).await;
  let seed_addr = *seed.local().addr_ref();

  // The exchange completes — the seed is contacted — even though the joiner
  // will drop the seed's Alive.
  let contacted = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join exchange completes");
  assert_eq!(
    contacted, 1,
    "the seed was contacted (the exchange completed)"
  );

  // The seed is out of the joiner's policy, so it is never admitted — even
  // across a window where convergence would otherwise happen.
  let admitted = wait_until(|| joiner.num_members() == 2, Duration::from_secs(2)).await;
  assert!(
    !admitted,
    "an out-of-policy seed must not be admitted, joiner saw {}",
    joiner.num_members()
  );
  assert_eq!(joiner.num_members(), 1, "the joiner has only itself");

  seed.shutdown().await.ok();
  joiner.shutdown().await.ok();
}
