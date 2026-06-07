//! CIDR peer-admission tests over TCP.
//!
//! A [`CidrPolicy`] installed as the alive delegate filters inbound Alive
//! messages by the peer's advertised IP: a peer outside the allow-list never
//! enters the membership table, even though the join exchange itself succeeds.

#![cfg(all(feature = "tcp", feature = "cidr"))]

use std::{net::SocketAddr, time::Duration};

use memberlist_compio::{
  CidrPolicy, FirstAddrResolver, MaybeResolved, Options, SocketAddrResolver, TcpMemberlist,
  TcpTransportOptions, VoidDelegate,
};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a `TcpMemberlist` on an OS-allocated loopback port, optionally with a
/// CIDR admission policy installed as the alive delegate.
async fn make_tcp(id: &str, policy: Option<CidrPolicy>) -> TcpMemberlist<SmolStr, SocketAddr> {
  let mut opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0))),
  );
  if let Some(policy) = policy {
    opts = opts.with_alive_delegate(policy);
  }
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
  let start = std::time::Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// A node whose CIDR allow-list excludes the peer's network ignores the peer's
/// Alive, so the peer never enters its membership table — even though the join
/// exchange itself completes (the peer still learns about us). The asymmetry is
/// the discriminator: B reaching two members proves the exchange worked, so A
/// staying at one is the CIDR block, not a failed join.
#[compio::test]
async fn cidr_policy_blocks_peer_outside_allowlist() {
  // A admits only 10.0.0.0/8; the loopback peer (127.0.0.1) is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_tcp("cidr-a", Some(policy)).await;
  let b = make_tcp("cidr-b", None).await;
  let a_addr = a.advertise_address();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  // B has no policy, so it admits A and reaches two members. This also proves
  // the join exchange mechanically succeeded.
  let b_converged = wait_until(|| b.member_count() == 2, Duration::from_secs(5)).await;
  assert!(
    b_converged,
    "B should see both nodes (join worked): b={}",
    b.member_count()
  );

  // A blocked B's advertised loopback IP, so A never added B.
  assert_eq!(
    a.member_count(),
    1,
    "A must reject the out-of-policy peer, saw {}",
    a.member_count()
  );

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}

/// Control / non-vacuity: a CIDR allow-list that DOES include the peer's network
/// admits it. The policy gates by IP — it does not block unconditionally — so an
/// in-policy peer converges normally.
#[compio::test]
async fn cidr_policy_admits_peer_inside_allowlist() {
  // A admits 127.0.0.0/8; the loopback peer IS in policy.
  let policy = CidrPolicy::try_from(["127.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_tcp("cidr-allow-a", Some(policy)).await;
  let b = make_tcp("cidr-allow-b", None).await;
  let a_addr = a.advertise_address();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let converged = wait_until(
    || a.member_count() == 2 && b.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "in-policy peer must be admitted: a={}, b={}",
    a.member_count(),
    b.member_count()
  );

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}

/// A joiner whose OWN policy excludes the seed still completes the join
/// EXCHANGE (the seed is contacted), but the seed's Alive is dropped, so the
/// seed never enters the joiner's membership. Join accounting counts contacted
/// exchanges, not admitted peers — a contacted seed can be absent from
/// membership when policy rejects it, so a green `join` does not imply the seed
/// was admitted.
#[compio::test]
async fn cidr_policy_on_joiner_blocks_seed_from_membership() {
  // The joiner admits only 10.0.0.0/8; its loopback seed is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let seed = make_tcp("cidr-seed", None).await;
  let joiner = make_tcp("cidr-joiner", Some(policy)).await;
  let seed_addr = seed.advertise_address();

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

  // The seed is out of the joiner's policy, so its Alive is dropped and it is
  // never admitted — even across a window where convergence would otherwise
  // happen.
  let admitted = wait_until(|| joiner.member_count() == 2, Duration::from_secs(2)).await;
  assert!(
    !admitted,
    "an out-of-policy seed must not be admitted, joiner saw {}",
    joiner.member_count()
  );
  assert_eq!(joiner.member_count(), 1, "the joiner has only itself");

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}
