//! CIDR peer-admission tests over TCP (reactor driver).
//!
//! Two install paths: a [`CidrPolicy`] as the alive delegate
//! ([`with_alive_delegate`]) filters inbound Alive messages by the peer's
//! advertised IP — a peer outside the allow-list never enters the membership
//! table, though the join exchange itself succeeds; while the driver-level
//! [`with_cidr_policy`] additionally filters the transport boundary, so a blocked
//! peer's reliable connection is rejected at `accept` and its join exchange fails.
//!
//! [`with_alive_delegate`]: memberlist_reactor::Options::with_alive_delegate
//! [`with_cidr_policy`]: memberlist_reactor::Options::with_cidr_policy

#![cfg(all(feature = "tcp", feature = "cidr"))]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{
  CidrPolicy, MaybeResolved, Memberlist, Options, SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

/// Build a reactor TCP node on an OS-allocated loopback port, optionally with a
/// CIDR admission policy installed as the alive delegate.
async fn make(id: &str, policy: Option<CidrPolicy>) -> Memberlist<SmolStr, SocketAddr> {
  let mut opts = Options::<SmolStr>::new();
  if let Some(policy) = policy {
    opts = opts.with_alive_delegate(policy);
  }
  Memberlist::<SmolStr, _>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    opts,
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

/// Build a reactor TCP node with a driver-level [`with_cidr_policy`] — the policy
/// filters the gossip source and reliable peer at the transport boundary AND the
/// advertised address at membership admission, all from this one setting.
async fn make_cidr(id: &str, policy: CidrPolicy) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<SmolStr>::new().with_cidr_policy(policy);
  Memberlist::<SmolStr, _>::tcp::<TokioRuntime, _, _>(
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

/// `with_cidr_policy` filters the RELIABLE layer: a blocked peer's stream is
/// rejected at `accept`, so its reliable join EXCHANGE fails outright — unlike
/// the membership-only `with_alive_delegate` path, where the exchange completes
/// and only the alive is ignored. Neither node learns the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_rejects_blocked_reliable_peer() {
  // A admits only 10.0.0.0/8; the loopback peer is blocked at the transport.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_cidr("cidr-tp-a", policy).await;
  let b = make("cidr-tp-b", None).await;
  let a_addr = *a.local().addr_ref();

  // B's reliable join is rejected at A's accept (B's loopback peer IP is out of
  // policy), so the push/pull exchange never happens and the join fails.
  let res = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  assert!(
    res.is_err(),
    "a blocked reliable peer's join must fail at the boundary, got {res:?}"
  );

  // Neither node learns the other: A rejected the connection, B's exchange errored.
  assert_eq!(a.num_members(), 1, "A must not admit the blocked peer");
  assert_eq!(
    b.num_members(),
    1,
    "B's rejected join leaves it with only itself"
  );

  a.shutdown().await.ok();
  b.shutdown().await.ok();
}

/// `with_cidr_policy` filters OUTBOUND dials too: a joiner whose OWN policy
/// excludes the seed has its reliable dial rejected at the transport boundary, so
/// the push/pull never happens and the join FAILS — the discriminator against the
/// membership-only `with_alive_delegate` path
/// (`cidr_policy_on_joiner_blocks_seed_from_membership`), where the exchange
/// completes and only the seed's Alive is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_blocks_joiner_outbound_dial() {
  // The joiner admits only 10.0.0.0/8; its loopback seed is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let seed = make("cidr-dial-seed", None).await;
  let joiner = make_cidr("cidr-dial-joiner", policy).await;
  let seed_addr = *seed.local().addr_ref();

  // The joiner's own policy excludes the loopback seed, so its outbound reliable
  // dial is rejected before connecting — the join fails outright.
  let res = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;
  assert!(
    res.is_err(),
    "an outbound dial to a CIDR-blocked seed must fail at the boundary, got {res:?}"
  );

  assert_eq!(joiner.num_members(), 1, "the joiner has only itself");
  assert_eq!(seed.num_members(), 1, "the seed was never contacted");

  joiner.shutdown().await.ok();
  seed.shutdown().await.ok();
}

/// `with_cidr_policy` gates the OUTBOUND unreliable plane too: a directed `send`
/// to a peer the node's OWN policy excludes fails at the boundary without
/// emitting a datagram (the reliable dial is gated at the Connect handler; this
/// is the unreliable counterpart).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_blocks_outbound_unreliable_send() {
  // The node admits only 10.0.0.0/8, so any loopback peer is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_cidr("cidr-send-a", policy).await;

  let res = a
    .send(
      "127.0.0.1:9999".parse().unwrap(),
      bytes::Bytes::from_static(b"blocked"),
    )
    .await;
  assert!(
    res.is_err(),
    "an unreliable send to a CIDR-blocked peer must fail, got {res:?}"
  );

  a.shutdown().await.ok();
}
