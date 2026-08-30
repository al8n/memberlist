//! #170: IPv4-mapped IPv6 advertise canonicalization over the reactor TCP driver.
//!
//! A resolved `::ffff:a.b.c.d` advertise is rewritten to its plain IPv4 form
//! BEFORE the bind, so the node has ONE published identity (no split across two
//! address families) and a mapped-config peer converges with a plain-IPv4 peer to
//! a single membership entry. Mirrors the compio `mapped_advertise_canonicalized_to_v4`
//! and `mapped_and_v4_spellings_converge_to_one_peer` tests.

#![cfg(feature = "tcp")]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{MaybeResolved, Memberlist, Options, SocketAddrResolver, VoidDelegate};
use smol_str::SmolStr;

/// Build a reactor TCP node advertising `advertise` (used to hand in a mapped
/// IPv6 advertise). The constructor canonicalizes and reads the bound address
/// back.
async fn make(id: &str, advertise: SocketAddr) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved(advertise),
    Options::new(),
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

/// A resolved mapped IPv6 advertise is canonicalized to IPv4 before binding, so
/// the node publishes a single IPv4 identity with a concrete resolved port.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mapped_advertise_canonicalized_to_v4() {
  let m = make("mapped-adv", "[::ffff:127.0.0.1]:0".parse().unwrap()).await;
  let advertise = m.advertise_address();
  assert!(
    advertise.is_ipv4(),
    "a mapped IPv6 advertise must publish as IPv4, got {advertise}"
  );
  assert_eq!(advertise.ip(), std::net::Ipv4Addr::LOCALHOST);
  assert_ne!(
    advertise.port(),
    0,
    "ephemeral :0 must resolve to a concrete port"
  );
  let _ = m.shutdown().await;
}

/// A mapped-config node and a plain-IPv4 node converge to ONE membership entry —
/// no split identity, no duplicate keyed at a second spelling. B (mapped config)
/// joins A (plain V4); both settle at exactly 2 members and A keys B at V4.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mapped_and_v4_spellings_converge_to_one_peer() {
  let a = make("mapped-a", "127.0.0.1:0".parse().unwrap()).await;
  let b = make("mapped-b", "[::ffff:127.0.0.1]:0".parse().unwrap()).await;

  assert!(
    b.advertise_address().is_ipv4(),
    "a mapped advertise must canonicalize to IPv4, got {}",
    b.advertise_address()
  );

  let a_addr = a.advertise_address();
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
    "cluster did not converge: a={} b={}",
    a.num_members(),
    b.num_members()
  );

  assert_eq!(
    a.num_members(),
    2,
    "A must key B once, not once per spelling"
  );

  // A keys B at a V4 address (B's canonical identity), never a mapped IPv6.
  let b_id = b.local().id_ref().clone();
  let b_in_a = a
    .members()
    .into_iter()
    .find(|n| *n.id_ref() == b_id)
    .expect("A must know B");
  assert!(
    b_in_a.address_ref().is_ipv4(),
    "A must key B at its canonical IPv4 address, got {}",
    b_in_a.address_ref()
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}
