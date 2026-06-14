//! Failure-detection tests over TCP.
//!
//! These verify that the compio driver runs the SWIM probe → suspicion → dead
//! pipeline now that the periodic schedulers are armed at driver-loop entry. A
//! node that dies silently (no graceful leave) must be detected by the
//! survivors purely through missed probe acks.
//!
//! Run single-threaded (`--test-threads=1`): the suspicion timing is sensitive
//! and parallel CPU contention can stretch the probe/timeout cadence.

#![cfg(feature = "tcp")]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::{Duration, Instant},
};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Memberlist, Options, SocketAddrResolver, TcpTransport,
  TcpTransportOptions, VoidDelegate,
};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Build an unlabeled `Memberlist` advertising `addr`.
async fn make_tcp(id: &str, addr: SocketAddr) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  );
  Memberlist::new(
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
    compio::time::sleep(Duration::from_millis(50)).await;
  }
  predicate()
}

/// A peer that dies silently (all of its `Memberlist` handles dropped, so its
/// command channel disconnects and its driver loop exits WITHOUT broadcasting a
/// leave) is detected by the survivors through the SWIM probe → suspicion →
/// dead pipeline, and reaped from the alive set.
///
/// Three nodes converge to `alive_count() == 3`; the third is then dropped. The
/// drop is the only crash signal — no graceful leave is sent — so the survivors
/// can only notice through missed probe acks. Within the failure-detection
/// window (probe 1s → timeout 500ms → suspicion ≈ a few s → reap) the two
/// survivors must converge to `alive_count() == 2`.
///
/// Three nodes (rather than two) make the suspicion path robust: with a
/// dedicated observer that is not itself the crashed node's only peer, the
/// indirect-probe and gossip-of-suspect paths both have a live witness, so the
/// detection does not hinge on a single direct-probe schedule landing inside
/// the window.
#[compio::test]
async fn crashed_peer_is_marked_not_alive() {
  let a = make_tcp("fd-a", loopback_addr(0)).await;
  let b = make_tcp("fd-b", loopback_addr(0)).await;
  let c = make_tcp("fd-c", loopback_addr(0)).await;
  let a_addr = a.advertise_address();

  // B and C both join the seed A; gossip then converges all three.
  let b_contacted = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("b joins a");
  assert_eq!(b_contacted, 1, "b contacts exactly one seed");
  let c_contacted = c
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("c joins a");
  assert_eq!(c_contacted, 1, "c contacts exactly one seed");

  let converged = wait_until(
    || a.alive_count() == 3 && b.alive_count() == 3 && c.alive_count() == 3,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge to 3 alive: a={}, b={}, c={}",
    a.alive_count(),
    b.alive_count(),
    c.alive_count()
  );

  // Crash C silently. `c` is the ONLY handle for that node (never cloned, no
  // `events()` stream retained — and `events()` would only clone the events
  // Receiver, not the command Sender, so it could not keep the driver alive
  // anyway). Dropping it disconnects C's command channel; the driver loop
  // breaks on the disconnected-recv arm without broadcasting a leave, and the
  // sockets close. The survivors see only the absence of probe acks.
  drop(c);

  // A and B must detect the silent death through SWIM and reap C. Window:
  // probe 1s → timeout 500ms → suspicion (≈ suspicion_mult × probe) → dead.
  let detected = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(12),
  )
  .await;
  assert!(
    detected,
    "survivors did not reap the crashed peer: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}
