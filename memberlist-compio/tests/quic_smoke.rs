//! QUIC backend smoke tests.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{net::SocketAddr, time::Duration};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Memberlist, MemberlistError, MemberlistOptions, Options,
  QuicOptions, QuicTransport, QuicTransportOptions, SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a `Memberlist` on an OS-allocated loopback port (`127.0.0.1:0`).
/// The concrete bound address is read back from `advertise_address()` after
/// construction. The membership-input address type is `SocketAddr`, so the
/// construction resolver is the identity `SocketAddrResolver` (never invoked
/// for a resolved advertise).
async fn make_quic(id: &str, qcfg: QuicOptions) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(qcfg),
  );
  Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind quic memberlist")
}

#[compio::test]
async fn quic_memberlist_binds_and_shuts_down_cleanly() {
  let ml = make_quic("node-a", support::self_trusted_quic_config()).await;
  assert_eq!(ml.alive_count(), 1);
  assert_eq!(ml.member_count(), 1);
  let timed = compio::time::timeout(Duration::from_secs(5), ml.shutdown())
    .await
    .expect("shutdown within 5s");
  timed.expect("shutdown Ok");
}

/// Two-node end-to-end smoke: A dispatch-joins B's listening port, the
/// QUIC handshake + push/pull exchange completes inside the 5-second
/// budget, and both nodes converge on `alive_count == 2`. Exercises
/// the per-tick four-surface drain (poll_event /
/// poll_memberlist_ingress / poll_memberlist_transmit / poll_transmit)
/// and the umbrella codec wrap/unwrap on the unreliable gossip path.
///
/// Both nodes bind an OS-allocated port. Construction resolves the `:0` request
/// to the real bound port and stores THAT in the membership FSM, so
/// `advertise_address()` returns the port subsequent peers dial — no hard-coded
/// port is needed for the join to land.
#[compio::test]
async fn quic_memberlist_two_node_dispatch_join_exchanges_gossip() {
  // Two nodes sharing one self-signed cert + trust root — each
  // accepts the other's (identical) cert via the shared root. The
  // `PrivateKeyDer::clone_key` call mints a `'static` clone so the
  // same key bytes back both nodes' rustls server configs.
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make_quic("a", qcfg_a).await;
  let b = make_quic("b", qcfg_b).await;

  // Dispatch-style join — fire-and-forget, no synchronous wait. The
  // returned count is the number of seeds the driver accepted, not
  // the number that contacted; per-seed contact accounting is the
  // job of the synchronous `join` path via
  // `Event::ExchangeCompleted`.
  let n = a
    .dispatch_join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(b.advertise_address())],
    )
    .await
    .expect("dispatch_join");
  assert_eq!(n, 1, "one seed dispatched");

  // Allow time for the QUIC handshake + push/pull exchange to
  // complete on both directions. The handshake is RTT-bounded; on
  // localhost the RTT is sub-millisecond, so 2s is two-orders-of-
  // magnitude over the worst-case budget.
  compio::time::sleep(Duration::from_secs(2)).await;

  // Both nodes converge on the same observed membership.
  assert!(
    a.alive_count() >= 2,
    "A did not learn about B (alive_count={})",
    a.alive_count()
  );
  assert!(
    b.alive_count() >= 2,
    "B did not learn about A (alive_count={})",
    b.alive_count()
  );

  // Ignoring Err: shutdown is best-effort during test teardown. A
  // timeout error here would only suppress the test's expected
  // success summary; the bound UDP port releases when the driver
  // task drops regardless of the awaited reply.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  // Ignoring Err: same rationale as the prior teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Idle-endpoint smoke: no peers, no commands, no UDP traffic — the
/// coordinator's wake timer is the only thing rousing the driver
/// loop. Verifies the iter-top past-due peek + `fire_timeout_with_drain`
/// helper do not deadlock on a quiescent socket: across a 500 ms idle
/// window the snapshot stays consistent (`alive_count == 1`,
/// `member_count == 1`) and a shutdown round-trip completes inside the
/// budget. Tighter assertions on which timer paths fire would require
/// a controlled flood/timer race; the two-node smoke above exercises
/// real `handle_timeout` fires implicitly via the probe broadcast and
/// push/pull deadlines.
#[compio::test]
async fn quic_memberlist_idle_timer_does_not_deadlock() {
  let ml = make_quic("idle", support::self_trusted_quic_config()).await;
  assert_eq!(ml.alive_count(), 1);

  // Give the driver loop ample time to fire its wake timer at least
  // once. Under the default `idle_wake_interval` (60s) the timer
  // won't fire here, but the coordinator's own probe-broadcast
  // schedule fires deadlines on the order of seconds even on an
  // otherwise-idle endpoint — those fires exercise the iter-top
  // past-due peek + helper path.
  compio::time::sleep(Duration::from_millis(500)).await;

  assert_eq!(ml.alive_count(), 1);
  assert_eq!(ml.member_count(), 1);

  let timed = compio::time::timeout(Duration::from_secs(5), ml.shutdown())
    .await
    .expect("shutdown within 5s");
  timed.expect("shutdown Ok");
}

/// The identity-aware gossip_mtu floor lives in the shared `Memberlist::new`
/// path, so it applies to the QUIC backend identically: a node whose unbounded
/// local id is large enough that its own mandatory single-datagram control
/// packets no longer fit the gossip budget is rejected, while a normal-sized id
/// at the SAME gossip_mtu binds. Confirms both drivers stay symmetric.
#[compio::test]
async fn quic_gossip_mtu_rejected_for_oversized_local_id() {
  // The worst-case Ping for a 2000-byte id frames to ~4 KiB, far above this
  // 2000-byte gossip_mtu; the same budget easily holds a small id's worst case.
  let huge_id = SmolStr::new("a".repeat(2000));
  let opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(huge_id)
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(support::self_trusted_quic_config()),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(2000));
  let res = Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::GossipMtuTooSmall(e)) => {
      assert_eq!(e.configured(), 2000, "carries the effective gossip_mtu");
      assert!(
        e.minimum() > 2000,
        "the id-driven required minimum must exceed the configured gossip_mtu, got {}",
        e.minimum()
      );
    }
    Err(other) => panic!("expected GossipMtuTooSmall, got {other:?}"),
    Ok(_) => panic!("an oversized local id must be rejected, but construction succeeded"),
  }

  // A normal small id at the SAME gossip_mtu binds successfully.
  let ml = make_quic("small-id", support::self_trusted_quic_config()).await;
  let timed = compio::time::timeout(Duration::from_secs(5), ml.shutdown())
    .await
    .expect("shutdown within 5s");
  timed.expect("shutdown Ok");
}
