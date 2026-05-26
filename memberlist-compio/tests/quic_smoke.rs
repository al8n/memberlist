//! QUIC backend smoke tests.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{net::SocketAddr, time::Duration};

use memberlist_compio::QuicMemberlist;
use memberlist_machine::config::EndpointConfig;
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

fn make_config(id: &str, port: u16) -> EndpointConfig<SmolStr, SocketAddr> {
  EndpointConfig::new(SmolStr::new(id), loopback_addr(port))
}

#[compio::test]
async fn quic_memberlist_binds_and_shuts_down_cleanly() {
  let cfg = make_config("node-a", 0); // OS-pick — avoids parallel-test collisions.
  let qcfg = support::self_trusted_quic_config();
  let ml = QuicMemberlist::new(cfg, qcfg).await.expect("bind");
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
/// Distinct fixed ports (7401, 7402) — config-supplied advertise
/// ports are what the membership FSM stores and what subsequent
/// peers must dial; OS-pick would leave the FSM advertising port 0.
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

  let a_port: u16 = 7401;
  let b_port: u16 = 7402;
  let a = QuicMemberlist::new(make_config("a-7401", a_port), qcfg_a)
    .await
    .expect("bind a");
  let b = QuicMemberlist::new(make_config("b-7402", b_port), qcfg_b)
    .await
    .expect("bind b");

  // Dispatch-style join — fire-and-forget, no synchronous wait. The
  // returned count is the number of seeds the driver accepted, not
  // the number that contacted; per-seed contact accounting is the
  // job of the synchronous `join_with` path via
  // `Event::ExchangeCompleted`.
  let n = a
    .dispatch_join(&[memberlist_compio::Address::from(loopback_addr(b_port))])
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
  let cfg = make_config("idle", 0);
  let qcfg = support::self_trusted_quic_config();
  let ml = QuicMemberlist::new(cfg, qcfg).await.expect("bind");
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
