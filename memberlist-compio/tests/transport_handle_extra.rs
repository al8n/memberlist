//! Handle-method coverage that the primary suites don't reach: the
//! post-`shutdown()` `Err(Shutdown)` short-circuits on the command-shipping
//! methods, the lock-free `metrics()` accessor, and the gossip-plane
//! `set_checksum_options` reconfiguration path.
//!
//! Every command method on a `Memberlist` clone reads `shutdown_flag` at entry
//! and returns `MemberlistError::Shutdown` once the driver's post-loop cleanup
//! has flipped it — proving the guard fires WITHOUT a buffered command (which
//! would otherwise hang on a reply whose Sender outlives the dropped Receiver).
//! `tcp_lifecycle::command_after_shutdown_returns_error_promptly` covers
//! `update_node_metadata` / `set_compression_options` / `set_encryption_options`
//! / `leave`; this file covers the remaining gated command methods.

// Excluded on Windows: constructing nodes here adds bind load that pushes the
// full integration suite past Windows' tight ephemeral-port range (WSAEACCES
// bind exhaustion, even with the driver's bind-retry). The behavior is
// platform-independent and covered on Unix, where the coverage job runs.
#![cfg(all(feature = "tcp", not(target_os = "windows")))]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use bytes::Bytes;
use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Memberlist, MemberlistError, Node, Options, SocketAddrResolver,
  TcpTransport, TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::metrics::Metrics;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

async fn make_tcp(id: &str) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0))),
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

/// `metrics()` is a lock-free accessor with no shutdown guard: a freshly
/// constructed node that has neither shed load nor gossiped reports the zero
/// `Metrics`, and the read still succeeds after `shutdown()` (the snapshot/
/// metrics cells outlive the driver loop via the shared `Rc`).
#[compio::test]
async fn metrics_reads_default_before_and_after_shutdown() {
  let m = make_tcp("metrics-node").await;
  // A clone shares the driver task + the metrics cell; keep it to read after
  // `shutdown(self)` consumes the original.
  let probe = m.clone();
  // No load-shedding has occurred on a quiescent single-node cluster.
  assert_eq!(
    probe.metrics(),
    Metrics::default(),
    "a quiescent node must report the zero metrics"
  );

  m.shutdown().await.expect("shutdown");

  // The accessor has no shutdown guard — it reads the last-published cell.
  assert_eq!(
    probe.metrics(),
    Metrics::default(),
    "metrics() must remain readable after shutdown"
  );
}

/// `set_checksum_options` on a running node succeeds (the gossip-plane checksum
/// policy is reconfigurable in place); after `shutdown()` it short-circuits
/// with `Shutdown` instead of buffering a command that could never be acked.
#[cfg(feature = "crc32")]
#[compio::test]
async fn set_checksum_options_running_then_shutdown() {
  use memberlist_proto::{ChecksumAlgorithm, ChecksumOptions};

  let m = make_tcp("checksum-node").await;
  let probe = m.clone();

  // A built-in algorithm (crc32 is enabled in this build) is accepted live.
  let opts = ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32);
  probe
    .set_checksum_options(opts)
    .await
    .expect("set_checksum_options must succeed on a running node");

  m.shutdown().await.expect("shutdown");

  let after = probe.set_checksum_options(opts).await;
  assert!(
    matches!(after, Err(MemberlistError::Shutdown)),
    "set_checksum_options after shutdown must be Shutdown, got {after:?}"
  );
}

/// Every remaining command-shipping handle method must return `Shutdown`
/// (never hang, never any other error) once the node has shut down. This pins
/// the entry-guard on the methods the lifecycle suite does not cover:
/// `join` / `dispatch_join` / `queue_user_broadcast` / `set_local_state` /
/// `set_ack_payload` / `send` / `send_many` / `ping`.
#[compio::test]
async fn all_command_methods_after_shutdown_return_shutdown_promptly() {
  let m = make_tcp("post-shutdown-extra").await;
  // `shutdown(self)` consumes the original; the clone shares the same (now
  // torn-down) driver and still observes the flipped `shutdown_flag`.
  let probe = m.clone();
  // A second seed so the join targets a real address (the call must still be
  // rejected by the shutdown guard BEFORE any resolution / command send).
  let seed = make_tcp("post-shutdown-seed").await;
  let seed_addr = seed.advertise_address();
  seed.shutdown().await.expect("seed shutdown");

  m.shutdown().await.expect("shutdown");

  let peer = loopback_addr(9);
  let start = std::time::Instant::now();

  let r_join = probe
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;
  let r_dispatch = probe
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;
  let r_broadcast = probe
    .queue_user_broadcast(Bytes::from_static(b"after-shutdown"))
    .await;
  let r_state = probe.set_local_state(Bytes::from_static(b"state")).await;
  let r_ack = probe.set_ack_payload(Bytes::from_static(b"ack")).await;
  let r_send = probe.send(peer, Bytes::from_static(b"u")).await;
  let r_send_many = probe
    .send_many(peer, [Bytes::from_static(b"a"), Bytes::from_static(b"b")])
    .await;
  let r_ping = probe.ping(Node::new(SmolStr::new("ghost"), peer)).await;

  let elapsed = start.elapsed();

  assert!(
    matches!(r_join, Err((_, MemberlistError::Shutdown))),
    "join: expected Shutdown, got {r_join:?}"
  );
  assert!(
    matches!(r_dispatch, Err(MemberlistError::Shutdown)),
    "dispatch_join: expected Shutdown, got {r_dispatch:?}"
  );
  assert!(
    matches!(r_broadcast, Err(MemberlistError::Shutdown)),
    "queue_user_broadcast: expected Shutdown, got {r_broadcast:?}"
  );
  assert!(
    matches!(r_state, Err(MemberlistError::Shutdown)),
    "set_local_state: expected Shutdown, got {r_state:?}"
  );
  assert!(
    matches!(r_ack, Err(MemberlistError::Shutdown)),
    "set_ack_payload: expected Shutdown, got {r_ack:?}"
  );
  assert!(
    matches!(r_send, Err(MemberlistError::Shutdown)),
    "send: expected Shutdown, got {r_send:?}"
  );
  assert!(
    matches!(r_send_many, Err(MemberlistError::Shutdown)),
    "send_many: expected Shutdown, got {r_send_many:?}"
  );
  assert!(
    matches!(r_ping, Err(MemberlistError::Shutdown)),
    "ping: expected Shutdown, got {r_ping:?}"
  );
  assert!(
    elapsed < Duration::from_millis(500),
    "post-shutdown command methods did not return promptly: {elapsed:?}"
  );
}
