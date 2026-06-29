//! TCP lifecycle / post-leave gating tests for the reactor driver.
//!
//! These bring up a converged two-node TCP cluster, gracefully `leave()` one
//! node, and verify every data-plane / control command the driver gates on a
//! running node is rejected with `Error::NotRunning` rather than falsely acked.
//! Also covers the directed-send `NotRunning` paths and a silent-peer ping
//! timeout, which the smoke / directed-io suites do not exercise.

#![cfg(feature = "tcp")]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use memberlist_reactor::{
  CompressionOptions, EncryptionOptions, Error, MaybeResolved, Memberlist, Node, Options,
  SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

async fn make(id: &str) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

/// Brings up `a` + `b`, joins `b` to `a`, and waits for both to see two
/// members. Returns the pair and `a`'s advertised address.
async fn converged_pair() -> (
  Memberlist<SmolStr, SocketAddr, TokioRuntime>,
  Memberlist<SmolStr, SocketAddr, TokioRuntime>,
  SocketAddr,
) {
  let a = make("life-a").await;
  let b = make("life-b").await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  let converged = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "pair did not converge");
  (a, b, a_addr)
}

/// After `leave()` returns (the machine's `LeftCluster` fired), `ping` is
/// rejected with `NotRunning`: the probe scheduler is stopped, so a new
/// application ping's completion event would never arrive and the caller would
/// otherwise hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_after_leave_is_rejected() {
  let (a, b, a_addr) = converged_pair().await;
  let target = Node::new(SmolStr::new("life-a"), a_addr);

  b.leave().await.expect("leave");

  let res = tokio::time::timeout(Duration::from_secs(5), b.ping(target))
    .await
    .expect("ping must not hang after leave");
  assert!(
    matches!(res, Err(Error::NotRunning)),
    "expected NotRunning from ping after leave, got {res:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `leave()`, both the unreliable `send` and reliable `send_reliable`
/// directed-send paths are rejected with `NotRunning` (the gossip / stream
/// coordinators are stopping).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directed_sends_after_leave_are_rejected() {
  let (a, b, a_addr) = converged_pair().await;

  b.leave().await.expect("leave");

  let unreliable = tokio::time::timeout(
    Duration::from_secs(5),
    b.send(a_addr, Bytes::from_static(b"x")),
  )
  .await
  .expect("send must not hang after leave");
  assert!(
    matches!(unreliable, Err(Error::NotRunning)),
    "expected NotRunning from send after leave, got {unreliable:?}"
  );

  let reliable = tokio::time::timeout(
    Duration::from_secs(5),
    b.send_reliable(a_addr, Bytes::from_static(b"y")),
  )
  .await
  .expect("send_reliable must not hang after leave");
  assert!(
    matches!(reliable, Err(Error::NotRunning)),
    "expected NotRunning from send_reliable after leave, got {reliable:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `leave()`, both policy mutations (`set_compression_options` /
/// `set_encryption_options`) are rejected with `NotRunning` rather than falsely
/// acked: the node emits no protocol traffic, so a new policy could never take
/// effect on the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_policy_options_after_leave_is_rejected() {
  let (a, b, _a_addr) = converged_pair().await;

  b.leave().await.expect("leave");

  let res_compr = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_compression_options(CompressionOptions::new()),
  )
  .await
  .expect("set_compression_options must not hang after leave");
  assert!(
    matches!(res_compr, Err(Error::NotRunning)),
    "expected NotRunning from set_compression_options after leave, got {res_compr:?}"
  );

  let res_enc = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_encryption_options(EncryptionOptions::new()),
  )
  .await
  .expect("set_encryption_options must not hang after leave");
  assert!(
    matches!(res_enc, Err(Error::NotRunning)),
    "expected NotRunning from set_encryption_options after leave, got {res_enc:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `leave()`, both the synchronous `join` (WaitForCompletion) and the
/// fire-and-forget `join_detached` (Dispatch) funnel through the same
/// running-node gate and are rejected with `NotRunning` rather than falsely
/// reporting any reached or dispatched seeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_after_leave_is_rejected() {
  let (a, b, a_addr) = converged_pair().await;

  b.leave().await.expect("leave");

  let res = tokio::time::timeout(
    Duration::from_secs(5),
    b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)]),
  )
  .await
  .expect("join must not hang after leave");
  assert!(
    matches!(res, Err((_, Error::NotRunning))),
    "expected NotRunning from join after leave, got {res:?}"
  );

  let res_detached = tokio::time::timeout(
    Duration::from_secs(5),
    b.join_detached(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)]),
  )
  .await
  .expect("join_detached must not hang after leave");
  assert!(
    matches!(res_detached, Err(Error::NotRunning)),
    "expected NotRunning from join_detached after leave, got {res_detached:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A second `leave()` racing the first joins the in-flight leave: both replies
/// resolve together on the single `LeftCluster`. Neither hangs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_leave_from_cloned_handles_both_succeed() {
  let (a, b, _a_addr) = converged_pair().await;
  let b2 = b.clone();

  let (r1, r2) = tokio::join!(
    tokio::time::timeout(Duration::from_secs(8), b.leave()),
    tokio::time::timeout(Duration::from_secs(8), b2.leave()),
  );
  assert!(matches!(r1, Ok(Ok(()))), "first leave: {r1:?}");
  assert!(
    matches!(r2, Ok(Ok(()))),
    "second leave joined the first: {r2:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// `leave()` on a node that already left (a second sequential leave) is a no-op
/// that still returns `Ok(())` — the machine is no longer running, so the leave
/// fires no second `LeftCluster`, and the driver replies immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_sequential_leave_is_ok_noop() {
  let m = make("double-leave").await;
  let first = tokio::time::timeout(Duration::from_secs(5), m.leave()).await;
  assert!(matches!(first, Ok(Ok(()))), "first leave: {first:?}");
  let second = tokio::time::timeout(Duration::from_secs(5), m.leave()).await;
  assert!(
    matches!(second, Ok(Ok(()))),
    "a second sequential leave is a no-op Ok, got {second:?}"
  );
  let _ = m.shutdown().await;
}

/// A directed `ping` to a silent peer (an address with no memberlist) times
/// out with `Err(PingTimeout)` rather than hanging: the probe FSM fires its
/// failure deadline and the driver resolves the parked waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_silent_peer_times_out() {
  let a = make("ping-silent-a").await;
  // A UDP socket that binds the port but never speaks memberlist: pings get no
  // ack, so the probe deadline fires.
  let silent = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind silent socket");
  let silent_addr = silent.local_addr().expect("silent local_addr");
  let target = Node::new(SmolStr::new("ghost"), silent_addr);

  let res = tokio::time::timeout(Duration::from_secs(20), a.ping(target))
    .await
    .expect("ping to a silent peer must resolve (timeout), not hang");
  assert!(
    matches!(res, Err(Error::PingTimeout)),
    "expected PingTimeout pinging a silent peer, got {res:?}"
  );

  let _ = a.shutdown().await;
}

/// `join` / `join_detached` with an EMPTY seed list is a no-op success: nothing
/// is dispatched, and the call returns `Ok(0)` rather than `JoinFailed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_empty_seeds_is_zero_contact_success() {
  let m = make("empty-seeds").await;
  let n = m
    .join(&SocketAddrResolver, &[])
    .await
    .expect("empty-seed join is a no-op success");
  assert!(n.is_empty(), "no seeds dispatched");
  let n2 = m
    .join_detached(&SocketAddrResolver, &[])
    .await
    .expect("empty-seed detached join is a no-op success");
  assert_eq!(n2, 0, "no seeds dispatched (detached)");
  let _ = m.shutdown().await;
}

/// `join_detached` dispatches its seeds and returns the dispatched count
/// immediately, without waiting for the push/pull exchanges; the cluster still
/// converges afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_detached_dispatches_and_converges() {
  let a = make("detach-a").await;
  let b = make("detach-b").await;
  let a_addr = *a.local().addr_ref();

  let dispatched = b
    .join_detached(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join_detached");
  assert_eq!(dispatched, 1, "one seed dispatched (fire-and-forget)");

  let converged = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "detached join did not converge");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// Every command on a live clone after `shutdown()` returns `Err(Shutdown)`
/// promptly rather than hanging on a reply no driver will send.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commands_after_shutdown_return_shutdown_promptly() {
  let m = make("post-shutdown").await;
  let clone = m.clone();
  m.shutdown().await.expect("shutdown");

  let start = std::time::Instant::now();
  let r_join = clone.join(&SocketAddrResolver, &[]).await;
  let r_leave = clone.leave().await;
  let r_send = clone
    .send("127.0.0.1:1".parse().unwrap(), Bytes::from_static(b"z"))
    .await;
  let r_compr = clone
    .set_compression_options(CompressionOptions::new())
    .await;
  let r_enc = clone.set_encryption_options(EncryptionOptions::new()).await;
  let elapsed = start.elapsed();

  assert!(
    matches!(r_join, Err((_, Error::Shutdown))),
    "join: {r_join:?}"
  );
  assert!(
    matches!(r_leave, Err(Error::Shutdown)),
    "leave: {r_leave:?}"
  );
  assert!(matches!(r_send, Err(Error::Shutdown)), "send: {r_send:?}");
  assert!(
    matches!(r_compr, Err(Error::Shutdown)),
    "compr: {r_compr:?}"
  );
  assert!(matches!(r_enc, Err(Error::Shutdown)), "enc: {r_enc:?}");
  assert!(
    elapsed < Duration::from_secs(2),
    "post-shutdown commands did not return promptly: {elapsed:?}"
  );
}

/// `events_dropped` / `observation_dropped` are readable on a fresh node and
/// start at zero (the recoverable / unrecoverable drop counters).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_counters_start_zero() {
  let m = make("counters").await;
  assert_eq!(m.events_dropped(), 0, "no event drops on a fresh node");
  assert_eq!(
    m.observation_dropped(),
    0,
    "no observation drops on a fresh node"
  );
  let _ = m.shutdown().await;
}
