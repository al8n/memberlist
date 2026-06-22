//! Handle-API coverage the smoke / lifecycle / directed-io suites do not reach:
//! the `set_checksum_options` runtime setter (live success, post-leave
//! `NotRunning`, post-shutdown `Shutdown`), the lock-free `snapshot` / `metrics`
//! accessors, the post-leave `NotRunning` rejection of the remaining gossip-state
//! setters (`update_node_metadata` / `queue_user_broadcast` / `set_local_state` /
//! `set_ack_payload`), and node construction over a bounded observation channel
//! and a custom `meta_max_size`.

#![cfg(feature = "tcp")]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use memberlist_reactor::{
  Channel, ChecksumAlgorithm, ChecksumOptions, Error, MaybeResolved, Memberlist, MemberlistOptions,
  Options, RuntimeOptions, SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

async fn make(id: &str) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  make_with(id, Options::new()).await
}

async fn make_with(
  id: &str,
  opts: Options<SmolStr>,
) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    opts,
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

/// Brings up `a` + `b`, joins `b` to `a`, and waits for both to see two members.
async fn converged_pair() -> (
  Memberlist<SmolStr, SocketAddr, TokioRuntime>,
  Memberlist<SmolStr, SocketAddr, TokioRuntime>,
  SocketAddr,
) {
  let a = make("hx-a").await;
  let b = make("hx-b").await;
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

/// `set_checksum_options` on a LIVE node validates and applies the gossip
/// checksum policy in place, replying `Ok(())` (the running success arm of the
/// handle setter — the suites configure checksum at construction, never via the
/// runtime setter).
#[cfg(feature = "crc32")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_checksum_options_live_succeeds() {
  let m = make("chk-live").await;
  let res = tokio::time::timeout(
    Duration::from_secs(5),
    m.set_checksum_options(ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32)),
  )
  .await
  .expect("set_checksum_options must resolve");
  assert!(
    res.is_ok(),
    "a built-in checksum algorithm is applied: {res:?}"
  );
  let _ = m.shutdown().await;
}

/// After `leave()`, `set_checksum_options` is rejected with `NotRunning` rather
/// than falsely acked: the node emits no gossip datagrams, so a new checksum
/// policy could never take effect on the wire.
#[cfg(feature = "crc32")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_checksum_options_after_leave_is_rejected() {
  let (a, b, _a_addr) = converged_pair().await;
  b.leave().await.expect("leave");

  let res = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_checksum_options(ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32)),
  )
  .await
  .expect("set_checksum_options must not hang after leave");
  assert!(
    matches!(res, Err(Error::NotRunning)),
    "expected NotRunning from set_checksum_options after leave, got {res:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `shutdown()`, `set_checksum_options` on a live clone returns
/// `Err(Shutdown)` promptly (the handle's pre-enqueue shutdown gate), rather than
/// hanging on a reply no driver will send.
#[cfg(feature = "crc32")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_checksum_options_after_shutdown_returns_shutdown() {
  let m = make("chk-shutdown").await;
  let clone = m.clone();
  m.shutdown().await.expect("shutdown");

  let res = tokio::time::timeout(
    Duration::from_secs(2),
    clone.set_checksum_options(ChecksumOptions::new()),
  )
  .await
  .expect("set_checksum_options must resolve promptly after shutdown");
  assert!(
    matches!(res, Err(Error::Shutdown)),
    "expected Shutdown from set_checksum_options after shutdown, got {res:?}"
  );
}

/// The lock-free `snapshot()` accessor returns the same membership the typed
/// accessors do: on a converged pair it carries both members and the correct
/// local id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_reflects_membership() {
  let (a, b, _a_addr) = converged_pair().await;

  let snap = a.snapshot();
  assert_eq!(
    snap.members().len(),
    a.num_members(),
    "snapshot membership matches num_members"
  );
  assert_eq!(snap.members().len(), 2, "both members are present");
  assert_eq!(
    snap.local_ref().id_ref().as_str(),
    "hx-a",
    "snapshot carries the local id"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// `metrics()` is readable lock-free on a fresh node and all load-shedding
/// counters start at zero (no gossip dropped, no member rejected, etc.).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_start_zero_on_fresh_node() {
  let m = make("metrics-fresh").await;
  let mx = m.metrics();
  assert_eq!(
    mx.gossip_ingress_dropped, 0,
    "no gossip dropped on a fresh node"
  );
  assert_eq!(mx.members_rejected, 0, "no member rejected on a fresh node");
  assert_eq!(
    mx.inbound_streams_rejected, 0,
    "no inbound stream rejected on a fresh node"
  );
  assert_eq!(
    mx.indirect_forwards_dropped, 0,
    "no indirect forward dropped on a fresh node"
  );
  assert_eq!(
    mx.ack_payloads_withheld, 0,
    "no ack payload withheld on a fresh node"
  );
  let _ = m.shutdown().await;
}

/// After `leave()`, every remaining gossip-state setter — `update_node_metadata`,
/// `queue_user_broadcast`, `set_local_state`, `set_ack_payload` — is rejected
/// with `NotRunning` rather than falsely acked: the schedulers are stopped, so
/// none of these could be gossiped / carried on the wire. The directed-io suites
/// only cover their live-node success arms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_state_setters_after_leave_are_rejected() {
  let (a, b, _a_addr) = converged_pair().await;
  b.leave().await.expect("leave");

  let meta = tokio::time::timeout(
    Duration::from_secs(5),
    b.update_node_metadata(b"web".to_vec()),
  )
  .await
  .expect("update_node_metadata must not hang after leave");
  assert!(
    matches!(meta, Err(Error::NotRunning)),
    "expected NotRunning from update_node_metadata after leave, got {meta:?}"
  );

  let bcast = tokio::time::timeout(
    Duration::from_secs(5),
    b.queue_user_broadcast(Bytes::from_static(b"bcast")),
  )
  .await
  .expect("queue_user_broadcast must not hang after leave");
  assert!(
    matches!(bcast, Err(Error::NotRunning)),
    "expected NotRunning from queue_user_broadcast after leave, got {bcast:?}"
  );

  let state = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_local_state(Bytes::from_static(b"state")),
  )
  .await
  .expect("set_local_state must not hang after leave");
  assert!(
    matches!(state, Err(Error::NotRunning)),
    "expected NotRunning from set_local_state after leave, got {state:?}"
  );

  let ack = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_ack_payload(Bytes::from_static(b"ack")),
  )
  .await
  .expect("set_ack_payload must not hang after leave");
  assert!(
    matches!(ack, Err(Error::NotRunning)),
    "expected NotRunning from set_ack_payload after leave, got {ack:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A node constructed with a BOUNDED observation channel (rather than the default
/// unbounded one) still converges and delivers the gossip-state setters — the
/// `Channel::Bounded` arm of the driver's observation-channel + byte-budget
/// construction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_observation_channel_node_converges() {
  let opts = Options::new()
    .with_runtime(RuntimeOptions::new().with_observation_channel(Channel::Bounded(8)));
  let a = make_with("bnd-a", opts).await;
  let b = make("bnd-b").await;
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
  assert!(
    converged.is_ok(),
    "a node with a bounded observation channel must still converge"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A node constructed with an explicit `meta_max_size` override binds and reports
/// its own identity — the `with_meta_max_size` branch of the memberlist-options
/// application that no other test exercises.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_meta_max_size_node_binds() {
  let opts = Options::new().with_memberlist(MemberlistOptions::new().with_meta_max_size(256));
  let m = make_with("meta-cap", opts).await;
  assert_eq!(
    m.local_id().as_str(),
    "meta-cap",
    "node binds with a custom meta cap"
  );
  assert_eq!(m.num_members(), 1, "the fresh node knows only itself");
  let _ = m.shutdown().await;
}
