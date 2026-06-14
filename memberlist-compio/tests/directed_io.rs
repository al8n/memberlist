//! Directed I/O integration tests — ping, unreliable send, and reliable send.
//!
//! Each test brings up a two-node TCP cluster, exercises the directed I/O
//! API, and asserts the expected outcome (RTT, delegate hook, round-trip).

#![cfg(feature = "tcp")]

use std::{
  borrow::Cow,
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use bytes::Bytes;
use memberlist_compio::{
  ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, Memberlist,
  MemberlistError, MemberlistOptions, NodeDelegate, Options, PingDelegate, SocketAddrResolver,
  TcpTransport, TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::{
  Node,
  typed::{Meta, NodeState},
};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

async fn make_tcp(id: &str, addr: SocketAddr) -> Memberlist<SmolStr, SocketAddr> {
  let ml_opts = MemberlistOptions::new()
    .with_label(Some(b"directed-io-test".to_vec()))
    .expect("valid label");
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_memberlist(ml_opts);
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
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

// ── ping (RTT) ──

/// A `Memberlist` handle backed by a delegate that records received user
/// messages, used for unreliable and reliable directed-send round-trip assertions.
type RecordingMemberlist = memberlist_compio::Memberlist<SmolStr, SocketAddr>;

/// Shared record of bytes delivered via `notify_user_msg`.
type Messages = Arc<Mutex<Vec<Vec<u8>>>>;

struct RecordingDelegate {
  msgs: Messages,
}

impl RecordingDelegate {
  fn new(msgs: Messages) -> Self {
    Self { msgs }
  }
}

impl EventDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_join(&self, _node: Arc<NodeState<SmolStr, SocketAddr>>) {}
}

impl NodeDelegate for RecordingDelegate {
  async fn notify_user_msg(&self, msg: Cow<'_, [u8]>) {
    self.msgs.lock().unwrap().push(msg.to_vec());
  }
}

impl PingDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

async fn make_recording_tcp(id: &str, addr: SocketAddr) -> (RecordingMemberlist, Messages) {
  let msgs: Messages = Arc::new(Mutex::new(Vec::new()));
  let ml_opts = MemberlistOptions::new()
    .with_label(Some(b"directed-io-test".to_vec()))
    .expect("valid label");
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_memberlist(ml_opts);
  let node = RecordingMemberlist::new(
    opts,
    RecordingDelegate::new(msgs.clone()),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording tcp memberlist");
  (node, msgs)
}

/// Ping between two live nodes returns a positive RTT.
#[compio::test]
async fn ping_live_node_returns_positive_rtt() {
  let seed = make_tcp("ping-seed", loopback_addr(0)).await;
  let seed_addr = *seed.local_node().addr_ref();
  let node_a = make_tcp("ping-a", loopback_addr(0)).await;

  let _contacted = node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join seed");

  // Wait for seed to appear in node_a's membership.
  let joined = wait_until(
    || node_a.by_id(&SmolStr::new("ping-seed")).is_some(),
    Duration::from_secs(10),
  )
  .await;
  assert!(joined, "seed did not appear in node_a's membership");

  // Build a Node handle for the seed to ping.
  let seed_node = Node::new(SmolStr::new("ping-seed"), seed_addr);
  let rtt = node_a.ping(seed_node).await.expect("ping should succeed");
  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  seed.shutdown().await.expect("seed shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

/// Ping to an unreachable/unroutable address returns Err.
#[compio::test]
async fn ping_unreachable_node_returns_err() {
  let node = make_tcp("ping-unreachable", loopback_addr(0)).await;
  // Port 1 is typically unreachable on loopback (no listener).
  let unreachable = Node::new(SmolStr::new("ghost"), loopback_addr(1));
  let result = node.ping(unreachable).await;
  assert!(
    result.is_err(),
    "ping to unreachable node should return Err, got {result:?}"
  );
  node.shutdown().await.expect("shutdown");
}

// ── unreliable directed send ──

/// An unreliable directed `send` from node_a reaches node_b's
/// `notify_user_msg` delegate hook.
#[compio::test]
async fn send_unreliable_round_trips_to_delegate() {
  let (node_b, b_msgs) = make_recording_tcp("send-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tcp("send-a", loopback_addr(0)).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"hello-unreliable");
  node_a
    .send(b_addr, payload.clone())
    .await
    .expect("send should succeed");

  let saw_msg = wait_until(
    || {
      b_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_slice() == payload.as_ref())
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    saw_msg,
    "notify_user_msg did not fire on node_b; received: {:?}",
    b_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

/// `send_many` with multiple payloads delivers all of them.
#[compio::test]
async fn send_many_unreliable_delivers_all_payloads() {
  let (node_b, b_msgs) = make_recording_tcp("send-many-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tcp("send-many-a", loopback_addr(0)).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  // Two small payloads fit in one compound datagram.
  node_a
    .send_many(
      b_addr,
      [Bytes::from_static(b"first"), Bytes::from_static(b"second")],
    )
    .await
    .expect("send_many should succeed");

  // Both messages should arrive; order is best-effort.
  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      let has_first = locked.iter().any(|m| m.as_slice() == b"first");
      let has_second = locked.iter().any(|m| m.as_slice() == b"second");
      has_first && has_second
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    saw_both,
    "both payloads did not arrive; received: {:?}",
    b_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

// ── reliable directed send ──

/// A reliable directed `send_reliable` from node_a reaches node_b's
/// `notify_user_msg` delegate hook.
#[compio::test]
async fn send_reliable_round_trips_to_delegate() {
  let (node_b, b_msgs) = make_recording_tcp("rel-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tcp("rel-a", loopback_addr(0)).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"hello-reliable");
  node_a
    .send_reliable(b_addr, payload.clone())
    .await
    .expect("send_reliable should succeed");

  let saw_msg = wait_until(
    || {
      b_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_slice() == payload.as_ref())
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    saw_msg,
    "notify_user_msg did not fire on node_b; received: {:?}",
    b_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

/// `send_many_reliable` delivers all payloads.
#[compio::test]
async fn send_many_reliable_delivers_all_payloads() {
  let (node_b, b_msgs) = make_recording_tcp("rel-many-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tcp("rel-many-a", loopback_addr(0)).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  node_a
    .send_many_reliable(
      b_addr,
      [
        Bytes::from_static(b"rel-first"),
        Bytes::from_static(b"rel-second"),
      ],
    )
    .await
    .expect("send_many_reliable should succeed");

  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      let has_first = locked.iter().any(|m| m.as_slice() == b"rel-first");
      let has_second = locked.iter().any(|m| m.as_slice() == b"rel-second");
      has_first && has_second
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    saw_both,
    "both reliable payloads did not arrive; received: {:?}",
    b_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

/// `send_reliable` to a node that has shut down returns Err.
#[compio::test]
async fn send_reliable_after_shutdown_returns_err() {
  let node_a = make_tcp("send-rel-err-a", loopback_addr(0)).await;
  // Keep a clone alive so we can call send_reliable after shutdown.
  let node_a2 = node_a.clone();

  // Shut down first, then attempt to send.
  node_a.shutdown().await.expect("shutdown");

  // After shutdown the command channel is closed — should return Err.
  let result = node_a2
    .send_reliable(loopback_addr(1), Bytes::from_static(b"post-shutdown"))
    .await;
  assert!(
    result.is_err(),
    "send_reliable after shutdown should return Err, got {result:?}"
  );
}

/// Ping on a shut-down node returns Err immediately (no hang).
#[compio::test]
async fn ping_after_shutdown_returns_err() {
  let node = make_tcp("ping-shutdown", loopback_addr(0)).await;
  // Keep a clone to call ping after shutdown.
  let node2 = node.clone();
  node.shutdown().await.expect("shutdown");

  let unreachable = Node::new(SmolStr::new("ghost"), loopback_addr(1));
  let result = node2.ping(unreachable).await;
  assert!(
    matches!(result, Err(MemberlistError::Shutdown)),
    "expected Shutdown, got {result:?}"
  );
}

/// Ping succeeds against a live peer that was never joined into membership.
///
/// `endpoint.ping` synthesises a transient `NodeState` for unknown targets,
/// so the membership table is irrelevant. This confirms the machine does not
/// reject pings to non-members.
#[compio::test]
async fn ping_without_join_returns_positive_rtt() {
  let node_b = make_tcp("ping-nojoin-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tcp("ping-nojoin-a", loopback_addr(0)).await;

  // Deliberately skip any join — node_b is not in node_a's membership table.
  assert!(
    node_a.by_id(&SmolStr::new("ping-nojoin-b")).is_none(),
    "node_b should not be a known member before the ping"
  );

  let target = Node::new(SmolStr::new("ping-nojoin-b"), b_addr);
  let rtt = node_a.ping(target).await.expect("ping should succeed");
  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

/// `send_reliable` to an UNREACHABLE address resolves to `Err` and does NOT
/// hang. The stream dial to a port with no listener fails, so no terminal
/// `ExchangeCompleted(Succeeded)` ever arrives. The driver must resolve the
/// parked waiter with `Err(SendFailed)` (when the dial is retired before a
/// `Connect`) or via the bridge's `ExchangeCompleted(Failed)` (when the OS
/// connect fails). Either way the await must complete with `Err`.
///
/// The await is wrapped in a short `compio::time::timeout` so a regression
/// that drops the failure (the historical empty-captured-set false `Ok`, or a
/// never-resolving park) fails fast here rather than hanging the suite.
#[compio::test]
async fn send_reliable_to_unreachable_returns_err_not_hang() {
  let node_a = make_tcp("rel-unreach-a", loopback_addr(0)).await;
  // Port 1 on loopback has no listener — the stream dial cannot establish.
  let unreachable = loopback_addr(1);

  let result = compio::time::timeout(
    Duration::from_secs(20),
    node_a.send_reliable(unreachable, Bytes::from_static(b"to-the-void")),
  )
  .await;

  match result {
    Ok(send_res) => assert!(
      send_res.is_err(),
      "send_reliable to an unreachable address MUST return Err, got {send_res:?}"
    ),
    Err(_elapsed) => panic!(
      "send_reliable to an unreachable address HUNG (timed out) — the parked \
       waiter never resolved; the pre-establishment dial failure was not \
       surfaced"
    ),
  }

  node_a.shutdown().await.expect("node_a shutdown");
}

/// `local_state()` / `by_id(local)` read IMMEDIATELY after `new()` — before
/// the driver task republishes its first endpoint-derived snapshot — already
/// reflects the configured `initial_meta`. The construction-time snapshot is
/// built independently of the machine endpoint, so it must carry the same
/// metadata the endpoint's local member will apply.
#[compio::test]
async fn local_state_carries_initial_meta_immediately_after_new() {
  let meta_bytes = Bytes::from_static(b"role=db,zone=eu");
  let meta = Meta::try_from(meta_bytes.clone()).expect("meta within cap");
  let ml_opts = MemberlistOptions::new()
    .with_label(Some(b"directed-io-test".to_vec()))
    .expect("valid label")
    .with_initial_meta(meta);
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("meta-node"))
      // OS-assigned port via :0.
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0))),
  )
  .with_memberlist(ml_opts);
  let node = Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist with initial_meta");

  // No await between `new()` and the read: this observes the initial,
  // construction-time snapshot, not a driver-republished one.
  let local = node.local_state();
  assert_eq!(
    local.meta_ref().as_ref(),
    meta_bytes.as_ref(),
    "local_state() right after new() must carry the configured initial_meta, \
     got {:?}",
    local.meta_ref().as_ref()
  );

  // The same metadata must be visible via `by_id(local)`.
  let by_id = node
    .by_id(&SmolStr::new("meta-node"))
    .expect("local node must be present in the initial snapshot");
  assert_eq!(
    by_id.meta_ref().as_ref(),
    meta_bytes.as_ref(),
    "by_id(local) right after new() must carry the configured initial_meta"
  );

  node.shutdown().await.expect("node shutdown");
}
