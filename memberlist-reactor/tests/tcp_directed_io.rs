//! TCP directed I/O integration tests — ping, unreliable send, and reliable send.
//!
//! Each test brings up a two-node TCP cluster, exercises the directed I/O API,
//! and asserts the expected outcome: a positive RTT for ping, delivery into the
//! peer's `notify_user_msg` for unreliable and reliable sends.

#![cfg(feature = "tcp")]

use std::{
  future::Future,
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::Duration,
};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use memberlist_proto::typed::Meta;
use memberlist_reactor::{
  Delegate, Error, MaybeResolved, Memberlist, MemberlistOptions, Node, Options, SocketAddrResolver,
  VoidDelegate,
};
use smol_str::SmolStr;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Builds a TCP node with OS-assigned loopback port and VoidDelegate.
async fn make(id: &str) -> Memberlist<SmolStr> {
  Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

/// Shared record of bytes delivered via `notify_user_msg`.
type Messages = Arc<Mutex<Vec<Vec<u8>>>>;

/// Delegate that records every `notify_user_msg` payload.
struct RecordingDelegate {
  msgs: Messages,
}

impl RecordingDelegate {
  fn new() -> (Self, Messages) {
    let msgs: Messages = Arc::new(Mutex::new(Vec::new()));
    (Self { msgs: msgs.clone() }, msgs)
  }
}

impl Delegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  fn notify_user_msg(&self, msg: Bytes) -> impl Future<Output = ()> + Send + '_ {
    let msgs = self.msgs.clone();
    async move {
      msgs.lock().unwrap().push(msg.to_vec());
    }
  }
}

async fn make_recording(id: &str) -> (Memberlist<SmolStr>, Messages) {
  let (delegate, msgs) = RecordingDelegate::new();
  let node = Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    delegate,
  )
  .await
  .expect("bind tcp memberlist");
  (node, msgs)
}

/// Polls `predicate` until it returns `true` or `deadline` elapses.
async fn wait_until(mut predicate: impl FnMut() -> bool, deadline: Duration) -> bool {
  let start = std::time::Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

// ── ping ──────────────────────────────────────────────────────────────────────

/// Pinging a live peer returns a positive RTT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_live_node_returns_positive_rtt() {
  let a = make("ping-a").await;
  let b = make("ping-b").await;
  let a_addr = *a.local().addr_ref();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  // Wait for a to appear in b's membership.
  let joined = wait_until(
    || b.by_id(&SmolStr::new("ping-a")).is_some(),
    Duration::from_secs(8),
  )
  .await;
  assert!(joined, "peer did not appear in membership");

  let target = Node::new(SmolStr::new("ping-a"), a_addr);
  let rtt = tokio::time::timeout(Duration::from_secs(8), b.ping(target))
    .await
    .expect("ping timed out")
    .expect("ping should succeed");

  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// Pinging after node shutdown returns `Err(Shutdown)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_after_shutdown_returns_err() {
  let node = make("ping-shutdown-a").await;
  let node2 = node.clone();
  node.shutdown().await.expect("shutdown");

  let target = Node::new(
    SmolStr::new("ghost"),
    "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
  );
  let result = tokio::time::timeout(Duration::from_secs(5), node2.ping(target))
    .await
    .expect("must not hang");
  assert!(
    matches!(result, Err(Error::Shutdown)),
    "expected Shutdown, got {result:?}"
  );
}

// ── unreliable send ───────────────────────────────────────────────────────────

/// An unreliable `send` reaches the peer's `notify_user_msg` delegate hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_unreliable_delivers_to_delegate() {
  let (b, b_msgs) = make_recording("send-unrel-b").await;
  let b_addr = *b.local().addr_ref();
  let a = make("send-unrel-a").await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"hello-unreliable");
  tokio::time::timeout(Duration::from_secs(5), a.send(b_addr, payload.clone()))
    .await
    .expect("send timed out")
    .expect("send should succeed");

  let saw_msg = wait_until(
    || {
      b_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_slice() == payload.as_ref())
    },
    Duration::from_secs(8),
  )
  .await;
  assert!(
    saw_msg,
    "notify_user_msg did not fire; received: {:?}",
    b_msgs.lock().unwrap()
  );

  let _ = b.shutdown().await;
  let _ = a.shutdown().await;
}

/// `send_many` with multiple payloads delivers all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_many_unreliable_delivers_all_payloads() {
  let (b, b_msgs) = make_recording("send-many-unrel-b").await;
  let b_addr = *b.local().addr_ref();
  let a = make("send-many-unrel-a").await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  tokio::time::timeout(
    Duration::from_secs(5),
    a.send_many(
      b_addr,
      [Bytes::from_static(b"first"), Bytes::from_static(b"second")],
    ),
  )
  .await
  .expect("send_many timed out")
  .expect("send_many should succeed");

  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      locked.iter().any(|m| m.as_slice() == b"first")
        && locked.iter().any(|m| m.as_slice() == b"second")
    },
    Duration::from_secs(8),
  )
  .await;
  assert!(
    saw_both,
    "both payloads did not arrive; received: {:?}",
    b_msgs.lock().unwrap()
  );

  let _ = b.shutdown().await;
  let _ = a.shutdown().await;
}

// ── reliable send ─────────────────────────────────────────────────────────────

/// A reliable `send_reliable` reaches the peer's `notify_user_msg` hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_reliable_delivers_to_delegate() {
  let (b, b_msgs) = make_recording("send-rel-b").await;
  let b_addr = *b.local().addr_ref();
  let a = make("send-rel-a").await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"hello-reliable");
  tokio::time::timeout(
    Duration::from_secs(8),
    a.send_reliable(b_addr, payload.clone()),
  )
  .await
  .expect("send_reliable timed out")
  .expect("send_reliable should succeed");

  let saw_msg = wait_until(
    || {
      b_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_slice() == payload.as_ref())
    },
    Duration::from_secs(8),
  )
  .await;
  assert!(
    saw_msg,
    "notify_user_msg did not fire; received: {:?}",
    b_msgs.lock().unwrap()
  );

  let _ = b.shutdown().await;
  let _ = a.shutdown().await;
}

/// `send_many_reliable` delivers all payloads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_many_reliable_delivers_all_payloads() {
  let (b, b_msgs) = make_recording("send-many-rel-b").await;
  let b_addr = *b.local().addr_ref();
  let a = make("send-many-rel-a").await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  tokio::time::timeout(
    Duration::from_secs(8),
    a.send_many_reliable(
      b_addr,
      [
        Bytes::from_static(b"rel-first"),
        Bytes::from_static(b"rel-second"),
      ],
    ),
  )
  .await
  .expect("send_many_reliable timed out")
  .expect("send_many_reliable should succeed");

  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      locked.iter().any(|m| m.as_slice() == b"rel-first")
        && locked.iter().any(|m| m.as_slice() == b"rel-second")
    },
    Duration::from_secs(8),
  )
  .await;
  assert!(
    saw_both,
    "both reliable payloads did not arrive; received: {:?}",
    b_msgs.lock().unwrap()
  );

  let _ = b.shutdown().await;
  let _ = a.shutdown().await;
}

/// `send_reliable` after shutdown returns `Err(Shutdown)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_reliable_after_shutdown_returns_err() {
  let a = make("send-rel-shutdown").await;
  let a2 = a.clone();
  a.shutdown().await.expect("shutdown");

  let result = tokio::time::timeout(
    Duration::from_secs(5),
    a2.send_reliable(
      "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
      Bytes::from_static(b"post-shutdown"),
    ),
  )
  .await
  .expect("must not hang");
  assert!(
    matches!(result, Err(Error::Shutdown)),
    "expected Shutdown, got {result:?}"
  );
}

/// `send_reliable` to an UNREACHABLE address resolves to `Err` and does NOT
/// hang. The stream dial to a port with no listener fails before any wire is
/// established; the driver must resolve the parked reliable-send waiter with
/// `Err(SendFailed)` (a connect failure is terminalized as a dial failure, NOT
/// fed as a benign EOF that a one-way `UserMessage` would map to success).
///
/// Wrapped in a `tokio::time::timeout` so a regression that drops the failure
/// (false `Ok`, or a never-resolving park) fails fast rather than hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_reliable_to_unreachable_returns_err_not_hang() {
  let a = make("send-rel-unreach").await;
  // Port 1 on loopback has no listener — the stream dial cannot establish.
  let unreachable = "127.0.0.1:1".parse::<SocketAddr>().unwrap();

  let result = tokio::time::timeout(
    Duration::from_secs(20),
    a.send_reliable(unreachable, Bytes::from_static(b"to-the-void")),
  )
  .await
  .expect(
    "send_reliable to an unreachable address HUNG (timed out) — the parked \
     waiter never resolved; the pre-establishment dial failure was not surfaced",
  );
  assert!(
    result.is_err(),
    "send_reliable to an unreachable address MUST return Err, got {result:?}"
  );

  let _ = a.shutdown().await;
}

/// `local_state()` / `by_id(local)` read IMMEDIATELY after construction
/// reflects the configured `initial_meta`. The reactor builds its initial
/// snapshot from the live endpoint (`snapshot_of(endpoint.endpoint_ref())`),
/// which already carries the applied metadata; this guards that property.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_state_carries_initial_meta_immediately_after_new() {
  let meta_bytes = Bytes::from_static(b"role=db,zone=eu");
  let meta = Meta::try_from(meta_bytes.clone()).expect("meta within cap");
  let node = Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new("meta-node"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(MemberlistOptions::new().with_initial_meta(meta)),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist with initial_meta");

  let local = node.local_state();
  assert_eq!(
    local.meta_ref().as_ref(),
    meta_bytes.as_ref(),
    "local_state() right after new() must carry the configured initial_meta, got {:?}",
    local.meta_ref().as_ref()
  );
  let by_id = node
    .by_id(&SmolStr::new("meta-node"))
    .expect("local node must be present in the initial snapshot");
  assert_eq!(
    by_id.meta_ref().as_ref(),
    meta_bytes.as_ref(),
    "by_id(local) right after new() must carry the configured initial_meta"
  );

  let _ = node.shutdown().await;
}
