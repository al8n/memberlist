//! QUIC directed I/O integration tests — ping, unreliable send, and reliable
//! send.
//!
//! Each test brings up a two-node QUIC cluster, exercises the directed I/O
//! API, and asserts the expected outcome (RTT, delegate hook, round-trip).

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{
  borrow::Cow,
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use bytes::Bytes;
use memberlist_compio::{
  ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, MemberlistError,
  NodeDelegate, Options, PingDelegate, QuicMemberlist, QuicOptions, QuicTransportOptions,
  SocketAddrResolver, VoidDelegate,
};
use memberlist_proto::{Node, typed::NodeState};
use rustls::RootCertStore;
use smol_str::SmolStr;

type QuicNode = QuicMemberlist<SmolStr, SocketAddr>;
type RecordingQuicNode = QuicMemberlist<SmolStr, SocketAddr, RecordingDelegate>;
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

fn loopback(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn two_node_configs() -> (QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let b = support::build_quic_config(cert, key, roots);
  (a, b)
}

async fn make_quic(id: &str, addr: SocketAddr, qcfg: QuicOptions) -> QuicNode {
  let opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_quic_config(qcfg),
  );
  QuicMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct quic memberlist")
}

async fn make_recording_quic(
  id: &str,
  addr: SocketAddr,
  qcfg: QuicOptions,
) -> (RecordingQuicNode, Messages) {
  let msgs: Messages = Arc::new(Mutex::new(Vec::new()));
  let opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_quic_config(qcfg),
  );
  let node = RecordingQuicNode::new(
    opts,
    RecordingDelegate::new(msgs.clone()),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording quic");
  (node, msgs)
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

/// Ping between two live QUIC nodes returns a positive RTT.
#[compio::test]
async fn quic_ping_live_node_returns_positive_rtt() {
  let (qcfg_a, qcfg_b) = two_node_configs();
  let seed = make_quic("qdio-ping-seed", loopback(0), qcfg_a).await;
  let seed_addr = seed.advertise_address();
  let node_a = make_quic("qdio-ping-a", loopback(0), qcfg_b).await;

  let _contacted = node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join seed");

  let joined = wait_until(
    || node_a.by_id(&SmolStr::new("qdio-ping-seed")).is_some(),
    Duration::from_secs(10),
  )
  .await;
  assert!(joined, "seed did not appear in node_a's membership");

  let seed_node = Node::new(SmolStr::new("qdio-ping-seed"), seed_addr);
  let rtt = node_a.ping(seed_node).await.expect("ping should succeed");
  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_a.shutdown()).await;
}

/// Ping to an unreachable QUIC address returns Err.
#[compio::test]
async fn quic_ping_unreachable_node_returns_err() {
  let qcfg = support::self_trusted_quic_config();
  let node = make_quic("qdio-ping-unreach", loopback(0), qcfg).await;
  let unreachable = Node::new(SmolStr::new("ghost"), loopback(1));
  let result = node.ping(unreachable).await;
  assert!(
    result.is_err(),
    "ping to unreachable node should return Err, got {result:?}"
  );
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node.shutdown()).await;
}

/// Ping on a shut-down QUIC node returns `Err(Shutdown)` immediately.
#[compio::test]
async fn quic_ping_after_shutdown_returns_err() {
  let qcfg = support::self_trusted_quic_config();
  let node = make_quic("qdio-ping-sd", loopback(0), qcfg).await;
  let node2 = node.clone();
  node.shutdown().await.expect("shutdown");

  let ghost = Node::new(SmolStr::new("ghost"), loopback(1));
  let result = node2.ping(ghost).await;
  assert!(
    matches!(result, Err(MemberlistError::Shutdown)),
    "expected Shutdown, got {result:?}"
  );
}

// ── unreliable directed send ──

/// An unreliable directed `send` from node_a reaches node_b's
/// `notify_user_msg` delegate hook over QUIC.
#[compio::test]
async fn quic_send_unreliable_round_trips_to_delegate() {
  let (qcfg_a, qcfg_b) = two_node_configs();
  let (node_b, b_msgs) = make_recording_quic("qdio-send-b", loopback(0), qcfg_a).await;
  let b_addr = node_b.advertise_address();
  let node_a = make_quic("qdio-send-a", loopback(0), qcfg_b).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"quic-hello-unreliable");
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

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_b.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_a.shutdown()).await;
}

/// `send_many` with multiple payloads delivers all of them over QUIC.
#[compio::test]
async fn quic_send_many_unreliable_delivers_all_payloads() {
  let (qcfg_a, qcfg_b) = two_node_configs();
  let (node_b, b_msgs) = make_recording_quic("qdio-smany-b", loopback(0), qcfg_a).await;
  let b_addr = node_b.advertise_address();
  let node_a = make_quic("qdio-smany-a", loopback(0), qcfg_b).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  node_a
    .send_many(
      b_addr,
      [
        Bytes::from_static(b"quic-first"),
        Bytes::from_static(b"quic-second"),
      ],
    )
    .await
    .expect("send_many should succeed");

  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      locked.iter().any(|m| m.as_slice() == b"quic-first")
        && locked.iter().any(|m| m.as_slice() == b"quic-second")
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    saw_both,
    "both QUIC payloads did not arrive; received: {:?}",
    b_msgs.lock().unwrap()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_b.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_a.shutdown()).await;
}

// ── reliable directed send ──

/// A reliable directed `send_reliable` from node_a reaches node_b's
/// `notify_user_msg` delegate hook over QUIC.
#[compio::test]
async fn quic_send_reliable_round_trips_to_delegate() {
  let (qcfg_a, qcfg_b) = two_node_configs();
  let (node_b, b_msgs) = make_recording_quic("qdio-rel-b", loopback(0), qcfg_a).await;
  let b_addr = node_b.advertise_address();
  let node_a = make_quic("qdio-rel-a", loopback(0), qcfg_b).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"quic-hello-reliable");
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
    "notify_user_msg did not fire on node_b via QUIC; received: {:?}",
    b_msgs.lock().unwrap()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_b.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_a.shutdown()).await;
}

/// `send_many_reliable` delivers all payloads over QUIC.
#[compio::test]
async fn quic_send_many_reliable_delivers_all_payloads() {
  let (qcfg_a, qcfg_b) = two_node_configs();
  let (node_b, b_msgs) = make_recording_quic("qdio-rmany-b", loopback(0), qcfg_a).await;
  let b_addr = node_b.advertise_address();
  let node_a = make_quic("qdio-rmany-a", loopback(0), qcfg_b).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  node_a
    .send_many_reliable(
      b_addr,
      [
        Bytes::from_static(b"quic-rel-first"),
        Bytes::from_static(b"quic-rel-second"),
      ],
    )
    .await
    .expect("send_many_reliable should succeed");

  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      locked.iter().any(|m| m.as_slice() == b"quic-rel-first")
        && locked.iter().any(|m| m.as_slice() == b"quic-rel-second")
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    saw_both,
    "both QUIC reliable payloads did not arrive; received: {:?}",
    b_msgs.lock().unwrap()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_b.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node_a.shutdown()).await;
}

/// `send_reliable` after QUIC shutdown returns `Err` immediately.
#[compio::test]
async fn quic_send_reliable_after_shutdown_returns_err() {
  let qcfg = support::self_trusted_quic_config();
  let node = make_quic("qdio-rel-sd", loopback(0), qcfg).await;
  let node2 = node.clone();
  node.shutdown().await.expect("shutdown");

  let result = node2
    .send_reliable(loopback(1), Bytes::from_static(b"post-shutdown"))
    .await;
  assert!(
    result.is_err(),
    "send_reliable after shutdown should return Err, got {result:?}"
  );
}

/// `send_reliable` to an UNREACHABLE QUIC address resolves to `Err` and does
/// NOT hang. The in-band QUIC dial to a port with no listener never completes
/// its handshake; the `QuicEndpoint` retires the dial intent at its exchange
/// deadline and emits `Event::ExchangeCompleted(Failed, UserMessage)`, keyed
/// by the `ExchangeId` the driver parked the reliable-send waiter under. The
/// driver must resolve that waiter with `Err(SendFailed)`.
///
/// The await is wrapped in a `compio::time::timeout` so a regression that
/// drops the `Failed` emission (leaving the waiter parked) fails fast here
/// rather than hanging the suite.
#[compio::test]
async fn quic_send_reliable_to_unreachable_returns_err_not_hang() {
  let qcfg = support::self_trusted_quic_config();
  let node = make_quic("qdio-rel-unreach", loopback(0), qcfg).await;
  // Port 1 on loopback has no QUIC listener — the dial handshake cannot
  // complete, so the exchange is retired at its deadline.
  let unreachable = loopback(1);

  let result = compio::time::timeout(
    Duration::from_secs(30),
    node.send_reliable(unreachable, Bytes::from_static(b"to-the-void")),
  )
  .await;

  match result {
    Ok(send_res) => assert!(
      send_res.is_err(),
      "QUIC send_reliable to an unreachable address MUST return Err, got {send_res:?}"
    ),
    Err(_elapsed) => panic!(
      "QUIC send_reliable to an unreachable address HUNG (timed out) — the \
       parked waiter never resolved; the UserMessage dial failure was not \
       surfaced as Event::ExchangeCompleted(Failed)"
    ),
  }

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node.shutdown()).await;
}
