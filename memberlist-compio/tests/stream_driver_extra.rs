//! Supplementary stream-driver integration coverage.
//!
//! Targets driver paths that need a live two-node TCP cluster rather than a
//! direct dispatch unit test: the `Channel::Unbounded` observation-channel
//! branch (the driver builds an unbounded obs queue and opts out of the
//! payload-byte backstop) and the reliable-send / ping completion accounting
//! that resolves a parked waiter from an `ExchangeCompleted` / `PingCompleted`
//! event delivered over a real wire.

// Excluded on Windows: the two-node TCP cluster joins here push the full
// integration suite past Windows' tight ephemeral-port range (WSAEACCES bind
// exhaustion, even with the driver's bind-retry). The driver behavior is
// platform-independent and covered on Unix, where the coverage job runs.
#![cfg(all(feature = "tcp", not(target_os = "windows")))]

use std::{
  borrow::Cow,
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::{Arc, Mutex},
  time::Duration,
};

use bytes::Bytes;
use memberlist_compio::{
  Channel, ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, Memberlist,
  NodeDelegate, Options, PingDelegate, RuntimeOptions, SocketAddrResolver, TcpTransport,
  TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::{Node, typed::NodeState};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = std::time::Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// Shared record of bytes delivered via `notify_user_msg`.
type Messages = Arc<Mutex<Vec<Vec<u8>>>>;

/// A delegate that records every received user message — used to prove the
/// observation task (fed by the driver's `Channel::Unbounded` obs queue) still
/// delivers application payloads.
struct RecordingDelegate {
  msgs: Messages,
}

impl EventDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_join(&self, _node: Arc<NodeState<SmolStr, SocketAddr>>) {}
}

impl NodeDelegate for RecordingDelegate {
  async fn notify_user_msg(&self, msg: Cow<'_, [u8]>) {
    // Ignoring poisoning is not silent failure: a poisoned lock means a prior
    // test-thread panic already failed the test, so unwrap surfaces that.
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

/// Build a node with a `VoidDelegate` and an explicit observation-channel
/// policy, so a test can pin the `Channel::Unbounded` driver branch.
async fn make_tcp_with_obs(
  id: &str,
  addr: SocketAddr,
  channel: Channel,
) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_runtime(RuntimeOptions::new().with_observation_channel(channel));
  Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist")
}

/// Build a recording-delegate node with an explicit observation-channel policy.
async fn make_recording_with_obs(
  id: &str,
  addr: SocketAddr,
  channel: Channel,
) -> (Memberlist<SmolStr, SocketAddr>, Messages) {
  let msgs: Messages = Arc::new(Mutex::new(Vec::new()));
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_runtime(RuntimeOptions::new().with_observation_channel(channel));
  let node = Memberlist::new(
    opts,
    RecordingDelegate { msgs: msgs.clone() },
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording tcp memberlist");
  (node, msgs)
}

/// A driver configured with `Channel::Unbounded` builds the unbounded
/// observation queue (and opts out of the payload-byte backstop). The
/// observation task must still deliver application data: a reliable directed
/// send from A reaches B's `notify_user_msg`, and the parked `send_reliable`
/// waiter resolves `Ok(())` from the terminal `ExchangeCompleted(UserMessage)`.
///
/// This drives the `Channel::Unbounded` obs-channel construction branch in the
/// stream driver loop — the default config (`Channel::Bounded`) never reaches
/// it — alongside the reliable-send completion accounting over a real wire.
#[compio::test]
async fn unbounded_obs_channel_delivers_reliable_send() {
  let (node_b, b_msgs) =
    make_recording_with_obs("unbounded-b", loopback_addr(0), Channel::Unbounded).await;
  let b_addr = node_b.advertise_address();
  let node_a = make_tcp_with_obs("unbounded-a", loopback_addr(0), Channel::Unbounded).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");
  let converged = wait_until(
    || node_a.member_count() == 2 && node_b.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    node_a.member_count(),
    node_b.member_count()
  );

  let payload = Bytes::from_static(b"unbounded-reliable");
  // The reliable send parks a waiter resolved by the terminal ExchangeCompleted.
  node_a
    .send_reliable(b_addr, payload.clone())
    .await
    .expect("send_reliable resolves Ok from the exchange completion");

  // The payload reached B's delegate THROUGH the unbounded observation queue.
  let saw = wait_until(
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
    saw,
    "reliable payload not delivered to B's delegate over the unbounded obs queue; got {:?}",
    b_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

/// With `Channel::Unbounded`, an application ping between converged nodes
/// resolves a positive RTT from the `Event::PingCompleted` the driver routes to
/// the parked `PendingPing` waiter — the completion-resolution arm of
/// `drain_events` exercised end-to-end on the unbounded obs path.
#[compio::test]
async fn unbounded_obs_channel_ping_resolves_rtt() {
  let seed = make_tcp_with_obs("uping-seed", loopback_addr(0), Channel::Unbounded).await;
  let seed_addr = seed.advertise_address();
  let node_a = make_tcp_with_obs("uping-a", loopback_addr(0), Channel::Unbounded).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join seed");
  let joined = wait_until(
    || node_a.by_id(&SmolStr::new("uping-seed")).is_some(),
    Duration::from_secs(10),
  )
  .await;
  assert!(joined, "seed did not appear in node_a's membership");

  let rtt = node_a
    .ping(Node::new(SmolStr::new("uping-seed"), seed_addr))
    .await
    .expect("ping resolves from PingCompleted");
  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  seed.shutdown().await.expect("seed shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}
