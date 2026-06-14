//! QUIC directed I/O integration tests — ping, unreliable send, and reliable send.
//!
//! Mirrors the TCP directed I/O tests but drives the QUIC backend.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{
  future::Future,
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::Duration,
};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use memberlist_reactor::{
  Delegate, Error, MaybeResolved, Memberlist, Node, Options, QuicOptions, SocketAddrResolver,
  VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

// ── helpers ───────────────────────────────────────────────────────────────────

fn shared_quic_pair() -> (QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let b = support::build_quic_config(cert, key, roots);
  (a, b)
}

async fn make(id: &str, qcfg: QuicOptions) -> Memberlist<SmolStr, SocketAddr> {
  Memberlist::<SmolStr, _>::quic::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    qcfg,
  )
  .await
  .expect("bind quic memberlist")
}

/// Shared record of bytes delivered via `notify_user_msg`.
type Messages = Arc<Mutex<Vec<Vec<u8>>>>;

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

async fn make_recording(
  id: &str,
  qcfg: QuicOptions,
) -> (Memberlist<SmolStr, SocketAddr>, Messages) {
  let (delegate, msgs) = RecordingDelegate::new();
  let node = Memberlist::<SmolStr, _>::quic::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    delegate,
    qcfg,
  )
  .await
  .expect("bind quic memberlist");
  (node, msgs)
}

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

// ── runtime control APIs over QUIC ──────────────────────────────────────────────

/// Records inbound user broadcasts and merged remote state for the control test.
#[derive(Clone, Default)]
struct ControlCaptures {
  user_msgs: Arc<Mutex<Vec<Vec<u8>>>>,
  remote_states: Arc<Mutex<Vec<Vec<u8>>>>,
}

struct ControlDelegate {
  c: ControlCaptures,
}

impl Delegate for ControlDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  fn notify_user_msg(&self, msg: Bytes) -> impl Future<Output = ()> + Send + '_ {
    let c = self.c.user_msgs.clone();
    async move {
      c.lock().unwrap().push(msg.to_vec());
    }
  }

  fn merge_remote_state(&self, state: Bytes, _join: bool) -> impl Future<Output = ()> + Send + '_ {
    let c = self.c.remote_states.clone();
    async move {
      c.lock().unwrap().push(state.to_vec());
    }
  }
}

async fn make_control(
  id: &str,
  qcfg: QuicOptions,
) -> (Memberlist<SmolStr, SocketAddr>, ControlCaptures) {
  let c = ControlCaptures::default();
  let node = Memberlist::<SmolStr, _>::quic::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    ControlDelegate { c: c.clone() },
    qcfg,
  )
  .await
  .expect("bind quic memberlist");
  (node, c)
}

/// The four runtime control commands dispatch through the QUIC driver: metadata,
/// broadcast, and local-state propagate to a joined peer, and the ack-payload
/// command is accepted while running. (Full ack-payload propagation is covered
/// by the transport-agnostic TCP test.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_control_apis_dispatch_to_peer() {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let (b, caps) = make_control("quic-ctrl-b", qcfg_b).await;
  let a = make("quic-ctrl-a", qcfg_a).await;
  let a_addr = *a.local().addr_ref();

  a.set_local_state(Bytes::from_static(b"app-state"))
    .await
    .expect("set_local_state");
  a.update_node_metadata(b"web".to_vec())
    .await
    .expect("update_node_metadata");
  a.set_ack_payload(Bytes::from_static(b"ackpay"))
    .await
    .expect("set_ack_payload accepted while running");

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let meta_seen = wait_until(
    || {
      b.by_id(&SmolStr::new("quic-ctrl-a"))
        .map(|n| n.meta_ref().as_ref() == b"web")
        .unwrap_or(false)
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    meta_seen,
    "peer never observed the updated metadata over QUIC"
  );

  let state_seen = wait_until(
    || {
      caps
        .remote_states
        .lock()
        .unwrap()
        .iter()
        .any(|s| s == b"app-state")
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(
    state_seen,
    "peer never merged the local-state snapshot over QUIC"
  );

  a.queue_user_broadcast(Bytes::from_static(b"bcast"))
    .await
    .expect("queue_user_broadcast");
  let bcast_seen = wait_until(
    || caps.user_msgs.lock().unwrap().iter().any(|m| m == b"bcast"),
    Duration::from_secs(10),
  )
  .await;
  assert!(
    bcast_seen,
    "peer never received the user broadcast over QUIC"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

// ── ping ──────────────────────────────────────────────────────────────────────

/// Pinging a live QUIC peer returns a positive RTT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_ping_live_node_returns_positive_rtt() {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let a = make("quic-ping-a", qcfg_a).await;
  let b = make("quic-ping-b", qcfg_b).await;
  let a_addr = *a.local().addr_ref();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let joined = wait_until(
    || b.by_id(&SmolStr::new("quic-ping-a")).is_some(),
    Duration::from_secs(8),
  )
  .await;
  assert!(joined, "peer did not appear in membership");

  let target = Node::new(SmolStr::new("quic-ping-a"), a_addr);
  let rtt = tokio::time::timeout(Duration::from_secs(8), b.ping(target))
    .await
    .expect("ping timed out")
    .expect("ping should succeed");

  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// Pinging after QUIC node shutdown returns `Err(Shutdown)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_ping_after_shutdown_returns_err() {
  let node = make("quic-ping-shutdown", support::self_trusted_quic_config()).await;
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

/// An unreliable `send` over QUIC reaches the peer's `notify_user_msg` hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_send_unreliable_delivers_to_delegate() {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let (b, b_msgs) = make_recording("quic-send-unrel-b", qcfg_b).await;
  let b_addr = *b.local().addr_ref();
  let a = make("quic-send-unrel-a", qcfg_a).await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"quic-hello-unreliable");
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

/// `send_many` with multiple unreliable payloads over QUIC delivers all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_send_many_unreliable_delivers_all_payloads() {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let (b, b_msgs) = make_recording("quic-send-many-unrel-b", qcfg_b).await;
  let b_addr = *b.local().addr_ref();
  let a = make("quic-send-many-unrel-a", qcfg_a).await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  tokio::time::timeout(
    Duration::from_secs(5),
    a.send_many(
      b_addr,
      [
        Bytes::from_static(b"quic-first"),
        Bytes::from_static(b"quic-second"),
      ],
    ),
  )
  .await
  .expect("send_many timed out")
  .expect("send_many should succeed");

  let saw_both = wait_until(
    || {
      let locked = b_msgs.lock().unwrap();
      locked.iter().any(|m| m.as_slice() == b"quic-first")
        && locked.iter().any(|m| m.as_slice() == b"quic-second")
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

/// A reliable `send_reliable` over QUIC reaches the peer's `notify_user_msg`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_send_reliable_delivers_to_delegate() {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let (b, b_msgs) = make_recording("quic-send-rel-b", qcfg_b).await;
  let b_addr = *b.local().addr_ref();
  let a = make("quic-send-rel-a", qcfg_a).await;

  a.join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"quic-hello-reliable");
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

/// `send_reliable` after QUIC shutdown returns `Err(Shutdown)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_send_reliable_after_shutdown_returns_err() {
  let a = make(
    "quic-send-rel-shutdown",
    support::self_trusted_quic_config(),
  )
  .await;
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

/// `send_reliable` to an UNREACHABLE QUIC address resolves to `Err` and does
/// NOT hang. The in-band QUIC dial to a port with no listener never completes
/// its handshake; the `QuicEndpoint` retires the dial intent at its exchange
/// deadline and emits `Event::ExchangeCompleted(Failed, UserMessage)`, which
/// resolves the driver's parked reliable-send waiter with `Err(SendFailed)`.
///
/// Wrapped in a `tokio::time::timeout` so a regression that drops the `Failed`
/// emission (leaving the waiter parked) fails fast rather than hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_send_reliable_to_unreachable_returns_err_not_hang() {
  let node = make("quic-send-rel-unreach", support::self_trusted_quic_config()).await;
  // Port 1 on loopback has no QUIC listener — the dial handshake cannot
  // complete, so the exchange is retired at its deadline.
  let unreachable = "127.0.0.1:1".parse::<SocketAddr>().unwrap();

  let result = tokio::time::timeout(
    Duration::from_secs(30),
    node.send_reliable(unreachable, Bytes::from_static(b"to-the-void")),
  )
  .await
  .expect(
    "QUIC send_reliable to an unreachable address HUNG (timed out) — the \
     parked waiter never resolved; the UserMessage dial failure was not \
     surfaced as Event::ExchangeCompleted(Failed)",
  );
  assert!(
    result.is_err(),
    "QUIC send_reliable to an unreachable address MUST return Err, got {result:?}"
  );

  let _ = node.shutdown().await;
}
