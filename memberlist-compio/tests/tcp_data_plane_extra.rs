//! Extra TCP data-plane / lifecycle integration tests that drive
//! happy-path command arms and driver event-dispatch paths not covered by the
//! primary suites: `set_local_state` success surfacing on a peer via
//! `merge_remote_state`, `set_ack_payload` success surfacing on the probing
//! peer via `notify_ping_complete`, a three-node convergence, leave observed
//! as a membership shrink, and the drop-counter accessors on a healthy node.

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
  MemberlistOptions, NodeDelegate, Options, PingDelegate, SocketAddrResolver, TcpTransport,
  TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::Node;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

const LABEL: &[u8] = b"dp-extra";

async fn make_tcp(id: &str, addr: SocketAddr) -> Memberlist<SmolStr, SocketAddr> {
  let ml_opts = MemberlistOptions::new()
    .with_label(Some(LABEL.to_vec()))
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

// ── recording delegate ──

/// Shared records the recording delegate populates.
#[derive(Default)]
struct Sink {
  merges: Mutex<Vec<Bytes>>,
  pings: Mutex<Vec<(SmolStr, Bytes)>>,
}

/// Observation delegate that records `merge_remote_state` payloads and
/// `notify_ping_complete` (peer-id, ack-payload) pairs. Every other hook is a
/// no-op (mirroring `VoidDelegate`).
struct RecordingDelegate {
  sink: Arc<Sink>,
}

impl EventDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl NodeDelegate for RecordingDelegate {
  async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
    self
      .sink
      .merges
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(buf));
  }

  async fn notify_user_msg(&self, _msg: Cow<'_, [u8]>) {}
}

impl PingDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_ping_complete(
    &self,
    peer_id: &SmolStr,
    _peer_addr: &SocketAddr,
    _rtt: Duration,
    payload: Bytes,
  ) {
    self
      .sink
      .pings
      .lock()
      .unwrap()
      .push((peer_id.clone(), payload));
  }
}

impl ConflictDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

type RecordingMemberlist = Memberlist<SmolStr, SocketAddr>;

async fn make_recording_tcp(id: &str, addr: SocketAddr) -> (RecordingMemberlist, Arc<Sink>) {
  let sink = Arc::new(Sink::default());
  let ml_opts = MemberlistOptions::new()
    .with_label(Some(LABEL.to_vec()))
    .expect("valid label");
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_memberlist(ml_opts);
  let node = RecordingMemberlist::new(
    opts,
    RecordingDelegate { sink: sink.clone() },
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording tcp memberlist");
  (node, sink)
}

// ── set_local_state happy path ──

/// `set_local_state` on a running node must succeed (the SetLocalState command
/// arm replies `Ok`), and the snapshot must ride a subsequent push/pull and
/// surface on the peer's `merge_remote_state`. This drives the command's
/// success arm plus the driver's `RemoteStateReceived` event dispatch.
#[compio::test]
async fn set_local_state_succeeds_and_surfaces_on_peer_merge() {
  // The PEER records merges; the snapshot-setter is a plain node.
  let (peer, peer_sink) = make_recording_tcp("sls-peer", loopback_addr(0)).await;
  let peer_addr = *peer.local_node().addr_ref();

  let setter = make_tcp("sls-setter", loopback_addr(0)).await;

  // Install the snapshot BEFORE the join so the very first push/pull from the
  // setter to the peer already carries it (the SetLocalState success arm).
  let snapshot = Bytes::from_static(b"app-state-v1");
  setter
    .set_local_state(snapshot.clone())
    .await
    .expect("set_local_state on a running node must succeed");

  setter
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(peer_addr)])
    .await
    .expect("join the peer");

  // The peer's merge_remote_state must observe the setter's snapshot bytes.
  let saw = wait_until(
    || {
      peer_sink
        .merges
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_ref() == snapshot.as_ref())
    },
    Duration::from_secs(40),
  )
  .await;
  assert!(
    saw,
    "peer's merge_remote_state never saw the set_local_state snapshot; recorded: {:?}",
    peer_sink.merges.lock().unwrap()
  );

  setter.shutdown().await.expect("setter shutdown");
  peer.shutdown().await.expect("peer shutdown");
}

// ── set_ack_payload happy path → notify_ping_complete ──

/// `set_ack_payload` on a running node succeeds and the payload surfaces on the
/// PROBING peer's `notify_ping_complete` when that peer issues an
/// application-level `ping()` (the only probe kind that emits
/// `Event::PingCompleted`; the periodic SWIM Detection probes do not). This
/// drives the set_ack_payload success arm and the driver's `PingCompleted`
/// event dispatch (the hook that reads the ack payload).
#[compio::test]
async fn set_ack_payload_surfaces_on_prober_ping_complete() {
  // The PROBER records ping completions and issues the application ping.
  let (prober, prober_sink) = make_recording_tcp("ack-prober", loopback_addr(0)).await;

  // The acker attaches a payload to its probe acks.
  let acker = make_tcp("ack-acker", loopback_addr(0)).await;
  let acker_addr = *acker.local_node().addr_ref();

  let ack_payload = Bytes::from_static(b"ack-rtt-data");
  acker
    .set_ack_payload(ack_payload.clone())
    .await
    .expect("set_ack_payload on a running node must succeed");

  // Converge the two nodes.
  prober
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(acker_addr)])
    .await
    .expect("join");
  let converged = wait_until(
    || prober.alive_count() == 2 && acker.alive_count() == 2,
    Duration::from_secs(40),
  )
  .await;
  assert!(converged, "cluster did not converge");

  // The prober issues an application ping at the acker. The acker's ack carries
  // the payload set above, which surfaces on the prober's notify_ping_complete
  // (PingCompleted is emitted only for application pings, not SWIM probes). The
  // directed `ping()` future itself resolves the RTT; the delegate hook fires
  // on the driver task slightly after, so poll for the recorded payload.
  let target = Node::new(SmolStr::new("ack-acker"), acker_addr);
  let rtt = prober.ping(target).await.expect("ping must succeed");
  assert!(rtt > Duration::ZERO, "ping RTT must be positive");

  let saw = wait_until(
    || {
      prober_sink
        .pings
        .lock()
        .unwrap()
        .iter()
        .any(|(id, p)| id == &SmolStr::new("ack-acker") && p.as_ref() == ack_payload.as_ref())
    },
    Duration::from_secs(40),
  )
  .await;
  assert!(
    saw,
    "prober's notify_ping_complete never carried the acker's ack payload; recorded: {:?}",
    prober_sink.pings.lock().unwrap()
  );

  prober.shutdown().await.expect("prober shutdown");
  acker.shutdown().await.expect("acker shutdown");
}

// ── three-node convergence ──

/// A three-node cluster converges: B and C both join A, and all three observe
/// a membership of 3. Exercises the driver's multi-peer bridge fan-out and
/// gossip propagation beyond the two-node base case.
#[compio::test]
async fn three_node_cluster_converges() {
  let a = make_tcp("tri-a", loopback_addr(0)).await;
  let a_addr = *a.local_node().addr_ref();
  let b = make_tcp("tri-b", loopback_addr(0)).await;
  let c = make_tcp("tri-c", loopback_addr(0)).await;

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("b joins a");
  c.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("c joins a");

  let converged = wait_until(
    || a.member_count() == 3 && b.member_count() == 3 && c.member_count() == 3,
    Duration::from_secs(40),
  )
  .await;
  assert!(
    converged,
    "three-node cluster did not converge: a={} b={} c={}",
    a.member_count(),
    b.member_count(),
    c.member_count()
  );
  assert_eq!(a.alive_count(), 3);

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
  c.shutdown().await.expect("c shutdown");
}

// ── leave observed as membership shrink ──

/// After a converged peer calls `leave()`, the watcher's `alive_count` drops
/// back to 1 (only itself alive) — the watcher applies the leaving node's
/// `Dead`-self notice and reaps it. Exercises the driver's inbound-leave
/// application path on the watching side via the snapshot, complementing the
/// event-stream-based leave tests.
#[compio::test]
async fn peer_leave_shrinks_watcher_alive_count() {
  let watcher = make_tcp("shrink-watcher", loopback_addr(0)).await;
  let watcher_addr = *watcher.local_node().addr_ref();
  let leaver = make_tcp("shrink-leaver", loopback_addr(0)).await;

  leaver
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(watcher_addr)],
    )
    .await
    .expect("join");
  let converged = wait_until(
    || watcher.alive_count() == 2 && leaver.alive_count() == 2,
    Duration::from_secs(40),
  )
  .await;
  assert!(converged, "cluster did not converge");

  leaver.leave().await.expect("leave");

  // The watcher must observe the leaver leave the alive set — alive_count drops
  // to 1 (just the watcher). This pins that the inbound-leave application path
  // runs on the watching side; the fast-vs-probe timing discrimination is
  // covered by the dedicated event-stream leave tests, so a generous window is
  // used here for robustness under load.
  let shrank = wait_until(|| watcher.alive_count() == 1, Duration::from_secs(15)).await;
  assert!(
    shrank,
    "watcher did not observe the leaver leave: alive_count={}",
    watcher.alive_count()
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), leaver.shutdown()).await;
  watcher.shutdown().await.expect("watcher shutdown");
}

// ── drop-counter accessors on a healthy node ──

/// `events_dropped` and `observation_dropped` are both zero on a freshly
/// converged, unstalled cluster — the default bounded observation channel is
/// never saturated by normal membership traffic, and the event stream has a
/// live subscriber. Exercises the two accessor paths on the happy side
/// (the saturation side is covered by the bounded-channel drop tests).
#[compio::test]
async fn drop_counters_are_zero_on_healthy_node() {
  let a = make_tcp("drops-a", loopback_addr(0)).await;
  let a_addr = *a.local_node().addr_ref();
  let b = make_tcp("drops-b", loopback_addr(0)).await;

  // Keep a live event subscriber so the EventStream is never a closed sink.
  let _events = b.events();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  let converged = wait_until(
    || a.member_count() == 2 && b.member_count() == 2,
    Duration::from_secs(40),
  )
  .await;
  assert!(converged, "cluster did not converge");

  // A small settling window for any in-flight gossip.
  compio::time::sleep(Duration::from_millis(200)).await;

  assert_eq!(
    a.events_dropped(),
    0,
    "no events should be dropped on a healthy node"
  );
  assert_eq!(
    a.observation_dropped(),
    0,
    "no observation events should be dropped on a healthy node"
  );
  assert_eq!(b.observation_dropped(), 0);

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}

// ── ping with a Node handle whose payload round-trips RTT ──

/// A directed `ping` to a live peer that was joined returns a positive RTT,
/// and a second ping to the same peer also succeeds — exercising repeated
/// directed-ping command dispatch against a stable bridge/datagram path.
#[compio::test]
async fn repeated_directed_ping_succeeds() {
  let seed = make_tcp("rping-seed", loopback_addr(0)).await;
  let seed_addr = *seed.local_node().addr_ref();
  let node = make_tcp("rping-node", loopback_addr(0)).await;

  node
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join");
  let joined = wait_until(
    || node.by_id(&SmolStr::new("rping-seed")).is_some(),
    Duration::from_secs(40),
  )
  .await;
  assert!(joined, "seed did not appear in membership");

  let target = Node::new(SmolStr::new("rping-seed"), seed_addr);
  for i in 0..2 {
    let rtt = node
      .ping(target.clone())
      .await
      .unwrap_or_else(|e| panic!("ping {i} failed: {e:?}"));
    assert!(rtt > Duration::ZERO, "ping {i} RTT must be positive");
  }
  // Both nodes remain alive across the repeated pings.
  assert_eq!(node.alive_count(), 2, "both nodes still alive after pings");

  seed.shutdown().await.expect("seed shutdown");
  node.shutdown().await.expect("node shutdown");
}
