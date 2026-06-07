//! Extra QUIC data-plane / lifecycle integration tests that drive happy-path
//! command arms and the QUIC driver's event-dispatch paths not covered by the
//! primary QUIC suites: `set_local_state` success surfacing on a peer via
//! `merge_remote_state`, `set_ack_payload` success surfacing on the probing
//! peer via `notify_ping_complete`, a three-node convergence, leave observed
//! as a membership shrink, and the drop-counter accessors on a healthy node.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{
  borrow::Cow,
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use bytes::Bytes;
use memberlist_compio::{
  ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, Memberlist,
  NodeDelegate, Options, PingDelegate, QuicMemberlist, QuicOptions, QuicTransport,
  QuicTransportOptions, SocketAddrResolver, VoidDelegate,
};
use memberlist_proto::Node;
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a pair of `QuicOptions` sharing one self-signed cert / root, so two
/// nodes can complete the TLS handshake against each other.
fn pair_configs() -> (QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let b = support::build_quic_config(cert, key, roots);
  (a, b)
}

/// Build a trio of `QuicOptions` sharing one self-signed cert / root.
fn trio_configs() -> (QuicOptions, QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let b = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let c = support::build_quic_config(cert, key, roots);
  (a, b, c)
}

async fn make_quic(id: &str, qcfg: QuicOptions) -> QuicMemberlist<SmolStr, SocketAddr> {
  let opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(qcfg),
  );
  QuicMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind quic memberlist")
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

#[derive(Default)]
struct Sink {
  merges: Mutex<Vec<Bytes>>,
  pings: Mutex<Vec<(SmolStr, Bytes)>>,
}

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

type RecordingMemberlist = Memberlist<QuicTransport<SmolStr, SocketAddr>, RecordingDelegate>;

async fn make_recording_quic(id: &str, qcfg: QuicOptions) -> (RecordingMemberlist, Arc<Sink>) {
  let sink = Arc::new(Sink::default());
  let opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(qcfg),
  );
  let node = RecordingMemberlist::new(
    opts,
    RecordingDelegate { sink: sink.clone() },
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind recording quic memberlist");
  (node, sink)
}

// ── set_local_state happy path ──

/// `set_local_state` on a running QUIC node succeeds and the snapshot rides a
/// subsequent push/pull to surface on the peer's `merge_remote_state`. Drives
/// the QUIC driver's SetLocalState success arm + `RemoteStateReceived` dispatch.
#[compio::test]
async fn quic_set_local_state_succeeds_and_surfaces_on_peer_merge() {
  let (qcfg_peer, qcfg_setter) = pair_configs();

  let (peer, peer_sink) = make_recording_quic("qsls-peer", qcfg_peer).await;
  let setter = make_quic("qsls-setter", qcfg_setter).await;

  let snapshot = Bytes::from_static(b"quic-app-state-v1");
  setter
    .set_local_state(snapshot.clone())
    .await
    .expect("set_local_state on a running node must succeed");

  setter
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(peer.advertise_address())],
    )
    .await
    .expect("join the peer");

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
    "QUIC peer's merge_remote_state never saw the set_local_state snapshot; recorded: {:?}",
    peer_sink.merges.lock().unwrap()
  );

  setter.shutdown().await.expect("setter shutdown");
  peer.shutdown().await.expect("peer shutdown");
}

// ── set_ack_payload → notify_ping_complete ──

/// `set_ack_payload` success on a QUIC node surfaces on the probing peer's
/// `notify_ping_complete` when that peer issues an application `ping()`. Drives
/// the QUIC driver's set_ack_payload success arm + `PingCompleted` dispatch.
#[compio::test]
async fn quic_set_ack_payload_surfaces_on_prober_ping_complete() {
  let (qcfg_prober, qcfg_acker) = pair_configs();

  let (prober, prober_sink) = make_recording_quic("qack-prober", qcfg_prober).await;
  let acker = make_quic("qack-acker", qcfg_acker).await;

  let ack_payload = Bytes::from_static(b"quic-ack-rtt-data");
  acker
    .set_ack_payload(ack_payload.clone())
    .await
    .expect("set_ack_payload on a running node must succeed");

  prober
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(acker.advertise_address())],
    )
    .await
    .expect("join");
  let converged = wait_until(
    || prober.alive_count() == 2 && acker.alive_count() == 2,
    Duration::from_secs(40),
  )
  .await;
  assert!(converged, "cluster did not converge");

  // The prober issues an application ping; the acker's ack carries the payload,
  // which surfaces on the prober's notify_ping_complete.
  let target = Node::new(SmolStr::new("qack-acker"), acker.advertise_address());
  let rtt = prober.ping(target).await.expect("ping must succeed");
  assert!(rtt > Duration::ZERO, "ping RTT must be positive");

  let saw = wait_until(
    || {
      prober_sink
        .pings
        .lock()
        .unwrap()
        .iter()
        .any(|(id, p)| id == &SmolStr::new("qack-acker") && p.as_ref() == ack_payload.as_ref())
    },
    Duration::from_secs(40),
  )
  .await;
  assert!(
    saw,
    "QUIC prober's notify_ping_complete never carried the ack payload; recorded: {:?}",
    prober_sink.pings.lock().unwrap()
  );

  prober.shutdown().await.expect("prober shutdown");
  acker.shutdown().await.expect("acker shutdown");
}

// ── three-node convergence ──

/// A three-node QUIC cluster converges: B and C both join A and all three
/// observe a membership of 3. Exercises the QUIC driver's multi-peer connection
/// fan-out and datagram gossip propagation beyond the two-node base case.
#[compio::test]
async fn quic_three_node_cluster_converges() {
  let (qcfg_a, qcfg_b, qcfg_c) = trio_configs();

  let a = make_quic("qtri-a", qcfg_a).await;
  let b = make_quic("qtri-b", qcfg_b).await;
  let c = make_quic("qtri-c", qcfg_c).await;

  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("b joins a");
  c.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("c joins a");

  let converged = wait_until(
    || a.member_count() == 3 && b.member_count() == 3 && c.member_count() == 3,
    Duration::from_secs(40),
  )
  .await;
  assert!(
    converged,
    "three-node QUIC cluster did not converge: a={} b={} c={}",
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

/// After a converged QUIC peer calls `leave()`, the watcher's `alive_count`
/// drops back to 1 as it applies the leaver's leave. Exercises the QUIC
/// driver's inbound-leave application path on the watching side via the
/// snapshot; the fast-vs-probe timing discrimination is covered by the
/// dedicated event-stream leave tests.
#[compio::test]
async fn quic_peer_leave_shrinks_watcher_alive_count() {
  let (qcfg_watcher, qcfg_leaver) = pair_configs();

  let watcher = make_quic("qshrink-watcher", qcfg_watcher).await;
  let leaver = make_quic("qshrink-leaver", qcfg_leaver).await;

  leaver
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(watcher.advertise_address())],
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

  let shrank = wait_until(|| watcher.alive_count() == 1, Duration::from_secs(15)).await;
  assert!(
    shrank,
    "watcher did not observe the QUIC leaver leave: alive_count={}",
    watcher.alive_count()
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), leaver.shutdown()).await;
  watcher.shutdown().await.expect("watcher shutdown");
}

// ── drop-counter accessors on a healthy node ──

/// `events_dropped` / `observation_dropped` are zero on a freshly converged,
/// unstalled QUIC cluster with a live event subscriber. Exercises the accessor
/// paths on the happy side (the saturation side is covered elsewhere).
#[compio::test]
async fn quic_drop_counters_are_zero_on_healthy_node() {
  let (qcfg_a, qcfg_b) = pair_configs();

  let a = make_quic("qdrops-a", qcfg_a).await;
  let b = make_quic("qdrops-b", qcfg_b).await;

  // Keep a live event subscriber so the EventStream is never a closed sink.
  let _events = b.events();

  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.member_count() == 2 && b.member_count() == 2,
    Duration::from_secs(40),
  )
  .await;
  assert!(converged, "cluster did not converge");

  compio::time::sleep(Duration::from_millis(200)).await;

  assert_eq!(
    a.events_dropped(),
    0,
    "no events should be dropped on a healthy QUIC node"
  );
  assert_eq!(
    a.observation_dropped(),
    0,
    "no observation events should be dropped on a healthy QUIC node"
  );
  assert_eq!(b.observation_dropped(), 0);

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}
