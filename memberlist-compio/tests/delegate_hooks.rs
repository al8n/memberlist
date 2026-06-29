//! Delegate hook tests — observation hooks fire on the driver thread, and a
//! machine admission predicate supplied via `Options` filters membership.
//!
//! `notify_join` / `notify_leave` / `notify_update` (from `EventDelegate`)
//! and `notify_ping_complete` (from `PingDelegate`) fire from the driver's
//! event-dispatch path before the event reaches any `EventStream`
//! subscriber. The first test pins the observable `notify_join` hook in a
//! two-node join.
//!
//! Admission is separate: the machine's `AliveDelegate` / `MergeDelegate`
//! (`Send + Sync`) are supplied via [`Options::with_alive_delegate`] /
//! [`Options::with_merge_delegate`], installed into the `Endpoint`, and run
//! inline inside the FSM. The second test installs an `AliveDelegate` that
//! rejects a peer id and asserts that peer never enters membership.

#![cfg(feature = "tcp")]

use std::{
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use memberlist_compio::{
  AliveDelegate, ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved,
  Memberlist, NodeDelegate, Options, PingDelegate, SocketAddrResolver, TcpTransport,
  TcpTransportOptions,
};
use memberlist_proto::typed::NodeState;
use smol_str::SmolStr;

/// Shared record of the peer ids a [`RecordingDelegate`] saw join.
type Joins = Arc<Mutex<Vec<SmolStr>>>;

/// Custom observation delegate that records every `notify_join` peer id into
/// a shared vector. The other three observation sub-traits are no-ops
/// (mirroring `VoidDelegate`), so only the membership-join hook is
/// observable.
struct RecordingDelegate {
  joins: Joins,
}

impl RecordingDelegate {
  fn new(joins: Joins) -> Self {
    Self { joins }
  }
}

impl EventDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_join(&self, node: Arc<NodeState<SmolStr, SocketAddr>>) {
    // Ignoring poisoning is not silent failure: a poisoned lock means a
    // prior test-thread panic already failed the test, so unwrap here
    // surfaces that as the panic it is.
    self.joins.lock().unwrap().push(node.id_ref().clone());
  }
}

// NodeDelegate's hooks all default to no-ops; the recording delegate keeps
// them.
impl NodeDelegate for RecordingDelegate {}

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

/// A `Memberlist` carrying a [`RecordingDelegate`] instead of the default
/// `VoidDelegate`.
type RecordingMemberlist = memberlist_compio::Memberlist<SmolStr, SocketAddr>;

/// Build a plain `VoidDelegate`-backed seed bound to `127.0.0.1:0`.
async fn make_seed(id: &str) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  );
  Memberlist::new(
    opts,
    memberlist_compio::VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct seed")
}

/// Build a joiner whose [`RecordingDelegate`] records peer joins into
/// `joins`.
async fn make_recording_joiner(id: &str, joins: Joins) -> RecordingMemberlist {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  );
  RecordingMemberlist::new(
    opts,
    RecordingDelegate::new(joins),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording joiner")
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

/// A custom delegate's `notify_join` fires for the seed when the joiner's
/// synchronous `join` brings the seed into membership. The hook runs on the
/// driver thread; the recorded id matches the seed.
#[compio::test]
async fn custom_delegate_notify_join_fires_on_two_node_join() {
  let seed = make_seed("hook-seed").await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let joins: Joins = Arc::new(Mutex::new(Vec::new()));
  let joiner = make_recording_joiner("hook-joiner", joins.clone()).await;

  let contacted = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join the seed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");

  // `notify_join` fires on the driver thread as the NodeJoined event is
  // dispatched; allow a bounded window for the driver to drain the event
  // past the join reply.
  let saw_seed = wait_until(
    || {
      joins
        .lock()
        .unwrap()
        .iter()
        .any(|id| id.as_str() == "hook-seed")
    },
    Duration::from_secs(5),
  )
  .await;
  assert!(
    saw_seed,
    "notify_join did not fire for the seed; recorded joins: {:?}",
    joins.lock().unwrap()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// Machine [`AliveDelegate`] that ignores every alive whose id equals the
/// configured `reject_id`. `Send + Sync` (the machine's admission trait),
/// installed into the `Endpoint` via `Options::with_alive_delegate`.
struct RejectAlive {
  reject_id: SmolStr,
}

impl AliveDelegate<SmolStr, SocketAddr> for RejectAlive {
  fn notify_alive(&self, peer: &NodeState<SmolStr, SocketAddr>) -> bool {
    peer.id_ref() != &self.reject_id
  }
}

/// Build a joiner whose `Options` installs a [`RejectAlive`] admission
/// predicate keyed on `reject_id`; observation stays the default
/// `VoidDelegate`.
async fn make_rejecting_joiner(id: &str, reject_id: &str) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_alive_delegate(RejectAlive {
    reject_id: SmolStr::new(reject_id),
  });
  Memberlist::new(
    opts,
    memberlist_compio::VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct rejecting joiner")
}

/// An `AliveDelegate` supplied via `Options` filters membership: the joiner
/// rejects the seed's alive, so even though `join` contacts the seed, the
/// seed never enters the joiner's membership snapshot. The rejection is
/// total (every alive for the seed id is ignored), so the seed's absence is
/// stable, not a race.
#[compio::test]
async fn alive_delegate_via_options_rejects_peer() {
  let seed = make_seed("reject-seed").await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let joiner = make_rejecting_joiner("reject-joiner", "reject-seed").await;

  // `join` reports network reachability (seeds contacted), independent of
  // admission, so it still succeeds against the reachable seed.
  let contacted = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join contacts the seed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");

  // Give the driver a settle window to drain any push/pull + gossip, then
  // assert the rejected seed never entered the joiner's membership.
  let seed_admitted = wait_until(
    || {
      joiner
        .snapshot()
        .members_slice()
        .iter()
        .any(|n| n.id_ref().as_str() == "reject-seed")
    },
    Duration::from_secs(2),
  )
  .await;
  assert!(
    !seed_admitted,
    "alive was rejected, yet the seed entered the joiner's membership: {:?}",
    joiner
      .snapshot()
      .members_slice()
      .iter()
      .map(|n| n.id_ref().clone())
      .collect::<Vec<_>>()
  );

  // The joiner still has itself as a member (sanity: the snapshot is live).
  assert!(
    joiner
      .snapshot()
      .members_slice()
      .iter()
      .any(|n| n.id_ref().as_str() == "reject-joiner"),
    "joiner must always contain itself"
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}
