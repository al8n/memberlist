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

// ── runtime control APIs (metadata / broadcast / local-state / ack payload) ─────

/// Shared captures for a [`ControlDelegate`].
#[derive(Clone, Default)]
struct ControlCaptures {
  user_msgs: Arc<Mutex<Vec<Vec<u8>>>>,
  ping_payloads: Arc<Mutex<Vec<Vec<u8>>>>,
  remote_states: Arc<Mutex<Vec<Vec<u8>>>>,
}

/// Records inbound user broadcasts, ping-ack payloads, and merged remote state.
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

  fn notify_ping_complete(
    &self,
    _peer_id: SmolStr,
    _peer_addr: SocketAddr,
    _rtt: Duration,
    payload: Bytes,
  ) -> impl Future<Output = ()> + Send + '_ {
    let c = self.c.ping_payloads.clone();
    async move {
      c.lock().unwrap().push(payload.to_vec());
    }
  }

  fn merge_remote_state(&self, state: Bytes, _join: bool) -> impl Future<Output = ()> + Send + '_ {
    let c = self.c.remote_states.clone();
    async move {
      c.lock().unwrap().push(state.to_vec());
    }
  }
}

async fn make_control(id: &str) -> (Memberlist<SmolStr>, ControlCaptures) {
  let c = ControlCaptures::default();
  let node = Memberlist::<SmolStr>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    ControlDelegate { c: c.clone() },
  )
  .await
  .expect("bind tcp memberlist");
  (node, c)
}

/// `update_node_metadata`, `queue_user_broadcast`, and `set_local_state` each
/// reach a joined peer: the peer sees the new metadata on the member, the
/// broadcast on its `notify_user_msg`, and the local state on its
/// `merge_remote_state`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_apis_propagate_to_peer() {
  let (b, caps) = make_control("ctrl-b").await;
  let a = make("ctrl-a").await;
  let a_addr = *a.local().addr_ref();

  // Set local state and metadata before the join so the join push/pull carries
  // the state and the metadata is gossiped as the cluster forms.
  a.set_local_state(Bytes::from_static(b"app-state"))
    .await
    .expect("set_local_state");
  a.update_node_metadata(b"web".to_vec())
    .await
    .expect("update_node_metadata");

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let meta_seen = wait_until(
    || {
      b.by_id(&SmolStr::new("ctrl-a"))
        .map(|n| n.meta_ref().as_ref() == b"web")
        .unwrap_or(false)
    },
    Duration::from_secs(10),
  )
  .await;
  assert!(meta_seen, "peer never observed the updated metadata");

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
  assert!(state_seen, "peer never merged the local-state snapshot");

  a.queue_user_broadcast(Bytes::from_static(b"bcast"))
    .await
    .expect("queue_user_broadcast");
  let bcast_seen = wait_until(
    || caps.user_msgs.lock().unwrap().iter().any(|m| m == b"bcast"),
    Duration::from_secs(10),
  )
  .await;
  assert!(bcast_seen, "peer never received the user broadcast");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A node's `set_ack_payload` rides its probe acks: the periodic failure
/// detector on the peer completes a probe and observes the payload in
/// `notify_ping_complete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_ack_payload_observed_by_prober() {
  let (b, caps) = make_control("ack-b").await;
  let a = make("ack-a").await;
  let a_addr = *a.local().addr_ref();

  a.set_ack_payload(Bytes::from_static(b"ackpay"))
    .await
    .expect("set_ack_payload");

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  let joined = wait_until(
    || b.by_id(&SmolStr::new("ack-a")).is_some(),
    Duration::from_secs(10),
  )
  .await;
  assert!(joined, "seed never appeared in membership");

  // Each ping of a draws an ack carrying a's configured payload, which surfaces
  // on b's `notify_ping_complete`.
  let target = Node::new(SmolStr::new("ack-a"), a_addr);
  let mut payload_seen = false;
  for _ in 0..20 {
    let _ = b.ping(target.clone()).await;
    if caps
      .ping_payloads
      .lock()
      .unwrap()
      .iter()
      .any(|p| p == b"ackpay")
    {
      payload_seen = true;
      break;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
  assert!(
    payload_seen,
    "prober never observed the configured ack payload"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
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

/// An oversized unreliable `send` payload is rejected rather than silently
/// dropped on the wire: a single `UserData` frame larger than the gossip MTU is
/// deterministically untransmittable, so the driver surfaces the machine's
/// `UserPacketExceedsMtu` as `Error::Proto` and never queues it. 1 MiB is far
/// over the default ~1400-byte gossip budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_send_is_rejected() {
  let a = make("oversized-a").await;
  let huge = Bytes::from(vec![0xABu8; 1024 * 1024]);

  let res = tokio::time::timeout(
    Duration::from_secs(5),
    a.send("127.0.0.1:9".parse::<SocketAddr>().unwrap(), huge),
  )
  .await
  .expect("send must resolve, not hang");
  assert!(
    matches!(
      res,
      Err(Error::Proto(memberlist_proto::Error::UserPacketExceedsMtu(
        ..
      )))
    ),
    "an oversized unreliable send must be rejected as UserPacketExceedsMtu, got {res:?}"
  );

  // A small payload on the SAME node is still accepted — the rejection is a
  // size cap, not a blanket failure.
  let ok = tokio::time::timeout(
    Duration::from_secs(5),
    a.send(
      "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
      Bytes::from_static(b"small"),
    ),
  )
  .await
  .expect("small send must resolve");
  assert!(
    ok.is_ok(),
    "a small directed send must still be accepted: {ok:?}"
  );

  let _ = a.shutdown().await;
}

/// `send_many` with no payloads is a no-op success (`Ok(())`): nothing to
/// frame, so the driver replies immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_many_empty_is_ok_noop() {
  let a = make("send-empty-a").await;
  let res = tokio::time::timeout(
    Duration::from_secs(5),
    a.send_many(
      "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
      std::iter::empty(),
    ),
  )
  .await
  .expect("empty send_many must resolve");
  assert!(res.is_ok(), "empty send_many is a no-op Ok, got {res:?}");
  let _ = a.shutdown().await;
}

/// `send_many_reliable` with no payloads is a no-op success (`Ok(())`): the
/// driver short-circuits with nothing to track.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_many_reliable_empty_is_ok_noop() {
  let a = make("send-rel-empty-a").await;
  let res = tokio::time::timeout(
    Duration::from_secs(5),
    a.send_many_reliable(
      "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
      std::iter::empty(),
    ),
  )
  .await
  .expect("empty send_many_reliable must resolve");
  assert!(
    res.is_ok(),
    "empty send_many_reliable is a no-op Ok, got {res:?}"
  );
  let _ = a.shutdown().await;
}

/// `set_compression_options` on a LIVE (running) node succeeds: the change
/// takes effect on the next outbound datagram, and the reply is `Ok(())`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_compression_options_on_live_node_succeeds() {
  use memberlist_reactor::CompressionOptions;

  let a = make("set-compr-a").await;
  let res = tokio::time::timeout(
    Duration::from_secs(5),
    a.set_compression_options(CompressionOptions::new()),
  )
  .await
  .expect("set_compression_options must resolve");
  assert!(
    res.is_ok(),
    "set_compression_options on a running node must succeed, got {res:?}"
  );
  let _ = a.shutdown().await;
}

/// A custom `AddressResolver` is invoked for an `Unresolved` seed: the join
/// resolves the host through the resolver, dispatches the resulting wire
/// address, and converges. Exercises the `resolve`/`join_inner` resolver branch
/// the all-`Resolved` smoke tests never reach.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_resolves_unresolved_seed_via_resolver() {
  use std::future::Future;

  use memberlist_reactor::AddressResolver;

  // Resolver mapping the sentinel label "seed" to a captured wire address.
  struct FixedResolver(SocketAddr);
  impl AddressResolver for FixedResolver {
    type Address = &'static str;
    type Error = std::convert::Infallible;
    fn resolve(
      &self,
      _addr: &&'static str,
    ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
      // The `let` before the async block keeps the explicit `+ Send` return
      // form the `AddressResolver` contract requires (not bare `async fn`).
      let a = self.0;
      async move { Ok(vec![a]) }
    }
  }

  let a = make("unres-a").await;
  let b = make("unres-b").await;
  let a_addr = *a.local().addr_ref();
  let resolver = FixedResolver(a_addr);

  let n = tokio::time::timeout(
    Duration::from_secs(8),
    b.join(&resolver, &[MaybeResolved::Unresolved("seed")]),
  )
  .await
  .expect("unresolved-seed join must resolve")
  .expect("join via resolver");
  assert_eq!(n, 1, "the resolved seed was contacted");

  let converged = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "resolver-seed cluster did not converge");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A non-empty seed list that resolves to ZERO wire addresses is a bootstrap
/// failure (`JoinFailed`), not a successful zero-contact join.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_resolver_yields_nothing_is_join_failed() {
  use std::future::Future;

  use memberlist_reactor::AddressResolver;

  struct EmptyResolver;
  impl AddressResolver for EmptyResolver {
    type Address = &'static str;
    type Error = std::convert::Infallible;
    fn resolve(
      &self,
      _addr: &&'static str,
    ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
      // Explicit `+ Send` form per the resolver contract.
      let out: Vec<SocketAddr> = Vec::new();
      async move { Ok(out) }
    }
  }

  let b = make("empty-res-b").await;
  let res = tokio::time::timeout(
    Duration::from_secs(8),
    b.join(&EmptyResolver, &[MaybeResolved::Unresolved("nothing")]),
  )
  .await
  .expect("join must resolve");
  assert!(
    matches!(res, Err(Error::JoinFailed(1))),
    "a non-empty seed resolving to nothing is JoinFailed, got {res:?}"
  );
  let _ = b.shutdown().await;
}

/// A resolver error propagates as `Error::Resolve` from `join`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_resolver_error_propagates() {
  use std::future::Future;

  use memberlist_reactor::AddressResolver;

  struct FailingResolver;
  impl AddressResolver for FailingResolver {
    // `std::io::Error` satisfies the resolver's `core::error::Error` bound, so
    // the test needs no extra error-derive dependency.
    type Address = &'static str;
    type Error = std::io::Error;
    fn resolve(
      &self,
      _addr: &&'static str,
    ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
      // Explicit `+ Send` form per the resolver contract.
      let err = std::io::Error::other("boom");
      async move { Err(err) }
    }
  }

  let b = make("res-err-b").await;
  let res = tokio::time::timeout(
    Duration::from_secs(8),
    b.join(&FailingResolver, &[MaybeResolved::Unresolved("bad")]),
  )
  .await
  .expect("join must resolve");
  assert!(
    matches!(res, Err(Error::Resolve(_))),
    "a resolver error must surface as Error::Resolve, got {res:?}"
  );
  let _ = b.shutdown().await;
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
