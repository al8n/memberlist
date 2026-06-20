//! TCP backend smoke tests — single-node liveness, a two-node join over the
//! reliable push/pull exchange, delegate/event observation, and leave.

#![cfg(feature = "tcp")]

use std::{
  future::Future,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use agnostic::tokio::TokioRuntime;
use futures_util::StreamExt;
use memberlist_reactor::{
  Delegate, Error, Event, MaybeResolved, Memberlist, NodeState, Options, RuntimeOptions,
  SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

/// Builds a TCP node advertising an OS-picked loopback port. The constructor
/// reads the bound address back, so the node advertises its real port and a peer
/// can dial it via `local().addr_ref()`.
async fn make(id: &str) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_knows_itself() {
  let m = make("node-a").await;
  assert_eq!(m.local().id_ref().as_str(), "node-a");
  assert_eq!(m.num_members(), 1, "a lone node counts itself");
  let _ = m.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_join_converge() {
  let a = make("a").await;
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();

  let n = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  assert_eq!(n, 1, "one seed dispatched");

  // Localhost RTT is sub-millisecond; 8s is far over the connect + push/pull
  // budget. Poll the lock-free snapshot until both nodes see two members.
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
    "did not converge: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A delegate that counts `notify_join` invocations.
struct RecordingDelegate {
  joins: Arc<AtomicUsize>,
}

impl Delegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  fn notify_join(
    &self,
    _node: Arc<NodeState<SmolStr, SocketAddr>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let joins = self.joins.clone();
    async move {
      joins.fetch_add(1, Ordering::Relaxed);
    }
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegate_and_event_stream_observe_join() {
  let joins = Arc::new(AtomicUsize::new(0));
  let a = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("a"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    RecordingDelegate {
      joins: joins.clone(),
    },
  )
  .await
  .expect("a");
  // Subscribe before the join so the NodeJoined event is observed.
  let mut a_events = a.events();
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  // The delegate observes a join within the budget.
  let observed = tokio::time::timeout(Duration::from_secs(8), async {
    while joins.load(Ordering::Relaxed) == 0 {
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(observed.is_ok(), "delegate never observed a join");

  // The event stream delivers a NodeJoined.
  let streamed = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      match a_events.next().await {
        Some(Event::NodeJoined(_)) => return true,
        Some(_) => {}
        None => return false,
      }
    }
  })
  .await;
  assert_eq!(
    streamed.ok(),
    Some(true),
    "event stream never delivered NodeJoined"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leave_completes_within_budget() {
  let a = make("a").await;
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  // leave() parks until the Dead-self notices reach the wire (LeftCluster); it
  // must resolve within the budget rather than hang.
  let left = tokio::time::timeout(Duration::from_secs(8), b.leave()).await;
  assert!(
    matches!(left, Ok(Ok(()))),
    "leave did not complete: {left:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_shutdown_is_idempotent() {
  let m = make("node").await;
  // The first shutdown stops the driver; a second must still return Ok rather
  // than wait forever on a reply no driver will send. Timeout-wrapped to catch a
  // regression to the hanging behaviour.
  let first = tokio::time::timeout(Duration::from_secs(5), m.shutdown()).await;
  assert!(matches!(first, Ok(Ok(()))), "first shutdown: {first:?}");
  let second = tokio::time::timeout(Duration::from_secs(5), m.shutdown()).await;
  assert!(
    matches!(second, Ok(Ok(()))),
    "second shutdown must be idempotent, not hang: {second:?}"
  );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_advertise_is_rejected() {
  // Binding the wildcard 0.0.0.0:0 would advertise an unspecified contact peers
  // cannot dial; construction must reject it rather than join as unreachable.
  let res = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("node"),
    MaybeResolved::Resolved("0.0.0.0:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await;
  let rejected = matches!(res, Err(Error::InvalidAdvertise(_)));
  assert!(rejected, "wildcard advertise must be rejected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_close_timeout_is_rejected() {
  // A zero close_timeout makes each post-Close graceful-drain write fire its
  // backstop immediately, so a graceful close RSTs its queued push/pull response
  // bytes instead of draining them; construction must reject it fail-fast.
  let res = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("zero-close"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_runtime(RuntimeOptions::new().with_close_timeout(Duration::ZERO)),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await;
  assert!(
    matches!(res, Err(Error::ZeroCloseTimeout)),
    "a zero close_timeout must be rejected with ZeroCloseTimeout"
  );

  // A nonzero close_timeout constructs.
  let ok = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("nonzero-close"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_runtime(RuntimeOptions::new().with_close_timeout(Duration::from_secs(10))),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("a nonzero close_timeout must construct");
  // Ignoring Err: best-effort teardown; the positive-control assertion already
  // passed, and the test does not depend on the shutdown reply.
  let _ = ok.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_listener_port() {
  let m = make("node").await;
  let addr = *m.local().addr_ref();
  let _ = m.shutdown().await;
  // The accept task must release the listener promptly after shutdown so the port
  // rebinds; before the cancellable-accept fix it stayed bound (blocked in
  // accept()) until a new inbound connection arrived. SO_REUSEADDR does not mask
  // this: an actively-listening port still rejects a fresh bind.
  let rebound = tokio::time::timeout(Duration::from_secs(5), async {
    loop {
      if std::net::TcpListener::bind(addr).is_ok() {
        return;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(rebound.is_ok(), "listener port not released after shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_seed_join_waits_for_both_exchanges() {
  let a = make("a").await;
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();
  // The same seed twice dispatches two exchanges; the join captures a distinct
  // ExchangeId for each at Connect and waits for every one to complete (the
  // machine may coalesce the redundant second contact). The point is it resolves
  // to a valid contacted count rather than hanging or completing early.
  let n = tokio::time::timeout(
    Duration::from_secs(8),
    b.join(
      &SocketAddrResolver,
      &[
        MaybeResolved::Resolved(a_addr),
        MaybeResolved::Resolved(a_addr),
      ],
    ),
  )
  .await
  .expect("join did not complete")
  .expect("join");
  assert!(
    (1..=2).contains(&n),
    "duplicate-seed join contacted count: {n}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_seed_joins_each_complete() {
  let a = make("a").await;
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();
  // Two handles join the same seed concurrently. Each must resolve with its own
  // result — completions are bound to joins by the ExchangeId captured at Connect
  // in dispatch order — rather than hanging or stealing the other's outcome.
  let b2 = b.clone();
  let seeds = [MaybeResolved::Resolved(a_addr)];
  let (first, second) = tokio::join!(
    tokio::time::timeout(Duration::from_secs(8), b.join(&SocketAddrResolver, &seeds)),
    tokio::time::timeout(Duration::from_secs(8), b2.join(&SocketAddrResolver, &seeds)),
  );
  assert!(matches!(first, Ok(Ok(n)) if n >= 1), "join 1: {first:?}");
  assert!(matches!(second, Ok(Ok(n)) if n >= 1), "join 2: {second:?}");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_join_after_detached_same_seed_completes() {
  let a = make("a").await;
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();
  // A fire-and-forget join queues a same-peer Connect ahead of the waiting join;
  // the waiting join must bind to its own exchange (not the detached one) and
  // still resolve rather than hang.
  let _ = b
    .join_detached(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  let resolved = tokio::time::timeout(
    Duration::from_secs(8),
    b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)]),
  )
  .await;
  assert!(
    resolved.is_ok(),
    "wait join hung behind a detached same-seed Connect"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// The bootstrap constructor resolves an `Unresolved` advertise address through
/// the supplied resolver (the `resolve_one` boundary path the all-`Resolved`
/// tests skip). A resolver mapping a host label to a concrete loopback address
/// constructs a node advertising that resolved address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_advertise_is_resolved_at_bootstrap() {
  use memberlist_reactor::AddressResolver;

  struct FixedResolver;
  impl AddressResolver for FixedResolver {
    type Address = &'static str;
    type Error = std::convert::Infallible;
    fn resolve(
      &self,
      _addr: &&'static str,
    ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
      // A `let` before the async block keeps this the explicit `+ Send`
      // form the `AddressResolver` contract requires (not bare `async fn`).
      let out = vec!["127.0.0.1:0".parse().unwrap()];
      async move { Ok(out) }
    }
  }

  let m = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &FixedResolver,
    SmolStr::new("unres-adv"),
    MaybeResolved::Unresolved("my-host"),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("an Unresolved advertise resolving to loopback must construct");
  assert_eq!(m.local().id_ref().as_str(), "unres-adv");
  // The advertised address is a concrete loopback port (the resolver's result,
  // read back post-bind).
  assert!(m.advertise_address().ip().is_loopback());
  let _ = m.shutdown().await;
}

/// A bootstrap `Unresolved` advertise whose resolver yields ZERO addresses
/// fails construction with `Error::NoAddresses` rather than binding an unusable
/// node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_advertise_empty_resolution_fails() {
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

  let res = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &EmptyResolver,
    SmolStr::new("no-adv"),
    MaybeResolved::Unresolved("nowhere"),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await;
  assert!(
    matches!(res, Err(Error::NoAddresses)),
    "an advertise resolving to nothing must fail with NoAddresses, got a non-NoAddresses result"
  );
}

/// Panics in `notify_join` but records `notify_leave`, to prove a delegate panic
/// does not kill the observation task.
struct PanicJoinDelegate {
  left: Arc<AtomicBool>,
}

impl Delegate for PanicJoinDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  // Manual `impl Future` per the Delegate trait convention; clippy would
  // otherwise want this single-panic body written as an `async fn`.
  #[allow(clippy::manual_async_fn)]
  fn notify_join(
    &self,
    _node: Arc<NodeState<SmolStr, SocketAddr>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    async move { panic!("intentional delegate panic in notify_join") }
  }

  fn notify_leave(
    &self,
    _node: Arc<NodeState<SmolStr, SocketAddr>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let left = self.left.clone();
    async move {
      left.store(true, Ordering::Relaxed);
    }
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_leave_is_observed_via_event_stream_and_membership() {
  // A converges with B; when B leaves, A must observe NodeLeft on its event
  // stream (the driver's NodeLeft dispatch + LeftCluster accounting) and B must
  // drop out of A's alive set.
  let a = make("leave-obs-a").await;
  let b = make("leave-obs-b").await;
  let a_addr = *a.local().addr_ref();
  let mut a_events = a.events();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  let converged = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_online_members() == 2 && b.num_online_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "cluster did not converge");

  // B leaves gracefully; .await returns once the Dead-self is on the wire.
  tokio::time::timeout(Duration::from_secs(8), b.leave())
    .await
    .expect("leave must not hang")
    .expect("leave");

  // A observes NodeLeft for B on the event stream.
  let saw_left = tokio::time::timeout(Duration::from_secs(8), async {
    while let Some(ev) = a_events.next().await {
      if let Event::NodeLeft(node) = ev
        && node.id_ref() == &SmolStr::new("leave-obs-b")
      {
        return true;
      }
    }
    false
  })
  .await
  .unwrap_or(false);
  assert!(saw_left, "A never observed NodeLeft(B) on its event stream");

  // B is no longer in A's alive set.
  let dropped = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_online_members() == 1 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    dropped.is_ok(),
    "B did not drop out of A's alive set after leaving"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegate_panic_does_not_wedge_observation_task() {
  let left = Arc::new(AtomicBool::new(false));
  let a = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("a"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    PanicJoinDelegate { left: left.clone() },
  )
  .await
  .expect("a");
  let b = make("b").await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  // notify_join panics (and is contained); the obs task must stay alive to
  // deliver the later notify_leave when b departs.
  let _ = tokio::time::timeout(Duration::from_secs(8), b.leave()).await;
  let observed = tokio::time::timeout(Duration::from_secs(8), async {
    while !left.load(Ordering::Relaxed) {
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    observed.is_ok(),
    "obs task did not survive a delegate panic"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}
