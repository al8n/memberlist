//! QUIC backend smoke tests — single-node liveness and a two-node join.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{
  future::Future,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  time::Duration,
};

use agnostic::tokio::TokioRuntime;
use futures_util::StreamExt;
use memberlist_reactor::{
  Delegate, Error, Event, MaybeResolved, Memberlist, NodeState, Options, QuicConfig,
  SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

/// Builds a QUIC node advertising an OS-picked loopback port. The constructor
/// reads the bound address back, so the node advertises its real port and a
/// peer can dial it via `local().addr_ref()`.
async fn make(id: &str, qcfg: QuicConfig) -> Memberlist<SmolStr> {
  Memberlist::<SmolStr>::quic::<TokioRuntime, _, _>(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_knows_itself() {
  let m = make("node-a", support::self_trusted_quic_config()).await;
  assert_eq!(m.local().id_ref().as_str(), "node-a");
  assert_eq!(m.num_members(), 1, "a lone node counts itself");
  let _ = m.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_join_converge() {
  // Both nodes share one self-signed cert + trust root, so each accepts the
  // other's (identical) cert through the shared root. `clone_key` mints a
  // 'static clone of the private key for the second node's server config.
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make("a", qcfg_a).await;
  let b = make("b", qcfg_b).await;
  let a_addr = *a.local().addr_ref();

  let n = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  assert_eq!(n, 1, "one seed dispatched");

  // Localhost RTT is sub-millisecond; 8s is far over the handshake + push/pull
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
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let joins = Arc::new(AtomicUsize::new(0));
  let a = Memberlist::<SmolStr>::quic::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new("a"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    RecordingDelegate {
      joins: joins.clone(),
    },
    qcfg_a,
  )
  .await
  .expect("a");
  // Subscribe before the join so the NodeJoined event is observed.
  let mut a_events = a.events();
  let b = make("b", qcfg_b).await;
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
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make("a", qcfg_a).await;
  let b = make("b", qcfg_b).await;
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
  let m = make("node", support::self_trusted_quic_config()).await;
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

/// `join` to an UNREACHABLE QUIC seed resolves to `Err` and does NOT hang.
/// The reactor QUIC driver parks every dispatched push/pull exchange in a
/// `PendingJoin` with NO deadline, resolving only when each parked
/// `ExchangeId` surfaces a terminal `ExchangeCompleted(PushPull)`. The dial to
/// an unbound port never completes its handshake, so the `QuicEndpoint` retires
/// the dial intent at its exchange deadline and (with the PushPull pre-bridge
/// fix) emits `ExchangeCompleted(PushPull, Failed)`, which drains the waiter
/// set and resolves the join with `JoinAllFailed`.
///
/// Wrapped in a `tokio::time::timeout` so a regression that drops the PushPull
/// `Failed` emission (leaving the deadline-less waiter parked) fails fast here
/// rather than hanging the suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_to_unreachable_quic_seed_returns_err_not_hang() {
  let node = make("quic-join-unreach", support::self_trusted_quic_config()).await;
  // Port 1 on loopback has no QUIC listener — the dial handshake cannot
  // complete, so the exchange is retired at its deadline.
  let unreachable = "127.0.0.1:1".parse::<SocketAddr>().unwrap();

  let result = tokio::time::timeout(
    Duration::from_secs(30),
    node.join(&SocketAddrResolver, &[MaybeResolved::Resolved(unreachable)]),
  )
  .await
  .expect(
    "QUIC join to an unreachable seed HUNG (timed out) — the deadline-less \
     PendingJoin waiter never drained; the PushPull dial failure was not \
     surfaced as Event::ExchangeCompleted(Failed)",
  );
  assert!(
    result.is_err(),
    "QUIC join to an unreachable seed MUST return Err (zero contact), got {result:?}"
  );

  let _ = node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_advertise_is_rejected() {
  // Binding the wildcard 0.0.0.0:0 would advertise an unspecified contact peers
  // cannot dial; construction must reject it rather than join as unreachable.
  let res = Memberlist::<SmolStr>::quic::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new("node"),
    MaybeResolved::Resolved("0.0.0.0:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    support::self_trusted_quic_config(),
  )
  .await;
  let rejected = matches!(res, Err(Error::InvalidAdvertise(_)));
  assert!(rejected, "wildcard advertise must be rejected");
}
