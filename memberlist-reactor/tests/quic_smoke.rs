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
  Delegate, Error, Event, MaybeResolved, Memberlist, MemberlistOptions, NodeState, Options,
  QuicConfig, SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

/// Builds a QUIC node advertising an OS-picked loopback port. The constructor
/// reads the bound address back, so the node advertises its real port and a
/// peer can dial it via `local().addr_ref()`.
async fn make(id: &str, qcfg: QuicConfig) -> Memberlist<SmolStr> {
  make_with_opts(id, qcfg, MemberlistOptions::new()).await
}

/// Like [`make`] but with explicit SWIM-level [`MemberlistOptions`].
async fn make_with_opts(
  id: &str,
  qcfg: QuicConfig,
  ml_opts: MemberlistOptions,
) -> Memberlist<SmolStr> {
  Memberlist::<SmolStr>::quic::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
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

/// A different cluster label over shared QUIC/TLS trust must not allow a node
/// to join a foreign cluster via the QUIC reliable plane.
///
/// Three nodes share one self-signed cert and trust root so every TLS
/// handshake succeeds. The cluster label on the reliable bridge is the sole
/// isolation mechanism:
///
/// - `a` (label "cluster-a") and `b` (label "cluster-b") must NOT converge.
/// - `a2` (label "cluster-a") must converge with `a`.
///
/// Uses OS-picked ports (port 0) so the three nodes never share a socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_label_isolates_reliable_plane() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  // All three nodes trust the same root so TLS never rejects; only the
  // reliable-stream cluster label distinguishes the clusters.
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_a2 = support::build_quic_config(cert, key, roots);

  let a_opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-a".to_vec()))
    .expect("valid label");
  let b_opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-b".to_vec()))
    .expect("valid label");
  let a2_opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-a".to_vec()))
    .expect("valid label");

  let a = make_with_opts("ql-a", qcfg_a, a_opts).await;
  let b = make_with_opts("ql-b", qcfg_b, b_opts).await;
  let a2 = make_with_opts("ql-a2", qcfg_a2, a2_opts).await;
  let a_addr = *a.local().addr_ref();

  // Cross-label join: B's stream label mismatch must cause A to reject it.
  // Ignoring Err: join failure on a mismatched label is expected here.
  let _ = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  tokio::time::sleep(Duration::from_millis(500)).await;
  assert_eq!(
    a.num_members(),
    1,
    "cross-label QUIC join must not add B to A: a has {} members",
    a.num_members()
  );
  assert_eq!(
    b.num_members(),
    1,
    "cross-label QUIC join must not add A to B: b has {} members",
    b.num_members()
  );

  // Same-label join: A2 must converge with A on the reliable plane.
  a2.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("same-label QUIC join must succeed");
  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if a.num_members() == 2 && a2.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    converged.is_ok(),
    "same-label QUIC cluster did not converge: a={}, a2={}",
    a.num_members(),
    a2.num_members()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = a.shutdown().await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = b.shutdown().await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = a2.shutdown().await;
}
