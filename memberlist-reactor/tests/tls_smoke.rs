//! TLS backend smoke tests — single-node liveness, a two-node join over the
//! reliable push/pull exchange tunnelled through TLS, and leave.

#![cfg(feature = "tls")]

#[path = "support/tls.rs"]
mod support;

use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  time::Duration,
};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use memberlist_reactor::{
  DriverOptions, Error, MaybeResolved, Memberlist, MemberlistOptions, Options, SocketAddrResolver,
  TlsOptions, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

/// Builds a TLS node advertising an OS-picked loopback port, verifying peers
/// against the cert behind `tls` and presenting `localhost` as the SNI (matching
/// the test cert's SAN).
async fn make(id: &str, tls: TlsOptions) -> Memberlist<SmolStr, SocketAddr> {
  Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    tls,
    |_: &SocketAddr| Some("localhost".to_string()),
  )
  .await
  .expect("bind tls memberlist")
}

/// Builds a labeled TLS node advertising an OS-picked loopback port. The cluster
/// label restricts which peers may complete the reliable push/pull exchange;
/// nodes with a different label are rejected at the stream layer even when the
/// TLS certificate is mutually trusted.
async fn make_labeled(id: &str, tls: TlsOptions, label: &[u8]) -> Memberlist<SmolStr, SocketAddr> {
  Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(
      MemberlistOptions::new()
        .with_label(Some(label.to_vec()))
        .expect("valid label"),
    ),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    tls,
    |_: &SocketAddr| Some("localhost".to_string()),
  )
  .await
  .expect("bind labeled tls memberlist")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_knows_itself() {
  let m = make("node-a", support::self_trusted_tls_options()).await;
  assert_eq!(m.local().id_ref().as_str(), "node-a");
  assert_eq!(m.num_members(), 1, "a lone node counts itself");
  let _ = m.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_join_converge() {
  // Both nodes present and trust the same self-signed localhost cert, so each
  // verifies the other's server cert through the shared root; `clone_key` mints a
  // 'static clone of the private key for the second node.
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let tls_a = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_b = support::build_tls_options(cert, key, roots);

  let a = make("a", tls_a).await;
  let b = make("b", tls_b).await;
  let a_addr = *a.local().addr_ref();

  let n = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  assert_eq!(n, 1, "one seed dispatched");

  // Localhost RTT is sub-millisecond; 8s is far over the TLS handshake +
  // push/pull budget. Poll the lock-free snapshot until both see two members.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leave_completes_within_budget() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let tls_a = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_b = support::build_tls_options(cert, key, roots);

  let a = make("a", tls_a).await;
  let b = make("b", tls_b).await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");

  let left = tokio::time::timeout(Duration::from_secs(8), b.leave()).await;
  assert!(
    matches!(left, Ok(Ok(()))),
    "leave did not complete: {left:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_close_timeout_is_rejected() {
  // The TLS backend funnels through the same stream constructor as TCP, so a
  // zero close_timeout (which RSTs a graceful close's queued push/pull bytes
  // instead of draining them) must be rejected fail-fast here too.
  let res = Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new("tls-zero-close"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_driver(DriverOptions::new().with_close_timeout(Duration::ZERO)),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    support::self_trusted_tls_options(),
    |_: &SocketAddr| Some("localhost".to_string()),
  )
  .await;
  assert!(
    matches!(res, Err(Error::ZeroCloseTimeout)),
    "a zero close_timeout must be rejected with ZeroCloseTimeout over TLS"
  );

  // A nonzero close_timeout constructs over TLS.
  let ok = Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new("tls-nonzero-close"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_driver(DriverOptions::new().with_close_timeout(Duration::from_secs(10))),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    support::self_trusted_tls_options(),
    |_: &SocketAddr| Some("localhost".to_string()),
  )
  .await
  .expect("a nonzero close_timeout must construct over TLS");
  // Ignoring Err: best-effort teardown; the positive-control assertion already
  // passed, and the test does not depend on the shutdown reply.
  let _ = ok.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_without_sni_fails_bounded() {
  // The sni_provider always returns None, so every TLS dial fails at dial setup,
  // before a Connect. join() must resolve with an error within the budget rather
  // than wait forever for an exchange that was never created.
  let b = Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new("b"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    support::self_trusted_tls_options(),
    |_: &SocketAddr| None,
  )
  .await
  .expect("b");
  let seed: SocketAddr = "127.0.0.1:65535".parse().unwrap();
  let res = tokio::time::timeout(
    Duration::from_secs(8),
    b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed)]),
  )
  .await;
  assert!(
    matches!(res, Ok(Err(_))),
    "join with no SNI should fail bounded, got {res:?}"
  );

  let _ = b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_many_reliable_partial_pre_connect_failure_reports_err() {
  // `send_many_reliable` where one payload connects and a LATER payload fails
  // BEFORE Connect must reply Err(SendFailed) — not a false Ok once the
  // connected exchange succeeds.
  //
  // The pre-Connect failure is provoked deterministically with a stateful SNI
  // resolver that returns Some("localhost") on its FIRST call and None
  // thereafter: a None SNI makes the TLS dial_context fail before any Connect,
  // so the second payload's dial is retired pre-Connect and never surfaces an
  // ExchangeCompleted. With two payloads (N = 2) and one captured exchange
  // (M = 1) the driver seeds `failed = N - M = 1`; even though the first
  // payload's exchange succeeds against the live seed, the final reply is
  // Err(SendFailed). Without the seeding fix the driver would park with
  // failed = 0, see the one connected exchange succeed, and falsely report Ok.
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let tls_seed = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_sender = support::build_tls_options(cert, key, roots);

  // Live TLS seed the first payload connects to.
  let seed = make("tls-pcf-seed", tls_seed).await;
  let seed_addr = *seed.local().addr_ref();

  // Sender's SNI resolver: Some on the first dial, None on every later dial.
  let sni_calls = Arc::new(AtomicUsize::new(0));
  let sni_calls_for_closure = sni_calls.clone();
  let sender = Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new("tls-pcf-sender"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    tls_sender,
    move |_: &SocketAddr| {
      if sni_calls_for_closure.fetch_add(1, Ordering::SeqCst) == 0 {
        Some("localhost".to_string())
      } else {
        None
      }
    },
  )
  .await
  .expect("sender construct");

  // Two payloads to the SAME live seed: payload 0 dials with SNI Some →
  // Connect → succeeds; payload 1 dials with SNI None → pre-Connect failure.
  let result = tokio::time::timeout(
    Duration::from_secs(15),
    sender.send_many_reliable(
      seed_addr,
      [
        Bytes::from_static(b"tls-pcf-first"),
        Bytes::from_static(b"tls-pcf-second"),
      ],
    ),
  )
  .await
  .expect("send_many_reliable must resolve, not hang");
  assert!(
    matches!(result, Err(Error::SendFailed)),
    "a partial pre-Connect failure must reply Err(SendFailed), got {result:?}"
  );
  assert!(
    sni_calls.load(Ordering::SeqCst) >= 2,
    "both payload dials must consult the SNI resolver; calls={}",
    sni_calls.load(Ordering::SeqCst)
  );

  let _ = sender.shutdown().await;
  let _ = seed.shutdown().await;
}

/// A reliable `send_reliable` over TLS reaches the peer's `notify_user_msg`
/// hook — the TLS reliable-stream data path end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_send_reliable_delivers_to_delegate() {
  use std::{future::Future, sync::Mutex};

  use memberlist_reactor::Delegate;

  type Messages = Arc<Mutex<Vec<Vec<u8>>>>;
  struct Recorder {
    msgs: Messages,
  }
  impl Delegate for Recorder {
    type Id = SmolStr;
    type Address = SocketAddr;
    fn notify_user_msg(&self, msg: Bytes) -> impl Future<Output = ()> + Send + '_ {
      let msgs = self.msgs.clone();
      async move { msgs.lock().unwrap().push(msg.to_vec()) }
    }
  }

  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let tls_seed = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_sender = support::build_tls_options(cert, key, roots);

  let msgs: Messages = Arc::new(Mutex::new(Vec::new()));
  let seed = Memberlist::<SmolStr, _>::tls::<TokioRuntime, _, _, _>(
    &SocketAddrResolver,
    SmolStr::new("tls-rel-seed"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    Recorder { msgs: msgs.clone() },
    tls_seed,
    |_: &SocketAddr| Some("localhost".to_string()),
  )
  .await
  .expect("seed");
  let seed_addr = *seed.local().addr_ref();
  let sender = make("tls-rel-sender", tls_sender).await;

  sender
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"tls-reliable-payload");
  tokio::time::timeout(
    Duration::from_secs(10),
    sender.send_reliable(seed_addr, payload.clone()),
  )
  .await
  .expect("send_reliable must not hang")
  .expect("send_reliable over TLS");

  let saw = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_slice() == payload.as_ref())
      {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    saw.is_ok(),
    "TLS reliable payload not delivered; received: {:?}",
    msgs.lock().unwrap()
  );

  let _ = sender.shutdown().await;
  let _ = seed.shutdown().await;
}

/// Over TLS, after `leave()` the directed sends and policy setters funnel
/// through the same running-node gate and are rejected with `NotRunning`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_commands_after_leave_are_rejected() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let tls_a = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_b = support::build_tls_options(cert, key, roots);

  let a = make("tls-leave-a", tls_a).await;
  let b = make("tls-leave-b", tls_b).await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  let converged = tokio::time::timeout(Duration::from_secs(8), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "TLS pair did not converge");

  b.leave().await.expect("leave");

  let send = tokio::time::timeout(
    Duration::from_secs(5),
    b.send_reliable(a_addr, Bytes::from_static(b"z")),
  )
  .await
  .expect("send_reliable must not hang after leave");
  assert!(
    matches!(send, Err(Error::NotRunning)),
    "expected NotRunning from TLS send_reliable after leave, got {send:?}"
  );

  let compr = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_compression_options(memberlist_reactor::CompressionOptions::new()),
  )
  .await
  .expect("set_compression_options must not hang after leave");
  assert!(
    matches!(compr, Err(Error::NotRunning)),
    "expected NotRunning from TLS set_compression_options after leave, got {compr:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A different cluster label over shared TLS trust must not merge on the
/// reliable plane: when two clusters share the same certificate and root CA,
/// only the label header in the reliable stream distinguishes them.
///
/// Three nodes share one self-signed cert and one trust root so every TLS
/// handshake succeeds. Nodes `a` (label "cluster-a") and `b` (label "cluster-b")
/// must NOT merge even though the TLS layer accepts both. Node `a2` (label
/// "cluster-a") MUST converge with `a`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_label_isolates_reliable_plane() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let tls_a = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_b = support::build_tls_options(cert.clone(), key.clone_key(), roots.clone());
  let tls_a2 = support::build_tls_options(cert, key, roots);

  let a = make_labeled("tls-iso-a", tls_a, b"cluster-a").await;
  let b = make_labeled("tls-iso-b", tls_b, b"cluster-b").await;
  let a2 = make_labeled("tls-iso-a2", tls_a2, b"cluster-a").await;
  let a_addr = *a.local().addr_ref();

  // A different label over shared TLS trust must not merge on the reliable plane.
  // Ignoring Err: join failure on a mismatched label is expected.
  let _ = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  tokio::time::sleep(Duration::from_millis(300)).await;
  assert_eq!(
    a.num_members(),
    1,
    "cross-label TLS join must not add B: a has {} members",
    a.num_members()
  );
  assert_eq!(
    b.num_members(),
    1,
    "cross-label TLS join must not add A: b has {} members",
    b.num_members()
  );

  // Same label over shared TLS trust must converge.
  a2.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("same-label TLS join must succeed");

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
    "same-label TLS cluster did not converge: a={}, a2={}",
    a.num_members(),
    a2.num_members()
  );

  // Ignoring Err: best-effort teardown; assertion already passed.
  let _ = a.shutdown().await;
  let _ = a2.shutdown().await;
  let _ = b.shutdown().await;
}
