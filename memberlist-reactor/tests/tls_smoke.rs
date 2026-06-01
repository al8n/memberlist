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
  DriverOptions, Error, MaybeResolved, Memberlist, Options, SocketAddrResolver, TlsOptions,
  VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

/// Builds a TLS node advertising an OS-picked loopback port, verifying peers
/// against the cert behind `tls` and presenting `localhost` as the SNI (matching
/// the test cert's SAN).
async fn make(id: &str, tls: TlsOptions) -> Memberlist<SmolStr> {
  Memberlist::<SmolStr>::tls::<TokioRuntime, _, _, _>(
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
  let res = Memberlist::<SmolStr>::tls::<TokioRuntime, _, _, _>(
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
  let ok = Memberlist::<SmolStr>::tls::<TokioRuntime, _, _, _>(
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
  let b = Memberlist::<SmolStr>::tls::<TokioRuntime, _, _, _>(
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
  let sender = Memberlist::<SmolStr>::tls::<TokioRuntime, _, _, _>(
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
