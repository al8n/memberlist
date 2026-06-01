//! TLS backend smoke tests — single-node liveness, a two-node join over the
//! reliable push/pull exchange tunnelled through TLS, and leave.

#![cfg(feature = "tls")]

#[path = "support/tls.rs"]
mod support;

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
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
