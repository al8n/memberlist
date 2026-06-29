//! CIDR peer-admission over QUIC (reactor driver): the OUTBOUND boundary.
//!
//! `with_cidr_policy` gates outbound QUIC the same way it gates the stream
//! driver's dials: a join toward a seed our own policy excludes emits no
//! handshake and fails immediately (a bounded `JoinFailed`), and a reliable send
//! to a blocked peer fails at the boundary without opening a stream — rather than
//! initiating a QUIC connection that only fails later when replies are dropped by
//! the receive-side filter.

#![cfg(all(feature = "quic-rustls-ring", feature = "cidr"))]

#[path = "support/quic.rs"]
mod support;

use std::net::SocketAddr;

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{
  CidrPolicy, Error, MaybeResolved, Memberlist, Options, QuicOptions, RuntimeOptions,
  SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a reactor QUIC node on an OS-allocated loopback port, optionally with a
/// driver-level CIDR policy.
async fn make_quic(
  id: &str,
  qcfg: QuicOptions,
  policy: Option<CidrPolicy>,
) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  let mut opts = Options::<SmolStr>::new();
  if let Some(policy) = policy {
    opts = opts.with_cidr_policy(policy);
  }
  Memberlist::<SmolStr, _, TokioRuntime>::quic(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    opts,
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    qcfg,
  )
  .await
  .expect("bind quic memberlist")
}

/// A shared-cert pair (each trusts the other's identical self-signed cert).
fn shared_quic_pair() -> (QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let b = support::build_quic_config(cert, key, roots);
  (a, b)
}

/// A joiner whose OWN policy excludes the seed emits no QUIC handshake toward it
/// and the join fails immediately, rather than dialing and failing later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_blocks_quic_joiner_outbound_dial() {
  let (qcfg_seed, qcfg_joiner) = shared_quic_pair();
  // The joiner admits only 10.0.0.0/8; its loopback seed is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let seed = make_quic("cidr-quic-dial-seed", qcfg_seed, None).await;
  let joiner = make_quic("cidr-quic-dial-joiner", qcfg_joiner, Some(policy)).await;
  let seed_addr = seed.advertise_address();

  let res = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;
  assert!(
    res.is_err(),
    "a QUIC join whose only seed is CIDR-blocked must fail, got {res:?}"
  );

  assert_eq!(joiner.num_members(), 1, "the joiner has only itself");
  assert_eq!(seed.num_members(), 1, "the seed was never contacted");

  joiner.shutdown().await.ok();
  seed.shutdown().await.ok();
}

/// A reliable user-message over QUIC to a CIDR-blocked peer fails at the
/// transport boundary without opening a stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_blocks_quic_reliable_send() {
  let (qcfg, _unused) = shared_quic_pair();
  // The node admits only 10.0.0.0/8, so any loopback peer is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_quic("cidr-quic-send", qcfg, Some(policy)).await;

  let res = a
    .send_reliable(loopback_addr(9999), bytes::Bytes::from_static(b"blocked"))
    .await;
  assert!(
    res.is_err(),
    "a reliable send to a CIDR-blocked peer must fail at the boundary, got {res:?}"
  );

  a.shutdown().await.ok();
}

/// Partial-CIDR-blocked join: one seed CIDR-blocked (skipped before `pending`)
/// plus one allowed-but-unreachable seed (enters `pending`, runs to deadline).
///
/// Regression for the bug where `JoinFailed.requested()` was derived from
/// `pending.len()` after the CIDR filter, which undercounted the blocked seed.
/// The correct denominator is the full resolved seed count (`addrs.len()`)
/// captured BEFORE the CIDR filter — matching the stream driver's semantics.
///
/// Uses a short `join_deadline` (500 ms) so the allowed-but-unreachable seed
/// runs to deadline quickly. Asserts `requested == 2` (both seeds counted) and
/// `contacted == 0` (neither was reached).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cidr_policy_partial_block_join_requested_counts_all_resolved_seeds() {
  use std::time::Duration;

  let (qcfg_seed, qcfg_joiner) = shared_quic_pair();
  // Start a real node just to obtain a bound loopback port. Its address
  // will be CIDR-blocked by the joiner's /32 policy, so no handshake fires.
  let seed = make_quic("cidr-partial-seed", qcfg_seed, None).await;
  let seed_addr = seed.advertise_address();

  // An unreachable loopback address (127.0.0.2, distinct from the seed's
  // 127.0.0.1). Port 30399 is unbound, so the QUIC datagram vanishes and the
  // exchange runs to the join deadline.
  let unreachable_addr: SocketAddr = "127.0.0.2:30399".parse().expect("loopback 2");

  // `CidrPolicy` is an allowlist. Admit ONLY the unreachable seed's IP
  // (127.0.0.2): this BLOCKS the real seed (127.0.0.1, skipped before `pending`)
  // and ADMITS the unreachable address (it enters `pending`, dials, and runs to
  // the join deadline). `seed_addr` is the blocked seed in the seeds list below.
  let policy = CidrPolicy::try_from([format!("{}/32", unreachable_addr.ip()).as_str()].as_slice())
    .expect("valid /32 cidr");

  // Build the joiner with a short join_deadline (500 ms) so the
  // allowed-but-unreachable exchange resolves quickly.
  let joiner = Memberlist::<SmolStr, _, TokioRuntime>::quic(
    &SocketAddrResolver,
    SmolStr::new("cidr-partial-joiner"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::<SmolStr>::new()
      .with_cidr_policy(policy)
      .with_runtime(RuntimeOptions::new().with_join_deadline(Duration::from_millis(500))),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    qcfg_joiner,
  )
  .await
  .expect("bind joiner");

  // Two seeds: seed_addr is CIDR-blocked (skipped before `pending`),
  // unreachable_addr is allowed but never responds.
  let seeds = [
    MaybeResolved::Resolved(seed_addr),
    MaybeResolved::Resolved(unreachable_addr),
  ];
  let result = joiner.join(&SocketAddrResolver, &seeds).await;

  match result {
    Err((reached, Error::JoinFailed(jf))) => {
      assert!(reached.is_empty(), "no peer was contacted");
      assert_eq!(
        jf.requested(),
        2,
        "requested must count ALL resolved seeds (including CIDR-blocked), \
         got {} (expected 2)",
        jf.requested()
      );
      assert_eq!(jf.contacted(), 0, "contacted must be zero");
    }
    other => panic!("expected JoinFailed for partial-CIDR-blocked join, got {other:?}"),
  }

  joiner.shutdown().await.ok();
  seed.shutdown().await.ok();
}
