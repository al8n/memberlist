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
  CidrPolicy, MaybeResolved, Memberlist, Options, QuicOptions, SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a reactor QUIC node on an OS-allocated loopback port, optionally with a
/// driver-level CIDR policy.
async fn make_quic(id: &str, qcfg: QuicOptions, policy: Option<CidrPolicy>) -> Memberlist<SmolStr> {
  let mut opts = Options::<SmolStr>::new();
  if let Some(policy) = policy {
    opts = opts.with_cidr_policy(policy);
  }
  Memberlist::<SmolStr>::quic::<TokioRuntime, _, _>(
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
