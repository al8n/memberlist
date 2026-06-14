//! CIDR peer-admission over QUIC: the driver-level source filter.
//!
//! `with_cidr_policy` drops every UDP packet from a blocked source IP before the
//! QUIC endpoint sees it, so a blocked peer completes no handshake and forms no
//! connection — unlike the membership-only `with_alive_delegate` path, which
//! admits the QUIC connection and only ignores the resulting Alive. This pins the
//! QUIC `handle_udp` source guard (both the main recv arm and the timeout peek).

#![cfg(all(feature = "quic-rustls-ring", feature = "cidr"))]

#[path = "support/quic.rs"]
mod support;

use std::{net::SocketAddr, time::Duration};

use memberlist_compio::{
  CidrPolicy, FirstAddrResolver, MaybeResolved, Memberlist, Options, QuicOptions, QuicTransport,
  QuicTransportOptions, SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Two nodes sharing one self-signed cert + trust root, so each accepts the
/// other's identical certificate (QUIC validates the server chain against the
/// client roots).
fn two_node_quic_configs() -> (QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);
  (qcfg_a, qcfg_b)
}

/// Build a `Memberlist` on an OS-allocated loopback port, optionally with a
/// driver-level CIDR policy that filters the gossip / QUIC datagram source.
async fn make_quic(
  id: &str,
  qcfg: QuicOptions,
  policy: Option<CidrPolicy>,
) -> Memberlist<SmolStr, SocketAddr> {
  let mut opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(qcfg),
  );
  if let Some(policy) = policy {
    opts = opts.with_cidr_policy(policy);
  }
  Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind quic memberlist")
}

/// A blocked source's QUIC handshake never reaches the endpoint: A's policy
/// excludes B's loopback source, so A drops every UDP packet from B before
/// `handle_udp`. B's connection cannot establish, so the push/pull join fails —
/// and neither node admits the other. This is the SOURCE filter, distinct from
/// the membership (advertised-address) filter: the packets never reach the QUIC
/// machine at all, so even an in-policy advertised address could not be admitted.
#[compio::test]
async fn cidr_policy_blocks_quic_handshake_from_blocked_source() {
  let (qcfg_a, qcfg_b) = two_node_quic_configs();
  // A admits only 10.0.0.0/8; B's loopback (127.0.0.1) source is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_quic("cidr-quic-a", qcfg_a, Some(policy)).await;
  let b = make_quic("cidr-quic-b", qcfg_b, None).await;
  let a_addr = a.advertise_address();

  // B dials A over QUIC. A drops B's handshake packets at the source filter, so
  // the connection never establishes and the join fails outright.
  let res = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  assert!(
    res.is_err(),
    "a blocked-source QUIC join must fail at the boundary, got {res:?}"
  );

  // Neither node learns the other: A dropped B's packets, B's handshake timed out.
  assert_eq!(a.member_count(), 1, "A must not admit the blocked peer");
  assert_eq!(
    b.member_count(),
    1,
    "B's failed handshake leaves it with only itself"
  );

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}

/// `with_cidr_policy` gates OUTBOUND QUIC too: a joiner whose OWN policy excludes
/// the seed emits NO handshake to it and the join fails immediately (a bounded
/// `JoinAllFailed`), rather than initiating a QUIC connection that only fails
/// later when the seed's replies are dropped by the receive-side filter.
#[compio::test]
async fn cidr_policy_blocks_quic_joiner_outbound_dial() {
  let (qcfg_seed, qcfg_joiner) = two_node_quic_configs();
  // The joiner admits only 10.0.0.0/8; its loopback seed is out of policy.
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  let seed = make_quic("cidr-quic-dial-seed", qcfg_seed, None).await;
  let joiner = make_quic("cidr-quic-dial-joiner", qcfg_joiner, Some(policy)).await;
  let seed_addr = seed.advertise_address();

  // The joiner's own policy excludes the loopback seed, so no QUIC handshake is
  // started toward it and the join fails outright.
  let res = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;
  assert!(
    res.is_err(),
    "a QUIC join whose only seed is CIDR-blocked must fail, got {res:?}"
  );

  assert_eq!(joiner.member_count(), 1, "the joiner has only itself");
  assert_eq!(seed.member_count(), 1, "the seed was never contacted");

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// A reliable user-message (`send_reliable`) over QUIC to a CIDR-blocked peer
/// fails at the transport boundary without opening a stream — the outbound
/// reliable analog of the join gate above.
#[compio::test]
async fn cidr_policy_blocks_quic_reliable_send() {
  let qcfg = support::self_trusted_quic_config();
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

  a.shutdown().await.expect("a shutdown");
}

/// Control / non-vacuity: a policy that INCLUDES the loopback source admits the
/// QUIC peer, so the two nodes converge normally. Proves the block above is the
/// CIDR source filter, not an unconditional QUIC failure.
#[compio::test]
async fn cidr_policy_admits_quic_peer_inside_allowlist() {
  let (qcfg_a, qcfg_b) = two_node_quic_configs();
  // A admits 127.0.0.0/8; B's loopback source IS in policy.
  let policy = CidrPolicy::try_from(["127.0.0.0/8"].as_slice()).expect("valid cidr");
  let a = make_quic("cidr-quic-allow-a", qcfg_a, Some(policy)).await;
  let b = make_quic("cidr-quic-allow-b", qcfg_b, None).await;
  let a_addr = a.advertise_address();

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("in-policy QUIC join succeeds");

  let start = std::time::Instant::now();
  let converged = loop {
    if a.member_count() == 2 && b.member_count() == 2 {
      break true;
    }
    if start.elapsed() > Duration::from_secs(8) {
      break false;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  };
  assert!(
    converged,
    "an in-policy QUIC peer must be admitted: a={}, b={}",
    a.member_count(),
    b.member_count()
  );

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}
