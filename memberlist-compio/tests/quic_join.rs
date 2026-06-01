//! QUIC backend `join_with` synchronous-contact tests.
//!
//! Exercises the QUIC driver's `JoinKind::WaitForCompletion` arm:
//! `dispatch_command` fans out one [`QuicEndpoint::start_push_pull`]
//! per resolved seed, captures the returned machine `StreamId` as an
//! [`ExchangeId`], and parks the waiter; `drain_actions` reduces
//! `pending_joins` on `Event::ExchangeCompleted` filtered to
//! `ExchangeKind::PushPull`; `reap_pending_joins` replies on either
//! `pending` empty or `deadline` elapsed.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{
  collections::HashSet,
  net::SocketAddr,
  time::{Duration, Instant},
};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, MemberlistError, MemberlistOptions, MergeDelegate, Options,
  QuicConfig, QuicMemberlist, QuicTransportOptions, Resolver, SocketAddrResolver, VoidDelegate,
};
use memberlist_proto::typed::{Meta, NodeState};
use rustls::RootCertStore;
use smol_str::SmolStr;

/// Test resolver that always resolves to an empty address list — models
/// a service-discovery resolver that finds no live endpoints under a
/// non-empty service key.
struct EmptyResolver;

impl Resolver for EmptyResolver {
  type Address = String;
  type Error = std::io::Error;

  async fn resolve(&self, _addr: &Self::Address) -> Result<Vec<SocketAddr>, Self::Error> {
    Ok(Vec::new())
  }
}

/// Machine admission `MergeDelegate` (`Send + Sync`) that rejects every join
/// merge — installed on a join initiator via `Options::with_merge_delegate`.
struct RejectMerge;

impl MergeDelegate<SmolStr, SocketAddr> for RejectMerge {
  fn notify_merge(&self, _peers: &[NodeState<SmolStr, SocketAddr>]) -> bool {
    false
  }
}

/// Spin-wait up to `deadline` for `predicate` to return true. Returns
/// false on timeout. Used because compio has no async assertion harness
/// — polling the lock-free snapshot is the documented observability path.
async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// Build the shared-cert QUIC configs for two-node tests: one self-signed
/// cert, both nodes in the same trust root so they mutually accept each
/// other's (identical) certificate.
fn two_node_quic_configs() -> (memberlist_compio::QuicConfig, memberlist_compio::QuicConfig) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);
  (qcfg_a, qcfg_b)
}

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a `QuicMemberlist` advertising `127.0.0.1:port` with the supplied
/// `QuicConfig`. The membership-input address type is `SocketAddr`, so the
/// construction resolver is the identity `SocketAddrResolver` (never invoked
/// for a resolved advertise).
async fn make_quic(id: &str, port: u16, qcfg: QuicConfig) -> QuicMemberlist<SmolStr, SocketAddr> {
  make_quic_with(id, port, qcfg, MemberlistOptions::new()).await
}

/// Like [`make_quic`] but with explicit SWIM-level [`MemberlistOptions`]
/// (gossip MTU, meta cap, initial meta).
async fn make_quic_with(
  id: &str,
  port: u16,
  qcfg: QuicConfig,
  mopts: MemberlistOptions,
) -> QuicMemberlist<SmolStr, SocketAddr> {
  let opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(port)))
      .with_quic_config(qcfg),
  )
  .with_memberlist(mopts);
  QuicMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind quic memberlist")
}

/// `join_with` against a reachable seed returns `Ok(1)` once the
/// outbound push/pull `ExchangeCompleted(Succeeded)` lands. Mirrors
/// the stream driver's `join_with_waits_for_actual_contact` test
/// (`memberlist-compio/tests/tcp_join.rs`).
///
/// Distinct fixed ports (the QUIC `advertise_addr` is what the
/// membership FSM stores and what subsequent peers must dial; an
/// OS-pick port 0 would leave the FSM advertising port 0 and the
/// dial would target the unbound port-0 alias). The chosen ports
/// avoid overlap with `quic_smoke` (7401/7402) and the second test
/// below.
#[compio::test]
async fn quic_join_with_waits_for_actual_contact() {
  // Two nodes sharing one self-signed cert + trust root — each
  // accepts the other's (identical) cert via the shared root.
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a_port: u16 = 7411;
  let b_port: u16 = 7412;
  let a = make_quic("a", a_port, qcfg_a).await;
  let b = make_quic("b", b_port, qcfg_b).await;

  let b_addr = loopback_addr(b_port);

  let n = a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join Ok");
  assert_eq!(n, 1, "one contacted exchange");
  assert!(
    a.alive_count() >= 2,
    "A did not learn about B (alive_count={})",
    a.alive_count()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// `join_with` against an unbound loopback port surfaces
/// `MemberlistError::JoinAllFailed { requested: 1, contacted: 0 }`
/// after the deadline elapses. Under QUIC the dial datagram vanishes
/// (no kernel responder to send a connection-refused back), so the
/// exchange runs to the full per-call deadline before reaping.
#[compio::test]
async fn quic_join_with_blackhole_surfaces_join_all_failed() {
  let qcfg = support::self_trusted_quic_config();
  // Local bind on OS-picked port 0 — A makes no inbound traffic
  // here, so the advertise-port-0 quirk that breaks two-node joins
  // is moot.
  let a = make_quic("a", 0, qcfg).await;

  // Port 30199 is unbound — UDP datagrams to it vanish into the
  // kernel (no listener responds), so the QUIC handshake never
  // completes and the exchange resolves only on deadline.
  let result = a
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(30199))],
    )
    .await;
  match result {
    Err(MemberlistError::JoinAllFailed(jf)) => {
      assert_eq!(jf.requested(), 1, "one seed requested");
      assert_eq!(jf.contacted(), 0, "no contacts to a blackhole");
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
}

/// `join` to an UNREACHABLE QUIC seed resolves to `Err` and does NOT hang,
/// timeout-wrapped so a regression fails fast. The compio QUIC `PendingJoin`
/// carries a deadline, so it cannot hang indefinitely, but the PushPull
/// pre-bridge fix lets the join resolve on the machine's
/// `ExchangeCompleted(PushPull, Failed)` emission (draining the waiter set)
/// rather than waiting out the full join deadline. The wrapper guards against
/// any regression that strands the waiter past the timeout.
#[compio::test]
async fn quic_join_to_unreachable_seed_returns_err_not_hang() {
  let qcfg = support::self_trusted_quic_config();
  let node = make_quic("qj-unreach", 0, qcfg).await;
  // Port 1 on loopback has no QUIC listener — the dial handshake cannot
  // complete, so the exchange is retired at its deadline.
  let unreachable = loopback_addr(1);

  let result = compio::time::timeout(
    Duration::from_secs(30),
    node.join(&SocketAddrResolver, &[MaybeResolved::Resolved(unreachable)]),
  )
  .await;
  match result {
    Ok(join_res) => assert!(
      join_res.is_err(),
      "QUIC join to an unreachable seed MUST return Err (zero contact), got {join_res:?}"
    ),
    Err(_elapsed) => panic!(
      "QUIC join to an unreachable seed HUNG (timed out) — the PendingJoin \
       waiter never resolved; the PushPull dial failure was not surfaced as \
       Event::ExchangeCompleted(Failed)"
    ),
  }

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), node.shutdown()).await;
}

/// A non-empty `seeds` slice whose resolver returns an empty address
/// vector must surface as `JoinAllFailed`, NOT a silent `Ok(0)`. A
/// service-discovery resolver that finds no endpoints for a configured
/// service key is the canonical failure case this test guards against.
///
/// Uses a single-node memberlist (port 7260); no peer is needed because
/// the failure happens before any outbound dial.
#[compio::test]
async fn join_with_empty_resolution_surfaces_join_all_failed() {
  let qcfg = support::self_trusted_quic_config();
  // This joiner resolves `String` service keys via `EmptyResolver`, so its
  // membership-input address type is `String` (not the default `HostAddr`).
  let joiner: QuicMemberlist<SmolStr, String> = QuicMemberlist::<SmolStr, String>::new(
    Options::new(
      QuicTransportOptions::<SmolStr, String>::new()
        .with_local_id(SmolStr::new("joiner"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(7260)))
        .with_quic_config(qcfg),
    ),
    VoidDelegate::default(),
    &EmptyResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("joiner construct");

  let seeds: Vec<MaybeResolved<String, SocketAddr>> = vec![
    MaybeResolved::Unresolved("svc-a".into()),
    MaybeResolved::Unresolved("svc-b".into()),
  ];
  let err = joiner
    .join(&EmptyResolver, &seeds)
    .await
    .expect_err("non-empty seeds resolving to empty address list must fail");
  match err {
    MemberlistError::JoinAllFailed(payload) => {
      assert_eq!(
        payload.requested(),
        seeds.len(),
        "requested reflects input seed count"
      );
      assert_eq!(payload.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  // Truly-empty input still short-circuits to Ok(0): no command sent,
  // no JoinAllFailed.
  let empty: Vec<MaybeResolved<String, SocketAddr>> = Vec::new();
  let count = joiner
    .join(&EmptyResolver, &empty)
    .await
    .expect("empty seeds returns Ok");
  assert_eq!(count, 0);

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// `join_with` against a UDP socket that swallows every datagram — the
/// QUIC handshake never receives an Initial response, so the exchange
/// runs to the full per-call deadline and surfaces `JoinAllFailed`.
///
/// Unlike the TCP banner test (which writes bytes the decoder rejects),
/// here no bytes arrive at all: the `UdpSocket` binds the port so the
/// OS does not ICMP-unreachable the datagram, but no reply is ever sent.
#[compio::test]
async fn join_with_against_unresponsive_udp_endpoint_surfaces_join_all_failed() {
  use compio::net::UdpSocket;

  let qcfg = support::self_trusted_quic_config();
  let joiner = make_quic("joiner", 7230, qcfg).await;

  // Bind a UDP socket that silently swallows every datagram — the
  // QUIC handshake fails because no Initial response arrives.
  let banner = UdpSocket::bind(loopback_addr(7231))
    .await
    .expect("banner bind");
  let _banner_guard = banner; // hold the binding; reads never performed

  let err = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7231))],
    )
    .await
    .expect_err("join against unresponsive UDP endpoint must not report contact");
  match err {
    MemberlistError::JoinAllFailed(payload) => {
      assert_eq!(payload.requested(), 1);
      assert_eq!(payload.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// A node configured with `with_gossip_mtu` above the historical 16 KiB
/// recv buffer must actually receive datagrams that approach the
/// configured size. Paired with `with_meta_max_size` raised past the
/// 512-byte Go-memberlist default, an Alive broadcast carrying a
/// ~18 KiB Meta surfaces over UDP gossip and both nodes converge.
#[compio::test]
async fn join_with_above_16kib_gossip_mtu_receives_large_alive_broadcasts() {
  let (qcfg_a, qcfg_b) = two_node_quic_configs();

  // ~18 KiB meta — above 16 KiB (old recv buffer cap) and below the
  // 32 KiB gossip_mtu configured below.
  let big_meta = Meta::try_from(vec![0x5au8; 18 * 1024]).expect("18 KiB within wire ceiling");

  let seed = make_quic_with(
    "seed",
    7210,
    qcfg_a,
    MemberlistOptions::new()
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(32 * 1024)
      .with_initial_meta(big_meta.clone()),
  )
  .await;

  let joiner = make_quic_with(
    "joiner",
    7211,
    qcfg_b,
    MemberlistOptions::new()
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(32 * 1024),
  )
  .await;

  let count = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7210))],
    )
    .await
    .expect("join against 18 KiB-meta seed");
  assert_eq!(count, 1);

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge under large-MTU + large-meta: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// A `gossip_mtu` whose wire datagram cannot fit a single UDP packet is an
/// impossible configuration (gossip is sent as one UDP datagram, hard-capped
/// at 65507 bytes after the 30-byte encryption wrapper, i.e. a 65477-byte
/// plaintext ceiling). `Memberlist::new` must reject it fail-fast with
/// `InvalidGossipMtu` BEFORE binding any socket, while a value just under the
/// ceiling constructs successfully.
#[compio::test]
async fn transport_new_rejects_impossible_gossip_mtu() {
  // 1 MiB plaintext gossip_mtu — far above the 65477-byte ceiling. A
  // near-MTU gossip packet built at this size would always exceed the
  // 65507-byte UDP datagram limit and be silently dropped.
  let opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("reject"))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(support::self_trusted_quic_config()),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(1 << 20));
  let res = QuicMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  // `Memberlist` is not `Debug`, so match rather than `{res:?}`-format the
  // whole `Result`.
  match res {
    Err(MemberlistError::InvalidGossipMtu(e)) => {
      assert_eq!(e.configured(), 1 << 20, "carries the configured value");
      assert_eq!(e.ceiling(), 65507 - 30, "carries the 65477-byte ceiling");
    }
    Err(other) => panic!("expected InvalidGossipMtu, got {other:?}"),
    Ok(_) => panic!("1 MiB gossip_mtu must be rejected, but construction succeeded"),
  }

  // Just under the ceiling (65476 < 65477) must construct successfully.
  let ok_opts = Options::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("accept"))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_quic_config(support::self_trusted_quic_config()),
  )
  .with_memberlist(MemberlistOptions::new().with_gossip_mtu(65507 - 30 - 1));
  let m = QuicMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("gossip_mtu just under the ceiling must construct");

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), m.shutdown()).await;
}

/// A joiner whose `meta_max_size` is smaller than a seed's must still
/// accept the seed's larger Meta — the cap is local-broadcast-only, NOT
/// a peer-rejection filter. The test pins the local-only semantic by
/// having the seed broadcast a 4 KiB meta while the joiner keeps the
/// default 512-byte cap.
#[compio::test]
async fn join_with_accepts_seed_with_larger_meta_cap() {
  let (qcfg_a, qcfg_b) = two_node_quic_configs();

  // 4 KiB meta — well past the joiner's default 512-byte cap.
  let big_meta = Meta::try_from(vec![0x42u8; 4096]).expect("4 KiB within wire ceiling");

  let seed = make_quic_with(
    "seed",
    7220,
    qcfg_a,
    MemberlistOptions::new()
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(8 * 1024)
      .with_initial_meta(big_meta.clone()),
  )
  .await;

  // Joiner keeps the default meta_max_size. It must still accept the
  // seed's 4 KiB meta and add the seed to membership.
  let joiner = make_quic_with(
    "joiner",
    7221,
    qcfg_b,
    MemberlistOptions::new().with_gossip_mtu(32 * 1024),
  )
  .await;

  let count = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7220))],
    )
    .await
    .expect("join against larger-meta seed");
  assert_eq!(count, 1);

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge under meta-cap mismatch: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// Duplicate seed addresses must produce duplicate push/pull exchanges,
/// each counted independently. Passing `[X, X]` to `join_with` queues
/// two outbound exchanges; both must terminate with `Succeeded` before
/// the call returns, and the returned count is 2 (exchanges), not 1
/// (distinct addresses).
#[compio::test]
async fn join_with_counts_each_outbound_exchange_for_duplicate_seeds() {
  let (qcfg_a, qcfg_b) = two_node_quic_configs();

  let seed = make_quic("seed", 7240, qcfg_a).await;
  let joiner = make_quic("joiner", 7241, qcfg_b).await;

  let count = joiner
    .join(
      &SocketAddrResolver,
      &[
        MaybeResolved::Resolved(loopback_addr(7240)),
        MaybeResolved::Resolved(loopback_addr(7240)),
      ],
    )
    .await
    .expect("join with duplicate seeds should succeed");
  assert_eq!(
    count, 2,
    "duplicate seed counts each outbound exchange independently"
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// `join_with`'s `contacted` count must reflect DIRECT per-seed
/// exchanges, NOT transitive membership discovery via a sibling seed.
///
/// Setup: A joins C so A's membership records C as Alive. C is then
/// dropped without a clean shutdown (no Leave broadcast). A joiner
/// separately calls `join_with(&[A, C])`. A is reachable; C's port
/// rejects QUIC handshakes (no listener). A's push/pull response merges
/// C-as-Alive into the joiner's membership, but C's own exchange never
/// completes. The counted result must be 1 (A only).
#[compio::test]
async fn join_with_does_not_count_transitively_discovered_seeds() {
  // Three distinct cert/key pairs so each node has its own identity,
  // but all share a single trust root for mutual acceptance.
  let (cert_a, key_a) = support::generate_localhost_cert();
  let (cert_c, key_c) = support::generate_localhost_cert();
  let (cert_j, key_j) = support::generate_localhost_cert();

  let mut roots = RootCertStore::empty();
  roots.add(cert_a.clone()).expect("root a");
  roots.add(cert_c.clone()).expect("root c");
  roots.add(cert_j.clone()).expect("root j");

  let qcfg_a = support::build_quic_config(cert_a, key_a, roots.clone());
  let qcfg_c = support::build_quic_config(cert_c, key_c, roots.clone());
  let qcfg_j = support::build_quic_config(cert_j, key_j, roots);

  let c = make_quic("c", 7250, qcfg_c).await;
  let a = make_quic("a", 7251, qcfg_a).await;

  // A joins C so A's membership records C as Alive.
  let n = a
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7250))],
    )
    .await
    .expect("a joins c");
  assert_eq!(n, 1, "A directly contacted C");

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(|| a.member_count() == 2, Duration::from_secs(10)).await;
  assert!(converged, "A did not learn C");

  // Drop C without shutdown: no Leave broadcast, so A's view of C
  // stays Alive. C's QUIC listener closes when the driver task is
  // cancelled by the last-handle drop.
  drop(c);
  compio::time::sleep(Duration::from_millis(100)).await;

  let joiner = make_quic("joiner", 7252, qcfg_j).await;

  let contacted = joiner
    .join(
      &SocketAddrResolver,
      &[
        MaybeResolved::Resolved(loopback_addr(7251)),
        MaybeResolved::Resolved(loopback_addr(7250)),
      ],
    )
    .await
    .expect("joiner contacts A even if C is unreachable");
  assert_eq!(
    contacted, 1,
    "join must not count transitively-discovered seed as contacted"
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// Concurrent `join_with` calls for the same seed address must use
/// CALL-SCOPED `ExchangeId` ownership: each call's `contacted` count
/// must only advance when its own dispatched exchange receives bytes,
/// never when a sibling call's overlapping exchange does.
#[compio::test]
async fn concurrent_join_with_same_seed_uses_call_scoped_eid_ownership() {
  // Three distinct cert/key pairs sharing one trust root — seed, j1, j2
  // all mutually accept each other's certificates.
  let (cert_seed, key_seed) = support::generate_localhost_cert();
  let (cert_j1, key_j1) = support::generate_localhost_cert();
  let (cert_j2, key_j2) = support::generate_localhost_cert();

  let mut roots = RootCertStore::empty();
  roots.add(cert_seed.clone()).expect("root seed");
  roots.add(cert_j1.clone()).expect("root j1");
  roots.add(cert_j2.clone()).expect("root j2");

  let qcfg_seed = support::build_quic_config(cert_seed, key_seed, roots.clone());
  let qcfg_j1 = support::build_quic_config(cert_j1, key_j1, roots.clone());
  let qcfg_j2 = support::build_quic_config(cert_j2, key_j2, roots);

  let seed = make_quic("seed", 7200, qcfg_seed).await;
  let j1 = make_quic("j1", 7201, qcfg_j1).await;
  let j2 = make_quic("j2", 7202, qcfg_j2).await;

  // Two concurrent join calls to the same seed. Each must resolve to
  // Ok(1) — its own exchange bytes — independently.
  let seeds = [MaybeResolved::Resolved(loopback_addr(7200))];
  let fut1 = j1.join(&SocketAddrResolver, &seeds);
  let fut2 = j2.join(&SocketAddrResolver, &seeds);
  let (n1, n2) = futures_util::future::join(fut1, fut2).await;
  let n1 = n1.expect("j1 contacts seed");
  let n2 = n2.expect("j2 contacts seed");
  assert_eq!(n1, 1, "j1 reports only its own seed contact");
  assert_eq!(n2, 1, "j2 reports only its own seed contact");

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), j1.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), j2.shutdown()).await;
}

/// Under large in-flight push/pull volume, completed exchanges must still
/// be reaped before the join deadline — no completion sitting behind the
/// `ITER_DRAIN_CAP` may be incorrectly timed out. Every concurrent
/// `join_with` call must return `Ok(1)`.
#[compio::test]
async fn join_with_under_inflight_pushpull_volume_does_not_timeout_completions() {
  let (qcfg_seed, qcfg_joiner) = two_node_quic_configs();

  let seed = make_quic("seed", 7270, qcfg_seed).await;
  let joiner = make_quic("joiner", 7271, qcfg_joiner).await;

  // Issue many concurrent join calls. With 64 in-flight exchanges the
  // driver collectively processes hundreds of completion events,
  // exercising the cap-then-deadline interaction. Every call must
  // return Ok(1).
  const N: usize = 64;
  let mut futures = Vec::with_capacity(N);
  for _ in 0..N {
    let joiner = joiner.clone();
    let seed_addr = loopback_addr(7270);
    futures.push(async move {
      joiner
        .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
        .await
        .expect("each parallel join must succeed")
    });
  }
  let counts: Vec<usize> = futures_util::future::join_all(futures).await;
  let unique_counts: HashSet<usize> = counts.iter().copied().collect();
  assert_eq!(
    unique_counts,
    HashSet::from([1usize]),
    "every parallel join must report Ok(1); got {counts:?}"
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// Mismatched CA chains: each node trusts only its own self-signed cert,
/// not the peer's. When A dials B, B presents `cert_b` and A's client
/// verifier walks `roots_a` (containing only `cert_a`), finds no matching
/// trust anchor, and rejects. The TLS handshake fails inside quinn-proto,
/// no `ExchangeCompleted(Succeeded)` is ever emitted for the dispatched
/// exchange, and the join reaps at the per-call deadline with
/// `MemberlistError::JoinAllFailed { requested: 1, contacted: 0 }`.
///
/// Uses fixed ports 7301/7302 — clear of every earlier test's range.
/// Runs for the full `DEFAULT_JOIN_DEADLINE` (~10s) before reaping.
#[compio::test]
async fn join_with_failing_tls_handshake_surfaces_join_all_failed() {
  let (cert_a, key_a) = support::generate_localhost_cert();
  let (cert_b, key_b) = support::generate_localhost_cert();

  // A trusts only its own cert (not B's).
  let mut roots_a = RootCertStore::empty();
  roots_a.add(cert_a.clone()).expect("root a");
  let qcfg_a = support::build_quic_config(cert_a, key_a, roots_a);

  // B trusts only its own cert (not A's). B presents `cert_b` on accept;
  // A's client verifier walks `roots_a` and rejects.
  let mut roots_b = RootCertStore::empty();
  roots_b.add(cert_b.clone()).expect("root b");
  let qcfg_b = support::build_quic_config(cert_b, key_b, roots_b);

  let a = make_quic("a", 7301, qcfg_a).await;
  let b = make_quic("b", 7302, qcfg_b).await;

  let result = a
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7302))],
    )
    .await;
  match result {
    Err(MemberlistError::JoinAllFailed(jf)) => {
      assert_eq!(jf.requested(), 1, "one seed requested");
      assert_eq!(jf.contacted(), 0, "TLS handshake failed; no contact");
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Mismatched server name: B's cert SANs only `"otherhost"`; A's
/// `QuicConfig::server_name` is configured to `"localhost"`. The dial
/// path threads that name into rustls's `ClientConfig`, which then
/// performs server-name verification against the presented cert's SANs.
/// No SAN matches `"localhost"`, so rustls rejects the cert before any
/// QUIC stream opens. The exchange never emits
/// `ExchangeCompleted(Succeeded)`, and the join reaps at deadline with
/// `MemberlistError::JoinAllFailed { requested: 1, contacted: 0 }`.
///
/// A's trust store contains B's cert so the trust-chain check passes —
/// the test isolates the SAN/SNI mismatch from the CA-mismatch failure
/// mode exercised by the test above.
#[compio::test]
async fn join_with_mismatched_server_name_surfaces_join_all_failed() {
  let (cert_a, key_a) = support::generate_localhost_cert();
  // B's cert: `"otherhost"` SAN only — no `"localhost"` SAN.
  let (cert_b, key_b) = support::generate_cert_with_sans(vec!["otherhost".into()]);

  // A's trust store contains B's cert so the trust chain validates;
  // only the server-name check fails.
  let mut roots_a = RootCertStore::empty();
  roots_a.add(cert_b.clone()).expect("a trusts b's cert");
  let qcfg_a = support::build_quic_config(cert_a, key_a, roots_a);

  // B's `build_quic_config` uses `with_no_client_auth`, so B's trust
  // store is unused on the server side. Seed it with B's own cert for
  // completeness.
  let mut roots_b = RootCertStore::empty();
  roots_b.add(cert_b.clone()).expect("self-root b");
  let qcfg_b = support::build_quic_config(cert_b, key_b, roots_b);

  let a = make_quic("a", 7311, qcfg_a).await;
  let b = make_quic("b", 7312, qcfg_b).await;

  let result = a
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7312))],
    )
    .await;
  match result {
    Err(MemberlistError::JoinAllFailed(jf)) => {
      assert_eq!(jf.requested(), 1, "one seed requested");
      assert_eq!(jf.contacted(), 0, "SAN mismatch rejected; no contact");
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Two-node join: A and B mutually join; both sides must report
/// `alive_count == 2` and `member_count == 2` after a settling window.
///
/// Uses `dispatch_join` (fire-and-forget) then polls `member_count`
/// — mirrors `two_node_join_converges_member_counts` in `tcp_join.rs`.
#[compio::test]
async fn two_node_join_converges_member_counts() {
  let (qcfg_a, qcfg_b) = two_node_quic_configs();

  let seed = make_quic("seed", 7280, qcfg_a).await;
  let joiner = make_quic("joiner", 7281, qcfg_b).await;

  let count = joiner
    .dispatch_join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7280))],
    )
    .await
    .expect("dispatch_join");
  assert_eq!(count, 1, "one seed address dispatched");

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  assert_eq!(joiner.alive_count(), 2);
  assert_eq!(seed.alive_count(), 2);

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// QUIC ingress must decode compound gossip datagrams.
///
/// SWIM piggyback bundles >= 2 membership broadcasts into a single
/// compound frame (tag byte `1`). The original `parse_message` call
/// rejected compound frames with `Err(_)`, so the `Err(_) => continue`
/// arm silently dropped the entire datagram. `parse_messages` handles
/// both plain and compound transparently.
///
/// The test injects a hand-crafted compound datagram (two Alive messages)
/// directly into B's UDP socket — bypassing the gossip scheduler, which
/// has no scheduled deadline until `start_scheduling` is called — and
/// asserts that both contained messages are applied to B's membership FSM.
///
/// If `parse_message` were used instead of `parse_messages`, B's
/// `alive_count` would remain at 2 (self + A only); with the fix it
/// reaches 3 (self + A + C).
#[compio::test]
async fn quic_compound_gossip_is_decoded_after_join() {
  use compio::{buf::BufResult, net::UdpSocket};
  use memberlist::codec::{EncodeOptions, encode_outgoing_compound};
  use memberlist_proto::{
    Node,
    typed::{Alive, Message},
  };

  let (qcfg_a, qcfg_b) = two_node_quic_configs();

  let a = make_quic("a", 7420, qcfg_a).await;
  let b = make_quic("b", 7421, qcfg_b).await;

  // B joins A via synchronous push/pull so B's membership contains A.
  // After this, B's alive_count == 2 (self + A).
  let n = b
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(7420))],
    )
    .await
    .expect("b joins a");
  assert_eq!(n, 1);
  assert!(
    b.alive_count() >= 2,
    "B did not learn A (alive_count={})",
    b.alive_count()
  );

  // Craft a compound datagram carrying two Alive messages:
  //   Alive(A, 127.0.0.1:7420, incarnation 1)
  //   Alive(C, 127.0.0.1:7422, incarnation 1)  ← phantom node C
  // Both messages must be decoded and applied by B's ingress path.
  // With `parse_message` (the bug), the Compound tag causes an Err and
  // the whole datagram is dropped. With `parse_messages` (the fix),
  // both Alives land in B's FSM and B reaches alive_count >= 3.
  let msg_a: Message<SmolStr, std::net::SocketAddr> = Message::Alive(Alive::new(
    1,
    Node::new(SmolStr::new("a"), loopback_addr(7420)),
  ));
  let msg_c: Message<SmolStr, std::net::SocketAddr> = Message::Alive(Alive::new(
    1,
    Node::new(SmolStr::new("c"), loopback_addr(7422)),
  ));
  let encode_opts = EncodeOptions::default();
  let compound_bytes =
    encode_outgoing_compound(&[msg_a, msg_c], &encode_opts).expect("compound encode");

  // Send the compound datagram to B's UDP port from an ephemeral
  // socket. B's driver classifies the first byte (Compound tag = 1)
  // as Class::Memberlist, buffers it in poll_memberlist_ingress, and
  // drain_actions decodes it via parse_messages.
  let sender = UdpSocket::bind(loopback_addr(0))
    .await
    .expect("ephemeral bind");
  let BufResult(res, _) = sender
    .send_to(compound_bytes.to_vec(), loopback_addr(7421))
    .await;
  res.expect("send_to b");

  // B must now apply both Alive messages. alive_count >= 3 means B
  // knows self + A + C. A was already in B's membership; C is new.
  let converged = wait_until(|| b.alive_count() >= 3, Duration::from_secs(5)).await;
  assert!(
    converged,
    "expected B to decode compound datagram and reach alive_count >= 3; got {}",
    b.alive_count()
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Regression (QUIC outbound push/pull-reply admission gate): a join INITIATOR
/// whose `MergeDelegate` rejects the seed must surface `JoinAllFailed` — the
/// rejected outbound reply terminalizes the exchange (the QUIC bridge fails it
/// with `AdmissionClosed`), so a rejected merge is never counted as a contacted
/// seed. Mirrors the TCP `rejected_merge_does_not_fire_on_reply_arm` regression
/// on the QUIC bridge.
#[compio::test]
async fn quic_join_initiator_merge_rejection_surfaces_join_all_failed() {
  let (qcfg_seed, qcfg_joiner) = two_node_quic_configs();

  let seed_port: u16 = 7461;
  let joiner_port: u16 = 7462;
  let seed = make_quic("rr-seed", seed_port, qcfg_seed).await;

  // The joiner installs a reject-all admission `MergeDelegate` via `Options`.
  let joiner: QuicMemberlist<SmolStr, SocketAddr> = QuicMemberlist::<SmolStr, SocketAddr>::new(
    Options::new(
      QuicTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(SmolStr::new("rr-joiner"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(joiner_port)))
        .with_quic_config(qcfg_joiner),
    )
    .with_merge_delegate(RejectMerge),
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind quic joiner");

  // The single seed's reply is rejected on the outbound arm → exchange failed →
  // contacted 0 → `JoinAllFailed` (not a spurious success).
  let result = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(loopback_addr(seed_port))],
    )
    .await;
  assert!(
    matches!(result, Err(MemberlistError::JoinAllFailed(_))),
    "a merge-rejected QUIC join must fail (the outbound exchange is terminalized), got {result:?}"
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
}
