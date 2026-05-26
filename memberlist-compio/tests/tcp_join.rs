//! Multi-node TCP join integration test.
//!
//! Exercises the full reliable-path pipeline: TCP listener accept on the
//! seed; outbound dial from the joiner; per-bridge byte mover on both
//! sides; push/pull request and response; coordinator state convergence.

#![cfg(feature = "tcp")]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::{Duration, Instant},
};

use memberlist_compio::{MemberlistError, Resolver, SocketAddrResolver, TcpMemberlist};
use memberlist_machine::{TcpOptions, config::EndpointConfig};
use memberlist_wire::typed::Meta;
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

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
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

#[compio::test]
async fn two_node_join_converges_member_counts() {
  let seed_addr = loopback_addr(7200);
  let joiner_addr = loopback_addr(7201);

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr),
    TcpOptions::new(Some(b"cluster-join".to_vec())),
  )
  .await
  .expect("seed construct");

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"cluster-join".to_vec())),
  )
  .await
  .expect("joiner construct");

  // Drive the join. The resolver is the identity pass-through since
  // the seed list already carries concrete SocketAddrs. `join_with`
  // returns the dispatched count immediately; convergence is
  // observed via the snapshot below.
  let count = joiner
    .dispatch_join_with(&SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with");
  assert_eq!(count, 1, "one seed address dispatched");

  // Wait for both sides to observe the cluster size grow to 2. The
  // push/pull exchange is a single TCP round-trip; convergence is
  // bounded by the driver loop cadence + the OS TCP handshake
  // latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  // Alive counts should match the member counts at steady state — every
  // member is Alive at incarnation 1.
  assert_eq!(joiner.alive_count(), 2);
  assert_eq!(seed.alive_count(), 2);

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// `join_with` (the synchronous variant) returns the actually-contacted
/// count, not the dispatched count. Against a single reachable seed it
/// must return `Ok(1)` once `NodeJoined` for the seed lands — before
/// `JOIN_DEADLINE` elapses.
#[compio::test]
async fn join_with_waits_for_actual_contact() {
  let seed_addr = loopback_addr(7210);
  let joiner_addr = loopback_addr(7211);

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr),
    TcpOptions::new(Some(b"cluster-join-sync".to_vec())),
  )
  .await
  .expect("seed construct");

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"cluster-join-sync".to_vec())),
  )
  .await
  .expect("joiner construct");

  let count = joiner
    .join_with(&SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with should succeed against a reachable seed");
  assert_eq!(count, 1, "one seed contacted");

  // join_with returns when the seed is contacted; on return both sides
  // must already observe the cluster size of 2 (no further wait).
  assert_eq!(joiner.member_count(), 2);
  // Seed-side may lag by one driver tick — its NodeJoined for the
  // joiner is emitted by handle_transport_data on the inbound bridge,
  // which schedules before the join_with reply lands. A bounded wait
  // absorbs the cross-task scheduling jitter; 5s is comfortable even
  // under high parallel-test load on macOS.
  let seed_converged = wait_until(|| seed.member_count() == 2, Duration::from_secs(5)).await;
  assert!(
    seed_converged,
    "seed side did not converge after join_with returned: seed={}",
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// `join_with` against an unreachable seed must surface
/// `MemberlistError::JoinAllFailed { requested: 1, contacted: 0 }` once
/// the deadline elapses. The dial loop fails fast against a closed
/// loopback port, so the call returns well before `JOIN_DEADLINE`.
#[compio::test]
async fn join_with_blackhole_surfaces_join_all_failed() {
  let joiner_addr = loopback_addr(7212);
  // 127.0.0.1 with a port that has nothing listening — connect()
  // returns ECONNREFUSED immediately on Linux/macOS loopback, so the
  // bridge enters its EOF/Error path and no NodeJoined ever fires.
  let blackhole = loopback_addr(7213);

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"cluster-join-fail".to_vec())),
  )
  .await
  .expect("joiner construct");

  let err = joiner
    .join_with(&SocketAddrResolver, &[blackhole])
    .await
    .expect_err("join_with against blackhole must fail");

  match err {
    MemberlistError::JoinAllFailed(payload) => {
      assert_eq!(payload.requested(), 1, "requested matches input seed count");
      assert_eq!(payload.contacted(), 0, "no contacts when seed is blackhole");
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  joiner.shutdown().await.expect("joiner shutdown");
}

/// A non-empty `seeds` slice whose resolver returns an empty address
/// vector must surface as `JoinAllFailed`, NOT a silent `Ok(0)`. A
/// service-discovery resolver that finds no endpoints for a configured
/// service key is the canonical failure case this test guards against.
#[compio::test]
async fn join_with_empty_resolution_surfaces_join_all_failed() {
  let joiner_addr = loopback_addr(7214);
  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"cluster-empty-resolve".to_vec())),
  )
  .await
  .expect("joiner construct");

  let seeds: Vec<String> = vec!["svc-a".into(), "svc-b".into()];
  let err = joiner
    .join_with(&EmptyResolver, &seeds)
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
  let empty: Vec<String> = Vec::new();
  let count = joiner
    .join_with(&EmptyResolver, &empty)
    .await
    .expect("empty seeds returns Ok");
  assert_eq!(count, 0);

  joiner.shutdown().await.expect("joiner shutdown");
}

/// A node configured with `with_gossip_mtu` above the historical 16 KiB
/// recv buffer must actually receive datagrams that approach the
/// configured size. Paired with `with_meta_max_size` raised past the
/// 512-byte Go-memberlist default, an Alive broadcast carrying a
/// 20 KiB Meta surfaces over UDP gossip and the two nodes converge.
#[compio::test]
async fn join_with_above_16kib_gossip_mtu_receives_large_alive_broadcasts() {
  let seed_addr = loopback_addr(7300);
  let joiner_addr = loopback_addr(7301);

  // 20 KiB meta — comfortably above 16 KiB (the old recv buffer cap)
  // and below the 32 KiB gossip_mtu configured below.
  let big_meta = Meta::try_from(vec![0x5au8; 20 * 1024]).expect("20 KiB within wire ceiling");

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr)
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(32 * 1024)
      .with_initial_meta(big_meta.clone()),
    TcpOptions::new(Some(b"large-mtu".to_vec())),
  )
  .await
  .expect("seed construct");

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr)
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(32 * 1024),
    TcpOptions::new(Some(b"large-mtu".to_vec())),
  )
  .await
  .expect("joiner construct");

  let count = joiner
    .join_with(&SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with against 20 KiB-meta seed");
  assert_eq!(count, 1);

  // Both sides converge; large UDP alive broadcasts flow at the
  // raised MTU because the per-iter recv buffer tracks the
  // configured gossip_mtu (+ ENCRYPTED_WRAPPER_OVERHEAD) rather
  // than the old 16 KiB hardcoded cap.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge under large-MTU + large-meta: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// A joiner whose `meta_max_size` is smaller than a seed's must still
/// accept the seed's larger Meta — the cap is local-broadcast-only, NOT
/// a peer-rejection filter. Refusing larger metas would silently
/// fragment the cluster (the bridge still reaps cleanly, so
/// `ExchangeCompleted` would fire `Succeeded` and `join_with` would
/// report `Ok(1)` even though the seed never landed in membership).
/// The test pins the local-only semantic by having the seed broadcast
/// a 4 KiB meta while the joiner keeps the default 512-byte cap.
#[compio::test]
async fn join_with_accepts_seed_with_larger_meta_cap() {
  let seed_addr = loopback_addr(7320);
  let joiner_addr = loopback_addr(7321);

  // 4 KiB meta — well past the joiner's default 512-byte cap.
  let big_meta = Meta::try_from(vec![0x42u8; 4096]).expect("4 KiB within wire ceiling");

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr)
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(8 * 1024)
      .with_initial_meta(big_meta.clone()),
    TcpOptions::new(Some(b"meta-cap-mismatch".to_vec())),
  )
  .await
  .expect("seed construct");

  // Joiner keeps the default meta_max_size (512). It must still
  // accept the seed's 4 KiB meta and add the seed to membership.
  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr).with_gossip_mtu(32 * 1024),
    TcpOptions::new(Some(b"meta-cap-mismatch".to_vec())),
  )
  .await
  .expect("joiner construct");

  let count = joiner
    .join_with(&SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with against larger-meta seed");
  assert_eq!(count, 1);
  // Convergence check: both nodes must observe the cluster, proving
  // the seed's Alive landed in joiner's membership despite the
  // local cap mismatch.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge under meta-cap mismatch: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// Duplicate seed addresses must produce duplicate push/pull exchanges,
/// each counted independently. Passing `[X, X]` to `join_with` queues
/// two outbound exchanges; both must terminate with `Succeeded` before
/// the call returns, and the returned count is the number of
/// successful exchanges (2 here), not the number of distinct
/// addresses (1).
#[compio::test]
async fn join_with_counts_each_outbound_exchange_for_duplicate_seeds() {
  let seed_addr = loopback_addr(7280);
  let joiner_addr = loopback_addr(7281);

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr),
    TcpOptions::new(Some(b"join-duplicate".to_vec())),
  )
  .await
  .expect("seed construct");

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"join-duplicate".to_vec())),
  )
  .await
  .expect("joiner construct");

  let count = joiner
    .join_with(&SocketAddrResolver, &[seed_addr, seed_addr])
    .await
    .expect("join_with with duplicate seeds should succeed");
  assert_eq!(
    count, 2,
    "duplicate seed counts each outbound exchange independently"
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// `join_with`'s `contacted` count must reflect DIRECT per-seed
/// exchanges, NOT transitive membership discovery via a sibling seed.
///
/// Setup: A and C form a cluster. C is then dropped without a clean
/// shutdown (no `Leave` broadcast), so A still considers C `Alive`
/// in its membership while C's TCP listener is gone. A joiner
/// separately calls `join_with(&[A, C])`. A is reachable; C's port
/// rejects connections. A's push/pull response merges C-as-Alive into
/// the joiner's membership (firing a `NodeJoined(C)`), but C's own
/// bridge never receives bytes. The fix must count A only — counting
/// C as well would hide partial bootstrap failures behind cluster
/// gossip.
#[compio::test]
async fn join_with_does_not_count_transitively_discovered_seeds() {
  let a_addr = loopback_addr(7240);
  let c_addr = loopback_addr(7241);
  let joiner_addr = loopback_addr(7242);

  let c = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("c"), c_addr),
    TcpOptions::new(Some(b"transitive-discover".to_vec())),
  )
  .await
  .expect("c construct");

  let a = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("a"), a_addr),
    TcpOptions::new(Some(b"transitive-discover".to_vec())),
  )
  .await
  .expect("a construct");

  // A joins C directly so A's membership records C as Alive.
  let n = a
    .join_with(&SocketAddrResolver, &[c_addr])
    .await
    .expect("a joins c");
  assert_eq!(n, 1, "A directly contacted C");
  let converged = wait_until(|| a.member_count() == 2, Duration::from_secs(2)).await;
  assert!(converged, "A did not learn C");

  // Drop C without `shutdown()`: no Leave broadcast, so A's view of
  // C stays Alive until A's next probe of C times out (far longer
  // than this test's window). C's TCP listener closes when the
  // driver task is cancelled by the last-handle drop.
  drop(c);
  compio::time::sleep(Duration::from_millis(50)).await;

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"transitive-discover".to_vec())),
  )
  .await
  .expect("joiner construct");

  let contacted = joiner
    .join_with(&SocketAddrResolver, &[a_addr, c_addr])
    .await
    .expect("joiner contacts A even if C is unreachable");
  assert_eq!(
    contacted, 1,
    "join_with counted a transitively-discovered seed as contacted"
  );

  a.shutdown().await.expect("a shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// Concurrent `join_with` calls for the same seed address must use
/// CALL-SCOPED `ExchangeId` ownership: a waiter's `contacted` count
/// must only advance when its OWN dispatched exchange receives bytes,
/// never when a sibling call's overlapping exchange does. The
/// `dispatch_command` inline action drain captures each
/// `start_push_pull`'s freshly-allocated `ExchangeId` and binds it to
/// the dispatching waiter only.
#[compio::test]
async fn concurrent_join_with_same_seed_uses_call_scoped_eid_ownership() {
  let seed_addr = loopback_addr(7250);
  let j1_addr = loopback_addr(7251);
  let j2_addr = loopback_addr(7252);

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr),
    TcpOptions::new(Some(b"concurrent-join".to_vec())),
  )
  .await
  .expect("seed construct");

  let j1 = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("j1"), j1_addr),
    TcpOptions::new(Some(b"concurrent-join".to_vec())),
  )
  .await
  .expect("j1 construct");

  let j2 = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("j2"), j2_addr),
    TcpOptions::new(Some(b"concurrent-join".to_vec())),
  )
  .await
  .expect("j2 construct");

  // Two concurrent join_with calls to the same seed. Each must
  // resolve to Ok(1) — its own bridge's bytes — independently. With
  // address-scoped (broken) ownership, the FIRST bytes arriving on
  // either bridge would advance BOTH waiters and could let one
  // return Ok(N>1) or hide a failed sibling exchange behind its own
  // sibling's success.
  let seeds = [seed_addr];
  let fut1 = j1.join_with(&SocketAddrResolver, &seeds);
  let fut2 = j2.join_with(&SocketAddrResolver, &seeds);
  let (n1, n2) = futures_util::future::join(fut1, fut2).await;
  let n1 = n1.expect("j1 contacts seed");
  let n2 = n2.expect("j2 contacts seed");
  assert_eq!(n1, 1, "j1 reports only its own seed contact");
  assert_eq!(n2, 1, "j2 reports only its own seed contact");

  seed.shutdown().await.expect("seed shutdown");
  j1.shutdown().await.expect("j1 shutdown");
  j2.shutdown().await.expect("j2 shutdown");
}

/// A `join_with` waiting near its deadline must not lose a successful
/// exchange whose completion bytes are already queued in the driver's
/// bridge_inbound channel behind the iter-top drain cap. With many
/// concurrent in-flight exchanges (well above `ITER_DRAIN_CAP = 256`
/// total bridge-inbound messages), the past-due path must drain every
/// queued completion before calling `handle_timeout`; otherwise a
/// successful response sitting behind the cap would be marked overdue
/// and surface as `JoinAllFailed`.
#[compio::test]
async fn join_with_under_bridge_backlog_does_not_timeout_arrived_completions() {
  use std::collections::HashSet;

  let seed_addr = loopback_addr(7290);
  let joiner_addr = loopback_addr(7291);

  let seed = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("seed"), seed_addr),
    TcpOptions::new(Some(b"backlog-cap".to_vec())),
  )
  .await
  .expect("seed construct");

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"backlog-cap".to_vec())),
  )
  .await
  .expect("joiner construct");

  // Issue many concurrent join_with calls in parallel. Each succeeds
  // independently; with 64 of them the bridge byte-mover tasks
  // collectively produce hundreds of BridgeInbound messages — enough
  // to test the cap-then-deadline interaction. Every call must
  // return Ok(1) — none may surface JoinAllFailed.
  const N: usize = 64;
  let mut futures = Vec::with_capacity(N);
  for _ in 0..N {
    let joiner = joiner.clone();
    let seeds = [seed_addr];
    futures.push(async move {
      joiner
        .join_with(&SocketAddrResolver, &seeds)
        .await
        .expect("each parallel join_with must succeed")
    });
  }
  let counts: Vec<usize> = futures_util::future::join_all(futures).await;
  let unique_counts: HashSet<usize> = counts.iter().copied().collect();
  assert_eq!(
    unique_counts,
    HashSet::from([1usize]),
    "every parallel join_with must report Ok(1); got {counts:?}"
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// `join_with` must NOT count contact based on raw transport bytes
/// alone. A non-memberlist TCP service (e.g. an SSH banner-speaking
/// peer, a wrong-protocol endpoint) can accept the dial and write
/// bytes that pass through the per-bridge byte mover but fail the
/// frame decoder inside `endpoint.handle_transport_data`. The fix
/// gates the `contacted++` increment on the seed becoming `Alive` in
/// coordinator membership after the bytes are processed — banner
/// traffic never reaches that state.
#[compio::test]
async fn join_with_against_banner_tcp_service_surfaces_join_all_failed() {
  use compio::{io::AsyncWriteExt, net::TcpListener};

  let joiner_addr = loopback_addr(7270);
  let banner_addr = loopback_addr(7271);

  // Spawn a TCP listener that immediately writes a non-memberlist
  // banner on every accepted connection.
  let banner_listener = TcpListener::bind(banner_addr)
    .await
    .expect("banner listener bind");
  compio::runtime::spawn(async move {
    if let Ok((mut stream, _)) = banner_listener.accept().await {
      // Ignoring Err: best-effort write into a test fixture; if the
      // peer hangs up before we finish, the test still asserts the
      // bytes were not enough to count contact.
      let _ = stream.write_all(b"SSH-2.0-FAKE_BANNER\r\n").await;
    }
  })
  .detach();

  let joiner = TcpMemberlist::new(
    EndpointConfig::new(SmolStr::new("joiner"), joiner_addr),
    TcpOptions::new(Some(b"banner-trust".to_vec())),
  )
  .await
  .expect("joiner construct");

  let err = joiner
    .join_with(&SocketAddrResolver, &[banner_addr])
    .await
    .expect_err("join_with against banner service must not report contact");
  match err {
    MemberlistError::JoinAllFailed(payload) => {
      assert_eq!(payload.requested(), 1);
      assert_eq!(payload.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }

  joiner.shutdown().await.expect("joiner shutdown");
}
