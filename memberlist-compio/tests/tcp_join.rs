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

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Memberlist, MemberlistError, MemberlistOptions, Options,
  Resolver, SocketAddrResolver, TcpTransport, TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::typed::Meta;
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

/// Build a labelled `Memberlist` advertising `addr`. The driver-layer
/// membership address is `SocketAddr`, so every seed handed to `join`
/// arrives as a `MaybeResolved::Resolved`.
async fn make_tcp(id: &str, addr: SocketAddr, label: &[u8]) -> Memberlist<SmolStr, SocketAddr> {
  make_tcp_with(id, addr, label, MemberlistOptions::new()).await
}

/// Like [`make_tcp`] but with explicit SWIM-level [`MemberlistOptions`]
/// (gossip MTU, meta cap, initial meta).
async fn make_tcp_with(
  id: &str,
  addr: SocketAddr,
  label: &[u8],
  mopts: MemberlistOptions,
) -> Memberlist<SmolStr, SocketAddr> {
  let mopts = mopts
    .with_label(Some(label.to_vec()))
    .expect("valid label bytes");
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_memberlist(mopts);
  Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist")
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
  let seed = make_tcp("seed", loopback_addr(0), b"cluster-join").await;
  let joiner = make_tcp("joiner", loopback_addr(0), b"cluster-join").await;

  // Drive the join. The resolver is the identity pass-through since
  // the seed list already carries concrete SocketAddrs. `dispatch_join`
  // returns the dispatched count immediately; convergence is
  // observed via the snapshot below.
  let count = joiner
    .dispatch_join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(seed.advertise_address())],
    )
    .await
    .expect("dispatch_join");
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

/// `join_with` (the synchronous variant) returns the actually-reached
/// address set, not the dispatched count. Against a single reachable seed it
/// must return a 1-element set once `NodeJoined` for the seed lands — before
/// `JOIN_DEADLINE` elapses.
#[compio::test]
async fn join_with_waits_for_actual_contact() {
  let seed = make_tcp("seed", loopback_addr(0), b"cluster-join-sync").await;
  let joiner = make_tcp("joiner", loopback_addr(0), b"cluster-join-sync").await;

  let contacted = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(seed.advertise_address())],
    )
    .await
    .expect("join should succeed against a reachable seed");
  assert_eq!(contacted.len(), 1, "one seed contacted");
  assert_eq!(
    contacted[0],
    seed.advertise_address(),
    "the reached set carries the seed address"
  );

  // join returns when the seed is contacted; on return both sides
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

/// `join` against an unreachable seed must surface
/// `MemberlistError::JoinFailed { requested: 1, contacted: 0 }` (with an empty
/// reached set) once the deadline elapses. The dial loop fails fast against a
/// closed loopback port, so the call returns well before `JOIN_DEADLINE`.
#[compio::test]
async fn join_with_blackhole_surfaces_join_failed() {
  // 127.0.0.1 with a port that has nothing listening — connect()
  // returns ECONNREFUSED immediately on Linux/macOS loopback, so the
  // bridge enters its EOF/Error path and no NodeJoined ever fires.
  // This port is never bound by any node, so it cannot collide.
  let blackhole = loopback_addr(7213);

  let joiner = make_tcp("joiner", loopback_addr(0), b"cluster-join-fail").await;

  let (reached, err) = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(blackhole)])
    .await
    .expect_err("join against blackhole must fail");
  assert!(
    reached.is_empty(),
    "all-failed carries an empty reached set"
  );

  match err {
    MemberlistError::JoinFailed(payload) => {
      assert_eq!(payload.requested(), 1, "requested matches input seed count");
      assert_eq!(payload.contacted(), 0, "no contacts when seed is blackhole");
    }
    other => panic!("expected JoinFailed, got {other:?}"),
  }

  joiner.shutdown().await.expect("joiner shutdown");
}

/// A non-empty `seeds` slice whose resolver returns an empty address
/// vector must surface as `JoinFailed`, NOT a silent `Ok(empty)`. A
/// service-discovery resolver that finds no endpoints for a configured
/// service key is the canonical failure case this test guards against.
#[compio::test]
async fn join_with_empty_resolution_surfaces_join_failed() {
  // This joiner resolves `String` service keys via `EmptyResolver`, so its
  // membership-input address type is `String` (not the default `HostAddr`).
  let joiner: Memberlist<SmolStr, String> = Memberlist::new(
    Options::<TcpTransport<SmolStr, String>>::new(
      TcpTransportOptions::<SmolStr, String>::new()
        .with_local_id(SmolStr::new("joiner"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0))),
    )
    .with_memberlist(
      MemberlistOptions::new()
        .with_label(Some(b"cluster-empty-resolve".to_vec()))
        .expect("valid label"),
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
  let (reached, err) = joiner
    .join(&EmptyResolver, &seeds)
    .await
    .expect_err("non-empty seeds resolving to empty address list must fail");
  assert!(
    reached.is_empty(),
    "all-failed carries an empty reached set"
  );

  match err {
    MemberlistError::JoinFailed(payload) => {
      assert_eq!(
        payload.requested(),
        seeds.len(),
        "requested reflects input seed count"
      );
      assert_eq!(payload.contacted(), 0);
    }
    other => panic!("expected JoinFailed, got {other:?}"),
  }

  // Truly-empty input still short-circuits to Ok(empty): no command sent,
  // no JoinFailed.
  let empty: Vec<MaybeResolved<String, SocketAddr>> = Vec::new();
  let reached = joiner
    .join(&EmptyResolver, &empty)
    .await
    .expect("empty seeds returns Ok");
  assert!(reached.is_empty(), "empty input ⇒ empty reached set");

  joiner.shutdown().await.expect("joiner shutdown");
}

/// A node configured with `with_gossip_mtu` above the historical 16 KiB
/// recv buffer must actually receive datagrams that approach the
/// configured size. Paired with `with_meta_max_size` raised past the
/// 512-byte Go-memberlist default, an Alive broadcast carrying a
/// 20 KiB Meta surfaces over UDP gossip and the two nodes converge.
#[compio::test]
async fn join_with_above_16kib_gossip_mtu_receives_large_alive_broadcasts() {
  // 20 KiB meta — comfortably above 16 KiB (the old recv buffer cap)
  // and below the 32 KiB gossip_mtu configured below.
  let big_meta = Meta::try_from(vec![0x5au8; 20 * 1024]).expect("20 KiB within wire ceiling");

  let seed = make_tcp_with(
    "seed",
    loopback_addr(0),
    b"large-mtu",
    MemberlistOptions::new()
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(32 * 1024)
      .with_initial_meta(big_meta.clone()),
  )
  .await;

  let joiner = make_tcp_with(
    "joiner",
    loopback_addr(0),
    b"large-mtu",
    MemberlistOptions::new()
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(32 * 1024),
  )
  .await;

  let contacted = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(seed.advertise_address())],
    )
    .await
    .expect("join against 20 KiB-meta seed");
  assert_eq!(contacted.len(), 1);

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
  // 4 KiB meta — well past the joiner's default 512-byte cap.
  let big_meta = Meta::try_from(vec![0x42u8; 4096]).expect("4 KiB within wire ceiling");

  let seed = make_tcp_with(
    "seed",
    loopback_addr(0),
    b"meta-cap-mismatch",
    MemberlistOptions::new()
      .with_gossip_mtu(32 * 1024)
      .with_meta_max_size(8 * 1024)
      .with_initial_meta(big_meta.clone()),
  )
  .await;

  // Joiner keeps the default meta_max_size (512). It must still
  // accept the seed's 4 KiB meta and add the seed to membership.
  let joiner = make_tcp_with(
    "joiner",
    loopback_addr(0),
    b"meta-cap-mismatch",
    MemberlistOptions::new().with_gossip_mtu(32 * 1024),
  )
  .await;

  let contacted = joiner
    .join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(seed.advertise_address())],
    )
    .await
    .expect("join against larger-meta seed");
  assert_eq!(contacted.len(), 1);
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
/// the call returns, and the returned reached set has 2 entries (one per
/// successful exchange), not 1 (distinct addresses).
#[compio::test]
async fn join_with_counts_each_outbound_exchange_for_duplicate_seeds() {
  let seed = make_tcp("seed", loopback_addr(0), b"join-duplicate").await;
  let joiner = make_tcp("joiner", loopback_addr(0), b"join-duplicate").await;

  let seed_addr = seed.advertise_address();
  let contacted = joiner
    .join(
      &SocketAddrResolver,
      &[
        MaybeResolved::Resolved(seed_addr),
        MaybeResolved::Resolved(seed_addr),
      ],
    )
    .await
    .expect("join with duplicate seeds should succeed");
  assert_eq!(
    contacted.len(),
    2,
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
  let c = make_tcp("c", loopback_addr(0), b"transitive-discover").await;
  let a = make_tcp("a", loopback_addr(0), b"transitive-discover").await;
  // Bind the joiner NOW, while C still holds its port, so a later `:0` bind can
  // never be handed C's freed address after C is dropped — which would point the
  // "closed transitive seed" at the live joiner and pollute the reached set.
  let joiner = make_tcp("joiner", loopback_addr(0), b"transitive-discover").await;

  // Capture C's bound address before C is dropped — the joiner below
  // still lists it as a seed (a now-closed port) to exercise the
  // transitive-discovery path.
  let a_addr = a.advertise_address();
  let c_addr = c.advertise_address();

  // A joins C directly so A's membership records C as Alive.
  let n = a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(c_addr)])
    .await
    .expect("a joins c");
  assert_eq!(n.len(), 1, "A directly contacted C");
  let converged = wait_until(|| a.member_count() == 2, Duration::from_secs(2)).await;
  assert!(converged, "A did not learn C");

  // Drop C without `shutdown()`: no Leave broadcast, so A's view of
  // C stays Alive until A's next probe of C times out (far longer
  // than this test's window). C's TCP listener closes when the
  // driver task is cancelled by the last-handle drop.
  drop(c);
  compio::time::sleep(Duration::from_millis(50)).await;

  let contacted = joiner
    .join(
      &SocketAddrResolver,
      &[
        MaybeResolved::Resolved(a_addr),
        MaybeResolved::Resolved(c_addr),
      ],
    )
    .await
    .expect("joiner contacts A even if C is unreachable");
  assert_eq!(
    contacted.len(),
    1,
    "join counted a transitively-discovered seed as contacted"
  );
  assert_eq!(contacted[0], a_addr, "only A is in the reached set");

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
  let seed = make_tcp("seed", loopback_addr(0), b"concurrent-join").await;
  let j1 = make_tcp("j1", loopback_addr(0), b"concurrent-join").await;
  let j2 = make_tcp("j2", loopback_addr(0), b"concurrent-join").await;

  let seed_addr = seed.advertise_address();

  // Two concurrent join calls to the same seed. Each must resolve to
  // Ok(1) — its own bridge's bytes — independently. With
  // address-scoped (broken) ownership, the FIRST bytes arriving on
  // either bridge would advance BOTH waiters and could let one
  // return Ok(N>1) or hide a failed sibling exchange behind its own
  // sibling's success.
  let seeds = [MaybeResolved::Resolved(seed_addr)];
  let fut1 = j1.join(&SocketAddrResolver, &seeds);
  let fut2 = j2.join(&SocketAddrResolver, &seeds);
  let (n1, n2) = futures_util::future::join(fut1, fut2).await;
  let n1 = n1.expect("j1 contacts seed");
  let n2 = n2.expect("j2 contacts seed");
  assert_eq!(n1.len(), 1, "j1 reports only its own seed contact");
  assert_eq!(n2.len(), 1, "j2 reports only its own seed contact");

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
/// and surface as `JoinFailed`.
#[compio::test]
async fn join_with_under_bridge_backlog_does_not_timeout_arrived_completions() {
  use std::collections::HashSet;

  let seed = make_tcp("seed", loopback_addr(0), b"backlog-cap").await;
  let joiner = make_tcp("joiner", loopback_addr(0), b"backlog-cap").await;

  let seed_addr = seed.advertise_address();

  // Issue many concurrent join calls in parallel. Each succeeds
  // independently; with 64 of them the bridge byte-mover tasks
  // collectively produce hundreds of BridgeInbound messages — enough
  // to test the cap-then-deadline interaction. Every call must
  // return a 1-element reached set — none may surface JoinFailed.
  const N: usize = 64;
  let mut futures = Vec::with_capacity(N);
  for _ in 0..N {
    let joiner = joiner.clone();
    let seeds = [MaybeResolved::Resolved(seed_addr)];
    futures.push(async move {
      joiner
        .join(&SocketAddrResolver, &seeds)
        .await
        .expect("each parallel join must succeed")
        .len()
    });
  }
  let counts: Vec<usize> = futures_util::future::join_all(futures).await;
  let unique_counts: HashSet<usize> = counts.iter().copied().collect();
  assert_eq!(
    unique_counts,
    HashSet::from([1usize]),
    "every parallel join must report a 1-element reached set; got {counts:?}"
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
async fn join_with_against_banner_tcp_service_surfaces_join_failed() {
  use compio::{io::AsyncWriteExt, net::TcpListener};

  // Spawn a TCP listener that immediately writes a non-memberlist
  // banner on every accepted connection. Bind `:0` and read back the
  // OS-assigned address so the joiner dials the real port.
  let banner_listener = TcpListener::bind(loopback_addr(0))
    .await
    .expect("banner listener bind");
  let banner_addr = banner_listener
    .local_addr()
    .expect("banner listener local addr");
  compio::runtime::spawn(async move {
    if let Ok((mut stream, _)) = banner_listener.accept().await {
      // Ignoring Err: best-effort write into a test fixture; if the
      // peer hangs up before we finish, the test still asserts the
      // bytes were not enough to count contact.
      let _ = stream.write_all(b"SSH-2.0-FAKE_BANNER\r\n").await;
    }
  })
  .detach();

  let joiner = make_tcp("joiner", loopback_addr(0), b"banner-trust").await;

  let (reached, err) = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(banner_addr)])
    .await
    .expect_err("join against banner service must not report contact");
  assert!(
    reached.is_empty(),
    "all-failed carries an empty reached set"
  );
  match err {
    MemberlistError::JoinFailed(payload) => {
      assert_eq!(payload.requested(), 1);
      assert_eq!(payload.contacted(), 0);
    }
    other => panic!("expected JoinFailed, got {other:?}"),
  }

  joiner.shutdown().await.expect("joiner shutdown");
}
