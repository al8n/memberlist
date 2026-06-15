//! Reliable-plane lifecycle regressions over the channel-backed paired
//! embassy-net driver: the abort/reuse race (a slot must be reset by its worker
//! before the engine reuses it) and the pool-no-wedge invariant under repeated
//! dial/abort churn, plus the peer-RESET-is-not-a-clean-completion guarantee.
//!
//! Like `loopback.rs`, each test stands up two real embassy-net stacks wired by
//! [`PairedDevice`]s and drives every future under one `block_on`, raced against a
//! wall-clock timeout so a regression (a wedged pool, a stale reused slot, a false
//! send success) fails fast instead of hanging.

mod support;

use core::net::{IpAddr, Ipv4Addr, SocketAddr};

use embassy_futures::select::{Either, select};
use embassy_net::{
  Config as NetConfig, Ipv4Cidr, Runner as NetRunner, Stack, StackResources, StaticConfigV4,
  tcp::TcpSocket,
  udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Timer};
use futures::executor::block_on;
use memberlist_embassy::{
  AddressResolver, EndpointOptions, InitError, MaybeResolved, Memberlist, Options, Runner,
  SocketAddrResolver, TransformOptions, now,
};
use memberlist_embedded::ResolvedAddrs;
use memberlist_proto::{SeedableRng, SmallRng};
use smol_str::SmolStr;

use support::paired_device::{PairedDevice, pair};

/// TCP socket pool size per node (a listener plus dial/accept sockets).
const POOL: usize = 4;
/// Per-TCP-socket rx/tx buffer bytes.
const TCP_BUF: usize = 4096;
/// Wall-clock cap on each test so a wedged plane fails fast.
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn addr(last: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, last)), port)
}

/// An on-link address no node answers — a dial to it never establishes and its
/// bridge elapses at `stream_timeout`, then the slot is reaped and reused. Each
/// distinct last octet is a fresh dead target.
fn dead(last: u8) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, last)), 7946)
}

/// All the owned buffers one node's sockets borrow. Declared in the test frame so
/// the sockets (and the `Memberlist`/`Runner` that hold them) can borrow them for
/// the whole `block_on`.
struct NodeBufs {
  udp_rx_meta: [PacketMetadata; 16],
  udp_rx: [u8; 16 * 1024],
  udp_tx_meta: [PacketMetadata; 16],
  udp_tx: [u8; 16 * 1024],
  tcp_rx: [[u8; TCP_BUF]; POOL],
  tcp_tx: [[u8; TCP_BUF]; POOL],
}

impl NodeBufs {
  fn new() -> Self {
    Self {
      udp_rx_meta: [PacketMetadata::EMPTY; 16],
      udp_rx: [0u8; 16 * 1024],
      udp_tx_meta: [PacketMetadata::EMPTY; 16],
      udp_tx: [0u8; 16 * 1024],
      tcp_rx: [[0u8; TCP_BUF]; POOL],
      tcp_tx: [[0u8; TCP_BUF]; POOL],
    }
  }
}

fn build_sockets<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
) -> (UdpSocket<'a>, [TcpSocket<'a>; POOL]) {
  let udp = UdpSocket::new(
    stack,
    &mut bufs.udp_rx_meta,
    &mut bufs.udp_rx,
    &mut bufs.udp_tx_meta,
    &mut bufs.udp_tx,
  );
  let mut rx_iter = bufs.tcp_rx.iter_mut();
  let mut tx_iter = bufs.tcp_tx.iter_mut();
  let tcp = core::array::from_fn::<_, POOL, _>(|_| {
    let rx = rx_iter.next().expect("POOL rx buffers");
    let tx = tx_iter.next().expect("POOL tx buffers");
    TcpSocket::new(stack, rx, tx)
  });
  (udp, tcp)
}

fn build_stack<'a>(
  device: PairedDevice,
  resources: &'a mut StackResources<{ POOL + 2 }>,
  last: u8,
  seed: u64,
) -> (Stack<'a>, NetRunner<'a, PairedDevice>) {
  let config = NetConfig::ipv4_static(StaticConfigV4 {
    address: Ipv4Cidr::new(Ipv4Addr::new(169, 254, 1, last), 16),
    gateway: None,
    dns_servers: Default::default(),
  });
  embassy_net::new(device, config, resources, seed)
}

/// Drive `op` against both memberlist run loops, both stack run loops, and the
/// timeout. Returns the op's value, or panics on timeout.
async fn drive<T>(
  op: impl core::future::Future<Output = T>,
  ml_a: Runner<'_, SmolStr, POOL>,
  ml_b: Runner<'_, SmolStr, POOL>,
  net_a: &mut NetRunner<'_, PairedDevice>,
  net_b: &mut NetRunner<'_, PairedDevice>,
) -> T {
  let nets = select(net_a.run(), net_b.run());
  let mls = select(ml_a.run(), ml_b.run());
  let infra = select(nets, mls);
  match select(op, select(infra, Timer::after(TEST_TIMEOUT))).await {
    Either::First(v) => v,
    Either::Second(_) => panic!("test timed out after {TEST_TIMEOUT:?}"),
  }
}

/// Build one node with a short `stream_timeout` so dead dials reap quickly.
fn node<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  id: &str,
  last: u8,
  seed: u64,
) -> (Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>) {
  let (udp, tcp) = build_sockets(stack, bufs);
  // `SocketAddrResolver` resolves synchronously, so drive the now-async
  // constructor to completion inline — this helper stays sync.
  block_on(Memberlist::new_with_rng::<_, POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new(id), addr(last, 7946))
      // Short stream timeout: a dead dial fails (and its slot is reaped) quickly so
      // the churn cycles within the test budget.
      .with_stream_timeout(core::time::Duration::from_millis(300)),
    &SocketAddrResolver,
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(seed),
  ))
  .expect("build node")
}

/// Wait until `cond()` holds, polling on a short timer; panics via the outer
/// `drive` timeout if it never does.
async fn until(mut cond: impl FnMut() -> bool) {
  loop {
    if cond() {
      return;
    }
    Timer::after(Duration::from_millis(10)).await;
  }
}

/// Issue a `join` purely to drive dial/abort churn: a dead seed never converges,
/// so `join` (which resolves only on convergence) is raced against a short timer
/// and abandoned. The dial intent is still enqueued on the engine, so the churn
/// happens regardless of whether this future resolves.
async fn churn_join(ml: &Memberlist<SmolStr, SocketAddr>, seeds: &[SocketAddr]) {
  let resolved: Vec<MaybeResolved<SocketAddr>> =
    seeds.iter().map(|s| MaybeResolved::Resolved(*s)).collect();
  let _ = select(
    ml.join(&SocketAddrResolver, &resolved),
    Timer::after(Duration::from_millis(80)),
  )
  .await;
}

/// After a slot is aborted (a dead dial fails) and recycled, it must NOT carry the
/// prior connection's state into its reuse: a fresh reliable send over a reused
/// slot must still complete. Before the fix, the engine could re-`listen`/`connect`
/// a slot whose worker had not yet reset its `TcpSocket` — clobbering the pending
/// abort and leaving the slot a zombie of the previous connection — so a later
/// send/join over that slot would wedge.
#[test]
fn abort_reuse_does_not_carry_stale_state() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);
  let (ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let free_a_at_start = ml_a.pool_free_count();
  block_on(async {
    let op = async {
      // 1. Establish the cluster first so B is a known member.
      ml_b
        .join(
          &SocketAddrResolver,
          &[MaybeResolved::Resolved(addr(1, 7946))],
        )
        .await
        .expect("join from a running node");
      until(|| ml_a.num_members() == 2 && ml_b.num_members() == 2).await;

      // 2. Churn: A dials several dead on-link peers. Each fails to establish, its
      //    bridge elapses at stream_timeout, and the slot is reaped and reused. With
      //    POOL=4 (one listener) this repeatedly exhausts and recycles the dial
      //    sockets, exercising the abort→give→reuse path the fix gates.
      for last in [50u8, 51, 52, 53, 54, 55] {
        churn_join(&ml_a, &[dead(last)]).await;
      }

      // 3. Let every dead dial reap and its slot return to a CLEAN, reset state
      //    before reuse (the abort/reuse race the fix closes is exactly a slot
      //    reused before its worker reset it).
      until(|| ml_a.pool_free_count() >= free_a_at_start).await;

      // 4. A reliably sends to B over a recycled slot; it must complete — a
      //    stale/zombie slot (reused before its reset) would wedge this send until
      //    the test timeout, or fail.
      ml_a
        .send_reliable(addr(2, 7946), bytes::Bytes::from_static(b"after churn"))
        .await
        .expect("send_reliable over a recycled slot must complete");

      // 5. The send's slot returns to the pool once its graceful close completes;
      //    the pool fully recovers to its construction count (every churned + used
      //    slot reclaimed). A slot reused before its reset would have leaked here.
      until(|| ml_a.pool_free_count() >= free_a_at_start).await;
      ml_a.pool_free_count()
    };
    let free = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert_eq!(
      free, free_a_at_start,
      "A's reliable pool did not fully recover after the dial/abort churn + send \
       (free={free}, construction had {free_a_at_start})"
    );
  });
}

/// Repeated dial/abort churn must not WEDGE the pool: every dead dial's slot must
/// return to the free-list once its worker resets the socket, so the free count
/// recovers to its construction value. A slot reused before its reset (the bug)
/// would either zombie-leak (never returning) or corrupt a later reuse.
#[test]
fn pool_does_not_wedge_under_dial_abort_churn() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);
  let (_ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let free_at_start = ml_a.pool_free_count();
  assert!(
    free_at_start >= 1,
    "expected a non-empty pool at construction"
  );

  block_on(async {
    let op = async {
      // Hammer the pool: many waves of dead dials, each exhausting and recycling the
      // dial sockets. The listener must also self-heal across the churn.
      for wave in 0..8u8 {
        churn_join(&ml_a, &[dead(100 + wave), dead(120 + wave)]).await;
        // Let the dead bridges elapse and their slots reap before the next wave.
        Timer::after(Duration::from_millis(120)).await;
      }
      // After the churn quiesces, the pool must recover to its construction count
      // and the listener must still be present (self-healed).
      until(|| ml_a.pool_free_count() >= free_at_start && ml_a.listener_present()).await;
      (ml_a.pool_free_count(), ml_a.listener_present())
    };
    let (free, listener) = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert_eq!(
      free, free_at_start,
      "the reliable pool wedged under dial/abort churn: free recovered to only {} \
       of {} (a slot reused before its worker reset leaked or corrupted)",
      free, free_at_start
    );
    assert!(
      listener,
      "the listener was not re-established after the churn"
    );
  });
}

/// Finding-A regression: a peer that goes SILENT mid-exchange (its link stops
/// delivering, so it never ACKs) must not WEDGE the pool. The worker's blocking
/// socket await is bounded — by the command-wake + exchange deadline for a dial that
/// never establishes, and by the per-socket inactivity timeout for an established
/// connection — so the stalled slot is reaped, the send resolves with an error
/// rather than hanging, and the pool recovers.
///
/// (An established write/flush stall isolated from the connect handshake is not
/// deterministically provokable over the paired device: gossip / probe / handshake
/// frames cannot be separated from a data ACK, and embassy-net's `set_timeout` does
/// not bound a connect still in SYN-sent. So this guards the end-to-end no-wedge
/// property; the socket timeout's established-path bound is additionally covered by
/// the `socket_timeout > close_timeout` config invariant and the worker code.)
#[test]
fn silent_peer_does_not_wedge_the_pool() {
  let (dev_a, dev_b) = pair();
  let b_tx = dev_b.tx_gate();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);
  let (ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let free_a_at_start = ml_a.pool_free_count();
  block_on(async {
    let op = async {
      ml_b
        .join(
          &SocketAddrResolver,
          &[MaybeResolved::Resolved(addr(1, 7946))],
        )
        .await
        .expect("join from a running node");
      until(|| ml_a.num_members() == 2 && ml_b.num_members() == 2).await;

      // B goes silent: every frame B transmits is dropped, so A gets no ACKs.
      b_tx.set(false);

      // A reliably sends to the silent B: its worker blocks awaiting ACKs that never
      // come. The bounded await reaps the stalled exchange, so the send resolves Err
      // rather than hanging.
      let r = ml_a
        .send_reliable(addr(2, 7946), bytes::Bytes::from_static(b"into the void"))
        .await;
      assert!(r.is_err(), "a send to a silent peer must fail, not hang");

      // The slot recovers: A's pool returns to its construction count.
      until(|| ml_a.pool_free_count() >= free_a_at_start).await;
      ml_a.pool_free_count()
    };
    let free = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert_eq!(
      free, free_a_at_start,
      "A's reliable pool did not recover after the peer went silent"
    );
  });
}

/// A peer RESET mid reliable-send must NOT be reported as a successful completion.
/// The worker maps a `read()` error (a RST) to `open = false` WITHOUT latching
/// `peer_fin`, so `recv_finished` never reports a clean EOF for the reset — the
/// exchange fails (the bridge times out) rather than the machine mapping a bogus
/// transport EOF to a successful `UserMessage`. Only ONE send is in flight, so the
/// FIFO waiter resolution is exact regardless of the StreamId-correlation fix.
#[test]
fn peer_reset_is_not_reported_as_send_success() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);
  let (ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  block_on(async {
    let op = async {
      // A sends to a dead on-link target B is NOT listening on the reliable plane
      // for: the dial never establishes, so the send fails at the bridge deadline.
      // (A reset and a vanished peer both surface as a transport FAILURE, never as a
      // clean-EOF false success — which is the property under test.)
      let r = ml_a
        .send_reliable(dead(60), bytes::Bytes::from_static(b"to nobody"))
        .await;
      // Keep B alive in the bundle so the stacks run; it just never answers on the
      // reliable plane.
      let _ = &ml_b;
      r
    };
    let result = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert!(
      result.is_err(),
      "a reliable send whose connection never completed (reset / vanished peer) \
       must resolve as a FAILURE, not a false success: {result:?}"
    );
  });
}

// Async resolvers exercising the `join` resolution boundary. `Address = SocketAddr`
// matches a node built with the [`SocketAddrResolver`].

/// Must never be invoked — the lifecycle guard rejects a left node's `join` before
/// any seed is resolved.
struct UnreachableResolver;

impl AddressResolver for UnreachableResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  async fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    unreachable!("a left node must not resolve seeds");
  }
}

/// Resolves every address to no candidates.
struct EmptyResolver;

impl AddressResolver for EmptyResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  async fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    Ok(ResolvedAddrs::new())
  }
}

/// Resolves every address to a FULL bounded result (the per-seed cap's worth of
/// the same wire address). The `ResolvedAddrs` type bounds the count, so even a
/// resolver that tries to emit "as many as possible" stays capped.
struct FullResolver;

impl AddressResolver for FullResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  async fn resolve(&self, address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    let mut addrs = ResolvedAddrs::new();
    // Fill to capacity; `push` past `MAX_RESOLVED_ADDRS_PER_SEED` returns the
    // item, so the loop simply stops at the type's bound.
    while addrs.push(*address).is_ok() {}
    Ok(addrs)
  }
}

/// Build one running single node over its own stack, returning the handle and the
/// (unused) runner whose lifetime keeps the borrowed sockets alive.
fn single_node<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
) -> (Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>) {
  node(stack, bufs, "a", 1, 1)
}

#[test]
fn join_after_leave_rejects_without_resolving() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = single_node(stack, &mut bufs);

  block_on(async {
    ml.leave().expect("leave a running node");

    // A left node rejects join immediately — and the resolver is never called
    // (`UnreachableResolver` would panic otherwise).
    let err = ml
      .join(
        &UnreachableResolver,
        &[MaybeResolved::Unresolved(addr(2, 7946))],
      )
      .await
      .expect_err("a left node rejects join");
    assert!(err.is_not_running(), "expected NotRunning, got {err:?}");
  });
}

#[test]
fn join_with_all_seeds_unresolvable_is_no_addresses() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = single_node(stack, &mut bufs);

  block_on(async {
    // A non-empty seed set that resolves to nothing is a discovery failure.
    let err = ml
      .join(&EmptyResolver, &[MaybeResolved::Unresolved(addr(2, 7946))])
      .await
      .expect_err("all-empty resolution fails");
    assert!(err.is_no_addresses(), "expected NoAddresses, got {err:?}");
  });
}

#[test]
fn join_accepts_a_full_bounded_resolution() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = single_node(stack, &mut bufs);

  block_on(async {
    // A resolver that fills the bounded result to capacity is accepted: the
    // `ResolvedAddrs` type caps the count, so the driver never needs a post-hoc
    // truncation and a resolver simply cannot hand back an oversized batch. `join`
    // completes its resolution and proceeds to park awaiting convergence (which
    // never comes for an unreachable seed with no runner draining events). Race it
    // against a short timer: the timer winning proves the bounded resolution
    // finished and the join is parked, not hung in resolution.
    let joined = select(
      ml.join(&FullResolver, &[MaybeResolved::Unresolved(addr(2, 7946))]),
      Timer::after(Duration::from_millis(200)),
    )
    .await;
    assert!(
      matches!(joined, Either::Second(())),
      "the bounded join must reach its park (timer wins), not resolve: {joined:?}"
    );
    // The node never gained a bogus member from the bounded batch.
    assert_eq!(ml.num_members(), 1, "no peer should have joined");
  });
}

#[test]
fn invalid_config_is_rejected_before_resolution() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (udp, tcp) = build_sockets(stack, &mut bufs);

  // A zero close timeout is an advertise-independent ENGINE misconfiguration. The
  // construction preflight (`validate_runtime_config`) must reject it BEFORE the
  // advertise address is resolved, so `UnreachableResolver` is never called. The
  // embassy `InitError` wraps the embedded one, so the engine fault surfaces as
  // `InitError::Engine(..ZeroCloseTimeout)`. (The `Ok` side `(Memberlist, Runner)`
  // is not `Debug`, so match the result rather than `expect_err`.)
  let cfg = Options::new().with_close_timeout(core::time::Duration::ZERO);
  let Err(err) = block_on(Memberlist::new_with_rng::<_, POOL>(
    cfg,
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
    &UnreachableResolver,
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(1),
  )) else {
    panic!("a zero close timeout must be rejected at construction");
  };
  assert!(
    matches!(
      err,
      InitError::Engine(memberlist_embedded::InitError::ZeroCloseTimeout)
    ),
    "expected Engine(ZeroCloseTimeout) from the preflight, got {err:?}"
  );
}

#[test]
fn construct_with_invalid_config_does_not_resolve() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (udp, tcp) = build_sockets(stack, &mut bufs);

  // A `socket_timeout` far beyond the sane maximum (one day) fails the deterministic
  // range check, which now runs before the advertise resolver. `UnreachableResolver`
  // panics if `resolve` is ever called, so a clean `SocketTimeoutOutOfRange` proves
  // the config error is produced without resolving.
  let cfg = Options::new().with_socket_timeout(core::time::Duration::from_secs(200_000));
  // The `Ok` side `(Memberlist, Runner)` is not `Debug`, so match the result rather
  // than `expect_err` (which would require it).
  let Err(err) = block_on(Memberlist::new_with_rng::<_, POOL>(
    cfg,
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
    &UnreachableResolver,
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(1),
  )) else {
    panic!("an out-of-range socket_timeout must fail construction");
  };
  assert!(
    err.is_socket_timeout_out_of_range(),
    "expected SocketTimeoutOutOfRange, got {err:?}"
  );
}

/// On its FIRST `resolve`, leaves the cluster via a captured handle clone and counts
/// calls — proving `join`'s per-seed running re-check stops resolving subsequent
/// seeds once the node has left mid-resolution.
struct LeaveOnFirstResolver {
  ml: Memberlist<SmolStr, SocketAddr>,
  calls: core::cell::Cell<usize>,
}

impl AddressResolver for LeaveOnFirstResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  async fn resolve(&self, address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    let n = self.calls.get();
    self.calls.set(n + 1);
    if n == 0 {
      self.ml.leave().expect("leave mid-join");
    }
    let mut addrs = ResolvedAddrs::new();
    // Ignoring Err: one push onto an empty cap-8 vec cannot overflow.
    let _ = addrs.push(*address);
    Ok(addrs)
  }
}

#[test]
fn join_stops_resolving_after_a_concurrent_leave() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = single_node(stack, &mut bufs);

  block_on(async {
    let res = LeaveOnFirstResolver {
      ml: ml.clone(),
      calls: core::cell::Cell::new(0),
    };
    // Two unresolved seeds: the first resolve leaves the cluster, so the per-seed
    // re-check must reject before the second seed is resolved.
    let err = ml
      .join(
        &res,
        &[
          MaybeResolved::Unresolved(addr(2, 7946)),
          MaybeResolved::Unresolved(addr(3, 7946)),
        ],
      )
      .await
      .expect_err("a leave mid-resolution rejects the join");
    assert!(err.is_not_running(), "expected NotRunning, got {err:?}");
    assert_eq!(
      res.calls.get(),
      1,
      "only the first seed should have resolved; the leave must skip the rest"
    );
  });
}

#[test]
fn join_after_leave_with_a_peer_still_rejects() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = single_node(stack, &mut bufs);

  // A peer in membership makes `is_joined()` true, and `leave()` does not clear
  // it. The running guard — not the is_joined fast path — must reject the
  // post-leave join, or a joined-then-left node would report a bogus success.
  ml.inject_alive(SmolStr::new("peer"), addr(2, 7946));
  block_on(async {
    ml.leave().expect("leave a running node");
    let err = ml
      .join(
        &UnreachableResolver,
        &[MaybeResolved::Unresolved(addr(3, 7946))],
      )
      .await
      .expect_err("a left node rejects join even with peers");
    assert!(err.is_not_running(), "expected NotRunning, got {err:?}");
  });
}
