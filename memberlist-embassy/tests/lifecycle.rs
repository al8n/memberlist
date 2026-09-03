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
use embassy_time::{Duration, Instant, Timer};
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

/// A pseudo-random, incompressible payload of `n` bytes (xorshift64), so the
/// framed reliable message is not shrunk below the peer's receive window by the
/// compression transform — the worker's established send is forced to stall
/// against a non-draining peer.
fn incompressible(n: usize) -> bytes::Bytes {
  let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
  let mut v = Vec::with_capacity(n);
  for _ in 0..n {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    v.push(x as u8);
  }
  bytes::Bytes::from(v)
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

/// A short engine close-timeout for the dead-on-link recovery tests. The worker
/// bounds each teardown RST flush to `close_timeout / 2` (see the worker's
/// `drain_teardown`), so a short close-timeout keeps a dead dial's slot recovering
/// quickly — well within the test budget — while its teardown still completes before
/// the engine's retiring deadline (`close_timeout`), keeping `teardown_overruns` at 0.
const FAST_CLOSE_TIMEOUT: core::time::Duration = core::time::Duration::from_millis(500);

/// Build one node with a short `stream_timeout` (so dead dials reap quickly) and the
/// given driver [`Options`].
fn node_with_opts<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  id: &str,
  last: u8,
  seed: u64,
  opts: Options,
) -> (Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>) {
  let (udp, tcp) = build_sockets(stack, bufs);
  // `SocketAddrResolver` resolves synchronously, so drive the now-async
  // constructor to completion inline — this helper stays sync.
  block_on(Memberlist::new_with_rng::<_, POOL>(
    opts,
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

/// Build one node with default driver options and a short `stream_timeout`.
fn node<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  id: &str,
  last: u8,
  seed: u64,
) -> (Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>) {
  node_with_opts(stack, bufs, id, last, seed, Options::new())
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

/// Repeated dial/abort churn must fully RECOVER the reliable pool, not merely avoid
/// leaking it. A dead ON-LINK dial whose ARP never resolves stalls in SYN-sent; the
/// exchange elapses at `stream_timeout` and the engine aborts + retires the slot. The
/// teardown RST likewise cannot egress, so the worker bounds its teardown flush with a
/// dedicated timer (`close_timeout / 2`) that frees the slot rather than pinning it.
/// After the churn quiesces every dial slot must be back in the free-list (nothing
/// left in the `retiring` ledger), the listener must self-heal, and no teardown may
/// have overrun the engine's retiring deadline (`teardown_overruns == 0`). A slot
/// reused before its worker reset would zombie-leak (absent from both free and
/// retiring); an unbounded teardown flush would pin a dead dial's slot in `retiring`
/// forever — both would fail the recovery assertions here.
#[test]
fn pool_does_not_wedge_under_dial_abort_churn() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node_with_opts(
    stack_a,
    &mut bufs_a,
    "a",
    1,
    1,
    Options::new().with_close_timeout(FAST_CLOSE_TIMEOUT),
  );
  let (_ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let free_at_start = ml_a.pool_free_count();
  assert!(
    free_at_start >= 1,
    "expected a non-empty pool at construction"
  );

  block_on(async {
    let op = async {
      // Hammer the pool: many waves of dead on-link dials, each exhausting and
      // recycling the dial sockets. The listener must also self-heal across the churn.
      for wave in 0..6u8 {
        churn_join(&ml_a, &[dead(100 + wave), dead(120 + wave)]).await;
        // Let the dead bridges elapse and their slots reap before the next wave.
        Timer::after(Duration::from_millis(120)).await;
      }
      // After the churn quiesces every dial slot must return to the free-list — none
      // left pinned in `retiring` — and the listener must still be present
      // (self-healed). A dial still mid-flight (a Dialing connection) or a teardown
      // still draining leaves the pool transiently short, so wait for it to settle.
      until(|| {
        ml_a.pool_free_count() >= free_at_start
          && ml_a.retiring_count() == 0
          && ml_a.listener_present()
      })
      .await;
      (
        ml_a.pool_free_count(),
        ml_a.retiring_count(),
        ml_a.teardown_overruns(),
        ml_a.listener_present(),
      )
    };
    let (free, retiring, overruns, listener) =
      drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert_eq!(
      free, free_at_start,
      "the reliable pool did not fully recover after the dial/abort churn: {free} free \
       of {free_at_start} dial slots (a slot reused before its worker reset, or an \
       unbounded teardown flush pinning a dead on-link dial, would strand it)"
    );
    assert_eq!(
      retiring, 0,
      "a slot was left pinned in the retiring ledger after the churn quiesced ({retiring})"
    );
    assert_eq!(
      overruns, 0,
      "a teardown overran the engine's retiring deadline during the churn ({overruns})"
    );
    assert!(
      listener,
      "the listener was not re-established after the churn"
    );
  });
}

/// A pre-trust peer that floods dials at DEAD on-link addresses (ARP never resolves,
/// so neither the SYN nor the teardown RST can egress) must not be able to
/// PERMANENTLY pin the reliable pool. Each such dial stalls in SYN-sent, the exchange
/// elapses at `stream_timeout`, and the engine aborts + retires the slot — but the
/// worker's teardown flush, which would otherwise wait for an RST that never egresses,
/// is bounded by a dedicated timer (`close_timeout / 2`). So every touched slot LEAVES
/// the retiring ledger within that bound, the free pool is restored, and — because the
/// bound is strictly below the engine's retiring deadline — no teardown overruns it
/// (`teardown_overruns == 0`). Without the bound each dead dial pins its slot forever
/// and the pool is exhausted after `POOL` dials — an unbounded remote slot-exhaustion
/// DoS — and this test hangs to the wall-clock cap.
#[test]
fn dead_on_link_dial_flood_recovers_every_slot() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node_with_opts(
    stack_a,
    &mut bufs_a,
    "a",
    1,
    1,
    Options::new().with_close_timeout(FAST_CLOSE_TIMEOUT),
  );
  let (_ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let free_at_start = ml_a.pool_free_count();
  assert!(
    free_at_start >= 1,
    "expected a non-empty pool at construction"
  );

  block_on(async {
    let op = async {
      // Saturate EVERY dial slot with dead on-link targets, then require the whole pool
      // to recover before the next wave — so each wave proves every dial slot both took
      // a dead teardown AND left the retiring ledger within the teardown bound.
      for wave in 0..4u8 {
        churn_join(
          &ml_a,
          &[dead(140 + wave), dead(150 + wave), dead(160 + wave)],
        )
        .await;
        until(|| ml_a.pool_free_count() >= free_at_start && ml_a.retiring_count() == 0).await;
        assert_eq!(
          ml_a.teardown_overruns(),
          0,
          "a teardown overran the engine's retiring deadline during wave {wave}"
        );
      }
      (
        ml_a.pool_free_count(),
        ml_a.retiring_count(),
        ml_a.teardown_overruns(),
      )
    };
    let (free, retiring, overruns) = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert_eq!(
      free, free_at_start,
      "the reliable pool did not fully recover after the dead on-link dial flood: \
       {free} free of {free_at_start} (an unbounded teardown flush pins each dead \
       dial's slot forever)"
    );
    assert_eq!(
      retiring, 0,
      "a slot was left pinned in the retiring ledger after the flood ({retiring})"
    );
    assert_eq!(
      overruns, 0,
      "a teardown overran the engine's retiring deadline during the flood ({overruns})"
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

/// An engine `Abort` must PREEMPT an in-flight ESTABLISHED send. A raw, non-reading
/// peer accepts A's reliable dial and then never drains: its receive window closes
/// while its stack keeps ACKing, so A's worker parks in an established `write`/`flush`
/// with the socket still "active" (the inactivity timeout is refreshed indefinitely).
/// When the exchange elapses at `stream_timeout` (300 ms) the engine posts `Abort`;
/// the worker must PREEMPT the parked send on that wake, reset the socket, and free
/// the slot — so the send resolves as an error and the pool recovers.
///
/// This is the load-bearing regression: before the fix the worker awaited the send
/// DIRECTLY (unraced), so a posted `Abort` was not observed until the far longer
/// per-socket inactivity timeout (15 s) — which exceeds this test's 5 s budget, so a
/// regression HANGS to the timeout rather than freeing the slot. Racing each
/// peer-dependent send against the command wake is what bounds it to one wake.
#[test]
fn abort_preempts_a_stalled_established_send() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);

  // A deliberately-small receive buffer for the non-reading peer: it fills fast (the
  // peer never reads), shutting the window so A's established send stalls well before
  // the payload drains.
  let mut peer_rx = [0u8; 512];
  let mut peer_tx = [0u8; 512];

  let free_a_at_start = ml_a.pool_free_count();
  block_on(async {
    // The non-reading peer: accept A's dial, then hold the socket open forever without
    // EVER reading. Its stack keeps ACKing (so A's socket stays active), but the
    // receive window stays shut — A's worker parks in the established send.
    let peer = async {
      let mut sock = TcpSocket::new(stack_b, &mut peer_rx, &mut peer_tx);
      sock
        .accept(7946)
        .await
        .expect("the raw peer accepts A's dial");
      core::future::pending::<()>().await;
    };

    let op = async {
      // Let the raw peer reach its `accept` before A dials it.
      Timer::after(Duration::from_millis(50)).await;
      // A large, incompressible payload guarantees the framed request exceeds the
      // peer's shut window, so the worker stalls in the WRITE/FLUSH (not merely the
      // reply read). The send must resolve as an error once the abort preempts it.
      let r = ml_a
        .send_reliable(addr(2, 7946), incompressible(16 * 1024))
        .await;
      assert!(
        r.is_err(),
        "a send to a non-draining peer must fail once the abort preempts it, not hang"
      );
      // The preempted slot resets and returns to the pool.
      until(|| ml_a.pool_free_count() >= free_a_at_start).await;
      ml_a.pool_free_count()
    };

    // Drive A's memberlist, both stacks, and the raw peer, raced against the op and
    // the wall-clock cap. (A bespoke drive: the peer is a raw socket, not a second
    // memberlist node, so the shared `drive` helper does not fit.)
    let infra = select(select(net_a.run(), net_b.run()), select(run_a.run(), peer));
    let free = match select(op, select(infra, Timer::after(TEST_TIMEOUT))).await {
      Either::First(v) => v,
      Either::Second(_) => panic!("abort_preempts_a_stalled_established_send timed out"),
    };
    assert_eq!(
      free, free_a_at_start,
      "A's reliable pool did not recover after the stalled send was preempted"
    );
  });
}

/// An ESTABLISHED abort whose RST cannot egress must recover the slot within ONE
/// teardown budget (`close_timeout / 2`), never two. The worker bounds the teardown
/// RST-egress flush exactly ONCE, in `reset_socket`; the established `Command::Abort`
/// arm therefore `abort()`s WITHOUT its own drain and relies on the caller's single
/// `reset_socket` drain. Were the abort arm to drain too, a non-egressing RST would
/// wait a full `close_timeout` (two `close_timeout / 2` bounds back to back) before the
/// slot resets — which the engine's retiring deadline (`now + close_timeout`, evaluated
/// on the pump BEFORE the workers) beats, ticking `teardown_overruns`.
///
/// A raw peer accepts A's reliable dial and never reads, so A's worker parks in an
/// established send with the peer's window shut. Once established, A's FRAMING gate is
/// closed (the device's `transmit` returns `None`), so the abort's RST — issued when
/// the exchange elapses at `stream_timeout` and the engine posts `Abort` — cannot be
/// framed and the teardown flush stalls, forcing the worker onto its dedicated teardown
/// timer (the real-world analog is an established peer whose neighbor entry has expired
/// and is now unreachable, so the RST cannot be framed). With the single drain the slot
/// frees within `close_timeout / 2` and no teardown overruns; a second drain in the
/// abort arm would double that to a full `close_timeout` and overrun the deadline.
///
/// This is the load-bearing regression for the single-drain invariant: it requires a
/// NON-egressing established-abort RST, which the `tx_gate` (deliver-then-drop, so
/// smoltcp counts the frame sent and the flush resolves) cannot produce — only the
/// `frame_gate` (a `None` transmit, so the frame stays pending and the flush stalls)
/// exposes the doubled budget.
#[test]
fn established_abort_with_non_egressing_rst_recovers() {
  let (dev_a, dev_b) = pair();
  // A cannot frame the abort's RST once this gate is closed (closed after the
  // connection establishes, so only the teardown RST — not the handshake — is stalled).
  let a_frame = dev_a.frame_gate();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  // A short close-timeout keeps the teardown bound (`close_timeout / 2`) well within
  // the test budget while still strictly below the engine's retiring deadline.
  let (ml_a, run_a) = node_with_opts(
    stack_a,
    &mut bufs_a,
    "a",
    1,
    1,
    Options::new().with_close_timeout(FAST_CLOSE_TIMEOUT),
  );

  // A deliberately-small receive buffer for the non-reading peer: it fills fast so A's
  // established send stalls (window shut) well before the payload drains, keeping the
  // exchange open until it elapses at `stream_timeout` and the engine aborts it.
  let mut peer_rx = [0u8; 512];
  let mut peer_tx = [0u8; 512];

  let free_a_at_start = ml_a.pool_free_count();
  block_on(async {
    // Set once the raw peer's `accept` resolves — a wire-confirmed handshake (A's dial
    // established) that the framing gate must not close before. A single-threaded `Cell`
    // shared by the peer task (writer) and the gate (reader) under one `block_on`.
    let peer_accepted = core::cell::Cell::new(false);
    // The non-reading peer: accept A's dial, then hold the socket open forever without
    // ever reading, so A's worker parks in the established send.
    let peer = async {
      let mut sock = TcpSocket::new(stack_b, &mut peer_rx, &mut peer_tx);
      sock
        .accept(7946)
        .await
        .expect("the raw peer accepts A's dial");
      // Signal the wire-confirmed handshake: A sent the final ACK (the peer received it),
      // so A's `connect` resolves with no further frames and its worker enters
      // `run_established`. The gate may now close to strand only the later abort's RST.
      peer_accepted.set(true);
      core::future::pending::<()>().await;
    };

    let op = async {
      // Let the raw peer reach its `accept` before A dials it.
      Timer::after(Duration::from_millis(50)).await;

      // Close A's framing gate only once the connection is CONFIRMED established, so the
      // stalled abort deterministically hits `run_established`'s established
      // `Command::Abort` arm (the single-drain site under test) rather than the
      // dial-preempt path — which is single-drain even on the pre-fix worker and would let
      // the buggy code false-pass. The raw peer's `accept` resolving proves the handshake
      // completed on the wire, so A's worker is (entering) `run_established`; closing the
      // gate here therefore strands only the later teardown RST, not the handshake. Bound
      // the wait and ASSERT establishment so a scheduling anomaly that never establishes
      // fails LOUDLY instead of silently taking the dialing path and passing. The bound is
      // well under `stream_timeout` (300 ms), so the gate always closes before the engine
      // posts `Abort`.
      let gate = async {
        let established = select(
          until(|| peer_accepted.get()),
          Timer::after(Duration::from_millis(200)),
        )
        .await;
        assert!(
          matches!(established, Either::First(())),
          "the raw peer never accepted A's dial within the establishment bound: the \
           reliable connection did not establish, so the abort could not reach \
           run_established's established Abort arm (the single-drain site under test)"
        );
        a_frame.set(false);
        core::future::pending::<()>().await
      };
      // A large, incompressible payload guarantees the framed request exceeds the peer's
      // shut window, so the worker is parked in the established send when the abort
      // arrives — it hits the `Command::Abort` arm, the single-drain site under test.
      let send = ml_a.send_reliable(addr(2, 7946), incompressible(16 * 1024));
      let r = match select(send, gate).await {
        Either::First(r) => r,
        Either::Second(()) => unreachable!("the framing-gate future never resolves"),
      };
      assert!(
        r.is_err(),
        "a send to a non-draining peer must fail once the abort preempts it, not hang"
      );

      // The abort has been issued (the send resolved at the exchange deadline). Time the
      // slot's recovery from here: a SINGLE teardown drain frees it within one budget
      // (`close_timeout / 2`); a double drain would take a full `close_timeout`.
      let aborted_at = Instant::now();
      until(|| ml_a.pool_free_count() >= free_a_at_start && ml_a.retiring_count() == 0).await;
      (
        aborted_at.elapsed(),
        ml_a.pool_free_count(),
        ml_a.retiring_count(),
        ml_a.teardown_overruns(),
      )
    };

    // A bespoke drive: the peer is a raw socket, not a second memberlist node, so the
    // shared `drive` helper does not fit.
    let infra = select(select(net_a.run(), net_b.run()), select(run_a.run(), peer));
    let (recovery, free, retiring, overruns) =
      match select(op, select(infra, Timer::after(TEST_TIMEOUT))).await {
        Either::First(v) => v,
        Either::Second(_) => {
          panic!("established_abort_with_non_egressing_rst_recovers timed out")
        }
      };
    assert_eq!(
      free, free_a_at_start,
      "A's reliable pool did not recover after the established abort ({free} of \
       {free_a_at_start})"
    );
    assert_eq!(
      retiring, 0,
      "a slot was left pinned in the retiring ledger after the established abort \
       ({retiring})"
    );
    assert_eq!(
      overruns, 0,
      "the established abort's teardown overran the engine's retiring deadline \
       ({overruns}) — a second drain in the abort arm doubles the teardown budget"
    );
    // One teardown budget is `close_timeout / 2` (250 ms); allow scheduling and the
    // 10 ms recovery-poll granularity, but stay well below a full `close_timeout`
    // (500 ms) so a double-drain regression (which needs the full close_timeout) also
    // fails this bound, not only the overrun assertion above.
    assert!(
      recovery < Duration::from_millis(400),
      "the slot took {recovery:?} to recover — beyond one teardown budget; a double \
       drain of the non-egressing RST would take a full close_timeout"
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

/// Distinct reliable-plane terminal paths each return their slot to the pool exactly
/// once. A graceful CLOSE (a completed exchange to a live peer) and a dial FAILURE (a
/// routable peer with no listener RSTs the SYN, so the slot resets cleanly), driven
/// repeatedly and interleaved, must each terminalize without leaking a slot (the free
/// count would stay BELOW construction) and without double-freeing one (it would rise
/// ABOVE construction). So the pool must recover to EXACTLY its construction count
/// after every permutation, and the node stays healthy (its listener self-heals,
/// membership holds) throughout.
#[test]
fn every_connect_close_abort_permutation_terminalizes() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);
  let (ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let free_at_start = ml_a.pool_free_count();
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

      for i in 0..3u8 {
        // CONNECT → graceful CLOSE: a live exchange completes and its slot reaps.
        ml_a
          .send_reliable(addr(2, 7946), bytes::Bytes::from_static(b"alive"))
          .await
          .expect("a reliable send to a live peer completes");
        until(|| ml_a.pool_free_count() >= free_at_start).await;
        assert_eq!(
          ml_a.pool_free_count(),
          free_at_start,
          "a graceful close neither leaked nor double-freed its slot (i={i})"
        );

        // CONNECT → dial FAILURE: a dial to a routable peer with no listener on the
        // port is RST'd, so the dial fails fast and its slot resets CLEANLY (the RST
        // egresses — B answers ARP — unlike an on-link black hole that pins teardown).
        assert!(
          ml_a
            .send_reliable(
              addr(2, 9000 + u16::from(i)),
              bytes::Bytes::from_static(b"nobody")
            )
            .await
            .is_err(),
          "a reliable send to a routable peer with no listener must fail (i={i})"
        );
        until(|| ml_a.pool_free_count() >= free_at_start).await;
        assert_eq!(
          ml_a.pool_free_count(),
          free_at_start,
          "a dial abort neither leaked nor double-freed its slot (i={i})"
        );
      }

      (
        ml_a.pool_free_count(),
        ml_a.num_members(),
        ml_a.listener_present(),
      )
    };
    let (free, members, listener) = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert_eq!(
      free, free_at_start,
      "the pool did not fully recover after the connect/close/abort permutations"
    );
    assert_eq!(members, 2, "membership must hold across the permutations");
    assert!(
      listener,
      "the listener must stay present across the permutations"
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

/// The guard above covers a join that STARTS after the leave. A join already
/// parked awaiting convergence needs the same answer: `leave()` keeps the member
/// list, so a waiter that consults membership alone when it resumes reports a
/// successful join for a node that is leaving — the very state the method's
/// opening guard rejects.
///
/// The wait is stepped by hand here (`poll` once to park it, `poll` again to
/// resume it) so the leave lands strictly between the two, with no runner able to
/// resolve the future in between. Membership is seeded through `inject_alive`, the
/// same public API the guard test above uses, rather than a live exchange: what
/// must be proven is the ORDER of the two checks when the wait resumes, and a
/// real join would have to win a race against the leave to reach that state.
#[test]
fn join_waiter_reports_not_running_when_leave_lands_before_it_resumes() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = single_node(stack, &mut bufs);

  block_on(async {
    // The node is not joined, so this makes its first offer and parks.
    let seeds = [MaybeResolved::Resolved(addr(2, 7946))];
    let mut joining = core::pin::pin!(ml.join(&SocketAddrResolver, &seeds));
    assert!(
      futures::poll!(joining.as_mut()).is_pending(),
      "the join must park awaiting convergence, not resolve on its first offer"
    );

    // While it is parked, both halves of the trap fall into place: a peer makes
    // `is_joined()` true, and another handle clone leaves without clearing it.
    ml.inject_alive(SmolStr::new("peer"), addr(2, 7946));
    assert!(
      ml.is_joined(),
      "the injected peer must make the node joined, or the test proves nothing"
    );
    ml.leave().expect("leave a running node");

    // Past the quarter-second re-offer interval the parked wait is due on both its
    // wake sources, so this poll runs the loop's checks.
    Timer::after(Duration::from_millis(400)).await;
    match futures::poll!(joining.as_mut()) {
      core::task::Poll::Ready(res) => {
        let err = res.expect_err("a parked join must not report success once the node has left");
        assert!(err.is_not_running(), "expected NotRunning, got {err:?}");
      }
      core::task::Poll::Pending => {
        panic!("the parked join did not resume after the re-offer interval elapsed")
      }
    }
  });
}
