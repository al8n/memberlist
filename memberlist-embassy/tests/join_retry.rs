//! A join whose seed dial loses the peer's reliable-listener race must recover on
//! its own, without the caller restarting it.
//!
//! A node accepts inbound reliable connections on ONE listener slot at a time, and
//! re-arms it only from whatever the pool has free at that instant. A node whose
//! pool is momentarily exhausted therefore has no listening socket at all, and its
//! stack RSTs the next SYN. The engine deliberately does NOT suppress a join seed
//! against an unrelated connection to the same address, so a seed dial can be the
//! one that loses that race: its exchange fails and is forgotten, leaving the
//! caller's join with nothing outstanding.
//!
//! The engine's contract is that the caller re-offers its seeds until joined, and
//! [`Memberlist::join`] is that caller. The first test drives the whole shape over
//! two real embassy-net stacks: B loses its listener to two unrelated inbound
//! connections, A's join dial is RST, the connections release, and the ORIGINAL
//! join future — never restarted — completes.
//!
//! The second covers the other half of that retry loop: two joins running at once
//! offer the union of their seeds, so the reachable peer is reached over the wire
//! even when only one of the two calls holds its address and the other's seeds
//! contend for the same single-slot seed queue.
//!
//! The third pins the COST of that retry. Re-offering is node-wide — one offer per
//! interval carrying every live join's seeds — so tripling the number of live joins
//! naming one fast-failing seed must not triple how often it is dialed.

// nested `if let X = ev { if cond }` kept for readability, as in the crate roots.
#![allow(clippy::collapsible_if)]

mod support;

use core::net::{IpAddr, Ipv4Addr, SocketAddr};

use embassy_futures::{
  join::join,
  select::{Either, select},
};
use embassy_net::{
  Config as NetConfig, IpEndpoint, Ipv4Cidr, Runner as NetRunner, Stack, StackResources,
  StaticConfigV4,
  tcp::TcpSocket,
  udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Instant, Timer};
use futures::executor::block_on;
use memberlist_embassy::{
  EndpointOptions, MaybeResolved, Memberlist, Options, Runner, SocketAddrResolver,
  TransformOptions, now,
};
use memberlist_proto::{
  SeedableRng, SmallRng,
  event::{Event, ExchangeKind},
};
use smol_str::SmolStr;

use support::paired_device::{PairedDevice, pair};

/// A's TCP pool: enough slots that A always has one free to dial B with, so the
/// only contended resource in the test is B's listener.
const POOL_A: usize = 4;
/// B's TCP pool: the functional minimum the driver accepts — one listener plus one
/// accept/dial slot. Two concurrent inbound connections consume both, so B has
/// nothing left to re-arm its listener from and its stack RSTs the next SYN.
const POOL_B: usize = 2;
/// Per-TCP-socket rx/tx buffer bytes.
const TCP_BUF: usize = 4096;
/// The reliable/gossip port both nodes bind.
const PORT: u16 = 7946;
/// Wall-clock cap on the test so a wedged plane fails fast. Larger than the
/// loopback suite's because the recovery deliberately waits out a re-offer
/// interval plus B's teardown of the two released connections.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on each intermediate `wait_for` step, kept well under `TEST_TIMEOUT` so a
/// step that never happens fails with its own message instead of the timeout.
const STEP_BUDGET: Duration = Duration::from_secs(3);

fn addr(last: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, last)), port)
}

/// All the owned buffers one node's sockets borrow, sized for an `N`-slot pool.
struct NodeBufs<const N: usize> {
  udp_rx_meta: [PacketMetadata; 16],
  udp_rx: [u8; 16 * 1024],
  udp_tx_meta: [PacketMetadata; 16],
  udp_tx: [u8; 16 * 1024],
  tcp_rx: [[u8; TCP_BUF]; N],
  tcp_tx: [[u8; TCP_BUF]; N],
}

impl<const N: usize> NodeBufs<N> {
  fn new() -> Self {
    Self {
      udp_rx_meta: [PacketMetadata::EMPTY; 16],
      udp_rx: [0u8; 16 * 1024],
      udp_tx_meta: [PacketMetadata::EMPTY; 16],
      udp_tx: [0u8; 16 * 1024],
      tcp_rx: [[0u8; TCP_BUF]; N],
      tcp_tx: [[0u8; TCP_BUF]; N],
    }
  }
}

/// Build one node's `UdpSocket` + `[TcpSocket; N]` over its `Stack` and bufs.
fn build_sockets<'a, const N: usize>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs<N>,
) -> (UdpSocket<'a>, [TcpSocket<'a>; N]) {
  let udp = UdpSocket::new(
    stack,
    &mut bufs.udp_rx_meta,
    &mut bufs.udp_rx,
    &mut bufs.udp_tx_meta,
    &mut bufs.udp_tx,
  );
  let mut rx_iter = bufs.tcp_rx.iter_mut();
  let mut tx_iter = bufs.tcp_tx.iter_mut();
  let tcp = core::array::from_fn::<_, N, _>(|_| {
    let rx = rx_iter.next().expect("N rx buffers");
    let tx = tx_iter.next().expect("N tx buffers");
    TcpSocket::new(stack, rx, tx)
  });
  (udp, tcp)
}

/// Build a static-IPv4 embassy-net stack over a paired device.
fn build_stack<'a, const S: usize>(
  device: PairedDevice,
  resources: &'a mut StackResources<S>,
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

/// Drive `op` against both memberlist run loops, both embassy-net stack run loops,
/// and the test timeout. Returns the op's value, or panics on timeout.
async fn drive<T>(
  op: impl core::future::Future<Output = T>,
  ml_a: Runner<'_, SmolStr, POOL_A>,
  ml_b: Runner<'_, SmolStr, POOL_B>,
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

/// Drive `op` against one memberlist run loop, one embassy-net stack run loop, and
/// the test timeout — the single-node counterpart of [`drive`], for a test whose
/// only peer is an address no dial can reach.
#[cfg(feature = "cidr")]
async fn drive_solo<T>(
  op: impl core::future::Future<Output = T>,
  ml: Runner<'_, SmolStr, POOL_A>,
  net: &mut NetRunner<'_, PairedDevice>,
) -> T {
  let infra = select(net.run(), ml.run());
  match select(op, select(infra, Timer::after(TEST_TIMEOUT))).await {
    Either::First(v) => v,
    Either::Second(_) => panic!("test timed out after {TEST_TIMEOUT:?}"),
  }
}

/// Count the seed dials to `peer` that failed over a `window` that STARTS at the
/// first such dial, draining the handle's event queue as it goes.
///
/// A seed-originated push/pull that fails is one dial attempt: the engine terminalizes
/// the exchange the moment the dial is refused, so one such event is one trip to the
/// wire (or, for a CIDR-blocked destination, one dial the policy refused in its place).
///
/// Anchoring the window on a dial rather than on the caller's own clock is what makes
/// two counts comparable. The node offers on a free-running node-wide clock the caller
/// cannot see, so a fixed window laid over it starts at an arbitrary phase and holds
/// one more or one fewer offer purely by where it landed. Both phases here start at an
/// offer instead, so both measure the same part of the same clock.
#[cfg(feature = "cidr")]
async fn count_failed_dials(
  ml: &Memberlist<SmolStr, SocketAddr>,
  window: Duration,
  peer: SocketAddr,
) -> usize {
  let mut dials = 0;
  let mut deadline = None;
  loop {
    if let Some(deadline) = deadline
      && Instant::now() >= deadline
    {
      return dials;
    }
    match ml.poll_event() {
      Some(Event::ExchangeCompleted(ec)) => {
        if ec.kind() == ExchangeKind::PushPull && !ec.outcome().is_succeeded() && *ec.peer() == peer
        {
          dials += 1;
          // The first dial anchors the window: everything counted after it is
          // measured from an offer, not from whenever this call happened to start.
          deadline.get_or_insert_with(|| Instant::now() + window);
        }
      }
      Some(_) => {}
      None => Timer::after(POLL_STEP).await,
    }
  }
}

/// Poll `pred` until it holds or `budget` elapses, yielding between checks so the
/// run loops make progress. Returns whether it held.
async fn wait_for(mut pred: impl FnMut() -> bool, budget: Duration) -> bool {
  let deadline = Instant::now() + budget;
  loop {
    if pred() {
      return true;
    }
    if Instant::now() >= deadline {
      return false;
    }
    Timer::after(Duration::from_millis(2)).await;
  }
}

/// The original join future recovers by itself after its seed dial loses B's
/// reliable listener to unrelated inbound connections.
///
/// Sequence: two unrelated connections exhaust B's pool so B has no listener; A's
/// `join([B])` dials into that window and is RST, failing the seed-originated
/// push/pull; the connections release and B re-arms; the same join future — never
/// restarted, never re-offered by the test — completes `Ok`.
#[test]
fn join_recovers_after_losing_the_listener_to_an_unrelated_connection() {
  let (dev_a, dev_b) = pair();
  // A also opens the two unrelated sockets, so its stack needs slots beyond the
  // memberlist pool and gossip socket.
  let mut res_a = StackResources::<{ POOL_A + 4 }>::new();
  let mut res_b = StackResources::<{ POOL_B + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::<POOL_A>::new();
  let mut bufs_b = NodeBufs::<POOL_B>::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  // Buffers for the two unrelated connections that occupy B's pool. They carry no
  // payload, so a small ring is enough.
  let mut hog0_rx = [0u8; 256];
  let mut hog0_tx = [0u8; 256];
  let mut hog1_rx = [0u8; 256];
  let mut hog1_tx = [0u8; 256];

  let start = now();
  let (ml_a, run_a) = block_on(Memberlist::new_with_rng::<_, POOL_A>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, PORT)),
    &SocketAddrResolver,
    udp_a,
    tcp_a,
    start,
    SmallRng::seed_from_u64(1),
  ))
  .expect("build node a");
  let (ml_b, run_b) = block_on(Memberlist::new_with_rng::<_, POOL_B>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, PORT)),
    &SocketAddrResolver,
    udp_b,
    tcp_b,
    start,
    SmallRng::seed_from_u64(2),
  ))
  .expect("build node b");

  block_on(async {
    let mut hog0 = TcpSocket::new(stack_a, &mut hog0_rx, &mut hog0_tx);
    let mut hog1 = TcpSocket::new(stack_a, &mut hog1_rx, &mut hog1_tx);
    let b_reliable = IpEndpoint::from(addr(2, PORT));

    let op = async {
      // 1. Take B's listener away. The first connection lands on B's listener slot
      //    and B re-arms from its one free slot; the second lands on that, leaving
      //    the pool empty and no listener installed.
      hog0
        .connect(b_reliable)
        .await
        .expect("first unrelated connection to B");
      // Let B finish accepting and re-arm from its last free slot before the second
      // connection — otherwise the second SYN races the re-arm and is itself RST,
      // which is the very collision under test but at the wrong point in the script.
      assert!(
        wait_for(
          || ml_b.accepted_inbound_count() == 1 && ml_b.listener_present(),
          STEP_BUDGET
        )
        .await,
        "B did not accept the first connection and re-arm its listener"
      );
      hog1
        .connect(b_reliable)
        .await
        .expect("second unrelated connection to B");
      assert!(
        wait_for(|| !ml_b.listener_present(), STEP_BUDGET).await,
        "B still has a listener after two inbound connections filled its pool"
      );
      assert_eq!(
        ml_b.pool_free_count(),
        0,
        "B's pool must be exhausted for the listener to stay down"
      );
      // Start from a clean event queue so the failure observed below is this join's.
      while ml_a.poll_event().is_some() {}

      // 2. Start the join INTO that window and, alongside it, the script that
      //    checks the dial loses and then releases B. `join` resolves only when
      //    BOTH finish, so the assertion is on the ORIGINAL join future.
      let seeds = [MaybeResolved::Resolved(addr(2, PORT))];
      let joining = ml_a.join(&SocketAddrResolver, &seeds);
      let script = async {
        // The seed dial hits a port with no listening socket and is RST, so its
        // push/pull exchange terminates Failed.
        let deadline = Instant::now() + STEP_BUDGET;
        let mut lost = false;
        while Instant::now() < deadline {
          match ml_a.poll_event() {
            Some(Event::ExchangeCompleted(ec)) => {
              if ec.kind() == ExchangeKind::PushPull && !ec.outcome().is_succeeded() {
                assert_eq!(
                  *ec.peer(),
                  addr(2, PORT),
                  "the failed push/pull must be the seed exchange to B"
                );
                lost = true;
                break;
              }
            }
            Some(_) => {}
            None => Timer::after(Duration::from_millis(2)).await,
          }
        }
        assert!(
          lost,
          "A's seed dial did not fail against B's missing listener"
        );
        assert!(
          !ml_a.is_joined(),
          "A must not be joined while its only seed dial has failed"
        );

        // 3. Release B: reset both unrelated connections so B reaps the slots and
        //    re-arms its listener.
        hog0.abort();
        // Ignoring Err: the flush only pushes the RST out; a peer that already tore
        // the connection down makes it fail, which is the state we wanted anyway.
        let _ = hog0.flush().await;
        hog1.abort();
        // Ignoring Err: as above.
        let _ = hog1.flush().await;
        assert!(
          wait_for(|| ml_b.listener_present(), STEP_BUDGET).await,
          "B did not re-arm its listener after the unrelated connections closed"
        );
      };

      let (joined, ()) = join(joining, script).await;
      joined.expect("the original join future recovered and completed");
    };
    drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
  });

  assert!(
    ml_a.is_joined(),
    "A did not converge after recovering the lost seed dial"
  );
  assert!(
    ml_a.by_id(&SmolStr::new("b")).is_some(),
    "A does not know B by id after the recovered join"
  );
}

/// Two joins running at once on one node converge when the ONLY reachable seed sits
/// in the second call's list and the first call's list is entirely unreachable.
///
/// Every live join offers the union of the running calls' seeds, so both offers here
/// name `{dead1, dead2, B}` — three addresses against a seed queue that holds one at
/// a time, with A's CIDR policy failing each dead dial inside the pump that admitted
/// it. What this proves over real stacks is that merging the offers costs neither
/// call anything: B is reached and the push/pull completes while the sibling call's
/// dead seeds keep taking turns in the same queue, the shared address is dialed once
/// rather than once per call (the engine dedups a seed already covered by a live join
/// exchange, whichever call offered it), and BOTH futures then resolve `Ok` on the
/// membership they converged on.
#[cfg(feature = "cidr")]
#[test]
fn concurrent_joins_converge_on_a_seed_only_one_of_them_holds() {
  use memberlist_proto::CidrPolicy;

  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL_A + 2 }>::new();
  let mut res_b = StackResources::<{ POOL_B + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::<POOL_A>::new();
  let mut bufs_b = NodeBufs::<POOL_B>::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  let start = now();
  // A's policy covers the paired link and nothing else, so the two seeds below are
  // rejected before `connect` and free the one seed-queue slot within the same pump.
  let (ml_a, run_a) = block_on(Memberlist::new_with_rng::<_, POOL_A>(
    Options::new()
      .with_max_pending_seeds(1)
      .with_cidr_policy(CidrPolicy::try_from(["169.254.0.0/16"].as_slice()).expect("valid cidr")),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, PORT)),
    &SocketAddrResolver,
    udp_a,
    tcp_a,
    start,
    SmallRng::seed_from_u64(1),
  ))
  .expect("build node a");
  let (ml_b, run_b) = block_on(Memberlist::new_with_rng::<_, POOL_B>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, PORT)),
    &SocketAddrResolver,
    udp_b,
    tcp_b,
    start,
    SmallRng::seed_from_u64(2),
  ))
  .expect("build node b");

  block_on(async {
    let unreachable = [
      MaybeResolved::Resolved(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        PORT,
      )),
      MaybeResolved::Resolved(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
        PORT,
      )),
    ];
    let reachable = [MaybeResolved::Resolved(addr(2, PORT))];

    let op = async {
      let (dead_side, live_side) = join(
        ml_a.join(&SocketAddrResolver, &unreachable),
        ml_a.join(&SocketAddrResolver, &reachable),
      )
      .await;
      dead_side.expect("the join holding only unreachable seeds completes on convergence");
      live_side.expect("the join holding the reachable seed completes");
    };
    drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
  });

  assert!(
    ml_a.is_joined(),
    "A did not converge on the seed only one of its two joins held"
  );
  assert!(
    ml_a.by_id(&SmolStr::new("b")).is_some(),
    "A does not know B by id after the merged join"
  );
  assert_eq!(
    ml_a.join_offer_addr_count(),
    0,
    "both joins have returned, so neither leaves seeds registered behind it"
  );
  assert_eq!(
    ml_b.accepted_inbound_count(),
    1,
    "the shared seed is dialed once for both joins, not once per join"
  );
}

/// Observation window for each phase of the rate comparison, measured from the
/// phase's FIRST dial — several offer intervals long, so a phase counts a handful of
/// dials rather than one.
#[cfg(feature = "cidr")]
const RATE_WINDOW: Duration = Duration::from_millis(1000);
/// How often a counting phase drains the handle's event queue, and so how late a
/// dial can be OBSERVED relative to the offer that made it. It is part of the rate
/// comparison's tolerance, not just a sleep.
#[cfg(feature = "cidr")]
const POLL_STEP: Duration = Duration::from_millis(2);
/// How much later than the second join the third one registers, so the three are
/// staggered across the offer interval rather than starting together.
#[cfg(feature = "cidr")]
const STAGGER: Duration = Duration::from_millis(120);
/// How far the two phases' dial counts may legally differ.
///
/// Each phase is a window of [`RATE_WINDOW`] anchored on one of its own dials, laid
/// over an offer clock the test cannot see. Two effects move a phase's count without
/// any change in the rate being measured:
///
/// * the offer clock only ever runs SLOWER than its nominal quarter second — each
///   offer schedules the next one from the pump's actual time, which is at or past
///   the due time — so a late pump can push the last offer of a window out of it and
///   the phase counts one fewer;
/// * a dial is counted when the phase next drains the event queue, up to
///   [`POLL_STEP`] after the offer that made it, and the anchor carries that lag
///   too — so a window can also reach one offer further than the clock alone would
///   put in it.
///
/// A window four intervals long therefore holds three to five dials for the SAME
/// per-interval rate, and two such windows can differ by two. Anything beyond that
/// is a rate that followed the number of callers; per-future re-offer loops, the
/// regression this pins, would triple it.
#[cfg(feature = "cidr")]
const RATE_TOLERANCE: usize = 2;

/// Three joins naming one fast-failing seed dial it no more often than one join does.
///
/// Offering is a NODE-wide activity: the run loop makes ONE offer per interval
/// carrying the union of every live join's seeds. A seed whose dial fails inside the
/// pump that admitted it is therefore re-dialed once per interval however many joins
/// are waiting on it — the engine's dedup cannot help here, because the failed
/// exchange is gone before the next offer arrives, so what holds the rate down is the
/// single paced offer alone.
///
/// The comparison is the assertion. One join runs for a window and its dials are
/// counted; two more joins naming the same seed then register at staggered points and
/// an identical window is counted again. Were each future to run its own re-offer
/// loop, the three phases-apart loops would offer at three different phases of the
/// interval and the count would rise with them; with one node-wide offer it does not
/// move. The window is measured rather than assumed, so the test needs no copy of the
/// driver's interval and stays honest if that interval changes.
///
/// Both windows are ANCHORED on a dial of their own phase, so both measure the same
/// part of the same free-running offer clock rather than two arbitrary phases of it,
/// and what remains of the alignment is the derived [`RATE_TOLERANCE`]. Comparing
/// windows laid down on the caller's clock instead would let the boundary alone
/// decide the counts, and could fail a node whose rate never moved.
///
/// A's CIDR policy admits the paired link and nothing else, so the seed is a routable
/// address the engine queues and dials, whose dial the policy then refuses inside the
/// same pump — a deterministic fast failure, with no peer having to cooperate.
#[cfg(feature = "cidr")]
#[test]
fn staggered_joins_dial_a_shared_failing_seed_once_per_interval() {
  use memberlist_proto::CidrPolicy;

  let (dev_a, _peer) = pair();
  let mut res_a = StackResources::<{ POOL_A + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);

  let mut bufs_a = NodeBufs::<POOL_A>::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);

  let start = now();
  let (ml_a, run_a) = block_on(Memberlist::new_with_rng::<_, POOL_A>(
    Options::new()
      .with_cidr_policy(CidrPolicy::try_from(["169.254.0.0/16"].as_slice()).expect("valid cidr")),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, PORT)),
    &SocketAddrResolver,
    udp_a,
    tcp_a,
    start,
    SmallRng::seed_from_u64(1),
  ))
  .expect("build node a");

  let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), PORT);
  let seeds = [MaybeResolved::Resolved(blocked)];

  block_on(async {
    let op = async {
      let mut first = Box::pin(ml_a.join(&SocketAddrResolver, &seeds));

      // Phase 1: one join. Its dials over the window set the baseline rate.
      let one = match select(
        first.as_mut(),
        count_failed_dials(&ml_a, RATE_WINDOW, blocked),
      )
      .await
      {
        Either::First(_) => panic!("a join whose only seed is CIDR-blocked cannot converge"),
        Either::Second(dials) => dials,
      };
      assert!(
        one >= 2,
        "the window must hold several offers for the comparison to mean anything, got {one} dials"
      );

      // Phase 2: two more joins naming the SAME seed, registered at different points
      // in the interval, over an identical window.
      let others = async {
        let second = ml_a.join(&SocketAddrResolver, &seeds);
        let third = async {
          Timer::after(STAGGER).await;
          ml_a.join(&SocketAddrResolver, &seeds).await
        };
        // Ignoring the results: neither join can resolve — their only seed is one
        // A's own policy blocks — so this await never returns, and the racing
        // counter is what ends the phase.
        let _ = join(second, third).await;
      };
      let watch = async {
        // Once all three are live, the shared seed is still ONE registered address:
        // the offer names it once, so it can be dialed at most once per offer.
        Timer::after(STAGGER + Duration::from_millis(20)).await;
        assert_eq!(
          ml_a.join_offer_addr_count(),
          1,
          "three joins naming one address must register one entry between them"
        );
      };
      let three = match select(
        join(first.as_mut(), others),
        join(watch, count_failed_dials(&ml_a, RATE_WINDOW, blocked)),
      )
      .await
      {
        Either::First(_) => panic!("a join whose only seed is CIDR-blocked cannot converge"),
        Either::Second(((), dials)) => dials,
      };

      assert!(
        three <= one + RATE_TOLERANCE,
        "three joins on one seed dialed it {three} times against {one} for a single \
         join — the offer rate must not follow the number of callers (two windows \
         over the same clock may differ by {RATE_TOLERANCE}; a per-join re-offer loop \
         would treble the count)"
      );
    };
    drive_solo(op, run_a, &mut net_a).await;
  });

  assert!(
    !ml_a.is_joined(),
    "the node cannot have joined through a seed its own policy blocks"
  );
  assert_eq!(
    ml_a.join_offer_addr_count(),
    0,
    "every join future has ended, so none leaves its seed registered"
  );
}
