//! Public-handle coverage for [`Memberlist`]: the sync query/accessor surface,
//! the directed best-effort send paths and their MTU error branches, the
//! `leave` running-state contract, the union concurrent `join` calls offer, and a
//! multi-node join → leave → query run over the loopback harness.
//!
//! Plus the two contracts about what an awaiting op ANSWERS: that dropping the
//! `Runner` future releases everything parked on it and refuses everything after
//! it, and that an engine refusal which is neither the lifecycle nor backpressure
//! reaches the caller as itself rather than as one of those.
//!
//! Mirrors `loopback.rs`'s substrate (two paired embassy-net stacks driven under
//! one `block_on`, raced against a wall-clock timeout). The single-node sweeps
//! exercise the sync forwards that need no peer (construction installs the local
//! node, so every accessor has a ground truth even before a pump); the cluster
//! test drives a real 2-node convergence, then a graceful leave, and reads the
//! membership through the handle's query methods.

mod support;

use core::{
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
  task::Poll,
};

use embassy_futures::select::{Either, select};
use embassy_net::{
  Config as NetConfig, Ipv4Cidr, Runner as NetRunner, Stack, StackResources, StaticConfigV4,
  tcp::TcpSocket,
  udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Timer};
use futures::{FutureExt, executor::block_on, poll};
#[cfg(compression)]
use memberlist_embassy::CompressionOptions;
use memberlist_embassy::{
  EndpointOptions, GOSSIP_READ_CAP, InitError, MaybeResolved, Memberlist, Node, OpError, Options,
  Runner, SocketAddrResolver, TransformOptions, event::Event, now,
};
use memberlist_embedded::InitError as EngineInitError;
use memberlist_proto::{SeedableRng, SmallRng, typed::State};
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

/// All the owned buffers one node's sockets borrow for the whole `block_on`.
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

/// Build one node (id, last-octet, seed) over an existing stack and bufs with a
/// caller-supplied `Options`, surfacing the construction verdict.
fn try_node_with<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  cfg: Options,
  id: &str,
  last: u8,
  seed: u64,
) -> Result<(Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>), InitError> {
  let (udp, tcp) = build_sockets(stack, bufs);
  // `SocketAddrResolver` resolves synchronously (it never suspends), so drive the
  // now-async constructor to completion inline — this helper stays sync and its
  // call sites (which run before the test's own `block_on`) are unchanged.
  block_on(Memberlist::new_with_rng::<_, POOL>(
    cfg,
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new(id), addr(last, 7946)),
    &SocketAddrResolver,
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(seed),
  ))
}

/// Build one node with a caller-supplied `Options` that is expected to construct.
fn node_with<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  cfg: Options,
  id: &str,
  last: u8,
  seed: u64,
) -> (Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>) {
  try_node_with(stack, bufs, cfg, id, last, seed).expect("build node")
}

/// Build one node (id, last-octet, seed) on the shipped defaults.
fn node<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  id: &str,
  last: u8,
  seed: u64,
) -> (Memberlist<SmolStr, SocketAddr>, Runner<'a, SmolStr, POOL>) {
  node_with(stack, bufs, Options::new(), id, last, seed)
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

/// A gossip UDP socket whose receive-packet capacity reaches the engine's per-pump
/// work ceiling is rejected at construction. The engine applies every datagram it
/// pops within the pump that popped it, so the accepted socket capacity IS a
/// pump's gossip work budget, and `GOSSIP_READ_CAP` is what bounds it. The bound is
/// strict: a socket sized exactly at the cap is rejected too (a pump takes one
/// probe pop past the ring's capacity to detect a mid-pump refill), while the
/// 16-slot socket every other test here uses constructs.
///
/// The rejection comes from the ENGINE, which screens the receive capacity the
/// gossip view over this socket declares, and reaches the caller through this
/// driver's `InitError::Engine`.
#[test]
fn udp_socket_at_or_above_the_gossip_read_cap_rejected() {
  /// A receive-packet ring sized exactly at the engine's per-pump read cap.
  struct OverCapBufs {
    rx_meta: [PacketMetadata; GOSSIP_READ_CAP],
    rx: [u8; 16 * 1024],
    tx_meta: [PacketMetadata; 16],
    tx: [u8; 16 * 1024],
    tcp_rx: [[u8; TCP_BUF]; POOL],
    tcp_tx: [[u8; TCP_BUF]; POOL],
  }

  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x3333_4444);

  let mut over = OverCapBufs {
    rx_meta: [PacketMetadata::EMPTY; GOSSIP_READ_CAP],
    rx: [0u8; 16 * 1024],
    tx_meta: [PacketMetadata::EMPTY; 16],
    tx: [0u8; 16 * 1024],
    tcp_rx: [[0u8; TCP_BUF]; POOL],
    tcp_tx: [[0u8; TCP_BUF]; POOL],
  };
  let udp = UdpSocket::new(
    stack,
    &mut over.rx_meta,
    &mut over.rx,
    &mut over.tx_meta,
    &mut over.tx,
  );
  let mut rx_iter = over.tcp_rx.iter_mut();
  let mut tx_iter = over.tcp_tx.iter_mut();
  let tcp = core::array::from_fn::<_, POOL, _>(|_| {
    TcpSocket::new(
      stack,
      rx_iter.next().expect("POOL rx buffers"),
      tx_iter.next().expect("POOL tx buffers"),
    )
  });
  let rejected = block_on(Memberlist::<SmolStr, SocketAddr>::new_with_rng::<_, POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("over"), addr(1, 7946)),
    &SocketAddrResolver,
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(1),
  ));
  match rejected.map(|_| ()) {
    Err(InitError::Engine(EngineInitError::GossipRecvCapacityTooLarge(n))) => assert_eq!(
      n, GOSSIP_READ_CAP,
      "the rejection carries the socket's receive-packet capacity"
    ),
    Err(other) => {
      panic!("a socket sized at the read cap must be rejected for its capacity, got {other}")
    }
    Ok(()) => panic!("a socket sized at the read cap must be rejected"),
  }

  // The 16-slot socket the rest of this suite uses is below the cap and builds.
  let mut bufs = NodeBufs::new();
  let (_ml, _run) = node(stack, &mut bufs, "under", 1, 2);
}

/// A zero `gossip_read_cap` is rejected as the knob itself. The receive-ring screen
/// is strictly below the cap, so zero admits no gossip ring at all — every socket
/// would be refused for a capacity no value could meet.
///
/// The verdict comes from the engine's config preflight, which construction runs
/// before it resolves the advertise address or binds a socket, and reaches the
/// caller through this driver's `InitError::Engine`.
#[test]
fn zero_gossip_read_cap_rejected() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let rejected = try_node_with(
    stack,
    &mut bufs,
    Options::new().with_gossip_read_cap(0),
    "zero-read-cap",
    1,
    1,
  );
  match rejected.map(|_| ()) {
    Err(InitError::Engine(EngineInitError::ZeroGossipReadCap)) => {}
    Err(other) => panic!("a zero gossip_read_cap must be rejected as the knob itself, got {other}"),
    Ok(()) => panic!("a zero gossip_read_cap must be rejected"),
  }
}

/// A zero `max_pending_seeds` is rejected as the knob itself. The engine queues a
/// join seed only while the queue is below the cap, so zero queues nothing: every
/// `join` would return `Ok` having silently dropped every seed.
#[test]
fn zero_max_pending_seeds_rejected() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let rejected = try_node_with(
    stack,
    &mut bufs,
    Options::new().with_max_pending_seeds(0),
    "zero-seeds",
    1,
    1,
  );
  match rejected.map(|_| ()) {
    Err(InitError::Engine(EngineInitError::ZeroMaxPendingSeeds)) => {}
    Err(other) => {
      panic!("a zero max_pending_seeds must be rejected as the knob itself, got {other}")
    }
    Ok(()) => panic!("a zero max_pending_seeds must be rejected"),
  }
}

/// A zero `max_pending_dials` is rejected as the knob itself. The cap bounds parked
/// dials in EXCESS of the free pool, so zero refuses every dial the pool cannot
/// absorb at once — including the first one made while the pool is momentarily
/// empty — leaving a busy node unable to join or send a reliable message.
#[test]
fn zero_max_pending_dials_rejected() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let rejected = try_node_with(
    stack,
    &mut bufs,
    Options::new().with_max_pending_dials(0),
    "zero-dials",
    1,
    1,
  );
  match rejected.map(|_| ()) {
    Err(InitError::Engine(EngineInitError::ZeroMaxPendingDials)) => {}
    Err(other) => {
      panic!("a zero max_pending_dials must be rejected as the knob itself, got {other}")
    }
    Ok(()) => panic!("a zero max_pending_dials must be rejected"),
  }
}

/// The sync query/accessor surface reports a coherent single-node view at
/// construction — every forward (`members` / `by_id` / `local_state` /
/// `health_score` / the predicates / the advertise address) reflects the local
/// node, before any pump and without a peer.
#[test]
fn single_node_accessor_surface_is_coherent() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node(stack, &mut bufs, "solo", 1, 1);

  let me = SmolStr::new("solo");
  let other = SmolStr::new("nobody");

  // Membership counts: exactly the local node, and it is not "joined".
  assert_eq!(ml.num_members(), 1, "a fresh node has only itself");
  assert!(!ml.is_joined(), "one member is not a joined cluster");
  assert_eq!(ml.members().len(), 1, "members() lists the local node");

  // Identity / advertise forwards.
  assert_eq!(ml.local_id(), me, "local_id is the configured id");
  assert_eq!(
    ml.advertise_address(),
    addr(1, 7946),
    "advertise_address is the configured one"
  );

  // by_id resolves the local node and is None for an unknown id.
  let local = ml.by_id(&me).expect("the local node is known by id");
  assert_eq!(local.id_ref(), &me);
  assert_eq!(local.address_ref(), &addr(1, 7946));
  assert!(ml.by_id(&other).is_none(), "an unknown id resolves to None");

  // local_state mirrors by_id(local) and the local node is never Dead from its
  // own perspective.
  let ls = ml.local_state();
  assert_eq!(ls.id_ref(), &me);
  assert!(!ml.is_dead(&me), "the local node is not Dead to itself");
  assert!(!ml.is_alive(&other), "an unknown id is not Alive");
  assert!(!ml.is_dead(&other), "an unknown id is not Dead");

  // The predicate / map-filter forwards agree with the unfiltered count.
  assert_eq!(
    ml.members_by(|_| true).len(),
    1,
    "members_by(all) matches every member"
  );
  assert_eq!(
    ml.members_by(|_| false).len(),
    0,
    "members_by(none) is empty"
  );
  assert_eq!(ml.num_members_by(|_| true), 1);
  assert_eq!(ml.num_members_by(|_| false), 0);
  let ids: Vec<SmolStr> = ml.members_map_by(|ns| Some(ns.id_ref().clone()));
  assert_eq!(
    ids,
    vec![me.clone()],
    "members_map_by collects each member id"
  );
  assert!(
    ml.members_map_by(|_| None::<()>).is_empty(),
    "members_map_by(None) collects nothing"
  );

  // online_members / num_online_members agree with each other.
  assert_eq!(ml.online_members().len(), ml.num_online_members());

  // The local health score is readable (0 = fully healthy on a quiet node).
  assert_eq!(ml.health_score(), 0, "a quiet local node is fully healthy");

  // Swapping the compression policy at runtime is a pure forward that leaves the
  // membership view untouched (it only re-policies future encodes).
  #[cfg(compression)]
  {
    ml.set_compression_options(CompressionOptions::new())
      .expect("compression options set on a running node");
    assert_eq!(
      ml.num_members(),
      1,
      "re-policying compression keeps the view"
    );
  }
}

/// The `#[doc(hidden)]` reliable-plane diagnostics report the construction-time
/// baseline: a non-empty pool with a listener installed, and zero outstanding
/// closing / half-closed / pending-dial / inbound-accept / correlation entries.
#[test]
fn reliable_plane_diagnostics_report_construction_baseline() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node(stack, &mut bufs, "diag", 1, 1);

  // One slot is dedicated to the listener at construction, so the free pool is
  // the remaining slots and the listener is present.
  assert!(
    ml.listener_present(),
    "the construction-time listener is installed"
  );
  assert_eq!(
    ml.pool_free_count(),
    POOL - 1,
    "one of POOL slots is the listener; the rest are free"
  );

  // Nothing has connected yet: every in-flight counter is zero.
  assert_eq!(ml.accepted_inbound_count(), 0);
  assert_eq!(ml.closing_count(), 0);
  assert_eq!(ml.half_closed_count(), 0);
  assert_eq!(ml.pending_dial_count(), 0);
  assert_eq!(ml.outbound_correlation_len(), 0);
}

/// The directed best-effort send paths accept an in-MTU payload and reject one
/// that overflows the gossip MTU with an `Err`, all without needing a peer (the
/// engine frames + enqueues, or rejects, synchronously).
#[test]
fn directed_send_paths_accept_small_and_reject_oversized() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node(stack, &mut bufs, "snd", 1, 1);

  let peer = addr(2, 7946);
  let small = bytes::Bytes::from_static(b"hi");
  // The default gossip MTU is 1400 bytes; 4096 framed bytes cannot fit.
  let huge = bytes::Bytes::from(vec![0u8; 4096]);

  // send / send_many / queue_user_broadcast all accept a small payload.
  ml.send(peer, small.clone())
    .expect("a small packet fits the MTU");
  ml.send_many(peer, &[small.clone(), small.clone()])
    .expect("two small packets compound within the MTU");
  ml.queue_user_broadcast(small.clone())
    .expect("a small broadcast fits the MTU");

  // …and reject an oversized one with an Err (rather than panicking or silently
  // truncating).
  assert!(
    ml.send(peer, huge.clone()).is_err(),
    "an over-MTU packet must be rejected"
  );
  assert!(
    ml.send_many(peer, core::slice::from_ref(&huge)).is_err(),
    "an over-MTU compound send must be rejected"
  );
  assert!(
    ml.queue_user_broadcast(huge).is_err(),
    "an over-MTU broadcast must be rejected"
  );
}

/// A reliable send refused because the dial backlog is full surfaces as
/// `OpError::DialBacklogFull` — the engine's call-site backpressure — and NOT as
/// `OpError::NotRunning`, which would tell the caller the node had left the cluster
/// and that retrying is pointless.
///
/// The engine bounds parked dials in EXCESS of the free reliable pool, so the first
/// `free + max_pending_dials` sends are admitted and the next is refused. No
/// [`Runner`] drives the pump here, so an admitted send parks on its completion
/// signal (`now_or_never` yields `None`), while the refused one answers on its first
/// poll, before it ever reaches that signal.
#[test]
fn reliable_send_past_the_dial_backlog_reports_backpressure() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();

  // Drive an explicit ceiling through the driver's own `Options` rather than the
  // engine default, so the bound this test asserts is the one it configured — and a
  // small one, to keep the admitted burst short.
  const MAX_PENDING_DIALS: usize = 2;
  let (ml, _run) = node_with(
    stack,
    &mut bufs,
    Options::new().with_max_pending_dials(MAX_PENDING_DIALS),
    "backlog",
    1,
    1,
  );
  let admitted = ml.pool_free_count() + MAX_PENDING_DIALS;

  // Within the free pool plus the excess bound every send is admitted and parks on
  // its completion signal: nothing pumps, so none of them can resolve.
  for i in 0..admitted {
    assert!(
      ml.send_reliable(addr(10 + i as u8, 7946), bytes::Bytes::from_static(b"x"))
        .now_or_never()
        .is_none(),
      "send {i} of {admitted} is within the bound, so it must be admitted and park, \
       not answer at the call site"
    );
  }

  // One past the bound: committed dials now exceed the free pool by the whole
  // ceiling, so the engine refuses this one where the caller can see it.
  match ml
    .send_reliable(
      addr(10 + admitted as u8, 7946),
      bytes::Bytes::from_static(b"over"),
    )
    .now_or_never()
  {
    Some(Err(e)) => assert!(
      e.is_dial_backlog_full(),
      "an over-bound reliable send must report the full dial backlog as backpressure, \
       got {e}"
    ),
    Some(Ok(())) => panic!("an over-bound reliable send must not report success"),
    None => panic!(
      "an over-bound reliable send must be refused at the call site, not parked on a \
       completion signal"
    ),
  }
}

/// Once the node has left, a reliable send reports `OpError::NotRunning` even with
/// the dial backlog saturated. `DialBacklogFull` invites the caller to pace and
/// retry; after `leave` there is nothing to wait for, so answering it would keep a
/// caller retrying a node that will never send again.
#[test]
fn reliable_send_after_leave_reports_not_running_over_the_backlog() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();

  const MAX_PENDING_DIALS: usize = 2;
  let (ml, _run) = node_with(
    stack,
    &mut bufs,
    Options::new().with_max_pending_dials(MAX_PENDING_DIALS),
    "left-backlog",
    1,
    1,
  );
  let admitted = ml.pool_free_count() + MAX_PENDING_DIALS;

  // Saturate the backlog exactly as the backpressure test does, then confirm the
  // node still answers the retryable refusal while it is running.
  for i in 0..admitted {
    assert!(
      ml.send_reliable(addr(10 + i as u8, 7946), bytes::Bytes::from_static(b"x"))
        .now_or_never()
        .is_none(),
      "send {i} of {admitted} is within the bound, so it must be admitted and park"
    );
  }
  match ml
    .send_reliable(
      addr(10 + admitted as u8, 7946),
      bytes::Bytes::from_static(b"over"),
    )
    .now_or_never()
  {
    Some(Err(e)) => assert!(
      e.is_dial_backlog_full(),
      "a running node over the bound must report backpressure, got {e}"
    ),
    other => panic!("expected the running node to refuse with backpressure, got {other:?}"),
  }

  // No pump runs between these, so the backlog the next send meets is the same one.
  ml.leave().expect("leave from a running node");

  match ml
    .send_reliable(
      addr(10 + admitted as u8 + 1, 7946),
      bytes::Bytes::from_static(b"after-leave"),
    )
    .now_or_never()
  {
    Some(Err(e)) => {
      assert!(
        !e.is_dial_backlog_full(),
        "a left node must not answer a reliable send as retryable backpressure, got {e}"
      );
      assert!(
        matches!(e, OpError::NotRunning),
        "a left node must refuse a reliable send as NotRunning, got {e}"
      );
    }
    other => panic!("expected the left node to refuse the send, got {other:?}"),
  }
}

/// The node's offer carries the UNION of the seeds of every `join` live at that
/// moment, and a call that ends takes only its own seeds back out of it.
///
/// The engine's over-cap seed admission rotates over the addresses ONE offer names,
/// so concurrent joins offering their own lists separately would share that rotation
/// while neither ever named the other's seeds. What this proves, in order:
///
/// 1. both calls' seeds are registered, so the union is the four distinct addresses;
/// 2. the offer made while both are live actually CARRIED all four — the engine
///    classifies every entry of an offer exactly once, so against a seed queue
///    already holding its one admission, three drops plus one dedup is a
///    four-address offer where either call's own list alone could only have produced
///    two;
/// 3. dropping the second future — the cancellation path a caller's own join deadline
///    takes — unregisters exactly its two addresses and leaves the first call's;
/// 4. the next offer then names two addresses again (one drop plus one dedup), so a
///    join that is no longer running leaves nothing in the union.
///
/// No [`Runner`] drives the pump here, so the offers are made through the same step
/// the run loop takes, nothing is dialed, and the seed queue never drains: the
/// engine's admission counters are then a direct read of what each offer named. A
/// one-seed queue makes every entry past the first admission visible as a drop or a
/// dedup.
#[test]
fn concurrent_joins_offer_their_union() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node_with(
    stack,
    &mut bufs,
    Options::new().with_max_pending_seeds(1),
    "union",
    1,
    1,
  );

  let first = [
    MaybeResolved::Resolved(addr(11, 7946)),
    MaybeResolved::Resolved(addr(12, 7946)),
  ];
  let second = [
    MaybeResolved::Resolved(addr(13, 7946)),
    MaybeResolved::Resolved(addr(14, 7946)),
  ];

  block_on(async {
    // Boxed so the futures can be polled by reference and then genuinely DROPPED —
    // a pinned stack local would outlive the binding and never run its guard.
    let mut joining_first = Box::pin(ml.join(&SocketAddrResolver, &first));
    let mut joining_second = Box::pin(ml.join(&SocketAddrResolver, &second));

    // Each first poll resolves the seeds (the resolver never suspends), registers
    // them with the node's join loop, and parks. The future offers nothing itself.
    assert!(
      poll!(joining_first.as_mut()).is_pending(),
      "the first join must park: the node has no peer to converge on"
    );
    assert_eq!(
      ml.join_offer_addr_count(),
      2,
      "the first join registers its own two seeds"
    );
    assert_eq!(
      ml.pending_seed_count(),
      0,
      "and offers nothing itself — offering is the run loop's step"
    );

    assert!(
      ml.offer_join_seeds_now(),
      "the node offers the registered seeds"
    );
    assert_eq!(
      ml.pending_seed_count(),
      1,
      "that offer fills the one-seed queue"
    );
    assert_eq!(
      ml.join_seeds_dropped(),
      1,
      "and sheds the other of the two addresses"
    );

    assert!(
      poll!(joining_second.as_mut()).is_pending(),
      "the second join must park alongside the first"
    );
    assert_eq!(
      ml.join_offer_addr_count(),
      4,
      "both live joins are registered, so the union is four distinct addresses"
    );

    // Past the offer interval the node offers again — now everything both live
    // joins named.
    Timer::after(Duration::from_millis(400)).await;
    assert!(ml.offer_join_seeds_now(), "the next offer is due");
    assert_eq!(
      ml.join_seeds_dropped(),
      4,
      "that offer met a full queue, so its three admissible entries are shed"
    );
    assert_eq!(
      ml.join_seeds_deduped(),
      1,
      "its fourth entry is the address already queued — only reachable if the offer \
       carried both joins' seeds"
    );
    assert_eq!(
      ml.pending_seed_count(),
      1,
      "a full queue admits nothing, so the offer changed no membership intent"
    );

    // Cancel the second join. Its guard releases exactly its own addresses.
    drop(joining_second);
    assert_eq!(
      ml.join_offer_addr_count(),
      2,
      "a cancelled join leaves only the still-running join's seeds in the union"
    );

    Timer::after(Duration::from_millis(400)).await;
    assert!(ml.offer_join_seeds_now(), "and the next offer is due again");
    assert_eq!(
      ml.join_seeds_dropped(),
      5,
      "it names two addresses against a full queue: one shed"
    );
    assert_eq!(
      ml.join_seeds_deduped(),
      2,
      "and one already queued — two entries in all, so the cancelled join's seeds are \
       gone from the offer"
    );
    assert_eq!(
      ml.join_offer_addr_count(),
      2,
      "re-offering does not re-register anything"
    );
    assert!(
      poll!(joining_first.as_mut()).is_pending(),
      "and the surviving join is still parked, waiting on the node's offers"
    );
  });
}

/// Eight concurrent joins cost the node ONE offer, not eight.
///
/// Re-offering is node-wide: the run loop makes a single offer per interval carrying
/// the union of every live join's seeds, and the join futures only register and wait.
/// Were each future to run its own re-offer loop instead, F of them would rebuild and
/// re-offer an F-address union apiece — Θ(F²) entries for the engine to classify per
/// interval — and each would dial the same seed again.
///
/// The counters here are the whole offer: eight one-seed joins are registered, one
/// offer classifies exactly eight entries (one queued, seven shed by the one-slot
/// cap), and a second call inside the same interval is paced away, so what the engine
/// sees in an interval does not grow with the number of callers. No future holds a
/// union buffer of its own — the offer hands the engine the registry's own address
/// slice — so the memory is one entry per distinct address however many joins name
/// it, which is what `join_offer_addr_count` reads back.
#[test]
fn many_concurrent_joins_make_one_union_offer() {
  const JOINS: usize = 8;

  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node_with(
    stack,
    &mut bufs,
    Options::new().with_max_pending_seeds(1),
    "many",
    1,
    1,
  );

  // One distinct seed per join, so the union is exactly `JOINS` addresses.
  let seeds: Vec<[MaybeResolved<SocketAddr>; 1]> = (0..JOINS)
    .map(|i| [MaybeResolved::Resolved(addr(20 + i as u8, 7946))])
    .collect();

  block_on(async {
    // Boxed so every future stays alive — and registered — for the whole test.
    let mut joining: Vec<_> = seeds
      .iter()
      .map(|s| Box::pin(ml.join(&SocketAddrResolver, s)))
      .collect();
    for (i, j) in joining.iter_mut().enumerate() {
      assert!(
        poll!(j.as_mut()).is_pending(),
        "join {i} must park: the node has no peer to converge on"
      );
    }
    assert_eq!(
      ml.join_offer_addr_count(),
      JOINS,
      "every live join is registered, one entry per distinct address"
    );
    assert_eq!(
      ml.pending_seed_count(),
      0,
      "and none of them offered anything itself"
    );

    assert!(
      ml.offer_join_seeds_now(),
      "the node makes its one offer for this interval"
    );
    assert_eq!(
      ml.pending_seed_count(),
      1,
      "the one-slot queue takes one address"
    );
    assert_eq!(
      ml.join_seeds_dropped(),
      (JOINS - 1) as u64,
      "and the offer's other seven entries are shed"
    );
    assert_eq!(
      ml.join_seeds_deduped(),
      0,
      "eight distinct addresses hold no duplicate"
    );

    // One offer's worth of work, and no more: a second call inside the interval is
    // paced away rather than classifying another eight entries.
    assert!(
      !ml.offer_join_seeds_now(),
      "a second offer inside the same interval must be paced away"
    );
    assert_eq!(
      ml.join_seeds_dropped(),
      (JOINS - 1) as u64,
      "so the engine classified exactly one offer's entries"
    );
    assert_eq!(
      ml.pending_seed_count(),
      1,
      "and the seed queue is untouched"
    );
  });
}

/// `leave` is idempotent on a single node: the first call begins the departure
/// and a repeated call once already leaving is a harmless `Ok` no-op (the
/// machine treats a re-leave as a benign shutdown retry, not an error). Enforced
/// without a peer — no live peers means the leave completes synchronously.
#[test]
fn leave_is_idempotent() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node(stack, &mut bufs, "leaver", 1, 1);

  ml.leave()
    .expect("the first leave on a running node succeeds");
  ml.leave()
    .expect("a repeated leave once already leaving is a harmless no-op");
}

/// A full two-node run that joins, queries the converged membership through the
/// handle, then has one node leave and the other observe the departure.
///
/// Drives the query forwards (`members` / `members_by` / `by_id` / `is_alive` /
/// `online_members` / `local_state`) over a REAL converged cluster — the values
/// the single-node sweep cannot reach — and exercises `leave` end to end: B
/// gossips its departure and A surfaces it (B leaves A's Alive set; A emits a
/// membership event).
#[test]
fn join_then_query_then_leave_over_loopback() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (ml_a, run_a) = node(stack_a, &mut bufs_a, "a", 1, 1);
  let (ml_b, run_b) = node(stack_b, &mut bufs_b, "b", 2, 2);

  let a_id = SmolStr::new("a");
  let b_id = SmolStr::new("b");

  block_on(async {
    let op = async {
      // 1. Converge to a 2-member cluster.
      ml_b
        .join(
          &SocketAddrResolver,
          &[MaybeResolved::Resolved(addr(1, 7946))],
        )
        .await
        .expect("join from a running node");
      until(|| ml_a.num_members() == 2 && ml_b.num_members() == 2).await;

      // 2. Query the converged membership through the handle's forwards. A knows
      //    B, sees it Alive, and lists it among the online members.
      assert!(
        ml_a.by_id(&b_id).is_some(),
        "A knows B by id after convergence"
      );
      assert!(ml_a.is_alive(&b_id), "A sees B Alive after convergence");
      assert_eq!(
        ml_a.num_online_members(),
        2,
        "both nodes are online from A's view"
      );
      assert_eq!(ml_a.online_members().len(), 2);

      // members_by / num_members_by filtering on the live FSM state: exactly the
      // two Alive members match.
      let alive = ml_a.members_by(|ns| ns.state() == State::Alive);
      assert_eq!(alive.len(), 2, "both members are Alive");
      assert_eq!(ml_a.num_members_by(|ns| ns.state() == State::Alive), 2);

      // members_map_by projects the converged id set (both a and b present).
      let mut ids = ml_a.members_map_by(|ns| Some(ns.id_ref().clone()));
      ids.sort();
      assert_eq!(
        ids,
        vec![a_id.clone(), b_id.clone()],
        "A's view is {{a, b}}"
      );

      // local_state is A itself and Alive on a converged node.
      let ls = ml_a.local_state();
      assert_eq!(ls.id_ref(), &a_id);
      assert_eq!(ls.state(), State::Alive, "A is Alive to itself");

      // 3. B leaves the cluster. Drain B's events until it has broadcast its own
      //    departure (LeftCluster), proving the leave path runs end to end.
      ml_b.leave().expect("B leaves the running cluster");
      let mut b_saw_left = false;
      until(|| {
        while let Some(ev) = ml_b.poll_event() {
          if matches!(ev, Event::LeftCluster) {
            b_saw_left = true;
          }
        }
        b_saw_left
      })
      .await;

      // 4. A eventually stops considering B a live online member (B gossiped its
      //    departure). A's own online set drops back to just itself.
      until(|| !ml_a.is_alive(&b_id) && ml_a.num_online_members() == 1).await;
    };
    drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
  });

  // After B's departure A still knows B as a member record (now not Alive) and
  // remains Alive itself.
  assert!(
    !ml_a.is_alive(&b_id),
    "A must not consider the departed B Alive"
  );
  assert!(ml_a.is_alive(&a_id), "A is still Alive to itself");
}

/// An on-link address no node answers, so an operation aimed at it can only be
/// ended by the run loop that owns its deadline.
fn silent(last: u8) -> SocketAddr {
  addr(last, 7946)
}

/// Dropping a polled `Runner::run` future must release EVERY operation parked on it
/// and refuse every later one, rather than leave the caller waiting on a pump that
/// no longer exists.
///
/// The run loop diverges, so the only way it ends is the caller dropping the future
/// — a `select` that lost, a task teardown. Nothing behind a parked op can happen
/// after that: a join's seeds are re-offered by the pump, a ping's ack and its
/// timeout are drained by the pump, and a reliable send's completion event is
/// emitted by the pump. A parked join is the sharpest case of all, because waking it
/// is not enough on its own: it re-checks the node's lifecycle and its membership,
/// and a node whose runner is gone has changed NEITHER — the engine is still
/// running, the member count is where it was — so without a terminal state to read
/// it would simply park again on the new epoch and hang until the caller's own
/// deadline.
///
/// Every op here is polled to its park while the run future is alive, and each is
/// aimed at an address no peer answers, so nothing but the teardown can resolve it.
#[test]
fn dropping_a_polled_runner_fails_every_parked_operation() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, run) = node(stack, &mut bufs, "stopped", 1, 1);

  let peer = silent(9);
  let seeds = [MaybeResolved::Resolved(peer)];

  block_on(async {
    // Boxed so the future can be DROPPED here, mid-test, rather than at the end of
    // the scope holding it.
    let mut run = Box::pin(run.run());
    // The guard that carries the teardown lives in the future's body, which — like
    // any `async fn` — starts on the first poll. A few polls run the pump loop to
    // its first park.
    for i in 0..3 {
      assert!(
        poll!(run.as_mut()).is_pending(),
        "the run loop diverges, so poll {i} cannot complete it"
      );
    }

    // Park one of each awaiting op on the live run loop.
    let mut joining = Box::pin(ml.join(&SocketAddrResolver, &seeds));
    assert!(
      poll!(joining.as_mut()).is_pending(),
      "a join whose only seed never answers cannot converge, so it parks"
    );
    let mut pinging = Box::pin(ml.ping(Node::new(SmolStr::new("silent"), peer)));
    assert!(
      poll!(pinging.as_mut()).is_pending(),
      "a ping to a silent peer parks on its completion signal"
    );
    let mut sending = Box::pin(ml.send_reliable(peer, bytes::Bytes::from_static(b"parked")));
    assert!(
      poll!(sending.as_mut()).is_pending(),
      "a reliable send to a silent peer parks on its completion signal"
    );

    // The run future goes away exactly as a lost `select` arm or a torn-down task
    // would take it.
    drop(run);

    match poll!(joining.as_mut()) {
      Poll::Ready(Err(e)) => assert!(
        e.is_runner_stopped(),
        "the parked join must report the gone run loop, got {e}"
      ),
      Poll::Ready(Ok(())) => panic!("a join that never converged must not report success"),
      Poll::Pending => panic!(
        "the parked join was left waiting on a pump that no longer exists — it must \
         be woken and answered by the teardown"
      ),
    }
    match poll!(pinging.as_mut()) {
      Poll::Ready(Err(e)) => assert!(
        e.is_runner_stopped(),
        "the parked ping must report the gone run loop, got {e}"
      ),
      Poll::Ready(Ok(rtt)) => panic!("a ping no peer acked must not report an rtt ({rtt:?})"),
      Poll::Pending => panic!("the parked ping was left waiting on a pump that no longer exists"),
    }
    match poll!(sending.as_mut()) {
      Poll::Ready(Err(e)) => assert!(
        e.is_runner_stopped(),
        "the parked send must report the gone run loop, got {e}"
      ),
      Poll::Ready(Ok(())) => panic!("a send that never completed must not report success"),
      Poll::Pending => panic!("the parked send was left waiting on a pump that no longer exists"),
    }

    // The parked ops released their registrations on the way out, so the node's
    // join offer holds nothing behind them.
    drop(joining);
    assert_eq!(
      ml.join_offer_addr_count(),
      0,
      "the answered join released the seed it was offering"
    );

    // And nothing new is accepted: each op is refused at the call site rather than
    // parked on a wake that can no longer arrive.
    match ml.join(&SocketAddrResolver, &seeds).now_or_never() {
      Some(Err(e)) => assert!(
        e.is_runner_stopped(),
        "a join issued after the teardown must be refused, got {e}"
      ),
      Some(Ok(())) => panic!("a join must not report success on a node with no run loop"),
      None => panic!("a join issued after the teardown must not park"),
    }
    match ml
      .ping(Node::new(SmolStr::new("silent"), peer))
      .now_or_never()
    {
      Some(Err(e)) => assert!(
        e.is_runner_stopped(),
        "a ping issued after the teardown must be refused, got {e}"
      ),
      Some(Ok(rtt)) => panic!("a ping must not report an rtt ({rtt:?}) with no run loop"),
      None => panic!("a ping issued after the teardown must not park"),
    }
    match ml
      .send_reliable(peer, bytes::Bytes::from_static(b"after"))
      .now_or_never()
    {
      Some(Err(e)) => assert!(
        e.is_runner_stopped(),
        "a send issued after the teardown must be refused, got {e}"
      ),
      Some(Ok(())) => panic!("a send must not report success on a node with no run loop"),
      None => panic!("a send issued after the teardown must not park"),
    }
  });
}

/// An engine refusal that is neither the node's lifecycle nor call-site backpressure
/// must reach the caller AS ITSELF, not folded into one of those two answers.
///
/// The engine's error type is `#[non_exhaustive]`, so a driver that maps everything
/// it does not recognise onto `NotRunning` tells the caller the node has left the
/// cluster — the one answer that says retrying is pointless — for a refusal about
/// something else entirely. Here the node is running, its backlog is empty, and the
/// only thing wrong is the TARGET: a scoped IPv6 address the compact wire layout
/// cannot encode, so the framed `Ping` to it could never be sent. The engine rejects
/// it up front and the handle carries that verdict through.
///
/// `send_reliable`'s own engine surface offers no third refusal today — it answers
/// the lifecycle, then the dial backlog, and every remaining path succeeds — so the
/// arm that would carry one is exercised through `ping`, which shares it.
#[test]
fn engine_refusals_reach_the_caller_as_themselves() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (ml, _run) = node(stack, &mut bufs, "refuser", 1, 1);

  // A scoped IPv6 address (nonzero scope_id): wire-decoded addresses always decode
  // that field to zero, so only a caller-supplied target can carry it.
  let scoped = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7946, 0, 1));

  match ml
    .ping(Node::new(SmolStr::new("scoped-peer"), scoped))
    .now_or_never()
  {
    Some(Err(e)) => {
      assert!(
        e.is_rejected(),
        "an unencodable ping target is the engine's own refusal, so it must be \
         carried through as one, got {e}"
      );
      assert!(
        !e.is_not_running(),
        "the node is running: reporting the lifecycle here would tell the caller to \
         stop, for a target it could simply change"
      );
      assert!(
        !e.is_dial_backlog_full() && !e.is_runner_stopped() && !e.is_ping_timeout(),
        "the refusal is about the target, not backpressure, the run loop or a \
         deadline"
      );
      assert!(
        std::error::Error::source(&e).is_some(),
        "the carried engine error is the reported error's source"
      );
    }
    Some(Ok(rtt)) => panic!("an unencodable ping target must not report an rtt ({rtt:?})"),
    None => panic!("an unencodable ping target must be refused at the call site, not parked"),
  }

  // The refusal is registered nowhere: the node is untouched and still running, so
  // an ordinary operation is still admitted and parks.
  assert!(
    ml.ping(Node::new(SmolStr::new("ordinary"), addr(9, 7946)))
      .now_or_never()
      .is_none(),
    "a wire-encodable target is admitted and parks on its completion signal"
  );
}
