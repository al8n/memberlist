//! Public-handle coverage for [`Memberlist`]: the sync query/accessor surface,
//! the directed best-effort send paths and their MTU error branches, the
//! `leave` running-state contract, and a multi-node join → leave → query run
//! over the loopback harness.
//!
//! Mirrors `loopback.rs`'s substrate (two paired embassy-net stacks driven under
//! one `block_on`, raced against a wall-clock timeout). The single-node sweeps
//! exercise the sync forwards that need no peer (construction installs the local
//! node, so every accessor has a ground truth even before a pump); the cluster
//! test drives a real 2-node convergence, then a graceful leave, and reads the
//! membership through the handle's query methods.

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
  CompressionOptions, EndpointOptions, Memberlist, Options, Runner, TransformOptions, event::Event,
  now,
};
use memberlist_proto::typed::State;
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

/// Build one node (id, last-octet, seed) over an existing stack and bufs.
fn node<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
  id: &str,
  last: u8,
  seed: u64,
) -> (Memberlist<SmolStr>, Runner<'a, SmolStr, POOL>) {
  let (udp, tcp) = build_sockets(stack, bufs);
  Memberlist::new::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new(id), addr(last, 7946)).with_rng_seed(seed),
    udp,
    tcp,
    now(),
  )
  .expect("build node")
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
  ml.set_compression_options(CompressionOptions::new())
    .expect("compression options set on a running node");
  assert_eq!(
    ml.num_members(),
    1,
    "re-policying compression keeps the view"
  );
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
        .join(&[addr(1, 7946)])
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
