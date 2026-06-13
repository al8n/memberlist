//! Two-node loopback over real embassy-net stacks wired by a channel-backed
//! paired [`Driver`](embassy_net::driver::Driver).
//!
//! Each test stands up two embassy-net stacks (static IPs `169.254.1.1/.2`), a
//! [`Memberlist`] + [`Runner`] on each, and drives every future concurrently
//! under one [`block_on`](futures::executor::block_on): the two memberlist run
//! loops, the two embassy-net stack run loops, and the operation under test,
//! raced against a 3 s timeout so a regression fails fast instead of hanging.
//!
//! These are the behavioral gate for the embassy driver: a join converging over
//! TCP push/pull, a reliable user message round-tripping, and an application ping
//! measuring an RTT — all over real sockets, not mocks.

// nested `if let X = ev { if cond }` kept for readability, as in the crate roots.
#![allow(clippy::collapsible_if)]

mod support;

use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant as StdInstant;

use embassy_futures::select::{Either, select};
use embassy_net::{
  Config as NetConfig, Ipv4Cidr, Runner as NetRunner, Stack, StackResources, StaticConfigV4,
  tcp::TcpSocket,
  udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Timer};
use futures::executor::block_on;
use memberlist_embassy::{
  EndpointOptions, InitError, Memberlist, Node, Options, Runner, TransformOptions, now,
};
use memberlist_proto::{SeedableRng, SmallRng, event::Event};
use smol_str::SmolStr;

use support::paired_device::{PairedDevice, pair};

/// TCP socket pool size per node (a listener plus dial/accept sockets).
const POOL: usize = 4;
/// Per-TCP-socket rx/tx buffer bytes.
const TCP_BUF: usize = 4096;
/// Wall-clock cap on each test so a wedged plane fails fast.
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

fn addr(last: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, last)), port)
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

/// Build one node's `UdpSocket` + `[TcpSocket; POOL]` over its `Stack` and bufs.
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
  // Build the TCP pool by pairing each rx buffer with its tx buffer. `iter_mut`
  // over both arrays in lockstep yields `POOL` distinct buffer pairs.
  let mut rx_iter = bufs.tcp_rx.iter_mut();
  let mut tx_iter = bufs.tcp_tx.iter_mut();
  let tcp = core::array::from_fn::<_, POOL, _>(|_| {
    let rx = rx_iter.next().expect("POOL rx buffers");
    let tx = tx_iter.next().expect("POOL tx buffers");
    TcpSocket::new(stack, rx, tx)
  });
  (udp, tcp)
}

/// Build a static-IPv4 embassy-net stack over a paired device.
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

/// Drive `op` to completion against both memberlist run loops, both embassy-net
/// stack run loops, and the test timeout. Returns the op's value, or panics on
/// timeout.
async fn drive<T>(
  op: impl core::future::Future<Output = T>,
  ml_a: Runner<'_, SmolStr, POOL>,
  ml_b: Runner<'_, SmolStr, POOL>,
  net_a: &mut NetRunner<'_, PairedDevice>,
  net_b: &mut NetRunner<'_, PairedDevice>,
) -> T {
  // The two embassy-net stacks must run for frames to cross the paired devices;
  // the two memberlist run loops pump the engines. Compose them as siblings, then
  // race the operation (and the timeout) against the whole always-running bundle.
  let nets = select(net_a.run(), net_b.run());
  let mls = select(ml_a.run(), ml_b.run());
  let infra = select(nets, mls);
  match select(op, select(infra, Timer::after(TEST_TIMEOUT))).await {
    Either::First(v) => v,
    Either::Second(_) => panic!("test timed out after {TEST_TIMEOUT:?}"),
  }
}

/// Substrate check: a raw UDP datagram crosses the two paired-device stacks.
/// Isolates the device/stack/waker plumbing from the memberlist protocol so a
/// convergence failure can be attributed correctly.
#[test]
fn raw_udp_crosses_the_paired_link() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (mut udp_a, _tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (mut udp_b, _tcp_b) = build_sockets(stack_b, &mut bufs_b);

  block_on(async {
    udp_a.bind(7000).expect("bind a");
    udp_b.bind(7000).expect("bind b");
    let op = async {
      let dst = embassy_net::IpEndpoint::from(addr(2, 7000));
      // Send a few times in case the first is dropped before B's socket is ready.
      let mut buf = [0u8; 32];
      loop {
        let _ = udp_a.send_to(b"ping", dst).await;
        match select(
          udp_b.recv_from(&mut buf),
          Timer::after(Duration::from_millis(50)),
        )
        .await
        {
          Either::First(Ok((n, _))) => return buf[..n].to_vec(),
          _ => continue,
        }
      }
    };
    let nets = select(net_a.run(), net_b.run());
    let got = match select(op, select(nets, Timer::after(TEST_TIMEOUT))).await {
      Either::First(v) => v,
      Either::Second(_) => panic!("raw UDP did not cross the link within {TEST_TIMEOUT:?}"),
    };
    assert_eq!(got, b"ping");
  });
}

/// Two nodes; B joins A as a seed; both converge on a 2-member view.
#[test]
fn two_node_join_converges() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  let now = now();
  let (ml_a, run_a) = Memberlist::new_with_rng::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
    udp_a,
    tcp_a,
    now,
    SmallRng::seed_from_u64(1),
  )
  .expect("build node a");
  let (ml_b, run_b) = Memberlist::new_with_rng::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946)),
    udp_b,
    tcp_b,
    now,
    SmallRng::seed_from_u64(2),
  )
  .expect("build node b");

  let start = StdInstant::now();
  block_on(async {
    // B joins A; then wait for both to see two members.
    let op = async {
      ml_b
        .join(&[addr(1, 7946)])
        .await
        .expect("join from a running node");
      // `join` resolves when B is joined; A learns B from the inbound push/pull.
      // Wait for BOTH to converge (A's view updates a tick after the exchange).
      loop {
        if ml_a.num_members() == 2 && ml_b.num_members() == 2 {
          break;
        }
        Timer::after(Duration::from_millis(10)).await;
      }
    };
    drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
  });
  let elapsed = start.elapsed();

  assert_eq!(ml_a.num_members(), 2, "A did not converge to 2 members");
  assert_eq!(ml_b.num_members(), 2, "B did not converge to 2 members");
  assert!(
    ml_a.by_id(&SmolStr::new("b")).is_some(),
    "A does not know B by id"
  );
  assert!(
    ml_b.by_id(&SmolStr::new("a")).is_some(),
    "B does not know A by id"
  );
  println!("two_node_join_converges: converged in {elapsed:?}");
}

/// After convergence, A reliably sends a payload to B; B observes it.
#[test]
fn send_reliable_round_trips() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  let now = now();
  let (ml_a, run_a) = Memberlist::new_with_rng::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
    udp_a,
    tcp_a,
    now,
    SmallRng::seed_from_u64(1),
  )
  .expect("build node a");
  let (ml_b, run_b) = Memberlist::new_with_rng::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946)),
    udp_b,
    tcp_b,
    now,
    SmallRng::seed_from_u64(2),
  )
  .expect("build node b");

  let payload = bytes::Bytes::from_static(b"reliable hello");
  let start = StdInstant::now();
  block_on(async {
    let op = async {
      // Converge first.
      ml_b
        .join(&[addr(1, 7946)])
        .await
        .expect("join from a running node");
      loop {
        if ml_a.num_members() == 2 && ml_b.num_members() == 2 {
          break;
        }
        Timer::after(Duration::from_millis(10)).await;
      }
      // A reliably sends to B; the call resolves Ok when the exchange completes.
      ml_a
        .send_reliable(addr(2, 7946), payload.clone())
        .await
        .expect("send_reliable Ok");
      // B observes the payload via poll_event (reliable user data → UserPacket).
      loop {
        if let Some(ev) = ml_b.poll_event() {
          if let Event::UserPacket(pkt) = ev {
            if pkt.data_ref() == &bytes::Bytes::from_static(b"reliable hello") {
              return true;
            }
          }
        } else {
          Timer::after(Duration::from_millis(5)).await;
        }
      }
    };
    let observed = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    assert!(observed, "B did not observe the reliable payload");
  });
  // Finding-B regression: every completed exchange — the join push/pull AND the
  // user-message send — was drained through `poll_event`, which prunes the engine's
  // outbound-StreamId correlation map, so it returns to empty on BOTH nodes rather
  // than leaking one entry per completed exchange.
  assert_eq!(
    ml_a.outbound_correlation_len(),
    0,
    "node A's send-correlation map leaked after the exchanges completed"
  );
  assert_eq!(
    ml_b.outbound_correlation_len(),
    0,
    "node B's send-correlation map leaked after the exchanges completed"
  );
  println!(
    "send_reliable_round_trips: completed in {:?}",
    start.elapsed()
  );
}

/// After convergence, A pings B; the ping resolves with an RTT.
#[test]
fn ping_succeeds() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  let now = now();
  let (ml_a, run_a) = Memberlist::new_with_rng::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
    udp_a,
    tcp_a,
    now,
    SmallRng::seed_from_u64(1),
  )
  .expect("build node a");
  let (ml_b, run_b) = Memberlist::new_with_rng::<POOL>(
    Options::new(),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946)),
    udp_b,
    tcp_b,
    now,
    SmallRng::seed_from_u64(2),
  )
  .expect("build node b");

  let start = StdInstant::now();
  block_on(async {
    let op = async {
      ml_b
        .join(&[addr(1, 7946)])
        .await
        .expect("join from a running node");
      loop {
        if ml_a.num_members() == 2 && ml_b.num_members() == 2 {
          break;
        }
        Timer::after(Duration::from_millis(10)).await;
      }
      // A pings B; resolves with the measured RTT.
      let b_node = Node::new(SmolStr::new("b"), addr(2, 7946));
      ml_a.ping(b_node).await
    };
    let rtt = drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
    let rtt = rtt.expect("ping resolved with an RTT");
    println!("ping_succeeds: rtt={rtt:?} (wall {:?})", start.elapsed());
  });
}

/// `Memberlist::new` must REJECT a socket timeout that does not exceed the engine's
/// own deadlines — the graceful-close bound (`close_timeout`) and the machine's
/// reliable-exchange deadline (`stream_timeout`) — rather than silently installing
/// one that could abort a slow-but-valid exchange. The invariant is ENFORCED at
/// construction, not merely documented for the default.
#[test]
fn rejects_socket_timeout_not_exceeding_engine_deadlines() {
  // Case 1: socket_timeout (5s) <= the 10s default close_timeout.
  {
    let (dev, _peer) = pair();
    let mut res = StackResources::<{ POOL + 2 }>::new();
    let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
    let mut bufs = NodeBufs::new();
    let (udp, tcp) = build_sockets(stack, &mut bufs);
    let r = Memberlist::new_with_rng::<POOL>(
      Options::new().with_socket_timeout(core::time::Duration::from_secs(5)),
      TransformOptions::default(),
      EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
      udp,
      tcp,
      now(),
      SmallRng::seed_from_u64(1),
    );
    assert!(
      matches!(r.map(|_| ()), Err(InitError::SocketTimeoutOutOfRange(_))),
      "a 5s socket timeout (<= the 10s close_timeout) must be rejected"
    );
  }
  // Case 2: socket_timeout (12s) exceeds close_timeout (10s) but NOT a raised
  // stream_timeout (20s).
  {
    let (dev, _peer) = pair();
    let mut res = StackResources::<{ POOL + 2 }>::new();
    let (stack, _net) = build_stack(dev, &mut res, 2, 0x3333_4444);
    let mut bufs = NodeBufs::new();
    let (udp, tcp) = build_sockets(stack, &mut bufs);
    let r = Memberlist::new_with_rng::<POOL>(
      Options::new().with_socket_timeout(core::time::Duration::from_secs(12)),
      TransformOptions::default(),
      EndpointOptions::new(SmolStr::new("b"), addr(2, 7946))
        .with_stream_timeout(core::time::Duration::from_secs(20)),
      udp,
      tcp,
      now(),
      SmallRng::seed_from_u64(2),
    );
    assert!(
      matches!(r.map(|_| ()), Err(InitError::SocketTimeoutOutOfRange(_))),
      "a 12s socket timeout (<= the 20s stream_timeout) must be rejected"
    );
  }
}

/// A socket timeout larger than `MAX_SOCKET_TIMEOUT` is REJECTED at construction, so
/// it never reaches the embassy-time-to-smoltcp duration conversion. No value a caller
/// can configure can therefore overflow that chain (at any tick rate) into a wrapped,
/// effectively-past deadline that would abort a TCP slot immediately. Rejecting rather
/// than clamping is deliberate: clamping is unsound because each downstream domain (the
/// tick count, `as_micros`, smoltcp's `i64` `Instant`) has a different overflow bound,
/// so no single clamp is safe at every tick rate.
#[test]
fn rejects_huge_socket_timeout() {
  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (udp, tcp) = build_sockets(stack, &mut bufs);
  let r = Memberlist::new_with_rng::<POOL>(
    Options::new().with_socket_timeout(core::time::Duration::from_secs(u64::MAX)),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)),
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(1),
  );
  assert!(
    matches!(r.map(|_| ()), Err(InitError::SocketTimeoutOutOfRange(_))),
    "a socket timeout above MAX_SOCKET_TIMEOUT must be rejected, not clamped"
  );
}

/// embassy-net installs the socket timeout into smoltcp floored to whole MICROSECONDS, so
/// a `socket_timeout` that beats a deadline by a sub-microsecond amount installs the same
/// microsecond value and must be rejected even though it is strictly larger as a
/// `core::Duration`. A one-nanosecond excess over a whole-second deadline floors away at
/// every tick rate, exercising the install-domain tie through `new` without depending on
/// the build's rate.
#[test]
fn rejects_socket_timeout_that_floors_onto_a_deadline() {
  let close = core::time::Duration::from_secs(2);
  let socket = close + core::time::Duration::from_nanos(1);

  let (dev, _peer) = pair();
  let mut res = StackResources::<{ POOL + 2 }>::new();
  let (stack, _net) = build_stack(dev, &mut res, 1, 0x1111_2222);
  let mut bufs = NodeBufs::new();
  let (udp, tcp) = build_sockets(stack, &mut bufs);
  let r = Memberlist::new_with_rng::<POOL>(
    Options::new()
      .with_close_timeout(close)
      .with_socket_timeout(socket),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946))
      .with_stream_timeout(core::time::Duration::from_secs(1)),
    udp,
    tcp,
    now(),
    SmallRng::seed_from_u64(1),
  );
  assert!(
    matches!(r.map(|_| ()), Err(InitError::SocketTimeoutOutOfRange(_))),
    "a socket timeout that floors onto close_timeout's microsecond must be rejected"
  );
}
