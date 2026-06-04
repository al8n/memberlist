//! Public-API parity tests for `memberlist-smoltcp`.
//!
//! Exercises the new query accessors and poll-based I/O methods added to
//! `Memberlist<I, D>` to match the compio/reactor API surface, adapted for the
//! `no_std` poll-based model.
//!
//! All tests use the existing 2-node loopback harness (`harness::link`) and the
//! deterministic `Clock`.
mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use bytes::Bytes;
use memberlist_proto::{EndpointOptions, Node, event::Event};
use memberlist_smoltcp::{Memberlist, Options, TransformOptions};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// Short-timeout config for deterministic tests.
fn mk(id: &str, ip: u8) -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
    .with_rng_seed(ip as u64)
    .with_probe_interval(Duration::from_millis(100))
    .with_probe_timeout(Duration::from_millis(50))
    .with_gossip_interval(Duration::from_millis(50))
    .with_stream_timeout(Duration::from_secs(10))
}

type NodeAndDev = (
  Memberlist<SmolStr, harness::PairedDevice>,
  harness::PairedDevice,
);
type TwoNodes = (NodeAndDev, NodeAndDev, harness::Clock);

/// Drive a 2-node cluster to join convergence (both see 2 members).
///
/// Returns the pair of nodes `(a, b)` ready for further assertions.
/// Uses a larger TCP pool (8) so concurrent reliable streams issued in tests
/// do not compete with in-flight probe/push-pull exchanges for sockets.
fn converge_two_nodes() -> TwoNodes {
  const BUDGET: u32 = 2000;
  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Use a larger TCP pool so concurrent test-issued reliable exchanges do not
  // compete with the ongoing probe/push-pull exchanges for sockets.
  let cfg = Options::new().with_tcp_pool_size(8);

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    cfg.clone(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1),
    &mut da,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    cfg,
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    mk("b", 2),
    &mut db,
    now,
  );
  a.start(now);
  b.start(now);
  b.join(&[addr(1, 7946)]);

  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if a.num_members() == 2 && b.num_members() == 2 {
      break;
    }
    clk.advance_ms(10);
  }
  assert_eq!(
    a.num_members(),
    2,
    "cluster did not converge (a has {} members)",
    a.num_members()
  );
  ((a, da), (b, db), clk)
}

// ── Query accessors ───────────────────────────────────────────────────────────

/// `local_id` returns the local node's id without allocating a new heap string.
#[test]
fn local_id_returns_local_node_id() {
  let (mut dev, _) = harness::link(1500);
  let clk = harness::Clock::new();
  let now = clk.now();
  let mut node: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1),
    &mut dev,
    now,
  );
  node.start(now);
  assert_eq!(node.local_id(), SmolStr::new("a"));
}

/// `advertise_address` returns the address the node was constructed with.
#[test]
fn advertise_address_returns_constructed_addr() {
  let (mut dev, _) = harness::link(1500);
  let clk = harness::Clock::new();
  let now = clk.now();
  let mut node: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1),
    &mut dev,
    now,
  );
  node.start(now);
  assert_eq!(node.advertise_address(), addr(1, 7946));
}

/// `local_state` returns an `Arc<NodeState>` for the local node; FSM state is Alive.
#[test]
fn local_state_is_alive() {
  use memberlist_proto::typed::State;
  let (mut dev, _) = harness::link(1500);
  let clk = harness::Clock::new();
  let now = clk.now();
  let mut node: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1),
    &mut dev,
    now,
  );
  node.start(now);
  let ls = node.local_state();
  assert_eq!(ls.id_ref(), &SmolStr::new("a"));
  assert_eq!(ls.state(), State::Alive);
}

/// `health_score` is 0 on a freshly started single node (fully healthy).
#[test]
fn health_score_zero_on_fresh_node() {
  let (mut dev, _) = harness::link(1500);
  let clk = harness::Clock::new();
  let now = clk.now();
  let mut node: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1),
    &mut dev,
    now,
  );
  node.start(now);
  assert_eq!(node.health_score(), 0);
}

/// On a converged 2-node cluster:
/// - `by_id("b")` returns the peer's NodeState with FSM liveness Alive.
/// - `online_members()` returns exactly 2 members, both Alive.
/// - `num_online_members() == 2`.
/// - `members()` returns 2 entries.
/// - `num_members_by(|ns| ns.id_ref() == "b")` == 1.
#[test]
fn query_accessors_on_converged_cluster() {
  use memberlist_proto::typed::State;
  let ((mut a, mut da), (mut b, mut db), mut clk) = converge_two_nodes();

  // Give gossip a few more ticks to stabilize liveness.
  for _ in 0..20 {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    clk.advance_ms(10);
  }

  // by_id
  let ns = a
    .by_id(&SmolStr::new("b"))
    .expect("node a should know peer b");
  assert_eq!(ns.state(), State::Alive);
  assert_eq!(ns.id_ref(), &SmolStr::new("b"));

  // online_members — both must be Alive
  let online = a.online_members();
  assert_eq!(
    online.len(),
    2,
    "expected 2 online members, got {}",
    online.len()
  );
  for ns in &online {
    assert_eq!(
      ns.state(),
      State::Alive,
      "online member {:?} has non-Alive state",
      ns.id_ref()
    );
  }

  // num_online_members
  assert_eq!(a.num_online_members(), 2);

  // members (all members, not just alive)
  assert_eq!(a.members().len(), 2);

  // num_members_by
  assert_eq!(a.num_members_by(|ns| ns.id_ref() == &SmolStr::new("b")), 1);

  // members_by
  let peers = a.members_by(|ns| ns.id_ref() != &SmolStr::new("a"));
  assert_eq!(peers.len(), 1);
  assert_eq!(peers[0].id_ref(), &SmolStr::new("b"));

  // members_map_by — extract addresses of all Alive members
  let addrs = a.members_map_by(|ns| {
    if ns.state() == State::Alive {
      Some(*ns.address_ref())
    } else {
      None
    }
  });
  assert_eq!(addrs.len(), 2);
}

// ── Ping ─────────────────────────────────────────────────────────────────────

/// `ping` returns a `PingId`; polling surfaces `PingCompleted` or `PingFailed`.
///
/// In a converged 2-node cluster, a ping from A to B should complete
/// (B is alive and reachable over UDP).
#[test]
fn ping_returns_ping_completed_on_reachable_peer() {
  use memberlist_smoltcp::PingId;
  let ((mut a, mut da), (mut b, mut db), mut clk) = converge_two_nodes();

  let target = Node::new(SmolStr::new("b"), addr(2, 7946));
  let ping_id: PingId = a.ping(target, clk.now());

  // Drive both nodes until PingCompleted or PingFailed surfaces, bounded.
  const BUDGET: u32 = 500;
  let mut completed = false;
  let mut failed = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    while let Some(ev) = a.poll_event() {
      match ev {
        Event::PingCompleted(c) if c.ping_id() == ping_id => {
          completed = true;
        }
        Event::PingFailed(f) if f.ping_id() == ping_id => {
          failed = true;
        }
        _ => {}
      }
    }
    if completed || failed {
      break;
    }
    clk.advance_ms(5);
  }
  assert!(
    completed || failed,
    "ping did not produce PingCompleted or PingFailed within budget"
  );
  // On a healthy 2-node cluster with the peer alive and network up, expect success.
  assert!(
    completed,
    "ping failed on a healthy reachable peer (completed={completed} failed={failed})"
  );
}

// ── send / send_many ──────────────────────────────────────────────────────────

/// `send` enqueues a UDP user packet; the peer observes `Event::UserPacket`.
#[test]
fn send_delivers_user_packet_to_peer() {
  let ((mut a, mut da), (mut b, mut db), mut clk) = converge_two_nodes();

  // Send a user packet from A to B's address.
  a.send(addr(2, 7946), Bytes::from_static(b"hello from a"))
    .expect("send should succeed");

  const BUDGET: u32 = 200;
  let mut received = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    while let Some(ev) = b.poll_event() {
      if let Event::UserPacket(pkt) = ev {
        if pkt.data_ref().as_ref() == b"hello from a" {
          received = true;
        }
      }
    }
    if received {
      break;
    }
    clk.advance_ms(5);
  }
  assert!(received, "peer b did not receive the user packet via send");
}

/// `send_many` delivers multiple payloads as a compound gossip datagram.
#[test]
fn send_many_delivers_multiple_packets_to_peer() {
  let ((mut a, mut da), (mut b, mut db), mut clk) = converge_two_nodes();

  let payloads: &[Bytes] = &[
    Bytes::from_static(b"pkt-1"),
    Bytes::from_static(b"pkt-2"),
    Bytes::from_static(b"pkt-3"),
  ];
  a.send_many(addr(2, 7946), payloads)
    .expect("send_many should succeed");

  const BUDGET: u32 = 200;
  let mut received: std::vec::Vec<bytes::Bytes> = std::vec::Vec::new();
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    while let Some(ev) = b.poll_event() {
      if let Event::UserPacket(pkt) = ev {
        received.push(pkt.data_ref().clone());
      }
    }
    if received.len() >= 3 {
      break;
    }
    clk.advance_ms(5);
  }
  assert!(
    received.contains(&Bytes::from_static(b"pkt-1")),
    "pkt-1 not received"
  );
  assert!(
    received.contains(&Bytes::from_static(b"pkt-2")),
    "pkt-2 not received"
  );
  assert!(
    received.contains(&Bytes::from_static(b"pkt-3")),
    "pkt-3 not received"
  );
}

// ── send_reliable ────────────────────────────────────────────────────────────

/// `send_reliable` dials a TCP stream and delivers the payload; the local node
/// observes `Event::ExchangeCompleted` with `kind = UserMessage`, and the peer
/// receives an `Event::UserPacket` with the sent bytes.
///
/// The smoltcp poll loop services `DialRequested` generically — ANY
/// `start_user_message` goes through the same `Connect` path as a join
/// push/pull, so `send_reliable` works exactly like join for the reliable plane.
#[test]
fn send_reliable_delivers_via_tcp_and_emits_exchange_completed() {
  use memberlist_proto::event::{ExchangeKind, ExchangeOutcome};
  use memberlist_smoltcp::StreamId;

  let ((mut a, mut da), (mut b, mut db), mut clk) = converge_two_nodes();

  let stream_id: StreamId = a.send_reliable(
    addr(2, 7946),
    Bytes::from_static(b"reliable msg"),
    clk.now(),
  );

  const BUDGET: u32 = 2000;
  let mut exchange_succeeded = false;
  let mut peer_received = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    // Watch B's events for the UserPacket carrying the sent bytes.
    while let Some(ev) = b.poll_event() {
      if let Event::UserPacket(pkt) = ev {
        if pkt.data_ref() == &Bytes::from_static(b"reliable msg") {
          peer_received = true;
        }
      }
    }
    // Watch A's events for ExchangeCompleted{kind=UserMessage, outcome=Succeeded}.
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.kind() == ExchangeKind::UserMessage && ec.outcome() == ExchangeOutcome::Succeeded {
          exchange_succeeded = true;
        }
      }
    }
    if exchange_succeeded && peer_received {
      break;
    }
    clk.advance_ms(10);
  }
  assert!(
    exchange_succeeded,
    "send_reliable did not complete with ExchangeCompleted(UserMessage, Succeeded) \
     within budget (stream_id={stream_id:?})"
  );
  assert!(
    peer_received,
    "peer did not receive UserPacket with the sent bytes within budget \
     (stream_id={stream_id:?})"
  );
}
