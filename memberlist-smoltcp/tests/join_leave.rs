//! Reliable-plane end-to-end: a node JOINS a seed via a real push/pull state
//! exchange over TCP, both converge on a 2-member view, then the joiner LEAVES
//! gracefully and both nodes observe the corresponding lifecycle event.
//!
//! Unlike `gossip.rs` (which pre-seeds membership with `inject_alive`), this
//! test exercises the full reliable plane: dial (`StreamAction::Connect` →
//! `tcp::Socket::connect`), the TCP three-way handshake over the paired device,
//! the listener accept (`accept_connection`), the bidirectional push/pull byte
//! pump, and graceful teardown. It is the proof that the dial/accept/pump
//! wiring actually carries a join to completion.

mod harness;

use core::net::{IpAddr, Ipv4Addr, SocketAddr};

use memberlist_proto::{EndpointOptions, Event};
use memberlist_smoltcp::{Options, Memberlist, TransformOptions};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// A real TCP push/pull join, a synced 2-member view, then a clean leave.
///
/// # Phase 1 — join (≤ 2 000 ticks × 10 ms = 20 s virtual budget)
///
/// B dials A as a seed. Both nodes poll each tick so the SYN/SYN-ACK/ACK and the
/// push/pull frames flow through the paired device FIFOs. The dial bridge's
/// handshake deadline (~10 s default) caps the exchange, but over the
/// zero-latency virtual link the push/pull settles in a handful of ticks; the
/// 20 s budget is generous headroom, not an expected duration. Convergence is
/// `num_members() == 2` on BOTH nodes — A learns B from the inbound push/pull,
/// B learns A from the reply.
///
/// # Phase 2 — leave (same budget)
///
/// B calls `leave`, which gossips the departure. A must observe `NodeLeft(b)`
/// and B must observe `LeftCluster`. Both nodes keep polling so the leave
/// broadcast and any reliable teardown frames cross the link.
#[test]
fn join_from_seed_then_clean_leave() {
  // Generous virtual-time budgets. The join settles in far fewer ticks over the
  // zero-latency link; this only bounds a wedged plane so the test fails loudly
  // rather than hanging.
  const BUDGET: u32 = 2000;

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Node A is the seed; node B is the joiner. Distinct rng seeds keep their
  // probe/gossip staggers independent.
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut da,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946)).with_rng_seed(2),
    &mut db,
    now,
  );

  a.start(now);
  b.start(now);

  // B joins via A as a seed: a REAL push/pull over TCP (no inject_alive).
  b.join(&[addr(1, 7946)]);

  // Drive both until both see 2 members (join push/pull synced state), bounded.
  let mut joined = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if a.num_members() == 2 && b.num_members() == 2 {
      joined = true;
      break;
    }
    clk.advance_ms(10);
  }
  assert!(
    joined,
    "join push/pull did not converge: a={} b={}",
    a.num_members(),
    b.num_members()
  );

  // B leaves; A must observe NodeLeft, B must observe LeftCluster.
  b.leave(clk.now()).expect("leave");
  let mut b_left = false;
  let mut a_saw_left = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    while let Some(ev) = b.poll_event() {
      if matches!(ev, Event::LeftCluster) {
        b_left = true;
      }
    }
    while let Some(ev) = a.poll_event() {
      if matches!(ev, Event::NodeLeft(_)) {
        a_saw_left = true;
      }
    }
    if b_left && a_saw_left {
      break;
    }
    clk.advance_ms(10);
  }
  assert!(b_left, "b did not complete leave (LeftCluster)");
  assert!(a_saw_left, "a did not observe b leaving (NodeLeft)");
}
