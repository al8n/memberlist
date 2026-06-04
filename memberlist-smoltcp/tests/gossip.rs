mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};
use memberlist_proto::{EndpointOptions, Instant};
use memberlist_smoltcp::{Memberlist, Options, TransformOptions};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// Build a short-timeout `EndpointOptions` for deterministic virtual-time tests.
///
/// Timings chosen so the full suspect→dead cycle completes in well under
/// 2 000 virtual milliseconds:
///
/// - `probe_interval = 100 ms`: probes fire every 100 ms; an initial random
///   stagger of 0..100 ms means the first probe fires within the first 100 ms
///   of the failure phase.
/// - `probe_timeout = 50 ms`: direct-ack window is 50 ms; in a 2-node cluster
///   there are no eligible indirect peers, so the probe terminates as failure
///   at the direct sub-window boundary.
/// - `suspicion_mult = 2`, `suspicion_max_timeout_mult = 2`:
///   `node_scale = max(log10(2), 1.0) = 1.0`;
///   `min = 100 * 2 * 1.0 = 200 ms`, `max = 200 * 2 = 400 ms`.
///   The suspicion timer fires at most 400 ms after the node is suspected.
/// - `gossip_interval = 50 ms`: gossip rounds fire often enough to keep both
///   members' Alive broadcasts circulating during the healthy phase.
fn mk(id: &str, ip: u8) -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
    .with_rng_seed(ip as u64)
    .with_probe_interval(Duration::from_millis(100))
    .with_probe_timeout(Duration::from_millis(50))
    .with_suspicion_mult(2)
    .with_suspicion_max_timeout_mult(2)
    .with_gossip_interval(Duration::from_millis(50))
}

/// Both nodes gossip each other Alive; the stopped node is detected as Dead.
///
/// # Phase 1 — healthy (N1 ticks × 10 ms = 500 ms virtual time)
///
/// Both A and B poll every tick with both devices cross-wired.  Within this
/// window, gossip + probe/ack cycles keep both members Alive.  Verified by
/// `num_members() == 2` and `is_alive("b")` on node A at the end.
///
/// # Phase 2 — failure (N2 ticks × 10 ms = 1 500 ms virtual time)
///
/// Only A polls (B is "stopped").  The path to Dead:
///
/// 1. A's probe fires (≤ 100 ms into phase 2).
/// 2. No ack within 50 ms → direct sub-window expires → fan-out.
/// 3. No eligible indirect peers in a 2-node cluster; reliable ping not
///    enabled → `probe_terminate_failure` runs immediately → B suspected.
/// 4. Suspicion timer (max 400 ms) fires → `process_dead` → B is Dead.
///
/// Total worst-case: 100 + 50 + 400 = 550 ms < 1 500 ms.
#[test]
fn two_nodes_gossip_and_detect_failure() {
  // N1: healthy phase — 50 ticks × 10 ms = 500 ms virtual time.
  const N1: u32 = 50;
  // N2: failure phase — 150 ticks × 10 ms = 1 500 ms virtual time.
  // Worst-case probe→suspect→dead path is ~550 ms; 1 500 ms is 2.7× margin.
  const N2: u32 = 150;

  let (mut dev_a, mut dev_b) = harness::link(1500);
  let mut clock = harness::Clock::new();
  let now: Instant = clock.now();

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    mk("b", 2),
    &mut dev_b,
    now,
  );

  a.start(now);
  b.start(now);

  // Pre-seed each node with the other as Alive instead of joining over TCP, so
  // this test isolates gossip/probe convergence — the equivalent of static
  // cluster membership configuration.
  a.inject_alive(SmolStr::new("b"), addr(2, 7946), clock.now());
  b.inject_alive(SmolStr::new("a"), addr(1, 7946), clock.now());

  // Phase 1: healthy — poll both nodes each tick so gossip + probe/ack
  // packets cross the virtual link. Both must stay Alive.
  for _ in 0..N1 {
    let _ = a.poll(clock.now(), &mut dev_a);
    let _ = b.poll(clock.now(), &mut dev_b);
    clock.advance_ms(10);
  }

  assert_eq!(
    a.num_members(),
    2,
    "A must see 2 members after healthy phase"
  );
  assert_eq!(
    b.num_members(),
    2,
    "B must see 2 members after healthy phase"
  );
  assert!(
    a.is_alive(&SmolStr::new("b")),
    "A must consider B Alive at the end of the healthy phase"
  );

  // Phase 2: failure — only A polls; B is stopped (no device events, no
  // poll calls). A must drive B through probe-timeout → suspect →
  // suspicion-timeout → dead within the virtual-time budget.
  for _ in 0..N2 {
    let _ = a.poll(clock.now(), &mut dev_a);
    clock.advance_ms(10);
  }

  assert!(
    a.is_dead(&SmolStr::new("b")),
    "A must declare the stopped node B as Dead after the failure phase \
     (probe_interval=100ms, probe_timeout=50ms, max_suspicion=400ms, \
     budget={}ms)",
    N2 * 10
  );
}
