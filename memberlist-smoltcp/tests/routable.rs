//! The driver validates every `SocketAddr` at the smoltcp boundary so untrusted
//! network input (a gossiped `Alive`) or bad configuration can never feed
//! smoltcp a non-routable destination — the unspecified address, a
//! multicast/broadcast IP, or port 0.
//!
//! smoltcp's socket layer already rejects the unspecified address and port 0 with
//! `Unaddressable`, and its route table / neighbor cache `assert!` the resolved
//! egress address is unicast. The driver screens to that SAME unicast predicate
//! at every boundary so such an address is never even stored as a member,
//! re-gossiped, dialed, or queued for send — turning silent per-datagram drops,
//! doomed dials, and propagated junk member addresses into clean early rejects,
//! and staying correct against smoltcp's documented routable-address contract.
//!
//! The defense is layered: a fail-fast advertise-address check at construction,
//! an `AliveDelegate` that drops a non-routable peer at admission (so it is never
//! stored and never re-gossiped), drops at the `join` / `inject_alive` API
//! entries, and a last-line unicast check at the egress chokepoints (gossip
//! `send_slice` and the reliable `connect`) that catches a bad address arriving
//! from ANY source. These tests cover each layer, with the Ethernet
//! filter-and-clean-egress test as the keystone.

mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use memberlist_proto::{EndpointOptions, Instant};
use memberlist_smoltcp::{
  EthernetAddress, HardwareAddress, InitError, InterfaceOptions, IpCidr, Memberlist, Options,
  TransformOptions,
};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// The unspecified address with the usual gossip port — a non-routable
/// destination (smoltcp's socket layer rejects it as `Unaddressable`).
fn unroutable() -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7946)
}

/// An `EndpointOptions` with short timers so a gossip/probe round (which would
/// select an admitted member as an egress destination) fires within a small tick
/// budget.
fn fast_ep(id: &str, ip: u8) -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
    .with_rng_seed(ip as u64)
    .with_probe_interval(Duration::from_millis(20))
    .with_probe_timeout(Duration::from_millis(10))
    .with_gossip_interval(Duration::from_millis(20))
}

/// An Ethernet-medium interface config (the medium whose neighbor cache holds
/// the egress unicast assertion).
fn eth_iface(ip: u8, mac_last: u8) -> InterfaceOptions {
  InterfaceOptions::new(HardwareAddress::Ethernet(EthernetAddress([
    0x02, 0x00, 0x00, 0x00, 0x00, mac_last,
  ])))
  .with_ip_addr(IpCidr::new(
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)).into(),
    24,
  ))
  .with_random_seed(0xC0FFEE + ip as u64)
}

/// Layer 3 — a node configured to advertise a non-routable address is rejected
/// at construction, not admitted as a self-description no peer can route to.
#[test]
fn non_routable_advertise_addr_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  // Advertise 0.0.0.0:7946 — unspecified, not a routable destination.
  let ep_cfg = EndpointOptions::new(SmolStr::new("a"), unroutable()).with_rng_seed(1);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  let err = res
    .map(|_| ())
    .expect_err("a non-routable advertise address must be a typed construction error");
  match err {
    InitError::NonRoutableAdvertiseAddr(a) => assert_eq!(a, unroutable()),
    other => panic!("expected NonRoutableAdvertiseAddr, got {other:?}"),
  }
}

/// A routable advertise address still constructs (the check rejects only the
/// non-routable case, not every address).
#[test]
fn routable_advertise_addr_constructs() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut dev,
    now,
  );
  assert!(res.is_ok(), "a routable advertise address must construct");
}

/// Layer 4 / Layer 2 — `inject_alive` with a non-routable peer is dropped: the
/// node never learns that id, so it can never be selected as an egress
/// destination. (The injected `Alive` is rejected both at the public entry and
/// by the admission delegate; the observable contract is that the member is
/// never admitted.)
#[test]
fn injected_non_routable_peer_is_dropped() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut m: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut dev,
    now,
  );
  assert_eq!(m.num_members(), 1, "sole member before inject");

  m.inject_alive(SmolStr::new("ghost"), unroutable(), now);

  assert_eq!(
    m.num_members(),
    1,
    "a non-routable injected peer must not be admitted"
  );
  assert!(
    !m.is_alive(&SmolStr::new("ghost")),
    "the dropped peer must not be Alive"
  );
}

/// A routable injected peer IS admitted (the drop is specific to non-routable
/// addresses, proving the guard is not over-broad).
#[test]
fn injected_routable_peer_is_admitted() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut m: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut dev,
    now,
  );
  m.inject_alive(SmolStr::new("peer"), addr(2, 7946), now);
  assert_eq!(
    m.num_members(),
    2,
    "a routable injected peer must be admitted"
  );
}

/// Layer 4 — `join` with a non-routable seed creates no dial and consumes no
/// pooled socket.
///
/// The seed is a multicast address — one smoltcp's `connect` would ACCEPT (it is
/// not the unspecified address and not port 0). A seed that reached a dial would
/// be drained into a `Connect`, take a pooled socket, and hold it once `connect`
/// succeeded; the free-pool count is therefore the strict signal. The seed is
/// instead dropped at `join`, so it never reaches a dial and the pool is
/// untouched.
#[test]
fn non_routable_seed_is_not_dialed() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let mut clk = harness::Clock::new();
  let now = clk.now();
  let mut m: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut dev,
    now,
  );
  m.start(now);
  // One poll to let the listener claim its socket so the free-pool count is at
  // its steady-state baseline before the join.
  let _ = m.poll(clk.now(), &mut dev);
  clk.advance_ms(10);
  let free_before = m.pool_free_count();

  // A multicast seed — non-routable, but accepted by smoltcp's `connect`, so a
  // queued dial WOULD take a socket.
  let mcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 7946);
  m.join(&[mcast]).expect("join from a running node");

  // Drive a few ticks; a queued seed would be drained into a Connect/dial on the
  // first of these. No panic on any.
  for _ in 0..5 {
    let _ = m.poll(clk.now(), &mut dev);
    clk.advance_ms(10);
  }

  assert_eq!(
    m.pool_free_count(),
    free_before,
    "a non-routable seed must not consume a pooled socket for a dial"
  );
  assert_eq!(
    m.pending_dial_count(),
    0,
    "a non-routable seed must not create a deferred dial"
  );
  assert_eq!(m.num_members(), 1, "no member learned from a dropped seed");
}

/// The keystone: a node on an ETHERNET interface — the medium whose neighbor
/// cache holds the release `assert!(is_unicast)` egress assertion — handed
/// non-routable peers (the unspecified address, a multicast IP, and a port-0
/// address) never admits any of them and drives a FULL poll cycle without
/// panicking or wedging.
///
/// # How it is driven
///
/// Each bad address is fed via `inject_alive`, the public path an admitted member
/// would normally enter through. Short probe/gossip intervals (20 ms) mean that an
/// admitted member would, within the tick budget, be selected as a gossip/probe
/// destination and drive the Ethernet egress resolution (`lookup_hardware_addr` /
/// neighbor cache) against its address. The `AliveDelegate` drops each at
/// admission so none is stored, and the egress chokepoints would drop the datagram
/// even if one were — so the loopback Ethernet poll runs clean and no bad peer is
/// ever a member.
///
/// # Scope of the guarantee
///
/// An admitted non-routable member would otherwise be re-gossiped to the cluster —
/// the propagation defect the admission filter closes — which is what the
/// `num_members` assertion pins down. The egress itself does not panic for these
/// specific addresses on smoltcp 0.13: `send_slice` / `connect` reject the
/// unspecified address and port 0 with `Unaddressable`, and multicast egress
/// derives a multicast MAC rather than reaching the neighbor assertion. The driver
/// screens to smoltcp's own unicast predicate regardless, so the guard holds if a
/// future smoltcp routes a non-unicast destination into those release assertions.
/// The assertions below pin the version-independent invariant: no non-routable
/// peer is ever admitted, and a full Ethernet poll cycle stays panic-free.
#[test]
fn unroutable_alive_is_filtered_and_ethernet_poll_stays_clean() {
  // Enough ticks at 10 ms to cross many 20 ms gossip/probe intervals, so an
  // admitted member would certainly have been targeted for egress.
  const BUDGET: u32 = 200;

  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ethernet);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  let mut m: Memberlist<SmolStr, _> = Memberlist::try_new(
    Options::new(),
    eth_iface(1, 0x01),
    TransformOptions::default(),
    fast_ep("a", 1),
    &mut dev,
    now,
  )
  .expect("ethernet node with a routable advertise addr must construct");
  m.start(now);

  // A peer "ghost" advertising the unspecified address — exactly what a hostile
  // or buggy remote could gossip. If stored, the next gossip/probe round would
  // target it on the Ethernet egress path.
  m.inject_alive(SmolStr::new("ghost"), unroutable(), now);

  // Also try a multicast and a port-0 address: every non-routable shape the
  // unicast screen rejects.
  m.inject_alive(
    SmolStr::new("mcast"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 7946),
    now,
  );
  m.inject_alive(SmolStr::new("port0"), addr(2, 0), now);

  // Drive a full poll cycle on the Ethernet device. Reaching the end without a
  // panic, and with no bad peer admitted, is the proof.
  for _ in 0..BUDGET {
    let _ = m.poll(clk.now(), &mut dev);
    clk.advance_ms(10);
  }

  // None of the non-routable peers were ever admitted (the local node is the
  // only member), so none could be selected as an egress destination.
  assert_eq!(
    m.num_members(),
    1,
    "no non-routable peer may be admitted as a member"
  );
  assert!(!m.is_alive(&SmolStr::new("ghost")));
  assert!(!m.is_alive(&SmolStr::new("mcast")));
  assert!(!m.is_alive(&SmolStr::new("port0")));
}
