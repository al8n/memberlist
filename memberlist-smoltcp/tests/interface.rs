//! The smoltcp interface the driver binds is configured through
//! [`InterfaceConfig`](memberlist_smoltcp::InterfaceConfig): the hardware
//! address selects the medium (validated against the bound device), the IP
//! addresses and routes are applied verbatim, and the random seed defaults to
//! system entropy but may be pinned. Misconfiguration surfaces as a typed
//! [`InitError`](memberlist_smoltcp::InitError) — never a panic from inside
//! smoltcp.

mod harness;

use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use memberlist_machine::{EndpointConfig, Instant};
use memberlist_smoltcp::{
  Config, EthernetAddress, GossipMtuTooLarge, HardwareAddress, InitError, InterfaceConfig, IpCidr,
  Medium, Memberlist, Route, TransformOptions,
};
use smol_str::SmolStr;

fn ep(id: &str, ip: u8) -> EndpointConfig<SmolStr, SocketAddr> {
  EndpointConfig::new(
    SmolStr::new(id),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), 7946),
  )
}

fn ip_cidr(ip: u8) -> IpCidr {
  IpCidr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)).into(), 24)
}

/// A configured-medium / device-medium mismatch is a clean typed error, NOT a
/// panic from inside smoltcp's `Interface::new`.
#[test]
fn medium_mismatch_is_a_clean_error_not_a_panic() {
  // `HardwareAddress::Ip` against an Ethernet-medium device.
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ethernet);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1));
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  // Inspect the error without moving/printing the `Ok(Memberlist)` (which is not
  // `Debug`): a successful construction here is itself the failure.
  let err = res
    .map(|_| ())
    .expect_err("medium mismatch must be an error, not a successful construct");
  match err {
    InitError::MediumMismatch(m) => {
      assert_eq!(m.expected, Medium::Ip);
      assert_eq!(m.actual, Medium::Ethernet);
    }
    other => panic!("expected MediumMismatch, got {other:?}"),
  }
}

/// An empty `ip_addrs` is rejected: the interface would accept no packets.
#[test]
fn missing_ip_address_is_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip); // no with_ip_addr
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::MissingIpAddress)),
    "empty ip_addrs must be rejected"
  );
}

/// With no explicit seed the interface seed is drawn from system entropy
/// (nonzero on a real host); with an explicit seed the stored value is exactly
/// that.
#[test]
fn random_seed_defaults_to_nonzero_entropy_and_is_pinnable() {
  let now: Instant = harness::Clock::new().now();

  // Default (None): entropy-drawn. Build twice; the seeds must be nonzero and
  // (on a real entropy host) differ between constructions.
  let mut dev1 = smoltcp::phy::Loopback::new(Medium::Ip);
  let a: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev1,
    now,
  );
  let mut dev2 = smoltcp::phy::Loopback::new(Medium::Ip);
  let b: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(2)),
    TransformOptions::default(),
    ep("b", 2),
    &mut dev2,
    now,
  );
  assert_ne!(a.interface_random_seed(), 0, "entropy seed must be nonzero");
  assert_ne!(b.interface_random_seed(), 0, "entropy seed must be nonzero");
  assert_ne!(
    a.interface_random_seed(),
    b.interface_random_seed(),
    "two entropy draws must differ"
  );

  // Pinned: stored verbatim.
  let mut dev3 = smoltcp::phy::Loopback::new(Medium::Ip);
  let c: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip)
      .with_ip_addr(ip_cidr(3))
      .with_random_seed(0x1234),
    TransformOptions::default(),
    ep("c", 3),
    &mut dev3,
    now,
  );
  assert_eq!(c.interface_random_seed(), 0x1234);
}

/// An Ethernet-medium device with a matching `HardwareAddress::Ethernet`
/// constructs successfully.
#[test]
fn ethernet_medium_device_constructs() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ethernet);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ethernet(EthernetAddress([
    0x02, 0x00, 0x00, 0x00, 0x00, 0x01,
  ])))
  .with_ip_addr(ip_cidr(1));
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    res.is_ok(),
    "ethernet medium + matching hw addr must construct"
  );
}

/// The IP-medium happy path: a matching `HardwareAddress::Ip` and one address
/// constructs successfully and starts as the sole member.
#[test]
fn ip_medium_happy_path_constructs() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1));
  let m: Memberlist<SmolStr, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  )
  .expect("happy path must construct");
  assert_eq!(m.num_members(), 1);
}

/// A multicast IP CIDR is rejected with a typed error rather than panicking out
/// of smoltcp's `update_ip_addrs` → `check_ip_addrs` (which `panic!`s on a
/// non-unicast, non-unspecified address). The whole point of `try_new` is that
/// it is fallible, so this MUST be `Err`, never an unwind.
#[test]
fn multicast_ip_address_is_rejected_not_panicked() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  // 224.0.0.1/4 is multicast: not unicast and not unspecified.
  let multicast = IpCidr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)).into(), 4);
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(multicast);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::NonUnicastIpAddress(_))),
    "a multicast IP CIDR must be a typed error, not a panic"
  );
}

/// The all-ones broadcast IP address is rejected: not unicast and not
/// unspecified, so smoltcp's `check_ip_addrs` would panic on it.
#[test]
fn broadcast_ip_address_is_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  // 255.255.255.255/32 is the limited broadcast address.
  let broadcast = IpCidr::new(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)).into(), 32);
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(broadcast);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::NonUnicastIpAddress(_))),
    "the limited broadcast address must be rejected"
  );
}

/// The unspecified address (`0.0.0.0`) is PERMITTED, mirroring smoltcp's
/// `check_ip_addrs` exactly (`!is_unicast() && !is_unspecified()`): the driver
/// must not over-reject what smoltcp itself accepts. Paired with a real unicast
/// address so the node is otherwise well-formed.
#[test]
fn unspecified_ip_address_is_permitted() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let unspecified = IpCidr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED).into(), 0);
  let iface = InterfaceConfig::new(HardwareAddress::Ip)
    .with_ip_addr(unspecified)
    .with_ip_addr(ip_cidr(1));
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    res.is_ok(),
    "the unspecified address must be accepted, mirroring smoltcp's check_ip_addrs"
  );
}

/// A broadcast (non-unicast) Ethernet MAC is rejected with a typed error.
/// smoltcp's `Interface::new` stores the configured hardware address WITHOUT
/// calling `check_hardware_addr`, so a broadcast MAC would not panic but would
/// install an invalid L2 source/acceptance identity. The driver validates it
/// up front instead.
#[test]
fn broadcast_mac_is_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ethernet);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ethernet(EthernetAddress::BROADCAST))
    .with_ip_addr(ip_cidr(1));
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::NonUnicastHardwareAddress(_))),
    "a broadcast MAC must be a typed error"
  );
}

/// A zero port is rejected with a typed error rather than panicking out of
/// smoltcp's `udp::Socket::bind` / `tcp::Socket::listen` (which reject port 0).
/// `try_new` is fallible, so a runtime-supplied zero port MUST be `Err`.
#[test]
fn zero_port_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1));
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new().with_port(0),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::ZeroPort)),
    "a zero port must be a typed error, not a panic"
  );
}

/// A route whose gateway (`via_router`) is non-unicast is rejected. On Ethernet
/// egress smoltcp's neighbor lookup asserts the next-hop protocol address is
/// unicast (a release `assert!`), so a multicast gateway would pass construction
/// and then crash the running node at first off-link egress. `try_new` rejects
/// it up front instead, on any medium.
#[test]
fn non_unicast_route_gateway_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  // Default IPv4 route via a multicast gateway (224.0.0.1 is not unicast).
  let route = Route {
    cidr: IpCidr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED).into(), 0),
    via_router: IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)).into(),
    preferred_until: None,
    expires_at: None,
  };
  let iface = InterfaceConfig::new(HardwareAddress::Ip)
    .with_ip_addr(ip_cidr(1))
    .with_route(route);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::NonUnicastRouteGateway(_))),
    "a non-unicast route gateway must be a typed error, not a deferred panic"
  );
}

/// A route whose prefix and gateway are different IP families is rejected: an
/// IPv4 prefix can never resolve a next hop through an IPv6 gateway.
#[test]
fn route_family_mismatch_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  // IPv4 default prefix via a (unicast) IPv6 gateway: passes the unicast check,
  // fails the family check.
  let route = Route {
    cidr: IpCidr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED).into(), 0),
    via_router: IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)).into(),
    preferred_until: None,
    expires_at: None,
  };
  let iface = InterfaceConfig::new(HardwareAddress::Ip)
    .with_ip_addr(ip_cidr(1))
    .with_route(route);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::RouteFamilyMismatch(_))),
    "a family-mismatched route must be a typed error"
  );
}

/// An advertised port that does not match the bound port is rejected: on a
/// direct interface a peer would route to a port nothing is listening on.
#[test]
fn advertise_port_mismatch_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1));
  // Bound ports are the default 7946; advertise a different port.
  let ep_cfg = EndpointConfig::new(
    SmolStr::new("a"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8000),
  );
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::AdvertisePortMismatch)),
    "advertise port != bound port must be rejected"
  );
}

/// An advertised IPv4 not assigned to the interface is rejected: smoltcp drops
/// inbound packets to a non-assigned destination, so peers would gossip and dial
/// an address the node's own interface discards.
#[test]
fn advertise_ipv4_not_local_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)); // 10.0.0.1
  // Port matches the bound default, but the IP is not on the interface.
  let ep_cfg = EndpointConfig::new(
    SmolStr::new("a"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946),
  );
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::AdvertiseAddrNotLocal(_))),
    "an advertised IP not on the interface must be rejected"
  );
}

/// The same for IPv6: an advertised v6 address absent from the interface is
/// rejected.
#[test]
fn advertise_ipv6_not_local_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(IpCidr::new(
    IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)).into(),
    64,
  ));
  let ep_cfg = EndpointConfig::new(
    SmolStr::new("a"),
    SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2)), 7946),
  );
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::AdvertiseAddrNotLocal(_))),
    "an advertised IPv6 not on the interface must be rejected"
  );
}

/// A node with several interface addresses may advertise any one of them: an
/// advertised IP that matches a configured interface address constructs.
#[test]
fn advertise_ip_matching_a_configured_interface_addr_constructs() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip)
    .with_ip_addr(ip_cidr(1)) // 10.0.0.1
    .with_ip_addr(ip_cidr(2)); // 10.0.0.2
  // Advertise the second configured address.
  let ep_cfg = EndpointConfig::new(
    SmolStr::new("a"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946),
  );
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  assert!(
    res.is_ok(),
    "an advertised IP matching a configured interface address must construct"
  );
}

/// A `gossip_mtu` of `usize::MAX` is rejected with a typed error rather than
/// panicking (checked build) or wrapping to an undersized UDP arena (release):
/// `try_new` sizes the UDP arenas from `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`,
/// which would overflow.
#[test]
fn gossip_mtu_usize_max_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let iface = InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1));
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    iface,
    TransformOptions::default(),
    ep("a", 1).with_gossip_mtu(usize::MAX),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::GossipMtuTooLarge(_))),
    "a usize::MAX gossip_mtu must be a typed error, not a panic/wrap"
  );
}

/// The gossip-MTU ceiling is exact: `ceiling + 1` is rejected, `ceiling` itself
/// constructs. The ceiling is read from the rejection so the test does not
/// hard-code the wrapper overhead.
#[test]
fn gossip_mtu_ceiling_boundary() {
  let clock = harness::Clock::new();

  // Discover the exact ceiling from an over-ceiling rejection.
  let mut dev0 = smoltcp::phy::Loopback::new(Medium::Ip);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1).with_gossip_mtu(usize::MAX),
    &mut dev0,
    clock.now(),
  );
  let ceiling = match res.map(|_| ()) {
    Err(InitError::GossipMtuTooLarge(GossipMtuTooLarge { ceiling, .. })) => ceiling,
    other => panic!("expected GossipMtuTooLarge, got {other:?}"),
  };

  // ceiling + 1 is rejected.
  let mut dev1 = smoltcp::phy::Loopback::new(Medium::Ip);
  let over: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1).with_gossip_mtu(ceiling + 1),
    &mut dev1,
    clock.now(),
  );
  assert!(
    matches!(over, Err(InitError::GossipMtuTooLarge(_))),
    "gossip_mtu = ceiling + 1 must be rejected"
  );

  // ceiling exactly constructs.
  let mut dev2 = smoltcp::phy::Loopback::new(Medium::Ip);
  let at: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1).with_gossip_mtu(ceiling),
    &mut dev2,
    clock.now(),
  );
  assert!(at.is_ok(), "gossip_mtu = ceiling must construct");
}

/// A UDP packet-slot count whose `packets × max-datagram` product overflows
/// `usize` is rejected before any allocation (it would otherwise wrap to an
/// undersized arena on a 32-bit target).
#[test]
fn oversized_udp_arena_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.udp_rx_packets = usize::MAX; // × (gossip_mtu + overhead) overflows usize
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::UdpArenaTooLarge)),
    "an overflowing UDP arena product must be a typed error, not a wrap"
  );
}

/// A TCP pool below 2 is rejected: 0 leaves no listener at all and 1 leaves the
/// listener holding the only socket with none free to dial a seed. Both `0` and
/// `1` must be `TcpPoolTooSmall`; the functional minimum `2` constructs.
#[test]
fn tcp_pool_size_below_two_rejected() {
  for bad in [0usize, 1] {
    let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
    let now: Instant = harness::Clock::new().now();
    let mut cfg = Config::new();
    cfg.tcp_pool_size = bad;
    let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
      cfg,
      InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
      TransformOptions::default(),
      ep("a", 1),
      &mut dev,
      now,
    );
    assert!(
      matches!(res, Err(InitError::TcpPoolTooSmall)),
      "tcp_pool_size = {bad} must be rejected as TcpPoolTooSmall"
    );
  }

  // The functional minimum constructs.
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.tcp_pool_size = 2;
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(res.is_ok(), "tcp_pool_size = 2 must construct");
}

/// A zero-byte TCP receive buffer is rejected: smoltcp's `RingBuffer::new` does
/// not panic on empty storage, so it would silently yield a socket that can
/// never receive (a dead reliable plane) rather than a construction error.
#[test]
fn zero_tcp_socket_rx_bytes_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.tcp_socket_rx_bytes = 0;
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::ZeroTcpSocketBuffer)),
    "a zero tcp_socket_rx_bytes must be rejected as ZeroTcpSocketBuffer"
  );
}

/// A zero-byte TCP transmit buffer is rejected for the same reason: an empty
/// ring is a socket that can never send.
#[test]
fn zero_tcp_socket_tx_bytes_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.tcp_socket_tx_bytes = 0;
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::ZeroTcpSocketBuffer)),
    "a zero tcp_socket_tx_bytes must be rejected as ZeroTcpSocketBuffer"
  );
}

/// A TCP receive buffer one byte over smoltcp's 1 GiB cap is rejected as a typed
/// error rather than panicking inside `tcp::Socket::new` (`> 1 << 30`).
///
/// The at-limit construct (`tcp_socket_rx_bytes == 1 << 30`) is intentionally
/// NOT exercised: the pool allocates `tcp_pool_size` such sockets, so a
/// successful construct would allocate multiple gibibytes of rx rings and risk
/// OOMing the test runner. The boundary is verified one-sided instead — the
/// over-limit value rejects and a normal small value (the default) constructs —
/// which pins the comparison without the heavyweight allocation.
#[test]
fn tcp_rx_buffer_above_1gib_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.tcp_socket_rx_bytes = (1 << 30) + 1;
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::TcpRxBufferTooLarge)),
    "tcp_socket_rx_bytes = (1 << 30) + 1 must be a typed error, not a smoltcp panic"
  );

  // A normal (default) rx buffer is well under the cap and constructs.
  let mut dev2 = smoltcp::phy::Loopback::new(Medium::Ip);
  let ok: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    Config::new(),
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev2,
    harness::Clock::new().now(),
  );
  assert!(ok.is_ok(), "a default-sized rx buffer must construct");
}

/// Zero UDP receive packet-metadata slots are rejected: the gossip ring could
/// never dequeue a datagram (a dead gossip plane) rather than fail loudly.
#[test]
fn zero_udp_rx_packets_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.udp_rx_packets = 0;
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::ZeroUdpPackets)),
    "zero udp_rx_packets must be rejected as ZeroUdpPackets"
  );
}

/// Zero UDP transmit packet-metadata slots are rejected for the same reason: the
/// gossip ring could never enqueue a datagram to send.
#[test]
fn zero_udp_tx_packets_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let mut cfg = Config::new();
  cfg.udp_tx_packets = 0;
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::ZeroUdpPackets)),
    "zero udp_tx_packets must be rejected as ZeroUdpPackets"
  );
}

/// A zero `close_timeout` is rejected: it would force-abort every graceful
/// reliable close immediately (deadline == `now`), truncating in-flight push/pull
/// responses instead of draining them.
#[test]
fn zero_close_timeout_rejected() {
  let mut dev = smoltcp::phy::Loopback::new(Medium::Ip);
  let now: Instant = harness::Clock::new().now();
  let cfg = Config::new().with_close_timeout(core::time::Duration::ZERO);
  let res: Result<Memberlist<SmolStr, _>, _> = Memberlist::try_new(
    cfg,
    InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(ip_cidr(1)),
    TransformOptions::default(),
    ep("a", 1),
    &mut dev,
    now,
  );
  assert!(
    matches!(res, Err(InitError::ZeroCloseTimeout)),
    "a zero close_timeout must be rejected as ZeroCloseTimeout"
  );
}
