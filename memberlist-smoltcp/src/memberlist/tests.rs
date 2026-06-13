use super::*;
use crate::{HardwareAddress, InterfaceOptions, IpCidr};
use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use smol_str::SmolStr;

fn addr(p: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), p)
}

fn ip_iface() -> InterfaceOptions {
  InterfaceOptions::new(HardwareAddress::Ip).with_ip_addr(IpCidr::new(
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)).into(),
    24,
  ))
}

#[test]
fn gossip_seed_domain_separates_and_diverges_per_node() {
  let iface_seed = 0x1234_5678_9abc_def0u64;
  let a: SocketAddr = "10.0.0.1:7946".parse().unwrap();
  let b: SocketAddr = "10.0.0.2:7946".parse().unwrap();

  // Domain separation: the gossip seed never equals the interface seed it is
  // derived from, so an observer who infers the interface seed from the TCP
  // stack does not thereby learn the gossip schedule.
  assert_ne!(gossip_seed_from(iface_seed, &a), iface_seed);

  // Per-node divergence: two nodes handed the SAME interface seed get distinct
  // gossip seeds because their advertise addresses differ.
  assert_ne!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &b)
  );

  // A one-port difference at the same IP also diverges.
  let a2: SocketAddr = "10.0.0.1:7947".parse().unwrap();
  assert_ne!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &a2)
  );

  // IPv6 nodes on the same port likewise diverge, and an IPv6 address does not
  // alias the IPv4 node that shares its port.
  let v6a: SocketAddr = "[fe80::1]:7946".parse().unwrap();
  let v6b: SocketAddr = "[fe80::2]:7946".parse().unwrap();
  assert_ne!(
    gossip_seed_from(iface_seed, &v6a),
    gossip_seed_from(iface_seed, &v6b)
  );
  assert_ne!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &v6a)
  );

  // Deterministic: identical inputs reproduce the same gossip seed.
  assert_eq!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &a)
  );
}

#[test]
fn new_node_is_sole_member() {
  let cfg = crate::Options::new();
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
  let m: Memberlist<SmolStr, _> = Memberlist::new(
    cfg,
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  assert_eq!(m.num_members(), 1);
}

#[test]
fn poll_emits_initial_gossip_and_a_deadline() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));
  let mut m: Memberlist<SmolStr, _> = Memberlist::new(
    crate::Options::new(),
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &mut dev,
    now,
  );
  m.start(now);
  let next = m.poll(now, &mut dev);
  assert!(next.is_some(), "scheduler must arm a deadline");
}

#[test]
fn endpoint_is_routable_matches_smoltcp_unicast() {
  use core::net::Ipv6Addr;

  // Unicast IPv4 with a non-zero port is the only routable case here.
  assert!(endpoint_is_routable(&addr(7946)));
  // Unicast IPv6 is routable too.
  assert!(endpoint_is_routable(&SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
    7946
  )));

  // Unspecified (0.0.0.0 / ::) is not unicast.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    7946
  )));
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    7946
  )));
  // Multicast (224.0.0.1) is not unicast.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
    7946
  )));
  // Limited broadcast (255.255.255.255) is not unicast.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
    7946
  )));
  // Port 0 is rejected even with a unicast IP: no socket can address it.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
    0
  )));
}
