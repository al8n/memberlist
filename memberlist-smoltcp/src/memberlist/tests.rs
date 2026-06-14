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
  let m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    cfg,
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &crate::SocketAddrResolver,
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
  let mut m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    crate::Options::new(),
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &crate::SocketAddrResolver,
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

// Resolvers exercising the `join` resolution boundary. `Address = SocketAddr`
// matches a node built with the [`SocketAddrResolver`](crate::SocketAddrResolver).

/// Must never be invoked — the lifecycle guard rejects a left node's `join`
/// before any seed is resolved.
struct UnreachableResolver;

impl crate::Resolver for UnreachableResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  fn resolve(
    &self,
    _address: &SocketAddr,
  ) -> Result<impl Iterator<Item = SocketAddr>, Self::Error> {
    unreachable!("a left node must not resolve seeds");
    #[allow(unreachable_code)]
    Ok(core::iter::empty::<SocketAddr>())
  }
}

/// Resolves every address to no candidates.
struct EmptyResolver;

impl crate::Resolver for EmptyResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  fn resolve(
    &self,
    _address: &SocketAddr,
  ) -> Result<impl Iterator<Item = SocketAddr>, Self::Error> {
    Ok(core::iter::empty())
  }
}

/// Resolves every address to an unbounded stream of the same wire address; the
/// per-seed cap must bound it.
struct InfiniteResolver;

impl crate::Resolver for InfiniteResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;
  fn resolve(&self, address: &SocketAddr) -> Result<impl Iterator<Item = SocketAddr>, Self::Error> {
    Ok(core::iter::repeat(*address))
  }
}

fn started_node(
  dev: &mut smoltcp::phy::Loopback,
  now: memberlist_proto::Instant,
) -> Memberlist<SmolStr, SocketAddr, smoltcp::phy::Loopback> {
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));
  let mut m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    crate::Options::new(),
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &crate::SocketAddrResolver,
    dev,
    now,
  );
  m.start(now);
  m
}

#[test]
fn join_after_leave_rejects_without_resolving() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);
  m.leave(now).expect("leave a running node");

  // A left node rejects join immediately — and the resolver is never called
  // (`UnreachableResolver` would panic otherwise).
  let err = m
    .join(
      &UnreachableResolver,
      &[crate::MaybeResolved::Unresolved(addr(7947))],
    )
    .expect_err("a left node rejects join");
  assert!(
    err.is_control(),
    "expected Control(NotRunning), got {err:?}"
  );
}

#[test]
fn join_with_all_seeds_unresolvable_is_no_addresses() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);

  // A non-empty seed set that resolves to nothing is a discovery failure.
  let err = m
    .join(
      &EmptyResolver,
      &[crate::MaybeResolved::Unresolved(addr(7947))],
    )
    .expect_err("all-empty resolution fails");
  assert!(err.is_no_addresses(), "expected NoAddresses, got {err:?}");
}

#[test]
fn join_bounds_a_runaway_resolver() {
  let mut dev = smoltcp::phy::Loopback::new(smoltcp::phy::Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);

  // The per-seed cap bounds the unbounded resolver iterator, so join returns
  // instead of allocating without end.
  m.join(
    &InfiniteResolver,
    &[crate::MaybeResolved::Unresolved(addr(7947))],
  )
  .expect("capped resolution joins");
}
