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
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
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
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
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

use core::{convert::Infallible, time::Duration};
use memberlist_embedded::ResolvedAddrs;
use smoltcp::phy::{Loopback, Medium};

/// Must never be invoked — the lifecycle guard rejects a left node's `join`
/// before any seed is resolved.
struct UnreachableResolver;

impl crate::Resolver for UnreachableResolver {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    unreachable!("a left node must not resolve seeds");
  }
}

/// Resolves every address to no candidates.
struct EmptyResolver;

impl crate::Resolver for EmptyResolver {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    Ok(ResolvedAddrs::new())
  }
}

/// Resolves every address to a FULL bounded result (the per-seed cap's worth of
/// the same wire address). The `ResolvedAddrs` type bounds the count, so even a
/// resolver that tries to emit "as many as possible" stays capped.
struct FullResolver;

impl crate::Resolver for FullResolver {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    let mut addrs = ResolvedAddrs::new();
    // Fill to capacity; `push` past `MAX_RESOLVED_ADDRS_PER_SEED` returns the
    // item, so the loop simply stops at the type's bound.
    while addrs.push(*address).is_ok() {}
    Ok(addrs)
  }
}

/// Panics if `resolve` is ever called — used to prove a construction error fires
/// BEFORE any address resolution.
struct PanicOnResolve;

impl crate::Resolver for PanicOnResolve {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    panic!("the config preflight must reject the node before any address is resolved");
  }
}

fn started_node(
  dev: &mut Loopback,
  now: memberlist_proto::Instant,
) -> Memberlist<SmolStr, SocketAddr, Loopback> {
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
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
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
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
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
fn join_accepts_a_full_bounded_resolution() {
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);

  // A resolver that fills the bounded result to capacity still joins. The
  // `ResolvedAddrs` type caps the count at `MAX_RESOLVED_ADDRS_PER_SEED`, so the
  // driver never needs a post-hoc `.take`; a resolver simply cannot hand back an
  // oversized result for the driver to allocate.
  m.join(
    &FullResolver,
    &[crate::MaybeResolved::Unresolved(addr(7947))],
  )
  .expect("a full bounded resolution joins");
}

#[test]
fn invalid_config_is_rejected_before_resolution() {
  use Duration;

  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));

  // A zero close timeout is an advertise-independent misconfiguration. The
  // construction preflight must reject it BEFORE the advertise address is
  // resolved, so `PanicOnResolve` is never called. (`Memberlist` is not `Debug`,
  // so match the result rather than `expect_err`.)
  let cfg = crate::Options::new().with_close_timeout(Duration::ZERO);
  let res = Memberlist::<SmolStr, SocketAddr, _>::try_new(
    cfg,
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &PanicOnResolve,
    &mut dev,
    now,
  );
  match res {
    Err(InitError::ZeroCloseTimeout) => {}
    Err(other) => panic!("expected ZeroCloseTimeout from the preflight, got {other:?}"),
    Ok(_) => panic!("a zero close timeout must be rejected at construction"),
  }
}
