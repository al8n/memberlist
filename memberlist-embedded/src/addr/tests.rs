use super::*;
use core::net::{Ipv4Addr, Ipv6Addr};

#[test]
fn routable_unicast_with_port() {
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 7946);
  assert!(socket_addr_is_routable(&v4));
  let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 7946);
  assert!(socket_addr_is_routable(&v6));
}

#[test]
fn port_zero_is_not_routable() {
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 0);
  assert!(!socket_addr_is_routable(&v4));
}

#[test]
fn unspecified_is_not_routable() {
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7946);
  assert!(!socket_addr_is_routable(&v4));
  let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 7946);
  assert!(!socket_addr_is_routable(&v6));
}

#[test]
fn multicast_is_not_routable() {
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 7946);
  assert!(!socket_addr_is_routable(&v4));
  let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)), 7946);
  assert!(!socket_addr_is_routable(&v6));
}

#[test]
fn ipv4_limited_broadcast_is_not_routable() {
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), 7946);
  assert!(!socket_addr_is_routable(&v4));
}

#[test]
fn seed_rank_key_is_stable_for_one_address() {
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 7946);
  assert_eq!(seed_rank_key(&v4), seed_rank_key(&v4));
  let same = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 7946);
  assert_eq!(
    seed_rank_key(&v4),
    seed_rank_key(&same),
    "the key must come from the address alone, so two spellings of one endpoint rank together"
  );
  let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 7)), 7946);
  assert_eq!(seed_rank_key(&v6), seed_rank_key(&v6));
}

#[test]
fn seed_rank_key_separates_ports_on_one_ip() {
  let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7));
  let a = SocketAddr::new(ip, 7946);
  let b = SocketAddr::new(ip, 7947);
  assert_ne!(
    seed_rank_key(&a),
    seed_rank_key(&b),
    "two ports on one host are two destinations and must rank apart"
  );

  let ip6 = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 7));
  assert_ne!(
    seed_rank_key(&SocketAddr::new(ip6, 7946)),
    seed_rank_key(&SocketAddr::new(ip6, 7947))
  );
}

#[test]
fn seed_rank_key_separates_distinct_addresses() {
  // Neighbouring IPv4 hosts on one port, the shape a seed list actually takes.
  let keys = [
    seed_rank_key(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
      7946,
    )),
    seed_rank_key(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
      7946,
    )),
    seed_rank_key(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)),
      7946,
    )),
    seed_rank_key(&SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)),
      7946,
    )),
    seed_rank_key(&SocketAddr::new(
      IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)),
      7946,
    )),
    seed_rank_key(&SocketAddr::new(
      IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 2)),
      7946,
    )),
  ];
  for (i, a) in keys.iter().enumerate() {
    for b in &keys[i + 1..] {
      assert_ne!(a, b, "distinct addresses must get distinct rank keys");
    }
  }
}

#[test]
fn seed_rank_key_separates_the_families() {
  // `10.0.0.1` and `::10.0.0.1` carry the same trailing bytes but are different
  // destinations on the wire, so the family must reach the key.
  let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7946);
  let v6 = SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0x0a00, 0x0001)),
    7946,
  );
  assert_ne!(seed_rank_key(&v4), seed_rank_key(&v6));
}
