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
