use super::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

fn v4(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}
fn v6(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
}

#[test]
fn first_addr_returns_first() {
  let r = FirstAddrResolver;
  let picked = r.pick(vec![v4(1), v4(2), v4(3)]).unwrap();
  assert_eq!(picked, v4(1));
}

#[test]
fn first_addr_errors_on_empty() {
  let r = FirstAddrResolver;
  let err = r.pick(vec![]).unwrap_err();
  assert!(matches!(err, AdvertiseResolutionError::Empty));
}

#[test]
fn ipv4_preferring_picks_ipv4_when_present() {
  let r = Ipv4PreferringResolver;
  let picked = r.pick(vec![v6(1), v4(2), v4(3)]).unwrap();
  assert_eq!(picked, v4(2));
}

#[test]
fn ipv4_preferring_falls_through_to_first_when_no_ipv4() {
  let r = Ipv4PreferringResolver;
  let picked = r.pick(vec![v6(1), v6(2)]).unwrap();
  assert_eq!(picked, v6(1));
}

#[test]
fn ipv4_preferring_errors_on_empty() {
  let r = Ipv4PreferringResolver;
  assert!(r.pick(vec![]).is_err());
}

#[test]
fn ipv6_preferring_picks_ipv6_when_present() {
  let r = Ipv6PreferringResolver;
  let picked = r.pick(vec![v4(1), v6(2), v4(3)]).unwrap();
  assert_eq!(picked, v6(2));
}

#[test]
fn ipv6_preferring_falls_through_to_first_when_no_ipv6() {
  let r = Ipv6PreferringResolver;
  let picked = r.pick(vec![v4(1), v4(2)]).unwrap();
  assert_eq!(picked, v4(1));
}
