use super::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

fn v4(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}
fn v6(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
}
/// An IPv4-mapped IPv6 address (`::ffff:127.0.0.1`) — an IPv6 `SocketAddr` that
/// names an IPv4 host.
fn mapped(port: u16) -> SocketAddr {
  SocketAddr::new(
    IpAddr::V6(Ipv4Addr::new(127, 0, 0, 1).to_ipv6_mapped()),
    port,
  )
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

// A mapped candidate is classified by its canonical family: IPv4-preferring
// selects it (it names an IPv4 host), and IPv6-preferring skips it in favor of a
// genuine IPv6 candidate.
#[test]
fn preferring_resolvers_classify_mapped_as_ipv4() {
  let mapped_first = vec![mapped(1), v6(2)];
  assert_eq!(
    Ipv4PreferringResolver.pick(mapped_first.clone()).unwrap(),
    mapped(1),
    "IPv4-preferring must select a mapped candidate as IPv4"
  );
  assert_eq!(
    Ipv6PreferringResolver.pick(mapped_first).unwrap(),
    v6(2),
    "IPv6-preferring must prefer the genuine IPv6 over a mapped (canonical-IPv4) candidate"
  );

  // With only a mapped candidate, IPv6-preferring falls through to it (first of
  // any family), since no genuine IPv6 candidate is present.
  assert_eq!(
    Ipv6PreferringResolver.pick(vec![mapped(3)]).unwrap(),
    mapped(3),
    "with no genuine IPv6, IPv6-preferring falls through to the only candidate"
  );
}
