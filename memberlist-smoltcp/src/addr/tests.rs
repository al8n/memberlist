use super::*;
use core::net::{IpAddr, Ipv4Addr, SocketAddr};

#[test]
fn socket_addr_round_trips_through_ip_endpoint() {
  let sa = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 7946);
  let ep = to_endpoint(sa);
  assert_eq!(from_endpoint(ep), sa);
}

#[test]
fn ipv6_socket_addr_round_trips() {
  use core::net::Ipv6Addr;
  let sa = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 7946);
  assert_eq!(from_endpoint(to_endpoint(sa)), sa);
}

#[test]
fn machine_instant_round_trips_through_smoltcp_instant() {
  let now = memberlist_proto::Instant::from_origin(core::time::Duration::from_millis(123_456));
  let s = to_smoltcp_instant(now);
  assert_eq!(from_smoltcp_instant(s), now);
}
