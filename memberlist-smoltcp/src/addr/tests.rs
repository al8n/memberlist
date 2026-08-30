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

#[test]
fn duration_converts_to_smoltcp_micros() {
  use smoltcp::time::Duration as SmoltcpDuration;
  // The default close_timeout (10 s), the value the driver installs as the socket
  // inactivity timeout, converts exactly.
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::from_secs(10)),
    SmoltcpDuration::from_secs(10)
  );
  // Sub-second precision is preserved at microsecond granularity.
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::from_micros(1_500_250)),
    SmoltcpDuration::from_micros(1_500_250)
  );
  // A non-zero timeout never converts to smoltcp's zero (which it treats as an
  // immediate abort of every socket).
  assert_ne!(
    to_smoltcp_duration(core::time::Duration::from_millis(1)),
    SmoltcpDuration::from_micros(0)
  );
}

#[test]
fn out_of_range_duration_saturates_without_panicking() {
  use smoltcp::time::Duration as SmoltcpDuration;
  // A duration whose microseconds exceed u64 saturates to a finite (very large)
  // timeout instead of overflowing smoltcp's own `From`, which multiplies whole
  // seconds by 1_000_000.
  assert_eq!(
    to_smoltcp_duration(core::time::Duration::MAX),
    SmoltcpDuration::from_micros(u64::MAX)
  );
}
