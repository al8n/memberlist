//! Boundary conversions between the driver's `core::net::SocketAddr` /
//! `memberlist_machine::Instant` and smoltcp's `IpEndpoint` / `Instant`.

use core::net::SocketAddr;
use memberlist_machine::Instant;
use smoltcp::{time::Instant as SmoltcpInstant, wire::IpEndpoint};

/// Convert a `SocketAddr` to a smoltcp `IpEndpoint`.
///
/// memberlist addresses are family-agnostic configuration, so this boundary
/// helper accepts both IPv4 and IPv6. smoltcp's `From<SocketAddr> for Endpoint`
/// requires both `proto-ipv4` and `proto-ipv6`, which this crate enables.
#[inline]
pub(crate) fn to_endpoint(addr: SocketAddr) -> IpEndpoint {
  addr.into()
}

/// Convert a smoltcp `IpEndpoint` back to a `SocketAddr`.
#[inline]
pub(crate) fn from_endpoint(ep: IpEndpoint) -> SocketAddr {
  ep.into()
}

/// Convert a `memberlist_machine::Instant` to a smoltcp `Instant` (millisecond granularity).
#[inline]
pub(crate) fn to_smoltcp_instant(now: Instant) -> SmoltcpInstant {
  debug_assert!(
    now.since_origin().as_millis() <= i64::MAX as u128,
    "instant too far from origin for smoltcp"
  );
  SmoltcpInstant::from_millis(now.since_origin().as_millis() as i64)
}

/// Convert a smoltcp `Instant` back to a `memberlist_machine::Instant`.
#[inline]
pub(crate) fn from_smoltcp_instant(t: SmoltcpInstant) -> Instant {
  debug_assert!(t.total_millis() >= 0, "smoltcp instant before origin");
  Instant::from_origin(core::time::Duration::from_millis(t.total_millis() as u64))
}

#[cfg(test)]
mod tests {
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
    let now = memberlist_machine::Instant::from_origin(core::time::Duration::from_millis(123_456));
    let s = to_smoltcp_instant(now);
    assert_eq!(from_smoltcp_instant(s), now);
  }
}
