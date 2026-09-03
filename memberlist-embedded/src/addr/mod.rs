//! Transport-neutral address screening shared by the embedded drivers.
//!
//! The memberlist machine is address-agnostic (`A = core::net::SocketAddr`): a
//! peer can gossip an `Alive` advertising a non-routable address (the
//! unspecified address, a multicast/broadcast IP, or port 0), the machine
//! stores it as a member, and a later gossip / probe / push-pull selects it as
//! a DESTINATION. A driver that does not screen such an address wastes
//! exchanges (the link layer silently drops the datagram and every dial fails)
//! and, on stacks that `assert!(addr.is_unicast())` during routing, risks a
//! release-mode panic. This module is the single transport-neutral validity
//! predicate every boundary that can feed an address toward the link layer
//! applies.

use core::net::{IpAddr, SocketAddr};

/// Whether `addr` is a routable unicast destination with a usable port.
///
/// `true` only for a unicast IP (not unspecified, not multicast, and — for
/// IPv4 — not the limited broadcast address) paired with a non-zero port. This
/// mirrors smoltcp's `IpAddress::is_unicast` (`x_is_unicast`) byte-for-byte:
/// smoltcp's IPv4/IPv6 addresses wrap `core::net::Ipv4Addr`/`Ipv6Addr` and its
/// unicast test is exactly `!(is_broadcast || is_multicast || is_unspecified)`
/// for IPv4 and `!(is_multicast || is_unspecified)` for IPv6. The `port != 0`
/// term additionally rejects port 0, which no socket can connect to or send a
/// meaningful datagram to.
#[inline]
#[must_use]
pub fn socket_addr_is_routable(addr: &SocketAddr) -> bool {
  ip_is_unicast(&addr.ip()) && addr.port() != 0
}

/// Whether `ip` is a unicast address, matching smoltcp's `is_unicast`.
#[inline]
fn ip_is_unicast(ip: &IpAddr) -> bool {
  match ip {
    IpAddr::V4(v4) => !(v4.is_broadcast() || v4.is_multicast() || v4.is_unspecified()),
    IpAddr::V6(v6) => !(v6.is_multicast() || v6.is_unspecified()),
  }
}

#[cfg(test)]
mod tests;
