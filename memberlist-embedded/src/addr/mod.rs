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

/// A stable per-address key, for ordering addresses by identity alone.
///
/// The value depends only on the address — its family, its IP bytes (IPv4 zero
/// extended into the low half, IPv6 taken whole) and its port — so two callers
/// naming the same endpoint get the same key, and a caller that reorders its
/// list, or re-resolves it into a different order, does not change any key.
/// That is what lets a queue with room for only part of an offered set pick the
/// same subset however the set is presented, and step to the next subset on the
/// following offer.
///
/// The mixing is a plain [splitmix64] finalizer chain: it spreads addresses that
/// differ in one octet or only in the port across the whole 64-bit range, so
/// consecutive addresses in a subnet do not clump into one arc of the key
/// circle. It is deliberately unkeyed and unseeded — stability WITHIN a process
/// is the whole requirement, and seeds are local configuration under this
/// crate's crash-stop threat model rather than attacker-supplied input, so
/// there is no adversary to make key collisions worth defending against.
///
/// [splitmix64]: https://prng.di.unimi.it/splitmix64.c
#[inline]
#[must_use]
pub(crate) fn seed_rank_key(addr: &SocketAddr) -> u64 {
  // The family tag keeps an IPv4 address distinct from the IPv6 address holding
  // the same trailing bytes (`10.0.0.1` vs `::10.0.0.1`), which are different
  // destinations on the wire.
  let (family, high, low) = match addr.ip() {
    IpAddr::V4(v4) => (4u64, 0u64, u64::from(u32::from_be_bytes(v4.octets()))),
    IpAddr::V6(v6) => {
      let octets = v6.octets();
      let mut high = [0u8; 8];
      let mut low = [0u8; 8];
      high.copy_from_slice(&octets[..8]);
      low.copy_from_slice(&octets[8..]);
      (6u64, u64::from_be_bytes(high), u64::from_be_bytes(low))
    }
  };

  let key = splitmix64_finalize(family ^ high);
  let key = splitmix64_finalize(key ^ low);
  splitmix64_finalize(key ^ u64::from(addr.port()))
}

/// The splitmix64 finalizer: an invertible avalanche over 64 bits.
#[inline]
const fn splitmix64_finalize(z: u64) -> u64 {
  let z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
  let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
  z ^ (z >> 31)
}

#[cfg(test)]
mod tests;
