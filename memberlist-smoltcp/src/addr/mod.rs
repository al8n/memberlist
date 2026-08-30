//! Boundary conversions between the driver's `core::net::SocketAddr` /
//! `memberlist_proto::Instant` and smoltcp's `IpEndpoint` / `Instant`.

use core::net::SocketAddr;
use memberlist_proto::Instant;
use smoltcp::{
  time::{Duration as SmoltcpDuration, Instant as SmoltcpInstant},
  wire::IpEndpoint,
};

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

/// Convert a `memberlist_proto::Instant` to a smoltcp `Instant` (millisecond granularity).
#[inline]
pub(crate) fn to_smoltcp_instant(now: Instant) -> SmoltcpInstant {
  debug_assert!(
    now.since_origin().as_millis() <= i64::MAX as u128,
    "instant too far from origin for smoltcp"
  );
  SmoltcpInstant::from_millis(now.since_origin().as_millis() as i64)
}

/// Convert a smoltcp `Instant` back to a `memberlist_proto::Instant`.
#[inline]
pub(crate) fn from_smoltcp_instant(t: SmoltcpInstant) -> Instant {
  debug_assert!(t.total_millis() >= 0, "smoltcp instant before origin");
  Instant::from_origin(core::time::Duration::from_millis(t.total_millis() as u64))
}

/// Convert a `core::time::Duration` to a smoltcp `Duration` (microsecond granularity).
///
/// Used to install the reliable-plane socket inactivity timeout. Saturates at
/// `u64::MAX` microseconds so an out-of-range configuration yields a finite (very
/// large) timeout rather than overflowing smoltcp's own `From` conversion, which
/// multiplies whole seconds by 1_000_000. The value this crate feeds it — the
/// `close_timeout`, default 10 s — is far inside range.
#[inline]
pub(crate) fn to_smoltcp_duration(d: core::time::Duration) -> SmoltcpDuration {
  SmoltcpDuration::from_micros(u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests;
