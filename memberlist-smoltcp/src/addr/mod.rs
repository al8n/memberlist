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

/// The largest millisecond value [`to_smoltcp_instant`] will scale into
/// smoltcp's internal `i64` microseconds.
///
/// smoltcp's `Instant` is `{ micros: i64 }`, and `Instant::from_millis`
/// computes `millis * 1000` before storing it — so the representable bound is
/// `i64::MAX` MICROSECONDS divided by 1000, not `i64::MAX` MILLISECONDS, which
/// would overflow that multiply and wrap negative.
const MAX_INSTANT_MILLIS: i64 = i64::MAX / 1000;

/// Convert a `memberlist_proto::Instant` to a smoltcp `Instant` (millisecond granularity).
///
/// Saturates `since_origin`'s milliseconds at [`MAX_INSTANT_MILLIS`] rather than
/// relying on a release-silent `debug_assert`, so an extreme instant still
/// yields a valid, positive, far-future smoltcp instant instead of a value that
/// overflows `from_millis`'s internal `* 1000` and wraps negative.
#[inline]
pub(crate) fn to_smoltcp_instant(now: Instant) -> SmoltcpInstant {
  let millis = now
    .since_origin()
    .as_millis()
    .min(MAX_INSTANT_MILLIS as u128) as i64;
  SmoltcpInstant::from_millis(millis)
}

/// Convert a smoltcp `Instant` back to a `memberlist_proto::Instant`.
#[inline]
pub(crate) fn from_smoltcp_instant(t: SmoltcpInstant) -> Instant {
  // Bounded by construction, not merely by convention: every smoltcp `Instant`
  // this driver converts back is `poll_at`'s return, computed by smoltcp's own
  // arithmetic over an `s_now` this driver produced via `to_smoltcp_instant`
  // above (itself always non-negative). A `debug_assert` catches a violation of
  // that invariant in development; release builds do not need to pay for a
  // saturating branch on a direction that cannot go negative without a bug
  // elsewhere in smoltcp's own timer arithmetic.
  debug_assert!(t.total_millis() >= 0, "smoltcp instant before origin");
  Instant::from_origin(core::time::Duration::from_millis(t.total_millis() as u64))
}

/// The largest `Duration`, in microseconds, [`to_smoltcp_duration`] will
/// install as a socket timeout: ~100 years, trivially safe headroom under
/// smoltcp's `i64::MAX` microsecond range (~292,471 years) even after adding a
/// running node's `since_origin` micros.
const MAX_TIMEOUT_MICROS: u64 = 100 * 365 * 24 * 60 * 60 * 1_000_000;

/// Convert a `core::time::Duration` to a smoltcp `Duration` (microsecond granularity).
///
/// Used to install the reliable-plane socket inactivity timeout. smoltcp
/// stores `Duration` as unsigned microseconds, but `Instant + Duration`
/// computes `self.micros + rhs.total_micros() as i64` — it casts those `u64`
/// microseconds to `i64` *before* adding. A `Duration` saturated all the way to
/// `u64::MAX` microseconds therefore casts to **-1**, installing a deadline one
/// tick BEFORE `now` and aborting the socket immediately — the opposite of a
/// "far future" timeout. A sub-microsecond `Duration` has the mirror problem:
/// it truncates to `0`, which smoltcp treats as abort-on-any-inactivity. Clamp
/// instead into `[1 µs, MAX_TIMEOUT_MICROS]`: at least one tick, and far enough
/// below `i64::MAX` that `Instant::now() + this` can never itself overflow or
/// cast negative. The value this crate feeds it — the `close_timeout`, default
/// 10 s — is far inside range.
#[inline]
pub(crate) fn to_smoltcp_duration(d: core::time::Duration) -> SmoltcpDuration {
  let micros = d.as_micros().clamp(1, MAX_TIMEOUT_MICROS as u128) as u64;
  SmoltcpDuration::from_micros(micros)
}

#[cfg(test)]
mod tests;
