//! CIDR peer-admission glue for the embedded engine.
//!
//! One [`with_cidr_policy`](crate::Options::with_cidr_policy) setting enforces a
//! [`CidrPolicy`](memberlist_proto::CidrPolicy) at the three IP-observable points
//! of a node: the gossip datagram source (engine recv) and the reliable peer
//! (engine accept), both via [`cidr_blocks`] at the transport boundary; and the
//! self-advertised address (membership admission), via the engine installing the
//! policy as the inner of its built-in routable-address alive filter.
//! [`CidrFilter`] carries the policy through the engine field without a `cfg` gate.

use core::net::IpAddr;
/// The CIDR policy carried by the engine: the real
/// [`CidrPolicy`](memberlist_proto::CidrPolicy) when the `cidr` feature is on, the
/// zero-sized `()` otherwise — so the engine field needs no `cfg` gate.
#[cfg(feature = "cidr")]
pub(crate) type CidrFilter = Option<memberlist_proto::CidrPolicy>;
#[cfg(not(feature = "cidr"))]
pub(crate) type CidrFilter = ();

/// Whether `ip` is blocked by the policy — the transport-boundary half of the
/// filter, applied to a gossip datagram source (recv) and a reliable peer
/// (accept). Always `false` when the `cidr` feature is off, so the guards
/// compile away to nothing.
#[cfg(feature = "cidr")]
#[inline]
pub(crate) fn cidr_blocks(filter: &CidrFilter, ip: IpAddr) -> bool {
  filter.as_ref().is_some_and(|policy| policy.is_blocked(&ip))
}
#[cfg(not(feature = "cidr"))]
#[inline]
pub(crate) fn cidr_blocks(_filter: &CidrFilter, _ip: IpAddr) -> bool {
  false
}
