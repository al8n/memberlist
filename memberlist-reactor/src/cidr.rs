//! CIDR peer-admission glue for the reactor drivers.
//!
//! One [`with_cidr_policy`](crate::Options::with_cidr_policy) setting enforces a
//! [`CidrPolicy`](memberlist_proto::CidrPolicy) at the three IP-observable points
//! of a node: the gossip datagram source (driver recv) and the reliable peer
//! (stream accept), both via [`cidr_blocks`] at the transport boundary; and the
//! self-advertised address (membership admission), via [`compose_alive`] folding
//! the policy into the alive delegate. [`CidrFilter`] carries the policy through
//! the driver field and constructor signatures without a `cfg` gate at each site.

/// The CIDR policy carried through the driver signatures: the real
/// [`CidrPolicy`](memberlist_proto::CidrPolicy) when the `cidr` feature is on, the
/// zero-sized `()` otherwise — so the [`Options`](crate::Options) field, the
/// driver field, and the constructor parameter need no `cfg` gate.
#[cfg(feature = "cidr")]
pub(crate) type CidrFilter = Option<memberlist_proto::CidrPolicy>;
#[cfg(not(feature = "cidr"))]
pub(crate) type CidrFilter = ();

/// Whether `ip` is blocked by the policy — the transport-boundary half of the
/// filter, applied to a gossip datagram source (recv) and a reliable peer
/// (accept). Always `false` when the `cidr` feature is off, so the guards
/// compile away to nothing.
#[cfg(all(
  feature = "cidr",
  any(feature = "quic", feature = "tcp", feature = "tls")
))]
#[inline]
pub(crate) fn cidr_blocks(filter: &CidrFilter, ip: std::net::IpAddr) -> bool {
  filter.as_ref().is_some_and(|policy| policy.is_blocked(&ip))
}
#[cfg(all(
  not(feature = "cidr"),
  any(feature = "quic", feature = "tcp", feature = "tls")
))]
#[inline]
pub(crate) fn cidr_blocks(_filter: &CidrFilter, _ip: std::net::IpAddr) -> bool {
  false
}

/// Folds the CIDR policy into the user's alive delegate so one policy also gates
/// membership admission by the peer's self-advertised address. With no user
/// delegate the policy stands alone; with one, a peer must pass BOTH. Returns the
/// delegate unchanged when the `cidr` feature is off.
#[cfg(all(
  feature = "cidr",
  any(feature = "quic", feature = "tcp", feature = "tls")
))]
pub(crate) fn compose_alive<I: 'static>(
  filter: &CidrFilter,
  alive: Option<Box<dyn memberlist_proto::AliveDelegate<I, std::net::SocketAddr>>>,
) -> Option<Box<dyn memberlist_proto::AliveDelegate<I, std::net::SocketAddr>>> {
  match (filter.clone(), alive) {
    (Some(policy), Some(user)) => Some(Box::new(memberlist_proto::CidrAnd::new(policy, user))),
    (Some(policy), None) => Some(Box::new(policy)),
    (None, user) => user,
  }
}
#[cfg(all(
  not(feature = "cidr"),
  any(feature = "quic", feature = "tcp", feature = "tls")
))]
pub(crate) fn compose_alive<I>(
  _filter: &CidrFilter,
  alive: Option<Box<dyn memberlist_proto::AliveDelegate<I, std::net::SocketAddr>>>,
) -> Option<Box<dyn memberlist_proto::AliveDelegate<I, std::net::SocketAddr>>> {
  alive
}
