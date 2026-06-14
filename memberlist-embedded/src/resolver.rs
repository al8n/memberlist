//! The [`MaybeResolved`] seed/advertise form shared by the embedded drivers.

/// A seed or advertise address that is either an already-resolved wire
/// [`SocketAddr`](core::net::SocketAddr) or an unresolved address to pass
/// through a resolver.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MaybeResolved<A> {
  /// An already-resolved wire address — used verbatim, no resolver needed.
  Resolved(core::net::SocketAddr),
  /// An unresolved address, resolved at the driver boundary.
  Unresolved(A),
}

impl<A> MaybeResolved<A> {
  /// Whether this is an already-resolved wire address.
  #[inline]
  pub const fn is_resolved(&self) -> bool {
    matches!(self, MaybeResolved::Resolved(_))
  }

  /// Whether this is an unresolved address awaiting the driver boundary.
  #[inline]
  pub const fn is_unresolved(&self) -> bool {
    matches!(self, MaybeResolved::Unresolved(_))
  }
}
