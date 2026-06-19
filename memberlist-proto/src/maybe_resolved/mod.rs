//! [`MaybeResolved`] — a caller-supplied address that may already be a wire
//! address or may still need resolving at the driver boundary.
//!
//! Shared by every driver: `Transport::new` takes one for the advertise
//! address, and the `join` family takes them for the seeds. A caller that
//! already holds a wire [`SocketAddr`] hands in `Resolved` (resolution is
//! skipped); otherwise it hands in `Unresolved`, which the driver passes
//! through its configured resolver.

use crate::CheapClone;
use core::net::SocketAddr;

/// An address that is either already resolved to a wire address or still needs
/// resolving at the driver boundary.
///
/// `R` is the resolved (wire) address type; it defaults to [`SocketAddr`], the
/// wire address every driver resolves to, so single-parameter spellings
/// (`MaybeResolved<A>`) read terse while a driver that resolves to a different
/// wire type can name it explicitly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaybeResolved<A, R = SocketAddr> {
  /// Already resolved — use directly, no resolver needed.
  Resolved(R),
  /// Unresolved — resolved at the driver boundary before use.
  Unresolved(A),
}

impl<A, R> MaybeResolved<A, R> {
  /// Construct a resolved variant.
  #[inline]
  pub const fn resolved(addr: R) -> Self {
    Self::Resolved(addr)
  }

  /// Construct an unresolved variant.
  #[inline]
  pub const fn unresolved(addr: A) -> Self {
    Self::Unresolved(addr)
  }

  /// Borrowing view: `MaybeResolved<A, R>` → `MaybeResolved<&A, &R>`.
  #[inline]
  pub const fn as_ref(&self) -> MaybeResolved<&A, &R> {
    match self {
      Self::Resolved(r) => MaybeResolved::Resolved(r),
      Self::Unresolved(a) => MaybeResolved::Unresolved(a),
    }
  }

  /// Mutable borrowing view.
  #[inline]
  pub const fn as_mut(&mut self) -> MaybeResolved<&mut A, &mut R> {
    match self {
      Self::Resolved(r) => MaybeResolved::Resolved(r),
      Self::Unresolved(a) => MaybeResolved::Unresolved(a),
    }
  }

  /// Whether this is an already-resolved wire address.
  #[inline]
  pub const fn is_resolved(&self) -> bool {
    matches!(self, Self::Resolved(_))
  }

  /// Whether this is an unresolved address awaiting the driver boundary.
  #[inline]
  pub const fn is_unresolved(&self) -> bool {
    matches!(self, Self::Unresolved(_))
  }
}

impl<A, R> CheapClone for MaybeResolved<A, R>
where
  A: CheapClone,
  R: CheapClone,
{
  fn cheap_clone(&self) -> Self {
    match self {
      Self::Resolved(r) => Self::Resolved(r.cheap_clone()),
      Self::Unresolved(a) => Self::Unresolved(a.cheap_clone()),
    }
  }
}

#[cfg(test)]
mod tests;
