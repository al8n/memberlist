//! `MaybeResolved<A, R>` — caller-side input that may or may not be
//! pre-resolved. Used by `Transport::new` (advertise address) and
//! `Memberlist::join` / `join_many` (seeds).

use memberlist_proto::CheapClone;

/// An address that may or may not be resolved.
///
/// `join`-family methods and `Transport::new` accept `MaybeResolved<A, R>` so
/// callers can hand in either an already-resolved socket address (skip
/// resolution) or an unresolved address (driver resolves via the supplied
/// `Resolver`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaybeResolved<A, R> {
  /// Already resolved — use directly.
  Resolved(R),
  /// Needs resolution before use.
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

  /// `true` if the variant is `Resolved`.
  #[inline]
  pub const fn is_resolved(&self) -> bool {
    matches!(self, Self::Resolved(_))
  }

  /// `true` if the variant is `Unresolved`.
  #[inline]
  pub const fn is_unresolved(&self) -> bool {
    matches!(self, Self::Unresolved(_))
  }
}

impl<A: CheapClone, R: CheapClone> CheapClone for MaybeResolved<A, R> {
  fn cheap_clone(&self) -> Self {
    match self {
      Self::Resolved(r) => Self::Resolved(r.cheap_clone()),
      Self::Unresolved(a) => Self::Unresolved(a.cheap_clone()),
    }
  }
}

#[cfg(test)]
mod tests;
