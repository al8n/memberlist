//! `MaybeResolved<A, R>` — caller-side input that may or may not be
//! pre-resolved. Used by `Transport::new` (advertise address) and
//! `Memberlist::join` / `join_many` (seeds).

use memberlist_wire::CheapClone;

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
mod tests {
  use super::*;
  use std::net::{IpAddr, Ipv4Addr, SocketAddr};

  #[test]
  fn resolved_constructor_and_accessors() {
    let sa: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
    let m: MaybeResolved<String, SocketAddr> = MaybeResolved::resolved(sa);
    assert!(m.is_resolved());
    assert!(!m.is_unresolved());
    match m.as_ref() {
      MaybeResolved::Resolved(r) => assert_eq!(r, &sa),
      MaybeResolved::Unresolved(_) => panic!("expected Resolved"),
    }
  }

  #[test]
  fn unresolved_constructor_and_accessors() {
    let m: MaybeResolved<String, SocketAddr> = MaybeResolved::unresolved("host:7946".to_string());
    assert!(!m.is_resolved());
    assert!(m.is_unresolved());
    match m.as_ref() {
      MaybeResolved::Unresolved(a) => assert_eq!(a.as_str(), "host:7946"),
      MaybeResolved::Resolved(_) => panic!("expected Unresolved"),
    }
  }

  #[test]
  fn cheap_clone_resolved() {
    let sa: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
    let m: MaybeResolved<smol_str::SmolStr, SocketAddr> = MaybeResolved::resolved(sa);
    use memberlist_wire::CheapClone;
    let m2 = m.cheap_clone();
    assert!(m2.is_resolved());
  }

  #[test]
  fn cheap_clone_unresolved() {
    let m: MaybeResolved<smol_str::SmolStr, SocketAddr> =
      MaybeResolved::unresolved(smol_str::SmolStr::new("host:7946"));
    use memberlist_wire::CheapClone;
    let m2 = m.cheap_clone();
    assert!(m2.is_unresolved());
  }
}
