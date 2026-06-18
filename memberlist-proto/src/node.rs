//! A node identifier paired with its advertised address.

use cheap_clone::CheapClone;
use core::fmt;

/// A node identifier `I` paired with its advertised address `A`.
///
/// Accessor-only; construct via [`Node::new`]. `Copy` is implemented when
/// both `I` and `A` are `Copy`, mirroring the standard pattern for small
/// container types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Node<I, A> {
  id: I,
  addr: A,
}

impl<I, A> Node<I, A> {
  /// Construct a new node pair.
  #[inline(always)]
  pub const fn new(id: I, addr: A) -> Self {
    Self { id, addr }
  }

  /// Borrow the node identifier.
  #[inline(always)]
  pub const fn id_ref(&self) -> &I {
    &self.id
  }

  /// Borrow the advertised address.
  #[inline(always)]
  pub const fn addr_ref(&self) -> &A {
    &self.addr
  }

  /// Consume and return the `(id, addr)` pair.
  #[inline(always)]
  pub fn into_parts(self) -> (I, A) {
    (self.id, self.addr)
  }
}

impl<I, A> fmt::Display for Node<I, A>
where
  I: fmt::Display,
  A: fmt::Display,
{
  #[inline]
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}({})", self.id, self.addr)
  }
}

impl<I, A> CheapClone for Node<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
  fn cheap_clone(&self) -> Self {
    Self {
      id: self.id.cheap_clone(),
      addr: self.addr.cheap_clone(),
    }
  }
}
