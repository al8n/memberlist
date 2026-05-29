//! `VoidDelegate<I, A>` — zero-cost default observation Delegate. Every hook
//! is a no-op (or returns default values).

use core::marker::PhantomData;

use super::{ConflictDelegate, Delegate, EventDelegate, NodeDelegate, PingDelegate};

/// Zero-cost default observation delegate. Every hook is a no-op.
pub struct VoidDelegate<I, A> {
  _phantom: PhantomData<fn(I, A)>,
}

impl<I, A> VoidDelegate<I, A> {
  /// Construct.
  #[inline]
  pub const fn new() -> Self {
    Self {
      _phantom: PhantomData,
    }
  }
}

impl<I, A> Default for VoidDelegate<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I: 'static, A: 'static> EventDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;
}

impl<I: 'static, A: 'static> ConflictDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;
}

impl<I: 'static, A: 'static> PingDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;
}

impl<I: 'static, A: 'static> NodeDelegate for VoidDelegate<I, A> {}

impl<I: 'static, A: 'static> Delegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;
}
