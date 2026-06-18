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

impl<I, A> EventDelegate for VoidDelegate<I, A>
where
  I: 'static,
  A: 'static,
{
  type Id = I;
  type Address = A;
}

impl<I, A> ConflictDelegate for VoidDelegate<I, A>
where
  I: 'static,
  A: 'static,
{
  type Id = I;
  type Address = A;
}

impl<I, A> PingDelegate for VoidDelegate<I, A>
where
  I: 'static,
  A: 'static,
{
  type Id = I;
  type Address = A;
}

impl<I, A> NodeDelegate for VoidDelegate<I, A>
where
  I: 'static,
  A: 'static,
{
}

impl<I, A> Delegate for VoidDelegate<I, A>
where
  I: 'static,
  A: 'static,
{
  type Id = I;
  type Address = A;
}
