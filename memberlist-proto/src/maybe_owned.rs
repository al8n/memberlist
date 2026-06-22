//! [`MaybeOwned`] — a borrowed-or-owned `?Sized` value.
//!
//! Like [`Cow`](std::borrow::Cow), but the owned side is a [`Box<T>`] rather
//! than `<T as ToOwned>::Owned`, so naming the type needs no `T: Clone` /
//! `ToOwned` bound. That lets it carry a slice whose element `Clone` is
//! conditional on generic parameters — e.g. `MaybeOwned<'_, [NodeState<I, A>]>`,
//! where `NodeState`'s `Clone` needs `I: Clone` and `A: Clone`. Used by
//! [`MergeDelegate`](crate::delegate::MergeDelegate) to hand a delegate the
//! remote peer view the machine already owns: a predicate borrows it through the
//! [`Deref`], a recorder takes it with [`into_vec`].
//!
//! [`into_vec`]: MaybeOwned::into_vec

use core::ops::Deref;
use std::{boxed::Box, vec::Vec};

/// A value that is either borrowed (`&'a T`) or owned ([`Box<T>`]).
///
/// Read it through the [`Deref`] to `T` (`view.iter()`, `&*view`); for a slice
/// `[E]`, take the elements with [`into_vec`](MaybeOwned::into_vec), which is
/// allocation-free when already [`Owned`](MaybeOwned::Owned).
#[derive(Debug)]
pub enum MaybeOwned<'a, T>
where
  T: ?Sized,
{
  /// A borrowed value.
  Borrowed(&'a T),
  /// An owned value.
  Owned(Box<T>),
}

impl<T> MaybeOwned<'_, T>
where
  T: ?Sized,
{
  /// Whether this is the [`Borrowed`](MaybeOwned::Borrowed) variant.
  #[inline]
  pub const fn is_borrowed(&self) -> bool {
    matches!(self, Self::Borrowed(_))
  }

  /// Whether this is the [`Owned`](MaybeOwned::Owned) variant.
  #[inline]
  pub const fn is_owned(&self) -> bool {
    matches!(self, Self::Owned(_))
  }
}

impl<T> MaybeOwned<'_, [T]>
where
  T: Clone,
{
  /// Take the elements as a `Vec`: allocation-free when already
  /// [`Owned`](MaybeOwned::Owned) (the boxed slice is unboxed in place), clones
  /// a [`Borrowed`](MaybeOwned::Borrowed) slice.
  #[inline]
  pub fn into_vec(self) -> Vec<T> {
    match self {
      Self::Borrowed(slice) => slice.to_vec(),
      Self::Owned(boxed) => boxed.into_vec(),
    }
  }
}

impl<T> Deref for MaybeOwned<'_, T>
where
  T: ?Sized,
{
  type Target = T;

  #[inline]
  fn deref(&self) -> &T {
    match self {
      Self::Borrowed(value) => value,
      Self::Owned(boxed) => boxed,
    }
  }
}

impl<'a, T> From<&'a T> for MaybeOwned<'a, T>
where
  T: ?Sized,
{
  #[inline]
  fn from(value: &'a T) -> Self {
    Self::Borrowed(value)
  }
}

impl<T> From<Box<T>> for MaybeOwned<'_, T>
where
  T: ?Sized,
{
  #[inline]
  fn from(boxed: Box<T>) -> Self {
    Self::Owned(boxed)
  }
}

impl<T> From<Vec<T>> for MaybeOwned<'_, [T]> {
  #[inline]
  fn from(vec: Vec<T>) -> Self {
    Self::Owned(vec.into_boxed_slice())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn borrowed_arm_predicates_deref_and_from_ref() {
    let value = 7u32;
    // `From<&T>` builds the Borrowed variant.
    let mo: MaybeOwned<'_, u32> = MaybeOwned::from(&value);
    assert!(mo.is_borrowed());
    assert!(!mo.is_owned());
    // Deref's Borrowed arm reaches the underlying value.
    assert_eq!(*mo, 7);
    assert_eq!(mo.checked_add(1), Some(8));
  }

  #[test]
  fn owned_arm_predicates_deref_and_from_box() {
    let mo: MaybeOwned<'_, u32> = MaybeOwned::from(Box::new(9u32));
    assert!(mo.is_owned());
    assert!(!mo.is_borrowed());
    // Deref's Owned arm reaches through the Box.
    assert_eq!(*mo, 9);
  }

  #[test]
  fn into_vec_clones_a_borrowed_slice() {
    let data = [1u32, 2, 3];
    let mo: MaybeOwned<'_, [u32]> = MaybeOwned::from(&data[..]);
    assert!(mo.is_borrowed());
    // Deref to the slice works (Borrowed arm) before consuming.
    assert_eq!(mo.len(), 3);
    assert_eq!(mo.into_vec(), std::vec![1, 2, 3]);
  }

  #[test]
  fn into_vec_unboxes_an_owned_slice_in_place() {
    // `From<Vec<T>>` boxes the slice; into_vec unboxes it (the Owned arm).
    let mo: MaybeOwned<'_, [u32]> = MaybeOwned::from(std::vec![4u32, 5, 6]);
    assert!(mo.is_owned());
    assert_eq!(&*mo, &[4, 5, 6]);
    assert_eq!(mo.into_vec(), std::vec![4, 5, 6]);
  }

  #[test]
  fn from_boxed_slice_is_owned() {
    let boxed: Box<[u32]> = std::vec![10u32, 11].into_boxed_slice();
    let mo: MaybeOwned<'_, [u32]> = MaybeOwned::from(boxed);
    assert!(mo.is_owned());
    assert_eq!(mo.into_vec(), std::vec![10, 11]);
  }

  #[test]
  fn unsized_str_target_through_both_arms() {
    // `?Sized` target: a borrowed `str` and an owned `Box<str>` both Deref.
    let borrowed: MaybeOwned<'_, str> = MaybeOwned::from("hi");
    assert!(borrowed.is_borrowed());
    assert_eq!(&*borrowed, "hi");

    let owned: MaybeOwned<'_, str> =
      MaybeOwned::from(std::string::String::from("yo").into_boxed_str());
    assert!(owned.is_owned());
    assert_eq!(&*owned, "yo");
  }
}
