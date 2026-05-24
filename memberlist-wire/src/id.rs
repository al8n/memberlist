//! Marker supertrait for node identifier types.

pub use cheap_clone::CheapClone;

/// Marker supertrait for node identifier types.
///
/// A node identifier is a wire-encodable, cheap-clonable, hashable,
/// displayable value with a `'static` lifetime — the union of bounds
/// memberlist machines need on the `I` type parameter throughout.
pub trait Id:
  crate::Data
  + CheapClone
  + core::hash::Hash
  + core::cmp::Eq
  + core::fmt::Debug
  + core::fmt::Display
  + Send
  + Sync
  + 'static
{
}

impl<T> Id for T where
  T: crate::Data
    + CheapClone
    + core::hash::Hash
    + core::cmp::Eq
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static
{
}
