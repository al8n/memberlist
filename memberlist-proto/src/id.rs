//! Marker supertrait for node identifier types.

pub use cheap_clone::CheapClone;
use core::{cmp::Eq, fmt, hash::Hash};

/// Marker supertrait for node identifier types.
///
/// A node identifier is a wire-encodable, cheap-clonable, hashable,
/// displayable value with a `'static` lifetime — the union of bounds
/// memberlist machines need on the `I` type parameter throughout.
pub trait Id:
  crate::Data + CheapClone + Hash + Eq + fmt::Debug + fmt::Display + Send + Sync + 'static
{
}

impl<T> Id for T where
  T: crate::Data + CheapClone + Hash + Eq + fmt::Debug + fmt::Display + Send + Sync + 'static
{
}
