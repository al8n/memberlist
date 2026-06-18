use super::*;

fn _assert_trait_namable<T>()
where
  T: Transport,
{
  // Unused: compile-time namability assertion — value intentionally discarded.
  let _ = core::any::TypeId::of::<T::Id>();
}
