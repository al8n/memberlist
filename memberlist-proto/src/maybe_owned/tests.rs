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
