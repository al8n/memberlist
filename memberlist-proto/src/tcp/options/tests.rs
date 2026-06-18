use crate::streams::{LabelOptions, LabelOptionsError};

#[test]
fn carries_label_and_check_policy() {
  let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
  assert_eq!(cfg.label(), Some(b"cluster-x".as_slice()));
  assert!(!cfg.is_skip_inbound_label_check());
  let cfg = LabelOptions::<()>::new_in(None, ()).skip_inbound_label_check();
  assert_eq!(cfg.label(), None);
  assert!(cfg.is_skip_inbound_label_check());
}

#[test]
fn rejects_overlong_label() {
  let too_long = vec![b'x'; 254];
  assert!(LabelOptions::<()>::try_new_in(Some(too_long), ()).is_err());
}

#[test]
fn empty_label_normalizes_to_none() {
  let cfg = LabelOptions::<()>::try_new_in(Some(Vec::new()), ()).unwrap();
  assert_eq!(cfg.label(), None);
}

#[test]
fn rejects_non_utf8_label() {
  assert!(LabelOptions::<()>::try_new_in(Some(vec![0xff, 0xfe]), ()).is_err());
}

/// `LabelOptionsError` is the single error type for label-validation failures
/// on `LabelOptions::try_new_in`.
#[test]
fn label_options_error_variants_are_exhaustive() {
  let too_long = vec![b'x'; 254];
  let e = LabelOptions::<()>::try_new_in(Some(too_long), ()).unwrap_err();
  assert!(matches!(e, LabelOptionsError::LabelTooLong));
  let e = LabelOptions::<()>::try_new_in(Some(vec![0xff, 0xfe]), ()).unwrap_err();
  assert!(matches!(e, LabelOptionsError::InvalidLabel));
}
