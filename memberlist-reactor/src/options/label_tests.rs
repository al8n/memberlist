use super::*;
use crate::error::Error;

#[test]
fn with_label_validates_too_long() {
  let long = vec![b'x'; 254]; // one over the 253-byte max
  let result = MemberlistOptions::new().with_label(Some(long));
  assert!(
    matches!(result, Err(Error::InvalidLabel(_))),
    "a label exceeding 253 bytes must be rejected"
  );
}

#[test]
fn with_label_validates_non_utf8() {
  let bad = vec![0xff, 0xfe];
  let result = MemberlistOptions::new().with_label(Some(bad));
  assert!(
    matches!(result, Err(Error::InvalidLabel(_))),
    "a non-UTF-8 label must be rejected"
  );
}

#[test]
fn with_label_accepts_valid() {
  let opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid ASCII label must be accepted");
  assert_eq!(opts.label(), Some(b"cluster-x".as_slice()));
  assert!(!opts.skip_inbound_label_check());
}

#[test]
fn with_skip_inbound_label_check_round_trips() {
  let opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid label")
    .with_skip_inbound_label_check(true);
  assert_eq!(opts.label(), Some(b"cluster-x".as_slice()));
  assert!(opts.skip_inbound_label_check());
}

#[test]
fn empty_label_normalizes_to_none() {
  let opts = MemberlistOptions::new()
    .with_label(Some(Vec::new()))
    .expect("empty label is valid");
  assert_eq!(opts.label(), None);
}
