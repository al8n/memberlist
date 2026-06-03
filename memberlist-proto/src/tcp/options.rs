//! Options bundle for the plain-TCP coordinator.
//!
//! # Cluster isolation on the plain-TCP reliable path
//!
//! The plain-TCP coordinator does not carry a TLS record layer. Cluster
//! isolation is instead provided by the **wire label**: a one-time prefix
//! written at stream start in the same byte format used by the frozen
//! `memberlist-proto` label frame (`[LABELED_TAG=12][len][label_bytes]`,
//! `memberlist/src/codec.rs`). Both the local node and every legitimate peer
//! write that prefix; inbound streams that present a different label (or no
//! label when one is configured) are rejected before any memberlist message
//! is decoded.
//!
//! The plain-TCP spelling of the reliable-plane options is
//! [`LabelOptions<()>`](crate::streams::LabelOptions): it bundles the label and
//! the inbound-check policy over the byte pipe's unit inner options. Construct
//! via [`LabelOptions::try_new_in`] (checked) or [`LabelOptions::new_in`]
//! (panics on an invalid label); the label / skip accessors are on
//! [`LabelOptions`].
//!
//! # Label length and encoding constraints
//!
//! The wire format stores the label length in a single `u8` and reserves two
//! bytes for the `[tag][len]` header, leaving **253 bytes** (`u8::MAX - 2`)
//! for the label itself (`memberlist-proto::Label::MAX_SIZE`,
//! `memberlist/src/codec.rs::MAX_LABEL_LEN`). Labels must also be valid
//! UTF-8 (required by `memberlist-proto::Label` so that labels round-trip
//! through `SmolStr` without loss).

#![cfg(feature = "tcp")]

#[cfg(test)]
mod tests {
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
}
