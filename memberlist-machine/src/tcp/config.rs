//! Configuration bundle for the plain-TCP coordinator.
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
//! `TcpConfig` bundles the label and the inbound-check policy. It is
//! deliberately accessor-only — all fields are private and the constructor
//! validates the label at construction time.
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

/// Errors returned by [`TcpConfig::try_new`].
#[derive(Debug, thiserror::Error)]
pub enum TcpConfigError {
  /// The label exceeds the 253-byte wire maximum (`u8::MAX - 2`), imposed
  /// by `memberlist-proto::Label::MAX_SIZE` / `memberlist/src/codec.rs`
  /// `MAX_LABEL_LEN`: the tag+len header occupies 2 bytes of the single-`u8`
  /// length field.
  #[error("label too long: {0} bytes (max 253)")]
  LabelTooLong(usize),

  /// The label bytes are not valid UTF-8, as required by
  /// `memberlist-proto::Label` (labels round-trip through `SmolStr`).
  #[error("label is not valid UTF-8")]
  InvalidLabel,
}

/// Maximum label length in bytes, matching `memberlist-proto::Label::MAX_SIZE`
/// and `memberlist/src/codec.rs::MAX_LABEL_LEN` (`u8::MAX - 2 = 253`).
const MAX_LABEL_LEN: usize = u8::MAX as usize - 2;

/// Immutable configuration bundle for the plain-TCP coordinator.
///
/// Carries the cluster label and the inbound label-check policy. All fields
/// are private; use the accessor methods. Construct via [`TcpConfig::try_new`]
/// (checked) or [`TcpConfig::new`] (panics on an invalid label).
pub struct TcpConfig {
  label: Option<Vec<u8>>,
  skip_inbound_label_check: bool,
}

impl TcpConfig {
  /// Build a `TcpConfig`, validating the label.
  ///
  /// - `Some(bytes)` where `bytes.is_empty()` normalizes to `None` (no label).
  /// - A present label must be ≤ 253 bytes (`memberlist-proto::Label::MAX_SIZE`)
  ///   and valid UTF-8 (`memberlist-proto::Label` stores labels as `SmolStr`).
  ///
  /// Returns [`TcpConfigError`] if either constraint is violated.
  pub fn try_new(label: Option<Vec<u8>>) -> Result<Self, TcpConfigError> {
    let label = match label {
      None => None,
      Some(bytes) if bytes.is_empty() => None,
      Some(bytes) => {
        if bytes.len() > MAX_LABEL_LEN {
          return Err(TcpConfigError::LabelTooLong(bytes.len()));
        }
        if core::str::from_utf8(&bytes).is_err() {
          return Err(TcpConfigError::InvalidLabel);
        }
        Some(bytes)
      }
    };

    Ok(Self {
      label,
      skip_inbound_label_check: false,
    })
  }

  /// Build a `TcpConfig` from a label, panicking if the label is invalid.
  ///
  /// Suitable for static / compile-time-known labels. Use [`TcpConfig::try_new`]
  /// for labels supplied at runtime.
  ///
  /// # Panics
  ///
  /// Panics if the label exceeds 253 bytes or is not valid UTF-8 (see
  /// [`TcpConfig::try_new`] for the exact constraints).
  pub fn new(label: Option<Vec<u8>>) -> Self {
    Self::try_new(label).expect("invalid TcpConfig label")
  }

  /// Set whether to skip the inbound label check.
  ///
  /// When `true`, the coordinator accepts streams whose label header does not
  /// match the local label. Defaults to `false`.
  pub fn with_skip_inbound_label_check(mut self, skip: bool) -> Self {
    self.skip_inbound_label_check = skip;
    self
  }

  /// The cluster label, or `None` if none was configured.
  pub fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// Whether the inbound label check is skipped.
  ///
  /// Defaults to `false`.
  pub fn skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn carries_label_and_check_policy() {
    let cfg = TcpConfig::new(Some(b"cluster-x".to_vec()));
    assert_eq!(cfg.label(), Some(b"cluster-x".as_slice()));
    assert!(!cfg.skip_inbound_label_check());
    let cfg = TcpConfig::new(None).with_skip_inbound_label_check(true);
    assert_eq!(cfg.label(), None);
    assert!(cfg.skip_inbound_label_check());
  }

  #[test]
  fn rejects_overlong_label() {
    let too_long = vec![b'x'; 254];
    assert!(TcpConfig::try_new(Some(too_long)).is_err());
  }

  #[test]
  fn empty_label_normalizes_to_none() {
    let cfg = TcpConfig::try_new(Some(Vec::new())).unwrap();
    assert_eq!(cfg.label(), None);
  }

  #[test]
  fn rejects_non_utf8_label() {
    assert!(TcpConfig::try_new(Some(vec![0xff, 0xfe])).is_err());
  }
}
