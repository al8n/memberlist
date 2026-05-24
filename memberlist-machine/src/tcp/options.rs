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
//! `TcpOptions` bundles the label and the inbound-check policy. It is
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

/// Errors returned by [`TcpOptions::try_new`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TcpOptionsError {
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

/// Immutable options bundle for the plain-TCP coordinator.
///
/// Carries the cluster label and the inbound label-check policy. All fields
/// are private; use the accessor methods. Construct via [`TcpOptions::try_new`]
/// (checked) or [`TcpOptions::new`] (panics on an invalid label).
pub struct TcpOptions {
  label: Option<Vec<u8>>,
  skip_inbound_label_check: bool,
}

impl TcpOptions {
  /// Build a `TcpOptions`, validating the label.
  ///
  /// - `Some(bytes)` where `bytes.is_empty()` normalizes to `None` (no label).
  /// - A present label must be ≤ 253 bytes (`memberlist-proto::Label::MAX_SIZE`)
  ///   and valid UTF-8 (`memberlist-proto::Label` stores labels as `SmolStr`).
  ///
  /// Returns [`TcpOptionsError`] if either constraint is violated.
  pub fn try_new(label: Option<Vec<u8>>) -> Result<Self, TcpOptionsError> {
    let label = match label {
      None => None,
      Some(bytes) if bytes.is_empty() => None,
      Some(bytes) => {
        if bytes.len() > MAX_LABEL_LEN {
          return Err(TcpOptionsError::LabelTooLong(bytes.len()));
        }
        if core::str::from_utf8(&bytes).is_err() {
          return Err(TcpOptionsError::InvalidLabel);
        }
        Some(bytes)
      }
    };

    Ok(Self {
      label,
      skip_inbound_label_check: false,
    })
  }

  /// Build a `TcpOptions` from a label, panicking if the label is invalid.
  ///
  /// Suitable for static / compile-time-known labels. Use [`TcpOptions::try_new`]
  /// for labels supplied at runtime.
  ///
  /// # Panics
  ///
  /// Panics if the label exceeds 253 bytes or is not valid UTF-8 (see
  /// [`TcpOptions::try_new`] for the exact constraints).
  pub fn new(label: Option<Vec<u8>>) -> Self {
    Self::try_new(label).expect("invalid TcpOptions label")
  }

  /// Setter: clear the cluster label in place (disables the label check).
  #[inline(always)]
  pub fn clear_label(&mut self) -> &mut Self {
    self.label = None;
    self
  }

  /// Setter: replace the cluster label in place, validating the bytes.
  ///
  /// An empty slice normalizes to `None` (no label).  Returns
  /// [`TcpOptionsError`] if the label exceeds 253 bytes or is not valid
  /// UTF-8.
  #[inline(always)]
  pub fn try_set_label(&mut self, val: Vec<u8>) -> Result<&mut Self, TcpOptionsError> {
    let normalized = if val.is_empty() {
      None
    } else {
      if val.len() > MAX_LABEL_LEN {
        return Err(TcpOptionsError::LabelTooLong(val.len()));
      }
      if core::str::from_utf8(&val).is_err() {
        return Err(TcpOptionsError::InvalidLabel);
      }
      Some(val)
    };
    self.label = normalized;
    Ok(self)
  }

  /// Builder: replace the cluster label (consuming), validating the bytes.
  ///
  /// An empty slice normalizes to `None` (no label). Returns
  /// [`TcpOptionsError`] if the label exceeds 253 bytes or is not valid
  /// UTF-8.
  #[inline(always)]
  pub fn try_with_label(mut self, val: Vec<u8>) -> Result<Self, TcpOptionsError> {
    self.try_set_label(val)?;
    Ok(self)
  }

  /// Setter: skip the inbound label check (set to `true`) in place.
  #[inline(always)]
  pub const fn set_skip_inbound_label_check(&mut self) -> &mut Self {
    self.skip_inbound_label_check = true;
    self
  }

  /// Setter: assign the raw `bool` value in place.
  #[inline(always)]
  pub const fn update_skip_inbound_label_check(&mut self, val: bool) -> &mut Self {
    self.skip_inbound_label_check = val;
    self
  }

  /// Builder: assign the raw `bool` value (consuming).
  ///
  /// When `true`, the coordinator accepts streams whose label header does not
  /// match the local label. Defaults to `false`.
  #[must_use]
  #[inline(always)]
  pub const fn maybe_skip_inbound_label_check(mut self, val: bool) -> Self {
    self.skip_inbound_label_check = val;
    self
  }

  /// Setter: enforce the inbound label check (set to `false`) in place.
  #[inline(always)]
  pub const fn clear_skip_inbound_label_check(&mut self) -> &mut Self {
    self.skip_inbound_label_check = false;
    self
  }

  /// Builder: skip the inbound label check.
  ///
  /// Sets the policy to `true`. Use
  /// [`maybe_skip_inbound_label_check`](Self::maybe_skip_inbound_label_check)
  /// to assign a raw `bool` instead.
  #[must_use]
  #[inline(always)]
  pub const fn with_skip_inbound_label_check(mut self) -> Self {
    self.skip_inbound_label_check = true;
    self
  }

  /// The cluster label, or `None` if none was configured.
  #[inline(always)]
  pub fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// Whether the inbound label check is skipped.
  ///
  /// Defaults to `false`.
  #[inline(always)]
  pub const fn is_skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn carries_label_and_check_policy() {
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    assert_eq!(cfg.label(), Some(b"cluster-x".as_slice()));
    assert!(!cfg.is_skip_inbound_label_check());
    let cfg = TcpOptions::new(None).with_skip_inbound_label_check();
    assert_eq!(cfg.label(), None);
    assert!(cfg.is_skip_inbound_label_check());
  }

  #[test]
  fn rejects_overlong_label() {
    let too_long = vec![b'x'; 254];
    assert!(TcpOptions::try_new(Some(too_long)).is_err());
  }

  #[test]
  fn empty_label_normalizes_to_none() {
    let cfg = TcpOptions::try_new(Some(Vec::new())).unwrap();
    assert_eq!(cfg.label(), None);
  }

  #[test]
  fn rejects_non_utf8_label() {
    assert!(TcpOptions::try_new(Some(vec![0xff, 0xfe])).is_err());
  }
}
