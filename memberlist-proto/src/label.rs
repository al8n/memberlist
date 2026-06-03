//! Cluster-label wire frame primitives shared across all transport planes.
//!
//! The cluster label is a one-time `[LABELED_TAG=12][len:u8][label]` header
//! prepended to a datagram (gossip/QUIC) or a reliable stream (TCP/TLS).
//! Byte-compatible with the frozen `memberlist-proto` label frame and
//! `memberlist-core/src/network.rs` encoder/decoder.
//!
//! # Encoding rules
//!
//! * An absent or empty label is "no label" — the encoder never emits a
//!   `[12][0]` empty-label header, and the decoder treats an empty expected
//!   label identically to a `None` expected label.
//! * The maximum label length is 253 bytes (`u8::MAX - 2`): the `[tag][len]`
//!   header occupies 2 bytes of the frame, leaving 253 for the label bytes
//!   themselves (`memberlist-proto::Label::MAX_SIZE`).
//! * Labels must be valid UTF-8, as required by `memberlist-proto::Label`
//!   (stored as `SmolStr`): a non-UTF-8 label is neither emitted nor accepted.
//!
//! # Inbound truth table
//!
//! See [`classify_header`] for the full per-case decision. The
//! `skip_inbound_label_check` flag suppresses only the missing-but-expected
//! case (an unlabeled inbound when a label is configured is accepted); a
//! present `[12]` header with no configured label is always rejected as
//! [`LabelError::DoubleLabel`], regardless of the flag.
//!
//! # no_std
//!
//! This module is no_std + alloc. All allocations use `alloc`-aliased paths
//! pulled in by the crate root.

#[cfg(not(feature = "std"))]
use std::vec::Vec;

/// Outer label tag byte (`[12]` in the wire stream).
///
/// Byte-identical to `memberlist-proto::LABEL_TAG` and the frozen codec's
/// constant, so every transport that uses this module is wire-compatible.
pub const LABELED_TAG: u8 = 12;

/// Label-header overhead: the tag byte plus the single-byte length field.
pub const LABEL_OVERHEAD: usize = 2;

/// Maximum encodable/acceptable label length.
///
/// Faithful to `memberlist-proto::Label::MAX_SIZE` (`u8::MAX - 2 = 253`):
/// the `[tag][len]` header occupies 2 bytes of the frame, leaving exactly
/// 253 bytes for the label itself. Labels of 254 or 255 bytes are invalid
/// on both the encode and decode paths.
pub const MAX_LABEL_LEN: usize = u8::MAX as usize - 2;

/// Errors that can arise from cluster-label frame operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LabelError {
  /// The label exceeds the 253-byte wire maximum (`MAX_LABEL_LEN`).
  #[error("label too long")]
  TooLong,
  /// The label bytes are not valid UTF-8, as required by
  /// `memberlist-proto::Label` (labels round-trip through `SmolStr`).
  #[error("label is not valid UTF-8")]
  NotUtf8,
  /// The inbound label did not match the expected label, or an unlabeled
  /// inbound arrived when a label was expected (and the inbound check is
  /// not suppressed).
  #[error("label mismatch")]
  Mismatch,
  /// A labeled `[12]...` header arrived but no local label is configured.
  /// The decoder rejects rather than silently strips it: an unlabeled node
  /// must not accept traffic labeled for another cluster.
  #[error("double label: labeled frame received but no label is configured")]
  DoubleLabel,
}

/// Outcome of validating the inbound label header against the configured
/// expectation. Returned by [`classify_header`].
#[derive(Debug)]
pub enum LabelOutcome {
  /// Header parsed and accepted. The carried `usize` is the number of
  /// leading bytes (`[12][len][label]`, or `0` for an accepted unlabeled
  /// inbound) that the caller must skip before the payload.
  Accepted(usize),
  /// Not enough bytes present to make a decision; the caller should buffer
  /// more bytes and retry.
  Incomplete,
  /// The header is present but invalid. The inner [`LabelError`] names the
  /// specific reason.
  Rejected(LabelError),
}

/// Normalize an optional label slice: an absent or empty label is "no label".
///
/// Returns the non-empty label bytes, or `None` when the input is absent or
/// empty. Identical semantics to the frozen `memberlist-proto` encoder
/// (`label_size > 0` gate) and the legacy `codec::effective_label`.
pub fn effective_label(label: Option<&[u8]>) -> Option<&[u8]> {
  match label {
    Some(l) if !l.is_empty() => Some(l),
    _ => None,
  }
}

/// Validate a label that will be stored or transmitted.
///
/// Accepts exactly the labels the frozen `memberlist-proto::Label` type
/// accepts: non-empty (empty is "no label", not an error here), ≤ 253 bytes,
/// and valid UTF-8.
///
/// Returns [`LabelError::TooLong`] or [`LabelError::NotUtf8`] on failure;
/// returns `Ok(())` for valid (including empty) labels. The caller is
/// responsible for treating an empty label as "no label".
pub fn validate_label(label: &[u8]) -> Result<(), LabelError> {
  if label.len() > MAX_LABEL_LEN {
    return Err(LabelError::TooLong);
  }
  if core::str::from_utf8(label).is_err() {
    return Err(LabelError::NotUtf8);
  }
  Ok(())
}

/// Encode a non-empty label as a `[LABELED_TAG][len][label]` prefix and push
/// it into `out`.
///
/// The caller is responsible for passing only non-empty, already-validated
/// labels (an empty label is "no label" and should emit nothing; an
/// over-long or non-UTF-8 label should have been caught by [`validate_label`]
/// at configuration time). The length is cast to `u8` under a `debug_assert`
/// that confirms it is in range.
pub fn encode_label_prefix(label: &[u8], out: &mut Vec<u8>) {
  debug_assert!(
    label.len() <= MAX_LABEL_LEN,
    "label length must be validated against MAX_LABEL_LEN before encoding"
  );
  out.reserve(LABEL_OVERHEAD + label.len());
  out.push(LABELED_TAG);
  out.push(label.len() as u8);
  out.extend_from_slice(label);
}

/// Classify the inbound label header in `buf` against `expected`.
///
/// This is the pure per-call decision extracted from the inbound-label
/// truth table. The caller is responsible for buffering partial reads until
/// this function returns something other than [`LabelOutcome::Incomplete`].
///
/// Truth table (faithful to `memberlist-core/src/network.rs` and the frozen
/// `memberlist-proto` decoder):
///
/// | first byte | `expected`   | `skip_inbound_label_check` | result                          |
/// |------------|--------------|----------------------------|---------------------------------|
/// | `12`       | `Some(x)` matching | —                  | `Accepted(2 + len)`             |
/// | `12`       | `Some(x)` mismatch | —                  | `Rejected(Mismatch)`            |
/// | `12`       | `None`       | any                        | `Rejected(DoubleLabel)`         |
/// | `!= 12`    | `None`       | —                          | `Accepted(0)` (unlabeled)       |
/// | `!= 12`    | `Some(_)`    | `false`                    | `Rejected(Mismatch)`            |
/// | `!= 12`    | `Some(_)`    | `true`                     | `Accepted(0)` (skip accepted)   |
/// | (truncated)| any          | any                        | `Incomplete`                    |
///
/// A declared length above `MAX_LABEL_LEN` is immediately rejected as
/// [`LabelError::TooLong`] without waiting for more bytes (the count can
/// never become valid). A non-UTF-8 label is rejected as
/// [`LabelError::NotUtf8`] before any match attempt.
///
/// Under `skip_inbound_label_check` the suppression applies only to the
/// non-tag (missing-label) rows above. A labeled node cannot skip-accept an
/// unlabeled peer whose FIRST reliable unit is exactly twelve bytes: that
/// unit's length varint encodes to [`LABELED_TAG`], so the header is
/// indistinguishable from a label frame at byte zero, is parsed as a
/// mismatched label, and is rejected. Treating a tag-byte header as a missing
/// label would also accept a genuinely mismatched label from another cluster,
/// so the reject is deliberate — skip suppresses only a missing label, never a
/// wrong one.
pub fn classify_header(
  buf: &[u8],
  expected: Option<&[u8]>,
  skip_inbound_label_check: bool,
) -> LabelOutcome {
  match buf.first() {
    None => LabelOutcome::Incomplete,
    Some(&LABELED_TAG) => {
      if buf.len() < LABEL_OVERHEAD {
        return LabelOutcome::Incomplete;
      }
      let len = buf[1] as usize;
      // A faithful peer never declares a length above the cap; reject
      // immediately without waiting for the remaining bytes.
      if len > MAX_LABEL_LEN {
        return LabelOutcome::Rejected(LabelError::TooLong);
      }
      if buf.len() < LABEL_OVERHEAD + len {
        return LabelOutcome::Incomplete;
      }
      let header_label = &buf[LABEL_OVERHEAD..LABEL_OVERHEAD + len];
      // Frozen `memberlist-proto::Label` is validated UTF-8; reject a
      // non-UTF-8 label before any match to close the "accepted if bytes
      // happen to match" footgun.
      if core::str::from_utf8(header_label).is_err() {
        return LabelOutcome::Rejected(LabelError::NotUtf8);
      }
      match expected {
        Some(exp) if exp == header_label => LabelOutcome::Accepted(LABEL_OVERHEAD + len),
        Some(_) => LabelOutcome::Rejected(LabelError::Mismatch),
        // Labeled frame but no local label: double label. Not suppressed by
        // `skip_inbound_label_check` (memberlist-core suppresses only the
        // missing-label case), so an unlabeled node never accepts another
        // cluster's labeled traffic.
        None => LabelOutcome::Rejected(LabelError::DoubleLabel),
      }
    }
    // First byte is not the label tag: the inbound is unlabeled.
    Some(_) => match expected {
      // No label expected: accept verbatim, consuming nothing.
      None => LabelOutcome::Accepted(0),
      // A label was expected but none arrived: reject, unless the inbound
      // check is suppressed, in which case accept as unlabeled.
      Some(_) => {
        if skip_inbound_label_check {
          LabelOutcome::Accepted(0)
        } else {
          LabelOutcome::Rejected(LabelError::Mismatch)
        }
      }
    },
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  // ── effective_label ────────────────────────────────────────────────────────

  #[test]
  fn effective_label_none_is_none() {
    assert!(effective_label(None).is_none());
  }

  #[test]
  fn effective_label_empty_is_none() {
    assert!(effective_label(Some(b"")).is_none());
  }

  #[test]
  fn effective_label_nonempty_is_some() {
    assert_eq!(effective_label(Some(b"x")), Some(b"x".as_slice()));
    assert_eq!(
      effective_label(Some(b"cluster-x")),
      Some(b"cluster-x".as_slice())
    );
  }

  // ── validate_label ─────────────────────────────────────────────────────────

  #[test]
  fn validate_label_empty_is_ok() {
    assert!(validate_label(b"").is_ok());
  }

  #[test]
  fn validate_label_max_len_is_ok() {
    let l = vec![b'x'; MAX_LABEL_LEN];
    assert!(validate_label(&l).is_ok());
  }

  #[test]
  fn validate_label_one_over_max_len_is_too_long() {
    let l = vec![b'x'; MAX_LABEL_LEN + 1];
    assert_eq!(validate_label(&l), Err(LabelError::TooLong));
  }

  #[test]
  fn validate_label_non_utf8_is_not_utf8() {
    assert_eq!(validate_label(&[0xff, 0xfe]), Err(LabelError::NotUtf8));
  }

  #[test]
  fn validate_label_valid_utf8_label_is_ok() {
    assert!(validate_label(b"cluster-x").is_ok());
  }

  // ── encode_label_prefix ────────────────────────────────────────────────────

  #[test]
  fn encode_label_prefix_produces_tag_len_label() {
    let mut out = Vec::new();
    encode_label_prefix(b"cluster-x", &mut out);
    assert_eq!(out[0], LABELED_TAG);
    assert_eq!(out[1] as usize, b"cluster-x".len());
    assert_eq!(&out[2..], b"cluster-x");
  }

  #[test]
  fn encode_label_prefix_single_byte_label() {
    let mut out = Vec::new();
    encode_label_prefix(b"x", &mut out);
    assert_eq!(out, &[LABELED_TAG, 1, b'x']);
  }

  #[test]
  fn encode_label_prefix_max_len_label() {
    let l = vec![b'a'; MAX_LABEL_LEN];
    let mut out = Vec::new();
    encode_label_prefix(&l, &mut out);
    assert_eq!(out[0], LABELED_TAG);
    assert_eq!(out[1] as usize, MAX_LABEL_LEN);
    assert_eq!(&out[2..], l.as_slice());
  }

  #[test]
  fn encode_label_prefix_appends_to_existing_buffer() {
    let mut out = vec![0xAAu8, 0xBBu8];
    encode_label_prefix(b"abc", &mut out);
    assert_eq!(out[0], 0xAA);
    assert_eq!(out[1], 0xBB);
    assert_eq!(out[2], LABELED_TAG);
    assert_eq!(out[3], 3);
    assert_eq!(&out[4..], b"abc");
  }

  // ── classify_header: labeled inbound ──────────────────────────────────────

  #[test]
  fn classify_header_matching_label_accepted() {
    let mut buf = vec![LABELED_TAG, 9u8];
    buf.extend_from_slice(b"cluster-x");
    match classify_header(&buf, Some(b"cluster-x"), false) {
      LabelOutcome::Accepted(n) => assert_eq!(n, LABEL_OVERHEAD + 9),
      other => panic!("expected Accepted, got {other:?}"),
    }
  }

  #[test]
  fn classify_header_mismatched_label_rejected_mismatch() {
    let mut buf = vec![LABELED_TAG, 5u8];
    buf.extend_from_slice(b"other");
    match classify_header(&buf, Some(b"cluster-x"), false) {
      LabelOutcome::Rejected(LabelError::Mismatch) => {}
      other => panic!("expected Rejected(Mismatch), got {other:?}"),
    }
  }

  #[test]
  fn classify_header_labeled_frame_no_expected_label_double_label() {
    let mut buf = vec![LABELED_TAG, 9u8];
    buf.extend_from_slice(b"cluster-x");
    // skip_inbound_label_check does NOT suppress DoubleLabel.
    match classify_header(&buf, None, true) {
      LabelOutcome::Rejected(LabelError::DoubleLabel) => {}
      other => panic!("expected Rejected(DoubleLabel), got {other:?}"),
    }
  }

  #[test]
  fn classify_header_empty_label_header_double_label() {
    // `[12][0]` — an empty-label header that the encoder never emits.
    let buf = [LABELED_TAG, 0u8];
    match classify_header(&buf, None, false) {
      LabelOutcome::Rejected(LabelError::DoubleLabel) => {}
      other => panic!("expected Rejected(DoubleLabel) for [12][0], got {other:?}"),
    }
  }

  #[test]
  fn classify_header_over_long_declared_length_too_long() {
    // `[12][254]` — two bytes are sufficient for the guard to fire immediately.
    let buf = [LABELED_TAG, 254u8];
    match classify_header(&buf, Some(b"cluster-x"), false) {
      LabelOutcome::Rejected(LabelError::TooLong) => {}
      other => panic!("expected Rejected(TooLong), got {other:?}"),
    }
  }

  #[test]
  fn classify_header_over_long_255_too_long() {
    let buf = [LABELED_TAG, 255u8];
    match classify_header(&buf, Some(b"cluster-x"), false) {
      LabelOutcome::Rejected(LabelError::TooLong) => {}
      other => panic!("expected Rejected(TooLong) for len=255, got {other:?}"),
    }
  }

  #[test]
  fn classify_header_max_len_253_accepted() {
    let mut buf = vec![LABELED_TAG, MAX_LABEL_LEN as u8];
    buf.extend_from_slice(&vec![b'x'; MAX_LABEL_LEN]);
    let expected = vec![b'x'; MAX_LABEL_LEN];
    match classify_header(&buf, Some(&expected), false) {
      LabelOutcome::Accepted(n) => assert_eq!(n, LABEL_OVERHEAD + MAX_LABEL_LEN),
      other => panic!("expected Accepted for 253-byte label, got {other:?}"),
    }
  }

  #[test]
  fn classify_header_non_utf8_label_rejected_before_match() {
    // `[12][2][0xff][0xfe]` — the bytes match the configured "expected" value,
    // but the UTF-8 check fires before the match, so it is rejected.
    let buf = [LABELED_TAG, 2u8, 0xff, 0xfe];
    match classify_header(&buf, Some(&[0xff, 0xfe]), false) {
      LabelOutcome::Rejected(LabelError::NotUtf8) => {}
      other => panic!("expected Rejected(NotUtf8), got {other:?}"),
    }
  }

  // ── classify_header: incomplete ────────────────────────────────────────────

  #[test]
  fn classify_header_empty_buf_incomplete() {
    assert!(matches!(
      classify_header(&[], Some(b"cluster-x"), false),
      LabelOutcome::Incomplete
    ));
  }

  #[test]
  fn classify_header_only_tag_byte_incomplete() {
    assert!(matches!(
      classify_header(&[LABELED_TAG], Some(b"cluster-x"), false),
      LabelOutcome::Incomplete
    ));
  }

  #[test]
  fn classify_header_partial_label_body_incomplete() {
    // Claims 9 label bytes, only 4 present after the header.
    let buf = [LABELED_TAG, 9u8, b'c', b'l', b'u', b's'];
    assert!(matches!(
      classify_header(&buf, Some(b"cluster-x"), false),
      LabelOutcome::Incomplete
    ));
  }

  // ── classify_header: unlabeled inbound ────────────────────────────────────

  #[test]
  fn classify_header_unlabeled_no_expected_accepted_zero() {
    match classify_header(b"payload", None, false) {
      LabelOutcome::Accepted(0) => {}
      other => panic!("expected Accepted(0), got {other:?}"),
    }
  }

  #[test]
  fn classify_header_unlabeled_with_expected_rejected_mismatch() {
    match classify_header(b"payload", Some(b"cluster-x"), false) {
      LabelOutcome::Rejected(LabelError::Mismatch) => {}
      other => panic!("expected Rejected(Mismatch), got {other:?}"),
    }
  }

  #[test]
  fn classify_header_unlabeled_with_expected_skip_accepted_zero() {
    match classify_header(b"payload", Some(b"cluster-x"), true) {
      LabelOutcome::Accepted(0) => {}
      other => panic!("expected Accepted(0) with skip, got {other:?}"),
    }
  }

  #[test]
  fn classify_header_skip_does_not_suppress_double_label() {
    // skip_inbound_label_check=true must NOT suppress DoubleLabel.
    let mut buf = vec![LABELED_TAG, 9u8];
    buf.extend_from_slice(b"cluster-x");
    match classify_header(&buf, None, true) {
      LabelOutcome::Rejected(LabelError::DoubleLabel) => {}
      other => panic!("expected Rejected(DoubleLabel) even with skip, got {other:?}"),
    }
  }

  #[test]
  fn classify_header_skip_does_not_rescue_twelve_byte_first_unit() {
    // A labeled node with skip enabled receives an unlabeled peer whose first
    // reliable unit is exactly twelve bytes: its length varint is LABELED_TAG,
    // so the header reads as a label frame and is parsed as a mismatched label.
    // skip suppresses only a missing label, never a wrong one, so the safe
    // outcome is a clean reject (no panic, no stall).
    let mut buf = vec![LABELED_TAG, 4u8];
    buf.extend_from_slice(b"zzzz");
    match classify_header(&buf, Some(b"cluster-x"), true) {
      LabelOutcome::Rejected(LabelError::Mismatch) => {}
      other => panic!("expected Rejected(Mismatch) even with skip, got {other:?}"),
    }
  }

  // ── round-trip: encode then classify ──────────────────────────────────────

  #[test]
  fn encode_then_classify_roundtrip() {
    let label = b"cluster-x";
    let mut buf = Vec::new();
    buf.extend_from_slice(b"inner-payload");
    let mut prefixed = Vec::new();
    encode_label_prefix(label, &mut prefixed);
    prefixed.extend_from_slice(b"inner-payload");

    match classify_header(&prefixed, Some(label), false) {
      LabelOutcome::Accepted(n) => {
        assert_eq!(n, LABEL_OVERHEAD + label.len());
        assert_eq!(&prefixed[n..], b"inner-payload");
      }
      other => panic!("expected Accepted, got {other:?}"),
    }
  }
}
