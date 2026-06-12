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
  assert_eq!(
    validate_label(&l),
    Err(LabelError::TooLong(MAX_LABEL_LEN + 1))
  );
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
    LabelOutcome::Rejected(LabelError::TooLong(_)) => {}
    other => panic!("expected Rejected(TooLong), got {other:?}"),
  }
}

#[test]
fn classify_header_over_long_255_too_long() {
  let buf = [LABELED_TAG, 255u8];
  match classify_header(&buf, Some(b"cluster-x"), false) {
    LabelOutcome::Rejected(LabelError::TooLong(_)) => {}
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
