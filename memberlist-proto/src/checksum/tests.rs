use super::*;

#[test]
fn algorithm_tag_roundtrip() {
  for algo in [
    ChecksumAlgorithm::Crc32,
    ChecksumAlgorithm::XxHash32,
    ChecksumAlgorithm::XxHash64,
    ChecksumAlgorithm::XxHash3,
    ChecksumAlgorithm::Murmur3,
  ] {
    assert_eq!(ChecksumAlgorithm::from_tag(algo.tag()), algo);
  }
}

#[test]
fn algorithm_tags_have_pinned_numeric_values() {
  // The numeric tags are a stable wire contract: a frame checksummed by one
  // node must verify on a peer built with a different backend set, so the
  // tag numbering may never silently drift.
  assert_eq!(ChecksumAlgorithm::Crc32.tag(), 1);
  assert_eq!(ChecksumAlgorithm::XxHash32.tag(), 2);
  assert_eq!(ChecksumAlgorithm::XxHash64.tag(), 3);
  assert_eq!(ChecksumAlgorithm::XxHash3.tag(), 4);
  assert_eq!(ChecksumAlgorithm::Murmur3.tag(), 5);
}

#[test]
fn unrecognized_tag_is_unknown() {
  assert_eq!(
    ChecksumAlgorithm::from_tag(0),
    ChecksumAlgorithm::Unknown(0)
  );
  assert_eq!(
    ChecksumAlgorithm::from_tag(200),
    ChecksumAlgorithm::Unknown(200)
  );
  // An `Unknown` round-trips its carried byte.
  assert_eq!(ChecksumAlgorithm::Unknown(200).tag(), 200);
}

#[test]
fn digest_sizes_are_pinned() {
  assert_eq!(ChecksumAlgorithm::Crc32.digest_size(), 4);
  assert_eq!(ChecksumAlgorithm::XxHash32.digest_size(), 4);
  assert_eq!(ChecksumAlgorithm::XxHash64.digest_size(), 8);
  assert_eq!(ChecksumAlgorithm::XxHash3.digest_size(), 8);
  assert_eq!(ChecksumAlgorithm::Murmur3.digest_size(), 4);
  assert_eq!(ChecksumAlgorithm::Unknown(9).digest_size(), 0);
}

#[cfg(feature = "crc32")]
#[test]
fn crc32_digest_is_deterministic_and_sized() {
  let payload = b"the quick brown fox jumps over the lazy dog";
  let a = digest(ChecksumAlgorithm::Crc32, payload).expect("crc32 digest");
  let b = digest(ChecksumAlgorithm::Crc32, payload).expect("crc32 digest");
  assert_eq!(a, b, "digest must be deterministic");
  assert_eq!(a.as_bytes().len(), 4);
  // The big-endian digest matches the raw 32-bit checksum.
  assert_eq!(a.as_bytes(), &crc32fast::hash(payload).to_be_bytes());
}

#[cfg(feature = "xxhash32")]
#[test]
fn xxhash32_digest_is_deterministic_and_sized() {
  let payload = b"the quick brown fox jumps over the lazy dog";
  let a = digest(ChecksumAlgorithm::XxHash32, payload).expect("xxhash32 digest");
  let b = digest(ChecksumAlgorithm::XxHash32, payload).expect("xxhash32 digest");
  assert_eq!(a, b);
  assert_eq!(a.as_bytes().len(), 4);
  assert_eq!(
    a.as_bytes(),
    &xxhash_rust::xxh32::xxh32(payload, 0).to_be_bytes()
  );
}

#[cfg(feature = "xxhash64")]
#[test]
fn xxhash64_digest_is_deterministic_and_sized() {
  let payload = b"the quick brown fox jumps over the lazy dog";
  let a = digest(ChecksumAlgorithm::XxHash64, payload).expect("xxhash64 digest");
  let b = digest(ChecksumAlgorithm::XxHash64, payload).expect("xxhash64 digest");
  assert_eq!(a, b);
  assert_eq!(a.as_bytes().len(), 8);
  assert_eq!(
    a.as_bytes(),
    &xxhash_rust::xxh64::xxh64(payload, 0).to_be_bytes()
  );
}

#[cfg(feature = "xxhash3")]
#[test]
fn xxhash3_digest_is_deterministic_and_sized() {
  let payload = b"the quick brown fox jumps over the lazy dog";
  let a = digest(ChecksumAlgorithm::XxHash3, payload).expect("xxhash3 digest");
  let b = digest(ChecksumAlgorithm::XxHash3, payload).expect("xxhash3 digest");
  assert_eq!(a, b);
  assert_eq!(a.as_bytes().len(), 8);
  assert_eq!(
    a.as_bytes(),
    &xxhash_rust::xxh3::xxh3_64(payload).to_be_bytes()
  );
}

#[cfg(feature = "murmur3")]
#[test]
fn murmur3_digest_is_deterministic_and_sized() {
  use core::hash::Hasher as _;
  let payload = b"the quick brown fox jumps over the lazy dog";
  let a = digest(ChecksumAlgorithm::Murmur3, payload).expect("murmur3 digest");
  let b = digest(ChecksumAlgorithm::Murmur3, payload).expect("murmur3 digest");
  assert_eq!(a, b);
  assert_eq!(a.as_bytes().len(), 4);
  let mut hasher = hash32::Murmur3Hasher::default();
  hasher.write(payload);
  assert_eq!(a.as_bytes(), &(hasher.finish() as u32).to_be_bytes());
}

#[cfg(feature = "crc32")]
#[test]
fn verify_accepts_correct_digest_and_rejects_corruption() {
  let payload = b"the quick brown fox jumps over the lazy dog".repeat(4);
  let d = digest(ChecksumAlgorithm::Crc32, &payload).expect("digest");
  // A correct digest verifies.
  verify(ChecksumAlgorithm::Crc32, &payload, d.as_bytes()).expect("correct digest verifies");
  // A single flipped payload byte fails verification.
  let mut corrupt = payload.clone();
  corrupt[0] ^= 0xff;
  assert_eq!(
    verify(ChecksumAlgorithm::Crc32, &corrupt, d.as_bytes()),
    Err(ChecksumError::Mismatch)
  );
  // A flipped digest byte also fails.
  let mut bad_digest = d.as_bytes().to_vec();
  bad_digest[0] ^= 0xff;
  assert_eq!(
    verify(ChecksumAlgorithm::Crc32, &payload, &bad_digest),
    Err(ChecksumError::Mismatch)
  );
}

#[test]
fn unknown_algorithm_digest_and_verify_error() {
  let algo = ChecksumAlgorithm::Unknown(99);
  assert_eq!(
    digest(algo, b"data"),
    Err(ChecksumError::UnknownAlgorithm(99))
  );
  assert_eq!(
    verify(algo, b"data", &[0, 0, 0, 0]),
    Err(ChecksumError::UnknownAlgorithm(99))
  );
}

#[test]
fn default_is_the_first_built_in_backend() {
  // With `crc32` enabled (the test build's verify feature set),
  // Default resolves to Crc32. The fallback chain mirrors the declaration
  // order otherwise; a no-backend build resolves to `Unknown(CRC32_TAG)`.
  let d = ChecksumAlgorithm::default();
  #[cfg(feature = "crc32")]
  assert_eq!(d, ChecksumAlgorithm::Crc32);
  #[cfg(not(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3"
  )))]
  assert_eq!(d, ChecksumAlgorithm::Unknown(CRC32_TAG));
  // Ignoring: with a checksum backend other than crc32 built (e.g. xxhash32
  // only), neither cfg'd assertion above compiles, leaving `d` unused.
  let _ = d;
}

#[test]
fn checksum_error_display_strings_are_nonempty() {
  let cases = [
    ChecksumError::Disabled(ChecksumAlgorithm::Crc32),
    ChecksumError::UnknownAlgorithm(9),
    ChecksumError::Mismatch,
  ];
  for e in &cases {
    assert!(!e.to_string().is_empty(), "empty display for {e:?}");
  }
}

#[test]
fn algorithm_display_matches_tags() {
  assert_eq!(ChecksumAlgorithm::Crc32.to_string(), "crc32");
  assert_eq!(ChecksumAlgorithm::XxHash32.to_string(), "xxhash32");
  assert_eq!(ChecksumAlgorithm::XxHash64.to_string(), "xxhash64");
  assert_eq!(ChecksumAlgorithm::XxHash3.to_string(), "xxhash3");
  assert_eq!(ChecksumAlgorithm::Murmur3.to_string(), "murmur3");
  assert_eq!(ChecksumAlgorithm::Unknown(7).to_string(), "unknown(7)");
}

#[test]
fn checksumed_tag_value_is_pinned() {
  // The wrapper tag is the framing `Checksumed` tag — a change is a
  // wire-protocol break.
  assert_eq!(CHECKSUMED_TAG, 15);
  assert_eq!(CHECKSUMED_TAG, crate::framing::MessageTag::Checksumed as u8);
}

#[cfg(feature = "crc32")]
#[test]
fn checksummed_frame_has_tag_algo_digest_header_and_roundtrips() {
  let payload = b"the quick brown fox jumps over the lazy dog".repeat(4);
  let frame = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &payload).expect("encode");
  // [CHECKSUMED_TAG][algo tag][4-byte crc32 digest][payload].
  assert_eq!(frame[0], CHECKSUMED_TAG);
  assert_eq!(frame[1], ChecksumAlgorithm::Crc32.tag());
  let digest_end = 2 + ChecksumAlgorithm::Crc32.digest_size();
  assert_eq!(
    &frame[digest_end..],
    &payload[..],
    "payload follows the digest"
  );
  // Decode verifies the digest and returns the payload slice.
  let back = decode_checksummed_frame(&frame).expect("decode");
  assert_eq!(back, &payload[..]);
}

#[cfg(feature = "crc32")]
#[test]
fn decode_rejects_a_flipped_payload_byte_as_mismatch() {
  let payload = b"reliable frame payload bytes".repeat(3);
  let mut frame = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &payload).expect("encode");
  // Flip the last byte (inside the payload, past the digest header).
  let last = frame.len() - 1;
  frame[last] ^= 0xff;
  assert!(matches!(
    decode_checksummed_frame(&frame),
    Err(FrameError::Checksum(ChecksumError::Mismatch))
  ));
}

#[test]
fn decode_rejects_unknown_algorithm_tag() {
  // A right leading tag but an unknown algorithm byte: the payload boundary
  // cannot be sized, so it is rejected as UnknownAlgorithm before slicing.
  let mut frame = vec![CHECKSUMED_TAG, 222u8];
  frame.extend_from_slice(b"whatever");
  assert!(matches!(
    decode_checksummed_frame(&frame),
    Err(FrameError::Checksum(ChecksumError::UnknownAlgorithm(222)))
  ));
}

#[test]
fn decode_rejects_a_non_checksum_leading_tag() {
  // A buffer not led by CHECKSUMED_TAG is not a checksum frame.
  let frame = [0xABu8, 1, 2, 3];
  assert!(matches!(
    decode_checksummed_frame(&frame),
    Err(FrameError::Checksum(ChecksumError::UnknownAlgorithm(0xAB)))
  ));
  // An empty buffer reports the zero placeholder tag rather than panicking.
  assert!(matches!(
    decode_checksummed_frame(&[]),
    Err(FrameError::Checksum(ChecksumError::UnknownAlgorithm(0)))
  ));
}

#[cfg(feature = "crc32")]
#[test]
fn decode_rejects_a_truncated_digest_as_mismatch() {
  // The header declares a 4-byte crc32 digest but only 2 digest bytes are
  // present (no payload). A recomputed digest over the empty remainder could
  // never match, so it is rejected as a mismatch.
  let frame = [CHECKSUMED_TAG, ChecksumAlgorithm::Crc32.tag(), 0x00, 0x00];
  assert!(matches!(
    decode_checksummed_frame(&frame),
    Err(FrameError::Checksum(ChecksumError::Mismatch))
  ));
}

#[test]
fn checksum_options_default_is_disabled() {
  let opts = ChecksumOptions::new();
  assert!(opts.algorithm().is_none());
  assert!(!opts.is_enabled());
  let outcome = opts.apply(b"data").expect("disabled never errors");
  assert!(matches!(outcome, ChecksumOutcome::Plain));
}

#[test]
fn checksum_options_default_matches_new() {
  assert_eq!(ChecksumOptions::default(), ChecksumOptions::new());
}

#[cfg(feature = "crc32")]
#[test]
fn checksum_options_builders_and_setters_select_algorithm() {
  let opts = ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32);
  assert_eq!(opts.algorithm(), Some(ChecksumAlgorithm::Crc32));
  assert!(opts.is_enabled());
  let outcome = opts.apply(b"payload").expect("backend ok");
  let framed = match outcome {
    ChecksumOutcome::Checksumed(f) => f,
    ChecksumOutcome::Plain => panic!("an enabled algorithm must wrap"),
  };
  assert_eq!(framed[0], CHECKSUMED_TAG);
  // In-place setters / clear.
  let mut opts = ChecksumOptions::new();
  opts.set_algorithm(ChecksumAlgorithm::Crc32);
  assert_eq!(opts.algorithm(), Some(ChecksumAlgorithm::Crc32));
  opts.clear_algorithm();
  assert!(opts.algorithm().is_none());
  opts.update_algorithm(Some(ChecksumAlgorithm::Crc32));
  assert_eq!(opts.algorithm(), Some(ChecksumAlgorithm::Crc32));
  let maybe = ChecksumOptions::new().maybe_algorithm(None);
  assert!(maybe.algorithm().is_none());
}

#[test]
fn checksum_outcome_equality() {
  assert_eq!(ChecksumOutcome::Plain, ChecksumOutcome::Plain);
  assert_eq!(
    ChecksumOutcome::Checksumed(vec![1, 2, 3]),
    ChecksumOutcome::Checksumed(vec![1, 2, 3])
  );
  assert_ne!(ChecksumOutcome::Plain, ChecksumOutcome::Checksumed(vec![1]));
}
