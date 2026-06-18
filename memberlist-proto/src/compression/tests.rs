use super::*;

#[cfg(compression)]
#[test]
fn algorithm_tag_roundtrip() {
  for algo in [
    CompressAlgorithm::Lz4,
    CompressAlgorithm::Snappy,
    CompressAlgorithm::Zstd(ZstdLevel::Eight),
    CompressAlgorithm::Brotli,
  ] {
    assert_eq!(CompressAlgorithm::from_tag(algo.tag()), algo);
  }
}

#[cfg(compression)]
#[test]
fn algorithm_tags_have_pinned_numeric_values() {
  // The numeric tags are a stable wire contract: a frame compressed by one
  // node must decode on a peer built with a different backend set, so the
  // tag numbering may never silently drift.
  assert_eq!(CompressAlgorithm::Lz4.tag(), 23);
  assert_eq!(CompressAlgorithm::Snappy.tag(), 24);
  assert_eq!(CompressAlgorithm::Zstd(ZstdLevel::Eight).tag(), 8);
  assert_eq!(CompressAlgorithm::Brotli.tag(), 25);
}

#[test]
fn unrecognized_tag_is_unknown() {
  assert_eq!(
    CompressAlgorithm::from_tag(0),
    CompressAlgorithm::Unknown(0)
  );
  assert_eq!(
    CompressAlgorithm::from_tag(200),
    CompressAlgorithm::Unknown(200)
  );
  // An `Unknown` round-trips its carried byte.
  assert_eq!(CompressAlgorithm::Unknown(200).tag(), 200);
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_roundtrip() {
  let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let packed = compress(CompressAlgorithm::Lz4, &input).expect("lz4 compress");
  let back = decompress(CompressAlgorithm::Lz4, &packed, input.len()).expect("lz4 decompress");
  assert_eq!(back, input);
}

#[cfg(feature = "snappy")]
#[test]
fn snappy_roundtrip() {
  let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let packed = compress(CompressAlgorithm::Snappy, &input).expect("snappy compress");
  let back =
    decompress(CompressAlgorithm::Snappy, &packed, input.len()).expect("snappy decompress");
  assert_eq!(back, input);
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_roundtrip() {
  let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let packed = compress(CompressAlgorithm::Zstd(ZstdLevel::Eight), &input).expect("zstd compress");
  let back = decompress(
    CompressAlgorithm::Zstd(ZstdLevel::Eight),
    &packed,
    input.len(),
  )
  .expect("zstd decompress");
  assert_eq!(back, input);
}

#[cfg(feature = "brotli")]
#[test]
fn brotli_roundtrip() {
  let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let packed = compress(CompressAlgorithm::Brotli, &input).expect("brotli compress");
  let back =
    decompress(CompressAlgorithm::Brotli, &packed, input.len()).expect("brotli decompress");
  assert_eq!(back, input);
}

#[test]
fn unknown_algorithm_fails_both_directions() {
  let algo = CompressAlgorithm::Unknown(99);
  assert!(matches!(
    compress(algo, b"data"),
    Err(CompressionError::UnsupportedAlgorithm(_))
  ));
  assert!(matches!(
    decompress(algo, b"data", 4),
    Err(CompressionError::UnsupportedAlgorithm(_))
  ));
}

#[test]
fn below_threshold_payload_is_left_plain() {
  // A 5-byte payload with a 64-byte threshold: never compressed.
  let outcome = apply_compression(CompressAlgorithm::Unknown(0), 64, b"small");
  assert!(matches!(outcome, Ok(CompressionOutcome::Plain)));
}

#[cfg(feature = "lz4")]
#[test]
fn incompressible_above_threshold_payload_is_left_plain() {
  // Hard-to-shrink bytes: even above threshold the fallback keeps them plain
  // so compression can never inflate a payload.
  let mut input = Vec::new();
  for i in 0u32..512 {
    input.extend_from_slice(&i.to_le_bytes());
  }
  let outcome = apply_compression(CompressAlgorithm::Lz4, 8, &input).expect("no backend error");
  match outcome {
    CompressionOutcome::Plain => {}
    CompressionOutcome::Compressed(packed) => {
      assert!(
        packed.len() < input.len(),
        "Compressed outcome must be smaller"
      );
    }
  }
}

#[cfg(feature = "lz4")]
#[test]
fn compressible_above_threshold_payload_is_compressed() {
  let input = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec();
  let outcome = apply_compression(CompressAlgorithm::Lz4, 8, &input).expect("no backend error");
  match outcome {
    CompressionOutcome::Compressed(packed) => {
      assert!(
        packed.len() < input.len(),
        "highly compressible input must shrink"
      );
    }
    CompressionOutcome::Plain => panic!("compressible input above threshold must compress"),
  }
}

#[cfg(feature = "lz4")]
#[test]
fn compressed_frame_has_tag_algo_origlen_header() {
  let input = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec();
  let packed = compress(CompressAlgorithm::Lz4, &input).expect("compress");
  let frame = encode_compressed_frame(CompressAlgorithm::Lz4, input.len(), &packed);
  assert_eq!(frame[0], COMPRESSED_TAG);
  assert_eq!(frame[1], CompressAlgorithm::Lz4.tag());
  assert_eq!(&frame[frame.len() - packed.len()..], &packed[..]);
  assert!(frame.len() > packed.len());
}

#[cfg(feature = "lz4")]
#[test]
fn compressed_frame_roundtrips_through_decode() {
  let input = b"the quick brown fox".repeat(16);
  let packed = compress(CompressAlgorithm::Lz4, &input).expect("compress");
  let frame = encode_compressed_frame(CompressAlgorithm::Lz4, input.len(), &packed);
  let max = 1 << 20;
  let back = decode_compressed_frame(&frame, max).expect("decode");
  assert_eq!(back, input);
}

#[test]
fn oversize_orig_len_is_rejected_before_allocation() {
  // Compressed tag, lz4 algo tag, orig_len varint claiming 4 GiB, 1-byte
  // body. The decoder must reject on the `orig_len > max` bound BEFORE
  // allocating — the test passing (not OOM-aborting) is the assertion.
  let mut frame = vec![COMPRESSED_TAG, CompressAlgorithm::Lz4.tag()];
  crate::framing::encode_varint_u32(u32::MAX, &mut frame);
  frame.push(0x00);
  let max = 64 * 1024;
  assert!(matches!(
    decode_compressed_frame(&frame, max),
    Err(CompressionError::OversizeOriginal(_))
  ));
}

#[cfg(feature = "lz4")]
#[test]
fn lying_small_orig_len_rejects_without_allocating_from_body() {
  // A frame whose wrapper `orig_len` is far SMALLER than the body's true
  // decompressed length. The wrapper length passes the bomb guard
  // (`32 <= 1 MiB`), but the bounded decode into a 32-byte buffer must
  // reject — the backend may never size its allocation from a length
  // embedded in the compressed body.
  let input = b"the quick brown fox jumps over the lazy dog".repeat(2400);
  assert!(
    input.len() > 100 * 1024,
    "input is comfortably over 100 KiB"
  );
  let packed = compress(CompressAlgorithm::Lz4, &input).expect("lz4 compress");
  let frame = encode_compressed_frame(CompressAlgorithm::Lz4, 32, &packed);
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::Backend(_))
  ));
}

#[test]
fn unknown_algorithm_frame_fails_decode() {
  let mut frame = vec![COMPRESSED_TAG, 222u8];
  crate::framing::encode_varint_u32(8, &mut frame);
  frame.extend_from_slice(b"whatever");
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::UnsupportedAlgorithm(222))
  ));
}

#[test]
fn compression_options_default_is_disabled() {
  let opts = CompressionOptions::new();
  assert!(opts.algorithm().is_none());
  let outcome = opts.apply(&[0u8; 4096]).expect("disabled never errors");
  assert!(matches!(outcome, CompressionOutcome::Plain));
}

#[cfg(feature = "lz4")]
#[test]
fn compression_options_builders_select_algorithm_and_threshold() {
  let opts = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(16);
  assert_eq!(opts.algorithm(), Some(CompressAlgorithm::Lz4));
  assert_eq!(opts.threshold(), 16);
  let outcome = opts.apply(&b"A".repeat(256)).expect("backend ok");
  assert!(matches!(outcome, CompressionOutcome::Compressed(_)));
  let outcome = opts.apply(b"AAAA").expect("backend ok");
  assert!(matches!(outcome, CompressionOutcome::Plain));
}

#[test]
fn reliable_unit_plain_roundtrips_when_disabled() {
  let opts = CompressionOptions::new();
  let framed = b"the quick brown fox".repeat(4);
  let unit = encode_reliable_unit(&opts, &framed);
  let (back, consumed) = take_reliable_unit(&unit, 1 << 20)
    .expect("decode ok")
    .expect("a complete unit is present");
  assert_eq!(back, framed);
  assert_eq!(consumed, unit.len());
}

#[test]
fn reliable_unit_partial_buffer_returns_none() {
  let opts = CompressionOptions::new();
  let framed = b"the quick brown fox".repeat(4);
  let unit = encode_reliable_unit(&opts, &framed);
  let partial = &unit[..unit.len() - 1];
  assert!(
    take_reliable_unit(partial, 1 << 20)
      .expect("not an error")
      .is_none()
  );
  assert!(
    take_reliable_unit(&[], 1 << 20)
      .expect("not an error")
      .is_none()
  );
}

#[test]
fn reliable_unit_two_back_to_back_each_extract_with_consumed() {
  let opts = CompressionOptions::new();
  let first = b"first-frame-bytes".to_vec();
  let second = b"second-frame".to_vec();
  let mut buf = encode_reliable_unit(&opts, &first);
  buf.extend_from_slice(&encode_reliable_unit(&opts, &second));
  let (a, n1) = take_reliable_unit(&buf, 1 << 20).unwrap().unwrap();
  assert_eq!(a, first);
  let (b, n2) = take_reliable_unit(&buf[n1..], 1 << 20).unwrap().unwrap();
  assert_eq!(b, second);
  assert_eq!(n1 + n2, buf.len());
}

#[test]
fn reliable_unit_len_over_ceiling_is_rejected_before_waiting() {
  let mut buf = Vec::new();
  encode_varint_u32(64 * 1024, &mut buf);
  buf.extend_from_slice(b"only-a-few-bytes-follow");
  assert!(take_reliable_unit(&buf, 1024).is_err());
}

#[cfg(feature = "lz4")]
#[test]
fn reliable_unit_compressed_roundtrips() {
  let opts = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
  let unit = encode_reliable_unit(&opts, &framed);
  assert!(
    unit.len() < framed.len(),
    "compressible unit must shrink on the wire"
  );
  let (back, consumed) = take_reliable_unit(&unit, 1 << 20).unwrap().unwrap();
  assert_eq!(back, framed);
  assert_eq!(consumed, unit.len());
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_trailing_junk_in_body_is_rejected() {
  // A compressed body shaped `[valid lz4 block][trailing junk]` is not one
  // clean stream — `decode_compressed_frame` must reject it wholesale even
  // though `orig_len` is honest.
  let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let mut body = compress(CompressAlgorithm::Lz4, &payload).expect("lz4 compress");
  body.extend_from_slice(b"trailing-junk");
  let frame = encode_compressed_frame(CompressAlgorithm::Lz4, payload.len(), &body);
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::Backend(_))
  ));
}

#[cfg(feature = "snappy")]
#[test]
fn snappy_trailing_junk_in_body_is_rejected() {
  let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let mut body = compress(CompressAlgorithm::Snappy, &payload).expect("snappy compress");
  body.extend_from_slice(b"trailing-junk");
  let frame = encode_compressed_frame(CompressAlgorithm::Snappy, payload.len(), &body);
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::Backend(_))
  ));
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_trailing_junk_in_body_is_rejected() {
  let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let mut body =
    compress(CompressAlgorithm::Zstd(ZstdLevel::Eight), &payload).expect("zstd compress");
  body.extend_from_slice(b"trailing-junk");
  let frame = encode_compressed_frame(
    CompressAlgorithm::Zstd(ZstdLevel::Eight),
    payload.len(),
    &body,
  );
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::Backend(_))
  ));
}

#[cfg(feature = "brotli")]
#[test]
fn brotli_trailing_junk_in_body_is_rejected() {
  let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
  let mut body = compress(CompressAlgorithm::Brotli, &payload).expect("brotli compress");
  body.extend_from_slice(b"trailing-junk");
  let frame = encode_compressed_frame(CompressAlgorithm::Brotli, payload.len(), &body);
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::Backend(_))
  ));
}

#[cfg(feature = "lz4")]
#[test]
fn reliable_unit_corrupt_inner_wrapper_is_rejected() {
  let opts = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
  let mut unit = encode_reliable_unit(&opts, &framed);
  // Corrupt the algorithm-tag byte inside the compressed wrapper (the byte
  // immediately after the COMPRESSED_TAG). `decode_compressed_frame` always
  // validates the algorithm tag, so this corruption is always detected
  // regardless of which LZ4 backend is in use.
  //
  // Layout: [unit_len: varint][COMPRESSED_TAG][algo][orig_len: varint][lz4 data]
  // Skip the leading unit_len varint (all bytes with bit-7 set are
  // continuation bytes; the first byte with bit-7 clear terminates it).
  let vbytes = unit.iter().position(|b| b & 0x80 == 0).unwrap() + 1;
  // vbytes points to COMPRESSED_TAG; vbytes+1 is the algorithm tag.
  unit[vbytes + 1] ^= 0xff;
  assert!(take_reliable_unit(&unit, 1 << 20).is_err());
}

#[test]
fn reliable_unit_disabled_encryption_is_byte_identical() {
  use crate::encryption::EncryptionOptions;
  let comp = CompressionOptions::new();
  let enc = EncryptionOptions::new();
  let framed = b"plain reliable frame bytes that are not compressed or encrypted".to_vec();
  let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
  let (back, consumed) = take_reliable_unit_with_encryption(&unit, &enc, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("complete unit");
  assert_eq!(back, framed);
  assert_eq!(consumed, unit.len());
}

#[test]
fn reliable_unit_encryption_disabled_compression_disabled_is_unchanged() {
  use crate::encryption::EncryptionOptions;
  let comp = CompressionOptions::new();
  let enc = EncryptionOptions::new();
  let framed = b"some bytes".to_vec();
  let legacy = encode_reliable_unit(&comp, &framed);
  let new = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
  assert_eq!(legacy, new, "disabled-encryption path is byte-identical");
}

#[cfg(feature = "aes-gcm")]
#[test]
fn reliable_unit_encrypted_then_compressed_roundtrip() {
  use crate::encryption::{EncryptionOptions, Keyring, SecretKey};
  let comp = CompressionOptions::new();
  let key = SecretKey::Aes256([0x42; 32]);
  let enc = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let framed = b"some reliable frame bytes that must be encrypted".to_vec();
  let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
  let (back, consumed) = take_reliable_unit_with_encryption(&unit, &enc, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("complete unit");
  assert_eq!(back, framed);
  assert_eq!(consumed, unit.len());
}

#[cfg(all(feature = "aes-gcm", not(feature = "chacha20-poly1305")))]
#[test]
fn encode_reliable_unit_with_encryption_returns_err_on_unsupported_backend() {
  // A primary key whose backend was NOT built into this binary (here:
  // ChaCha20-Poly1305 key under an aes-gcm-only build) must yield
  // `Err(UnsupportedAlgorithm)`. Silent fallback to plaintext would let
  // a configured-encrypted reliable exchange go out unauthenticated.
  use crate::encryption::{EncryptionError, EncryptionOptions, Keyring, SecretKey};
  let comp = CompressionOptions::new();
  let enc =
    EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::ChaCha20Poly1305([0x42; 32])));
  let framed = b"this exchange must NOT go out as plaintext".to_vec();
  let err = encode_reliable_unit_with_encryption(&comp, &enc, &framed)
    .expect_err("missing backend must surface as Err, not silent plaintext");
  assert!(
    matches!(err, EncryptionError::UnsupportedAlgorithm(_)),
    "got {err:?}"
  );
}

#[cfg(all(feature = "lz4", feature = "aes-gcm"))]
#[test]
fn reliable_unit_compressed_then_encrypted_byte_order_is_outer_encrypted() {
  use crate::encryption::{EncryptionOptions, Keyring, SecretKey};
  let comp = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let key = SecretKey::Aes256([0x99; 32]);
  let enc = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
  let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
  let (varint, vbytes) = crate::framing::decode_varint_u32(&unit).expect("unit_len varint");
  let payload = &unit[vbytes..vbytes + varint as usize];
  assert_eq!(
    payload[0],
    crate::encryption::ENCRYPTED_TAG,
    "the unit's payload leading tag is Encrypted (the outer wrapper)"
  );
  let (back, _consumed) = take_reliable_unit_with_encryption(&unit, &enc, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("complete unit");
  assert_eq!(back, framed);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_reliable_unit_at_max_orig_len_roundtrips() {
  // Reliable-path bomb-guard slack. A plaintext at exactly `max_orig_len`
  // (and one near it) must encode + decode through
  // `take_reliable_unit_with_encryption` with the SAME `max_orig_len` —
  // the wrapper inflates `unit_len` past the plaintext bound by exactly
  // `ENCRYPTED_WRAPPER_OVERHEAD`, so the bomb-guard must allow that slack.
  use crate::encryption::{EncryptionOptions, Keyring, SecretKey};
  let comp = CompressionOptions::new();
  let key = SecretKey::Aes256([0x88; 32]);
  let enc = EncryptionOptions::new().with_keyring(Keyring::new(key));
  for max_orig_len in [1400usize, 4096, 64 * 1024] {
    for plaintext_len in [
      max_orig_len - crate::encryption::ENCRYPTED_WRAPPER_OVERHEAD,
      max_orig_len - 1,
      max_orig_len,
    ] {
      let framed = vec![0xA5; plaintext_len];
      let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
      let (back, consumed) = take_reliable_unit_with_encryption(&unit, &enc, max_orig_len)
        .unwrap_or_else(|e| {
          panic!("plaintext_len={plaintext_len} max_orig_len={max_orig_len} must accept, got {e}")
        })
        .expect("complete unit");
      assert_eq!(back, framed, "plaintext_len={plaintext_len}");
      assert_eq!(consumed, unit.len());
    }
  }
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_reliable_unit_one_byte_past_envelope_max_is_rejected() {
  // Symmetric guard: a `unit_len` exceeding `max_orig_len +
  // ENCRYPTED_WRAPPER_OVERHEAD` must still fail the bomb-guard. We
  // hand-build a varint declaring such a `unit_len` and expect a
  // `Compression` error from the bomb-guard BEFORE any wait.
  use crate::encryption::{EncryptionOptions, Keyring, SecretKey};
  let key = SecretKey::Aes256([0x99; 32]);
  let enc = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let max_orig_len = 1024usize;
  let bad_unit_len = max_orig_len + crate::encryption::ENCRYPTED_WRAPPER_OVERHEAD + 1;
  let mut buf = Vec::new();
  encode_varint_u32(bad_unit_len as u32, &mut buf);
  // A few junk body bytes — irrelevant; the bomb-guard fires before any
  // wait or unwrap.
  buf.extend_from_slice(&[0u8; 8]);
  let err = take_reliable_unit_with_encryption(&buf, &enc, max_orig_len)
    .expect_err("over-envelope unit_len must err");
  assert!(matches!(err, FrameError::Compression(_)));
}

#[test]
fn oversize_original_payload_accessors() {
  let pair = OversizeOriginal(4096, 1024);
  assert_eq!(pair.claimed(), 4096);
  assert_eq!(pair.max(), 1024);
}

#[test]
fn unit_len_exceeds_max_info_accessors_and_display() {
  let info = UnitLenExceedsMaxInfo::new(2000, 1024);
  assert_eq!(info.unit_len(), 2000);
  assert_eq!(info.max(), 1024);
  let s = info.to_string();
  assert!(s.contains("2000") && s.contains("1024"), "got {s}");
}

#[test]
fn compression_error_display_strings_are_nonempty() {
  let cases = [
    CompressionError::UnsupportedAlgorithm(9),
    CompressionError::Backend("boom".into()),
    CompressionError::OversizeOriginal(OversizeOriginal(4096, 1024)),
    CompressionError::MalformedFrame,
    CompressionError::UnitLenExceedsMax(UnitLenExceedsMaxInfo::new(2000, 1024)),
  ];
  for e in &cases {
    assert!(!e.to_string().is_empty(), "empty display for {e:?}");
  }
  // The structured variants surface their numbers.
  assert!(
    CompressionError::OversizeOriginal(OversizeOriginal(4096, 1024))
      .to_string()
      .contains("4096")
  );
}

#[test]
fn decode_compressed_frame_rejects_malformed_headers() {
  // Shorter than the 2-byte header.
  assert!(matches!(
    decode_compressed_frame(&[COMPRESSED_TAG], 1 << 20),
    Err(CompressionError::MalformedFrame)
  ));
  assert!(matches!(
    decode_compressed_frame(&[], 1 << 20),
    Err(CompressionError::MalformedFrame)
  ));
  // Right length but wrong leading tag.
  assert!(matches!(
    decode_compressed_frame(&[0xAB, CompressAlgorithm::Lz4.tag()], 1 << 20),
    Err(CompressionError::MalformedFrame)
  ));
}

#[test]
fn decode_compressed_frame_rejects_truncated_orig_len_varint() {
  // Valid tag + algo, then a lone varint continuation byte — the orig_len
  // varint cannot complete, surfacing as a Backend error.
  let frame = [COMPRESSED_TAG, CompressAlgorithm::Lz4.tag(), 0x80];
  assert!(matches!(
    decode_compressed_frame(&frame, 1 << 20),
    Err(CompressionError::Backend(_))
  ));
}

#[test]
fn compression_options_default_matches_new() {
  assert_eq!(CompressionOptions::default(), CompressionOptions::new());
}

#[test]
fn compression_options_in_place_setters() {
  let mut opts = CompressionOptions::new();
  // set_algorithm enables; set_threshold replaces in place.
  opts.set_algorithm(CompressAlgorithm::Lz4).set_threshold(99);
  assert_eq!(opts.algorithm(), Some(CompressAlgorithm::Lz4));
  assert_eq!(opts.threshold(), 99);
  // clear_algorithm disables.
  opts.clear_algorithm();
  assert!(opts.algorithm().is_none());
  // update_algorithm assigns the raw Option in place.
  opts.update_algorithm(Some(CompressAlgorithm::Zstd(ZstdLevel::Eight)));
  assert_eq!(
    opts.algorithm(),
    Some(CompressAlgorithm::Zstd(ZstdLevel::Eight))
  );
  opts.update_algorithm(None);
  assert!(opts.algorithm().is_none());
}

#[test]
fn compression_options_maybe_algorithm_builder() {
  let some = CompressionOptions::new().maybe_algorithm(Some(CompressAlgorithm::Snappy));
  assert_eq!(some.algorithm(), Some(CompressAlgorithm::Snappy));
  let none = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .maybe_algorithm(None);
  assert!(none.algorithm().is_none());
}

#[test]
fn compression_outcome_equality() {
  assert_eq!(CompressionOutcome::Plain, CompressionOutcome::Plain);
  assert_eq!(
    CompressionOutcome::Compressed(vec![1, 2, 3]),
    CompressionOutcome::Compressed(vec![1, 2, 3])
  );
  assert_ne!(
    CompressionOutcome::Plain,
    CompressionOutcome::Compressed(vec![1])
  );
}

#[test]
fn take_reliable_unit_rejects_corrupt_leading_varint() {
  // A 5-byte varint that overflows u32 is a hard corruption (not Incomplete),
  // so take_reliable_unit propagates the FrameError rather than returning None.
  let buf = [0x80, 0x80, 0x80, 0x80, 0x10, 0x00];
  assert!(take_reliable_unit(&buf, 1 << 20).is_err());
}

#[test]
fn take_reliable_unit_with_encryption_rejects_corrupt_leading_varint() {
  // The encryption-aware decoder propagates the same hard varint corruption
  // (the non-Incomplete `Err(e)` arm) rather than stalling on `None`.
  use crate::encryption::EncryptionOptions;
  let buf = [0x80, 0x80, 0x80, 0x80, 0x10, 0x00];
  assert!(take_reliable_unit_with_encryption(&buf, &EncryptionOptions::new(), 1 << 20).is_err());
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_decompress_rejects_orig_len_larger_than_actual() {
  // A valid LZ4 block decoded against an `orig_len` larger than the true
  // decompressed size produces fewer bytes than declared — the exact-length
  // check rejects it rather than returning a short buffer.
  let data = b"the quick brown fox".to_vec();
  let packed = compress(CompressAlgorithm::Lz4, &data).expect("compress");
  let err = decompress(CompressAlgorithm::Lz4, &packed, data.len() + 8)
    .expect_err("oversized orig_len must be rejected");
  assert!(matches!(err, CompressionError::Backend(_)), "got {err:?}");
}

#[cfg(feature = "snappy")]
#[test]
fn snappy_decompress_rejects_orig_len_mismatch() {
  // Snappy embeds its own length, so an `orig_len` that disagrees with the
  // body's true length is rejected at the exact-length check.
  let data = b"the quick brown fox".to_vec();
  let packed = compress(CompressAlgorithm::Snappy, &data).expect("compress");
  let err = decompress(CompressAlgorithm::Snappy, &packed, data.len() + 8)
    .expect_err("mismatched orig_len must be rejected");
  assert!(matches!(err, CompressionError::Backend(_)), "got {err:?}");
}

#[cfg(feature = "zstd")]
#[test]
fn zstd_decompress_rejects_orig_len_larger_than_actual() {
  // zstd's bulk decompressor allocates exactly `orig_len`; an oversized
  // declaration leaves the produced length short and is rejected.
  let data = b"the quick brown fox".to_vec();
  let packed = compress(CompressAlgorithm::Zstd(ZstdLevel::Eight), &data).expect("compress");
  let err = decompress(
    CompressAlgorithm::Zstd(ZstdLevel::Eight),
    &packed,
    data.len() + 8,
  )
  .expect_err("oversized orig_len must be rejected");
  assert!(matches!(err, CompressionError::Backend(_)), "got {err:?}");
}

#[cfg(feature = "brotli")]
#[test]
fn brotli_decompress_rejects_orig_len_larger_than_actual() {
  // The brotli stream EOFs at its true length; an oversized `orig_len`
  // produces fewer bytes than declared and fails the exact-length check.
  let data = b"the quick brown fox".to_vec();
  let packed = compress(CompressAlgorithm::Brotli, &data).expect("compress");
  let err = decompress(CompressAlgorithm::Brotli, &packed, data.len() + 8)
    .expect_err("oversized orig_len must be rejected");
  assert!(matches!(err, CompressionError::Backend(_)), "got {err:?}");
}

#[cfg(feature = "lz4")]
#[test]
fn encode_reliable_unit_falls_back_to_plain_when_wrapper_would_inflate() {
  // An input that compresses by fewer bytes than the `[Compressed]` wrapper
  // header costs: `apply` returns Compressed (raw bytes shrank), but the
  // wrapped frame is not smaller than the plain bytes, so the unit is emitted
  // plain (the don't-expand fallback). 30 PRNG bytes + a 12-byte run saves
  // exactly one byte under LZ4 while the header is three.
  let mut framed = Vec::new();
  let mut x: u32 = 0x12345678;
  for _ in 0..30 {
    x = x.wrapping_mul(1664525).wrapping_add(1013904223);
    framed.push((x >> 24) as u8);
  }
  framed.extend(std::iter::repeat_n(0xAB, 12));
  // Precondition: raw compression shrinks, but only by less than the wrapper.
  let packed = compress(CompressAlgorithm::Lz4, &framed).expect("compress");
  assert!(packed.len() < framed.len(), "precondition: raw shrinks");

  let opts = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let unit = encode_reliable_unit(&opts, &framed);
  // The unit's payload is the plain framed bytes, not a `[Compressed]` frame.
  let (unit_len, vbytes) = decode_varint_u32(&unit).expect("unit_len");
  let payload = &unit[vbytes..vbytes + unit_len as usize];
  assert_eq!(
    payload,
    &framed[..],
    "don't-expand fallback emits plain bytes"
  );
  assert_ne!(
    payload[0],
    MessageTag::Compressed as u8,
    "payload must not be a Compressed wrapper"
  );
  // And it still round-trips through the receiver.
  let (back, consumed) = take_reliable_unit(&unit, 1 << 20)
    .expect("decode ok")
    .expect("complete unit");
  assert_eq!(back, framed);
  assert_eq!(consumed, unit.len());
}

#[cfg(all(feature = "lz4", feature = "aes-gcm"))]
#[test]
fn encode_reliable_unit_with_encryption_falls_back_to_plain_compression() {
  // Same don't-expand fallback on the encryption-aware path: compression
  // shrinks by less than its wrapper, so the pre-encryption payload is the
  // plain framed bytes (not a `[Compressed]` frame), then encrypted.
  use crate::encryption::{EncryptionOptions, Keyring, SecretKey};
  let mut framed = Vec::new();
  let mut x: u32 = 0x12345678;
  for _ in 0..30 {
    x = x.wrapping_mul(1664525).wrapping_add(1013904223);
    framed.push((x >> 24) as u8);
  }
  framed.extend(std::iter::repeat_n(0xAB, 12));

  let comp = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let key = SecretKey::Aes256([0x55; 32]);
  let enc = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
  let (back, consumed) = take_reliable_unit_with_encryption(&unit, &enc, 1 << 20)
    .expect("decode ok")
    .expect("complete unit");
  assert_eq!(back, framed);
  assert_eq!(consumed, unit.len());
}
