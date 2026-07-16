use super::*;
use crate::messages::memberlist::v1::Node;

#[test]
fn varint_roundtrip() {
  for value in [0u32, 1, 127, 128, 16383, 16384, u32::MAX] {
    let mut buf = Vec::new();
    encode_varint_u32(value, &mut buf);
    let (decoded, consumed) = decode_varint_u32(&buf).unwrap();
    assert_eq!(value, decoded, "value {value}");
    assert_eq!(consumed, buf.len(), "value {value} length");
  }
}

#[test]
fn varint_max_five_byte_boundary_is_accepted() {
  // u32::MAX encodes as `FF FF FF FF 0F`; the 5th byte is exactly the
  // 0x0f ceiling and must still decode (regression guard for the new
  // 5th-byte overflow check).
  let buf = [0xff, 0xff, 0xff, 0xff, 0x0f];
  let (v, n) = decode_varint_u32(&buf).expect("max u32 must decode");
  assert_eq!(v, u32::MAX);
  assert_eq!(n, 5);
}

#[test]
fn varint_overflowing_fifth_byte_is_rejected_not_aliased() {
  // `80 80 80 80 10` encodes 2^32. Without the fifth-byte guard the
  // shift wraps the u32 to 0 with the continuation bit clear, so the
  // value decoded as length 0 and `decode_plain_frame` accepted a bogus
  // empty-body frame.
  for fifth in [0x10u8, 0x7f, 0x80, 0xff] {
    let buf = [0x80, 0x80, 0x80, 0x80, fifth];
    assert!(
      matches!(decode_varint_u32(&buf), Err(FrameError::VarintOverflow)),
      "fifth byte {fifth:#x} must overflow, not alias"
    );
  }
  // And it must not be silently accepted at the frame layer either:
  // `[tag=UserData][80 80 80 80 10][...]` must NOT decode as a 0-len frame.
  let mut frame = vec![MessageTag::UserData as u8, 0x80, 0x80, 0x80, 0x80, 0x10];
  frame.extend_from_slice(b"trailing");
  assert!(
    matches!(decode_plain_frame(&frame), Err(FrameError::VarintOverflow)),
    "a 2^32 length prefix must be rejected, not accepted as an empty frame"
  );
}

#[test]
fn truncated_varint_is_incomplete_not_overflow() {
  // A length prefix cut mid-continuation needs more bytes; it must
  // report Incomplete (so streaming callers wait) rather than the hard
  // VarintOverflow corruption error.
  for prefix in [vec![0x80], vec![0x80, 0x80], vec![0x80, 0x80, 0x80, 0x80]] {
    assert!(
      matches!(decode_varint_u32(&prefix), Err(FrameError::Incomplete(_))),
      "truncated varint {prefix:?} must be Incomplete"
    );
  }
  // Through the frame decoder: tag present, length prefix truncated.
  let frame = [MessageTag::Ping as u8, 0x80, 0x80];
  match decode_plain_frame(&frame) {
    Err(FrameError::Incomplete(f)) => {
      assert_eq!(f.available, frame.len());
      assert!(f.required > f.available);
    }
    other => panic!("expected Incomplete, got {other:?}"),
  }
}

#[test]
fn plain_frame_roundtrip_for_alive() {
  let alive = Alive {
    incarnation: Some(7),
    meta: bytes::Bytes::new(),
    node: Some(Node {
      id: Some(bytes::Bytes::from_static(b"node-a")),
      addr: Some(bytes::Bytes::from_static(b"127.0.0.1:7000")),
      ..Default::default()
    })
    .into(),
    protocol_version: Some(1),
    delegate_version: Some(1),
    ..Default::default()
  };
  let msg = AnyMessage::Alive(alive.clone());
  let encoded = encode_message(&msg).expect("encode");
  assert_eq!(encoded[0], MessageTag::Alive as u8);

  let (consumed, decoded) = decode_message(&encoded).expect("decode");
  assert_eq!(consumed, encoded.len());
  match decoded {
    AnyMessage::Alive(d) => {
      assert_eq!(d.incarnation, alive.incarnation);
      assert_eq!(d.protocol_version, alive.protocol_version);
    }
    _ => panic!("wrong variant"),
  }
}

#[test]
fn outer_wrapping_matches_legacy_byte_layout() {
  // Outer wrapping is `[TAG][VARINT_LEN][BODY]`. Verify byte positions.
  // This is the byte-level contract that must be preserved across versions.
  let body = b"hello";
  let frame = encode_plain_frame(MessageTag::UserData, body).expect("encode");
  // First byte: tag
  assert_eq!(frame[0], MessageTag::UserData as u8);
  // Second byte: varint for length 5 — fits in a single byte (< 0x80).
  assert_eq!(frame[1], 0x05);
  // Remaining bytes: body unchanged.
  assert_eq!(&frame[2..], body);
}

#[test]
fn incomplete_frame_returns_error() {
  let mut frame = encode_plain_frame(MessageTag::Nack, b"x").expect("encode");
  frame.pop(); // truncate body by one byte
  let err = decode_plain_frame(&frame).unwrap_err();
  assert!(matches!(err, FrameError::Incomplete(_)));
}

#[test]
fn decode_plain_frame_rejects_body_len_exceeding_buffer_without_panicking() {
  // A frame that declares a ~4 GiB body but carries only a handful of bytes.
  // On a 64-bit host `header_len + body_len` does not overflow usize, so this
  // drives the same over-buffer branch a 32-bit wrap would otherwise take: the
  // decoder must report Incomplete, never panic or slice out of range.
  let mut frame = Vec::new();
  frame.push(MessageTag::UserData as u8);
  encode_varint_u32(u32::MAX, &mut frame); // declared body length ~4 GiB
  frame.extend_from_slice(b"only-a-few-bytes");
  match decode_plain_frame(&frame) {
    Err(FrameError::Incomplete(f)) => {
      assert_eq!(f.available(), frame.len());
      assert!(
        f.required() > f.available(),
        "required {} must exceed available {}",
        f.required(),
        f.available()
      );
    }
    other => panic!("expected Incomplete, got {other:?}"),
  }
}

#[test]
#[cfg(target_pointer_width = "32")]
fn decode_plain_frame_body_len_overflow_saturates_to_incomplete_on_32bit() {
  // On a 32-bit target `header_len + u32::MAX` overflows usize. The checked
  // addition must surface Incomplete with a saturated `required`, never a value
  // wrapped below header_len that would pass the length guard and panic the
  // body slice.
  let mut frame = Vec::new();
  frame.push(MessageTag::UserData as u8);
  encode_varint_u32(u32::MAX, &mut frame);
  frame.extend_from_slice(b"body");
  match decode_plain_frame(&frame) {
    Err(FrameError::Incomplete(f)) => {
      assert_eq!(f.required(), usize::MAX, "overflow must saturate required");
    }
    other => panic!("expected Incomplete, got {other:?}"),
  }
}

fn sample_ping() -> AnyMessage {
  AnyMessage::Ping(Ping {
    sequence_number: Some(7),
    source: Some(Node {
      id: Some(bytes::Bytes::from_static(b"a")),
      addr: Some(bytes::Bytes::from_static(b"\x7f\x00\x00\x01")),
      ..Default::default()
    })
    .into(),
    target: Some(Node {
      id: Some(bytes::Bytes::from_static(b"b")),
      addr: Some(bytes::Bytes::from_static(b"\x7f\x00\x00\x02")),
      ..Default::default()
    })
    .into(),
    ..Default::default()
  })
}

fn sample_ack() -> AnyMessage {
  AnyMessage::Ack(Ack {
    sequence_number: 9,
    ..Default::default()
  })
}

#[test]
fn compound_roundtrip_splits_into_ordered_parts() {
  let msgs = [sample_ping(), sample_ack(), sample_ping()];
  let buf = encode_compound(&msgs).expect("encode");
  assert_eq!(buf[0], MessageTag::Compound as u8);
  let parts = decode_compound(&buf).expect("decode");
  assert_eq!(parts.len(), 3);
  let decoded: Vec<AnyMessage> = parts
    .iter()
    .map(|p| decode_message(p).expect("part decodes").1)
    .collect();
  assert_eq!(decoded, vec![sample_ping(), sample_ack(), sample_ping()]);
}

#[test]
fn compound_encode_rejects_fewer_than_two() {
  assert!(matches!(encode_compound(&[]), Err(FrameError::Decode)));
  assert!(matches!(
    encode_compound(&[sample_ping()]),
    Err(FrameError::Decode)
  ));
}

#[test]
fn compound_decode_rejects_count_below_two() {
  let part = encode_message(&sample_ping()).unwrap();
  let mut buf = vec![MessageTag::Compound as u8];
  encode_varint_u32(1, &mut buf);
  encode_varint_u32(part.len() as u32, &mut buf);
  buf.extend_from_slice(&part);
  assert!(matches!(decode_compound(&buf), Err(FrameError::Decode)));
}

#[test]
fn compound_decode_rejects_truncated_part() {
  let msgs = [sample_ping(), sample_ack()];
  let mut buf = encode_compound(&msgs).unwrap();
  buf.truncate(buf.len() - 1);
  assert!(matches!(
    decode_compound(&buf),
    Err(FrameError::Incomplete(..))
  ));
}

#[test]
fn compound_decode_rejects_truncated_count_varint() {
  // [Compound][0x80]: count varint cut after a continuation byte — the
  // adversary's first probe. Must surface as Incomplete, not panic.
  let buf = [MessageTag::Compound as u8, 0x80];
  assert!(matches!(
    decode_compound(&buf),
    Err(FrameError::Incomplete(..))
  ));
}

#[test]
fn compound_decode_rejects_truncated_inner_len_varint() {
  // Valid [Compound][count=2][len0][part0] but the entire second part
  // (its inner_len varint included) is absent: the loop's varint read
  // hits an empty slice and must report Incomplete. `count(2)` is <=
  // the bytes remaining after the header so the DoS guard does not fire
  // first — this exercises the per-part Incomplete path specifically.
  let p0 = encode_message(&sample_ping()).unwrap();
  let mut buf = vec![MessageTag::Compound as u8];
  encode_varint_u32(2, &mut buf);
  encode_varint_u32(p0.len() as u32, &mut buf);
  buf.extend_from_slice(&p0);
  assert!(matches!(
    decode_compound(&buf),
    Err(FrameError::Incomplete(..))
  ));
}

#[test]
fn compound_decode_rejects_oversized_count_without_huge_alloc() {
  // A 6-byte datagram whose count varint decodes to u32::MAX must be
  // rejected as Decode BEFORE any `Vec::with_capacity(count)` — a
  // single malformed gossip packet must not abort the process via a
  // multi-GB allocation. The test passing (rather than OOM-aborting the
  // harness) is the assertion.
  let buf = [MessageTag::Compound as u8, 0xff, 0xff, 0xff, 0xff, 0x0f];
  assert!(matches!(decode_compound(&buf), Err(FrameError::Decode)));
}

#[test]
fn compound_decode_rejects_trailing_bytes() {
  let msgs = [sample_ping(), sample_ack()];
  let mut buf = encode_compound(&msgs).unwrap();
  buf.push(0xAB);
  assert!(matches!(decode_compound(&buf), Err(FrameError::Decode)));
}

#[test]
fn decode_message_still_rejects_compound_tag() {
  let buf = encode_compound(&[sample_ping(), sample_ack()]).unwrap();
  assert!(matches!(
    decode_message(&buf),
    Err(FrameError::UnknownTag(1))
  ));
}

#[test]
fn compound_overhead_constants_are_upper_bounds() {
  assert_eq!(COMPOUND_TAG_LEN, 1);
  assert_eq!(COMPOUND_MAX_COUNT_PREFIX_LEN, 5);
  assert_eq!(COMPOUND_MAX_PART_PREFIX_LEN, 5);
}

#[cfg(feature = "lz4")]
#[test]
fn unwrap_loop_strips_compression_off_a_plain_frame() {
  use crate::compression::{CompressAlgorithm, compress, encode_compressed_frame};
  let inner = encode_message(&sample_ping()).expect("encode ping");
  let packed = compress(CompressAlgorithm::Lz4, &inner).expect("compress");
  let wrapped = encode_compressed_frame(CompressAlgorithm::Lz4, inner.len(), &packed);
  let unwrapped = unwrap_transforms(&wrapped, 1 << 20).expect("unwrap");
  assert_eq!(unwrapped, inner);
  let (_consumed, msg) = decode_message(&unwrapped).expect("decode inner");
  assert_eq!(msg, sample_ping());
}

#[cfg(feature = "lz4")]
#[test]
fn unwrap_loop_strips_compression_off_a_compound_frame() {
  use crate::compression::{CompressAlgorithm, compress, encode_compressed_frame};
  let inner = encode_compound(&[sample_ping(), sample_ack()]).expect("encode compound");
  let packed = compress(CompressAlgorithm::Lz4, &inner).expect("compress");
  let wrapped = encode_compressed_frame(CompressAlgorithm::Lz4, inner.len(), &packed);
  let unwrapped = unwrap_transforms(&wrapped, 1 << 20).expect("unwrap");
  assert_eq!(unwrapped, inner);
  let parts = decode_compound(&unwrapped).expect("decode compound");
  assert_eq!(parts.len(), 2);
}

#[test]
fn unwrap_loop_passes_a_non_wrapper_frame_through() {
  let inner = encode_message(&sample_ping()).expect("encode ping");
  let out = unwrap_transforms(&inner, 1 << 20).expect("unwrap");
  assert_eq!(out, inner);
}

#[cfg(compression)]
#[test]
fn unwrap_loop_rejects_an_unknown_algorithm_wrapper() {
  let mut frame = vec![MessageTag::Compressed as u8, 222u8];
  encode_varint_u32(4, &mut frame);
  frame.extend_from_slice(b"data");
  assert!(matches!(
    unwrap_transforms(&frame, 1 << 20),
    Err(FrameError::Compression(_))
  ));
}

#[test]
fn multibyte_length_body_round_trips_through_fallible_encode() {
  // Encode is fallible — it rejects bodies whose length overflows the
  // u32 prefix instead of silently truncating via `as u32`. A
  // large-but-representable body must still encode Ok and round-trip
  // with a correct multi-byte varint length. The >u32::MAX rejection is
  // structurally guaranteed by `u32::try_from` and is not unit-testable
  // without a >4 GiB allocation.
  let body = vec![0xABu8; 200_000];
  let frame = encode_plain_frame(MessageTag::UserData, &body).expect("encode");
  assert_eq!(frame[0], MessageTag::UserData as u8);
  let (tag, decoded_body, consumed) = decode_plain_frame(&frame).expect("decode");
  assert_eq!(tag, MessageTag::UserData);
  assert_eq!(decoded_body, &body[..]);
  assert_eq!(consumed, frame.len());
  // The error variant is wired for the overflow path. `u32::MAX as usize + 1`
  // only exists as a distinct value on 64-bit; on a 32-bit target
  // `usize::MAX == u32::MAX` and the addition overflows at compile time.
  #[cfg(target_pointer_width = "64")]
  {
    let e = FrameError::FrameTooLarge(u32::MAX as usize + 1);
    assert!(e.to_string().contains("too large"));
  }
}

#[cfg(encryption)]
#[test]
fn framing_encrypted_tag_value_is_pinned() {
  // Pinned numeric value — a change is a wire-protocol break.
  assert_eq!(MessageTag::Encrypted as u8, 13);
  // The encryption module's mirror constant must agree.
  assert_eq!(
    MessageTag::Encrypted as u8,
    crate::encryption::ENCRYPTED_TAG
  );
}

#[cfg(checksum)]
#[test]
fn framing_checksumed_tag_value_is_pinned() {
  // Pinned numeric value — a change is a wire-protocol break.
  assert_eq!(MessageTag::Checksumed as u8, 15);
  // The checksum module's mirror constant must agree.
  assert_eq!(
    MessageTag::Checksumed as u8,
    crate::checksum::CHECKSUMED_TAG
  );
}

#[cfg(feature = "crc32")]
#[test]
fn unwrap_loop_verifies_and_strips_a_checksum_wrapper() {
  use crate::checksum::{ChecksumAlgorithm, encode_checksummed_frame};
  let inner = encode_message(&sample_ping()).expect("encode ping");
  let wrapped = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &inner).expect("checksum");
  assert_eq!(wrapped[0], MessageTag::Checksumed as u8);
  let unwrapped = unwrap_transforms(&wrapped, 1 << 20).expect("unwrap");
  assert_eq!(unwrapped, inner);
  let (_consumed, msg) = decode_message(&unwrapped).expect("decode inner");
  assert_eq!(msg, sample_ping());
}

#[cfg(feature = "crc32")]
#[test]
fn unwrap_loop_rejects_a_corrupted_checksum_payload() {
  use crate::checksum::{ChecksumAlgorithm, encode_checksummed_frame};
  let inner = encode_message(&sample_ping()).expect("encode ping");
  let mut wrapped = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &inner).expect("checksum");
  // Flip the final payload byte so the recomputed digest disagrees.
  let last = wrapped.len() - 1;
  wrapped[last] ^= 0xff;
  assert!(matches!(
    unwrap_transforms(&wrapped, 1 << 20),
    Err(FrameError::Checksum(
      crate::checksum::ChecksumError::Mismatch
    ))
  ));
}

#[cfg(all(feature = "crc32", feature = "lz4"))]
#[test]
fn unwrap_loop_strips_checksum_then_compression_in_stack_order() {
  // The wire stack is `[Checksumed[[Compressed][frame]]]` (inner→outer:
  // compress → checksum). The unwrap loop strips the checksum wrapper
  // (verifying the digest) then the compression wrapper.
  use crate::{
    checksum::{ChecksumAlgorithm, encode_checksummed_frame},
    compression::{CompressAlgorithm, compress, encode_compressed_frame},
  };
  let inner = encode_message(&sample_ping()).expect("encode ping");
  let packed = compress(CompressAlgorithm::Lz4, &inner).expect("compress");
  let compressed = encode_compressed_frame(CompressAlgorithm::Lz4, inner.len(), &packed);
  let wrapped = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &compressed).expect("checksum");
  assert_eq!(wrapped[0], MessageTag::Checksumed as u8);
  let unwrapped = unwrap_transforms(&wrapped, 1 << 20).expect("unwrap");
  assert_eq!(unwrapped, inner);
  let (_consumed, msg) = decode_message(&unwrapped).expect("decode inner");
  assert_eq!(msg, sample_ping());
}

#[cfg(encryption)]
#[test]
fn unwrap_loop_rejects_unknown_encrypted_algorithm() {
  // A buffer led by ENCRYPTED_TAG with an unknown algorithm tag fails the
  // unwrap with FrameError::Encryption.
  let mut frame = vec![MessageTag::Encrypted as u8, 222u8];
  frame.extend_from_slice(&[0u8; 12]); // nonce
  frame.extend_from_slice(&[0u8; 32]); // body large enough to look plausible
  let opts = crate::encryption::EncryptionOptions::new();
  let err =
    unwrap_transforms_with_encryption(&frame, 1 << 20, &opts).expect_err("unknown algo must err");
  assert!(matches!(err, FrameError::Encryption(_)));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_mode_rejects_unencrypted_plaintext_frame_at_outer_layer() {
  // Strict-mode entry check. A coordinator with a configured keyring must
  // NOT accept an outer frame whose leading byte is a plain-message tag —
  // a network attacker could otherwise inject unauthenticated SWIM
  // membership traffic into a cluster the operator believes is
  // integrity-protected. The check fires before any decoding and is
  // feature-independent (no AEAD backend is exercised here).
  use crate::encryption::{EncryptionOptions, Keyring, SecretKey};
  // Any keyring enables encryption; the check fires on the leading byte
  // alone, so the AES backend is not required for this regression.
  let key = SecretKey::Aes128([0u8; 16]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let plain = encode_message(&sample_ping()).expect("encode ping");
  let err = unwrap_transforms_with_encryption(&plain, 1 << 20, &opts)
    .expect_err("plain frame on encrypted path must be rejected");
  assert!(matches!(err, FrameError::Encryption(_)));
  // A compound-led plain frame is also rejected (the same outermost-tag check).
  let compound = encode_compound(&[sample_ping(), sample_ack()]).expect("encode compound");
  let err = unwrap_transforms_with_encryption(&compound, 1 << 20, &opts)
    .expect_err("plain compound on encrypted path must be rejected");
  assert!(matches!(err, FrameError::Encryption(_)));
  // An empty buffer on the encrypted path is `Empty`, not silently accepted.
  let err = unwrap_transforms_with_encryption(&[], 1 << 20, &opts)
    .expect_err("empty buffer on encrypted path must error");
  assert!(matches!(err, FrameError::Empty));
}

#[cfg(all(feature = "lz4", feature = "aes-gcm"))]
#[test]
fn unwrap_loop_strips_encrypted_then_compressed() {
  use crate::{
    compression::{CompressAlgorithm, compress, encode_compressed_frame},
    encryption::{EncryptAlgorithm, EncryptionOptions, Keyring, SecretKey, encode_encrypted_frame},
  };
  let inner = encode_message(&sample_ping()).expect("encode ping");
  // Inner Compressed wrapper.
  let packed = compress(CompressAlgorithm::Lz4, &inner).expect("compress");
  let compressed = encode_compressed_frame(CompressAlgorithm::Lz4, inner.len(), &packed);
  // Outer Encrypted wrapper.
  let key = SecretKey::Aes256([0x42; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let wrapped =
    encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &compressed).expect("encode");
  assert_eq!(wrapped[0], MessageTag::Encrypted as u8);

  let unwrapped = unwrap_transforms_with_encryption(&wrapped, 1 << 20, &opts).expect("unwrap");
  assert_eq!(unwrapped.as_ref(), inner.as_slice());
  let (_consumed, msg) = decode_message(unwrapped.as_ref()).expect("decode inner");
  assert_eq!(msg, sample_ping());
}

#[test]
fn message_tag_try_from_roundtrips_all_valid_bytes() {
  let valid = [
    (1u8, MessageTag::Compound),
    (2, MessageTag::Ping),
    (3, MessageTag::IndirectPing),
    (4, MessageTag::Ack),
    (5, MessageTag::Suspect),
    (6, MessageTag::Alive),
    (7, MessageTag::Dead),
    (8, MessageTag::PushPull),
    (9, MessageTag::UserData),
    (10, MessageTag::Nack),
    (11, MessageTag::ErrorResponse),
    (13, MessageTag::Encrypted),
    (14, MessageTag::Compressed),
    (15, MessageTag::Checksumed),
  ];
  for (byte, tag) in valid {
    assert_eq!(MessageTag::try_from(byte).unwrap(), tag);
    assert_eq!(tag as u8, byte);
  }
  // Reserved / unassigned tags are rejected. Tag 12 (Labeled) is handled by
  // the codec's label layer, not this enum.
  for byte in [0u8, 12, 16, 200, 255] {
    assert!(matches!(
      MessageTag::try_from(byte),
      Err(FrameError::UnknownTag(b)) if b == byte
    ));
  }
}

#[test]
fn message_tag_for_message_maps_each_variant() {
  use crate::messages::memberlist::v1::{
    Alive, Dead, ErrorResponse, IndirectPing, Nack, PushPull, Suspect, UserData,
  };
  let cases: [(AnyMessage, MessageTag); 10] = [
    (AnyMessage::Alive(Alive::default()), MessageTag::Alive),
    (AnyMessage::Suspect(Suspect::default()), MessageTag::Suspect),
    (AnyMessage::Dead(Dead::default()), MessageTag::Dead),
    (sample_ping(), MessageTag::Ping),
    (
      AnyMessage::IndirectPing(IndirectPing::default()),
      MessageTag::IndirectPing,
    ),
    (sample_ack(), MessageTag::Ack),
    (AnyMessage::Nack(Nack::default()), MessageTag::Nack),
    (
      AnyMessage::PushPull(PushPull::default()),
      MessageTag::PushPull,
    ),
    (
      AnyMessage::UserData(UserData::default()),
      MessageTag::UserData,
    ),
    (
      AnyMessage::ErrorResponse(ErrorResponse::default()),
      MessageTag::ErrorResponse,
    ),
  ];
  for (msg, tag) in cases {
    assert_eq!(MessageTag::for_message(&msg), tag);
  }
}

#[test]
fn incomplete_frame_accessors_and_display() {
  let f = IncompleteFrame::new(3, 10);
  assert_eq!(f.available(), 3);
  assert_eq!(f.required(), 10);
  let s = f.to_string();
  assert!(s.contains('3') && s.contains("10"), "got {s}");
}

#[test]
fn frame_error_display_strings_are_nonempty() {
  let mut cases = vec![
    FrameError::Empty,
    FrameError::Incomplete(IncompleteFrame::new(1, 2)),
    FrameError::UnknownTag(99),
    FrameError::VarintOverflow,
    FrameError::Decode,
  ];
  // `1 << 33` only exists as a distinct usize value on 64-bit; on a 32-bit
  // target `usize::MAX == u32::MAX` and the shift overflows at compile time.
  #[cfg(target_pointer_width = "64")]
  cases.push(FrameError::FrameTooLarge(1 << 33));
  for e in cases {
    assert!(!e.to_string().is_empty(), "empty display for {e:?}");
  }
}

#[test]
fn decode_plain_frame_rejects_empty_and_unknown_tag() {
  assert!(matches!(decode_plain_frame(&[]), Err(FrameError::Empty)));
  // Tag 0 is not a valid MessageTag.
  assert!(matches!(
    decode_plain_frame(&[0u8, 0]),
    Err(FrameError::UnknownTag(0))
  ));
}

#[test]
fn decode_message_zerocopy_roundtrips_and_rejects_wrappers() {
  // A clean plain frame decodes through the zero-copy path.
  let frame = Bytes::from(encode_message(&sample_ack()).expect("encode"));
  let (consumed, msg) = decode_message_zerocopy(&frame).expect("zerocopy decode");
  assert_eq!(consumed, frame.len());
  assert_eq!(msg, sample_ack());

  // The four outer-wrapper tags are rejected as UnknownTag (they are not
  // decoded at the plain-frame layer), matching `decode_message`.
  for tag in [
    MessageTag::Compound,
    MessageTag::Encrypted,
    MessageTag::Compressed,
    MessageTag::Checksumed,
  ] {
    let raw = Bytes::from(encode_plain_frame(tag, b"body").expect("encode wrapper frame"));
    assert!(
      matches!(
        decode_message_zerocopy(&raw),
        Err(FrameError::UnknownTag(b)) if b == tag as u8
      ),
      "tag {tag:?} must be rejected by the zerocopy path"
    );
  }
}

#[test]
fn decode_message_rejects_transform_wrapper_tags() {
  for tag in [
    MessageTag::Encrypted,
    MessageTag::Compressed,
    MessageTag::Checksumed,
  ] {
    let raw = encode_plain_frame(tag, b"body").expect("encode wrapper frame");
    assert!(
      matches!(
        decode_message(&raw),
        Err(FrameError::UnknownTag(b)) if b == tag as u8
      ),
      "decode_message must reject {tag:?}"
    );
  }
}

#[test]
fn decode_message_rejects_corrupt_body() {
  // A valid Ping tag + length but garbage body bytes ⇒ buffa decode fails ⇒
  // FrameError::Decode (not a panic).
  let frame = encode_plain_frame(MessageTag::Ping, &[0xFF, 0xFF, 0xFF, 0xFF]).expect("encode");
  assert!(matches!(decode_message(&frame), Err(FrameError::Decode)));
}

#[test]
fn decode_compound_rejects_empty_and_non_compound_lead() {
  assert!(matches!(decode_compound(&[]), Err(FrameError::Empty)));
  // A plain (non-compound) frame fed to decode_compound is UnknownTag.
  let plain = encode_message(&sample_ping()).expect("encode");
  assert!(matches!(
    decode_compound(&plain),
    Err(FrameError::UnknownTag(_))
  ));
}

/// One representative (non-empty-bodied) value of every [`AnyMessage`]
/// variant, so encode + both decode paths exercise each per-variant arm.
fn one_of_each_variant() -> [AnyMessage; 10] {
  use crate::messages::memberlist::v1::{
    Alive, Dead, ErrorResponse, IndirectPing, Nack, PushPull, Suspect, UserData,
  };
  [
    AnyMessage::Alive(Alive {
      incarnation: Some(3),
      ..Default::default()
    }),
    AnyMessage::Suspect(Suspect {
      incarnation: Some(4),
      ..Default::default()
    }),
    AnyMessage::Dead(Dead {
      incarnation: Some(5),
      ..Default::default()
    }),
    sample_ping(),
    AnyMessage::IndirectPing(IndirectPing {
      sequence_number: Some(6),
      ..Default::default()
    }),
    sample_ack(),
    AnyMessage::Nack(Nack {
      sequence_number: 7,
      ..Default::default()
    }),
    AnyMessage::PushPull(PushPull {
      join: true,
      ..Default::default()
    }),
    AnyMessage::UserData(UserData {
      data: bytes::Bytes::from_static(b"payload"),
      ..Default::default()
    }),
    AnyMessage::ErrorResponse(ErrorResponse {
      error: "boom".into(),
      ..Default::default()
    }),
  ]
}

#[test]
fn every_variant_round_trips_through_decode_message() {
  // Exercises encode_body_into + body_encoded_len + the per-variant
  // decode_message arm for all 10 message types (not just Ping/Ack).
  for msg in one_of_each_variant() {
    let encoded = encode_message(&msg).expect("encode");
    let (consumed, decoded) = decode_message(&encoded).expect("decode");
    assert_eq!(consumed, encoded.len(), "consumed mismatch for {msg:?}");
    assert_eq!(decoded, msg, "round-trip mismatch for {msg:?}");
  }
}

#[test]
fn every_variant_round_trips_through_zerocopy() {
  // Same coverage for the zero-copy decode path's per-variant arms.
  for msg in one_of_each_variant() {
    let encoded = Bytes::from(encode_message(&msg).expect("encode"));
    let (consumed, decoded) = decode_message_zerocopy(&encoded).expect("zerocopy decode");
    assert_eq!(consumed, encoded.len(), "consumed mismatch for {msg:?}");
    assert_eq!(decoded, msg, "round-trip mismatch for {msg:?}");
  }
}

#[test]
fn corrupt_body_is_rejected_for_representative_variants() {
  // A frame whose tag is valid but whose body is malformed protobuf must
  // surface FrameError::Decode (the per-variant `.map_err` arm), not panic.
  // `[0x0A, 0x05]` is a length-delimited field (tag 1) declaring 5 payload
  // bytes that are absent — buffa rejects it for any message shape.
  for tag in [
    MessageTag::Alive,
    MessageTag::Suspect,
    MessageTag::Dead,
    MessageTag::IndirectPing,
    MessageTag::Nack,
    MessageTag::PushPull,
    MessageTag::ErrorResponse,
  ] {
    let frame = encode_plain_frame(tag, &[0x0A, 0x05]).expect("encode");
    assert!(
      matches!(decode_message(&frame), Err(FrameError::Decode)),
      "decode_message must reject corrupt {tag:?} body"
    );
    let zc = Bytes::from(encode_plain_frame(tag, &[0x0A, 0x05]).expect("encode"));
    assert!(
      matches!(decode_message_zerocopy(&zc), Err(FrameError::Decode)),
      "zerocopy must reject corrupt {tag:?} body"
    );
  }
}

#[test]
fn compound_decode_rejects_overflowing_count_varint() {
  // `[Compound][80 80 80 80 10]` — the count varint encodes 2^32, which the
  // u32 varint decoder rejects as VarintOverflow. `decode_compound` must
  // propagate that hard error (the non-Incomplete `Err(e)` arm), not stall.
  let buf = [MessageTag::Compound as u8, 0x80, 0x80, 0x80, 0x80, 0x10];
  assert!(matches!(
    decode_compound(&buf),
    Err(FrameError::VarintOverflow)
  ));
}

#[test]
fn compound_decode_rejects_overflowing_inner_len_varint() {
  // A structurally valid header `[Compound][count=2]` followed by a part
  // whose inner_len varint overflows a u32 surfaces the per-part loop's
  // non-Incomplete `Err(e)` propagation arm.
  let mut buf = vec![MessageTag::Compound as u8];
  encode_varint_u32(2, &mut buf);
  // Overflowing inner_len varint (2^32) for the first part.
  buf.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x10]);
  assert!(matches!(
    decode_compound(&buf),
    Err(FrameError::VarintOverflow)
  ));
}
