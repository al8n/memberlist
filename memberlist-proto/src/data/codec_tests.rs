//! Roundtrip and edge-case coverage for the [`Data`] / [`DataRef`] wire impls
//! and the [`wire_type`](crate::wire_type) tag helpers.

use std::{
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
  sync::Arc,
  time::Duration,
};

use bytes::Bytes;
use smol_str::SmolStr;

use super::{Data, DecodeError, EncodeError, InsufficientBufferCapacity};
use crate::{
  Node,
  wire_type::{WireType, merge, skip, split},
};

/// Encode into an exact-sized buffer, decode it back, and assert the value and
/// the consumed/written lengths all agree. Exercises `encoded_len` + `encode` +
/// `decode` + `from_ref` for one value.
fn roundtrip<T>(val: T)
where
  T: Data + PartialEq + core::fmt::Debug,
{
  let len = val.encoded_len();
  let mut buf = std::vec![0u8; len];
  let written = val.encode(&mut buf).expect("encode into exact buffer");
  assert_eq!(
    written, len,
    "encoded_len must equal bytes written for {val:?}"
  );
  let (read, decoded) = T::decode(&buf).expect("decode");
  assert_eq!(
    read, written,
    "decode must consume every written byte for {val:?}"
  );
  assert_eq!(decoded, val, "roundtrip must preserve {val:?}");
}

/// As [`roundtrip`] but through the length-delimited framing helpers. For a
/// non-length-delimited wire type these degrade to the bare `encode`/`decode`,
/// so the call is valid for every `Data` type.
fn roundtrip_length_delimited<T>(val: T)
where
  T: Data + PartialEq + core::fmt::Debug,
{
  let len = val.encoded_len_with_length_delimited();
  let mut buf = std::vec![0u8; len];
  let written = val
    .encode_length_delimited(&mut buf)
    .expect("encode_length_delimited");
  assert_eq!(
    written, len,
    "encoded_len_with_length_delimited must equal bytes written"
  );
  let (read, decoded) = T::decode_length_delimited(&buf).expect("decode_length_delimited");
  assert_eq!(
    read, written,
    "decode_length_delimited must consume every byte"
  );
  assert_eq!(
    decoded, val,
    "length-delimited roundtrip must preserve {val:?}"
  );
}

/// Run both framings plus the `to_vec` / `to_bytes` convenience encoders.
fn check<T>(val: T)
where
  T: Data + Clone + PartialEq + core::fmt::Debug,
{
  roundtrip(val.clone());
  roundtrip_length_delimited(val.clone());

  // The vec/bytes convenience encoders must agree with `encode`.
  let mut buf = std::vec![0u8; val.encoded_len()];
  val.encode(&mut buf).expect("encode");
  assert_eq!(val.encode_to_vec().expect("encode_to_vec"), buf);
  assert_eq!(
    val.encode_to_bytes().expect("encode_to_bytes").as_ref(),
    &buf[..]
  );

  let ld = val
    .encode_length_delimited_to_vec()
    .expect("encode_length_delimited_to_vec");
  assert_eq!(ld.len(), val.encoded_len_with_length_delimited());
  assert_eq!(
    val
      .encode_length_delimited_to_bytes()
      .expect("to_bytes")
      .as_ref(),
    &ld[..]
  );
}

#[test]
fn unsigned_integers() {
  for v in [0u16, 1, 300, u16::MAX] {
    check(v);
  }
  for v in [0u32, 1, 70_000, u32::MAX] {
    check(v);
  }
  for v in [0u64, 1, 1 << 40, u64::MAX] {
    check(v);
  }
  for v in [0u128, 1, 1 << 80, u128::MAX] {
    check(v);
  }
}

#[test]
fn signed_integers() {
  for v in [0i8, 1, -1, i8::MIN, i8::MAX] {
    check(v);
  }
  for v in [0i16, 1, -1, i16::MIN, i16::MAX] {
    check(v);
  }
  for v in [0i32, 1, -1, i32::MIN, i32::MAX] {
    check(v);
  }
  for v in [0i64, 1, -1, i64::MIN, i64::MAX] {
    check(v);
  }
  for v in [0i128, 1, -1, i128::MIN, i128::MAX] {
    check(v);
  }
}

#[test]
fn byte_bool_char_float_duration() {
  for v in [0u8, 1, 0x7f, u8::MAX] {
    check(v);
  }
  check(true);
  check(false);
  for c in ['a', 'Z', '0', '\0', '🦀', '\u{10FFFF}'] {
    check(c);
  }
  for v in [0.0f32, 1.0, -1.5, f32::MIN, f32::MAX] {
    check(v);
  }
  for v in [0.0f64, 1.0, -1.5, f64::MIN, f64::MAX] {
    check(v);
  }
  for d in [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_nanos(123_456_789),
    Duration::new(u32::MAX as u64, 999_999_999),
  ] {
    check(d);
  }
}

#[test]
fn ip_and_socket_addrs() {
  check(Ipv4Addr::new(127, 0, 0, 1));
  check(Ipv4Addr::UNSPECIFIED);
  check(Ipv6Addr::LOCALHOST);
  check(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
  check(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
  check(IpAddr::V6(Ipv6Addr::LOCALHOST));
  check(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 1), 8080));
  check(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 9000, 0, 0));
  check(SocketAddr::from((Ipv4Addr::new(1, 2, 3, 4), 53)));
  check(SocketAddr::from((Ipv6Addr::LOCALHOST, 443)));
}

#[test]
fn strings() {
  for s in ["", "hello", "a longer string with spaces", "✓ unicode 🦀"] {
    check(String::from(s));
    check(SmolStr::from(s));
    check(Arc::<str>::from(s));
    check(Box::<str>::from(s));
    check(triomphe::Arc::<str>::from(s));
  }
}

#[test]
fn byte_containers() {
  for b in [&b""[..], &b"x"[..], &b"hello world"[..]] {
    check(Bytes::copy_from_slice(b));
    check(b.to_vec());
    check(Box::<[u8]>::from(b));
    check(Arc::<[u8]>::from(b));
  }
  check([0u8; 4]);
  check([1u8, 2, 3, 4, 5, 6, 7, 8]);
  check([0xABu8; 16]);
}

#[test]
fn smart_pointer_wrappers() {
  check(Arc::new(42u32));
  check(Box::new(-7i64));
  check(triomphe::Arc::new(true));
  check(Arc::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 1))));
}

#[test]
fn tuples() {
  check((1u32, true));
  check((
    SmolStr::from("id"),
    SocketAddr::from((Ipv4Addr::LOCALHOST, 7000)),
  ));
  check((0u8, 0u64));
  check((Ipv6Addr::LOCALHOST, 65535u16));
}

#[test]
fn nodes() {
  check(Node::new(
    42u64,
    SocketAddr::from((Ipv4Addr::new(127, 0, 0, 1), 7946)),
  ));
  check(Node::new(
    SmolStr::from("node-a"),
    SocketAddr::from((Ipv6Addr::LOCALHOST, 7946)),
  ));
  check(Node::new(
    0u32,
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
  ));
}

#[test]
fn encode_rejects_undersized_buffer() {
  let val = 0xDEAD_BEEFu32;
  let need = val.encoded_len();
  let mut tiny = std::vec![0u8; need - 1];
  let err = val
    .encode(&mut tiny)
    .expect_err("encode must reject an undersized buffer");
  assert!(
    matches!(err, EncodeError::InsufficientBuffer(_)),
    "got {err:?}"
  );
}

#[test]
fn decode_rejects_truncated_length_delimited() {
  // A length-delimited string framed as `[len][bytes]`, then chopped so the
  // declared length runs past the buffer.
  let s = String::from("a non-trivial payload");
  let framed = s
    .encode_length_delimited_to_vec()
    .expect("encode_length_delimited");
  let err = String::decode_length_delimited(&framed[..framed.len() - 3])
    .expect_err("a truncated length-delimited frame must fail to decode");
  assert!(matches!(err, DecodeError::BufferUnderflow), "got {err:?}");
}

#[test]
fn wire_type_tag_merge_split_roundtrip() {
  for ty in [
    WireType::Byte,
    WireType::Varint,
    WireType::LengthDelimited,
    WireType::Fixed32,
    WireType::Fixed64,
  ] {
    for tag in 0u8..8 {
      let byte = merge(ty, tag);
      let (wire, decoded_tag) = split(byte);
      assert_eq!(wire, ty as u8);
      assert_eq!(decoded_tag, tag);
      assert_eq!(WireType::try_from(wire).unwrap(), ty);
    }
  }
}

#[test]
fn wire_type_try_from_and_display() {
  let cases = [
    (0u8, WireType::Byte, "byte"),
    (1, WireType::Varint, "varint"),
    (2, WireType::LengthDelimited, "length-delimited"),
    (3, WireType::Fixed32, "fixed32"),
    (4, WireType::Fixed64, "fixed64"),
  ];
  for (raw, ty, name) in cases {
    assert_eq!(WireType::try_from(raw).unwrap(), ty);
    assert_eq!(ty.as_str(), name);
    assert_eq!(ty.to_string(), name);
  }
  for invalid in [5u8, 6, 7, 255] {
    assert_eq!(WireType::try_from(invalid), Err(invalid));
  }
}

#[test]
fn skip_advances_past_each_wire_type() {
  // Empty input skips nothing.
  assert_eq!(skip("t", &[]).unwrap(), 0);

  // Byte: tag + 1 payload byte.
  let mut buf = std::vec![merge(WireType::Byte, 1)];
  buf.push(0xAA);
  assert_eq!(skip("t", &buf).unwrap(), 2);

  // Varint: tag + varint bytes (300 takes two bytes).
  let mut buf = std::vec![merge(WireType::Varint, 1)];
  varing::encode_u64_varint_to(300, {
    buf.resize(1 + varing::encoded_u64_varint_len(300).get(), 0);
    &mut buf[1..]
  })
  .unwrap();
  assert_eq!(skip("t", &buf).unwrap(), buf.len());

  // Length-delimited: tag + len varint + payload.
  let payload = [9u8; 5];
  let mut buf = std::vec![merge(WireType::LengthDelimited, 1)];
  buf.push(payload.len() as u8); // small length encodes as a single varint byte
  buf.extend_from_slice(&payload);
  assert_eq!(skip("t", &buf).unwrap(), buf.len());

  // Fixed32 / Fixed64: tag + 4 / 8 payload bytes.
  let buf = [merge(WireType::Fixed32, 1), 0, 0, 0, 0];
  assert_eq!(skip("t", &buf).unwrap(), 5);
  let buf = [merge(WireType::Fixed64, 1), 0, 0, 0, 0, 0, 0, 0, 0];
  assert_eq!(skip("t", &buf).unwrap(), 9);
}

#[test]
fn skip_rejects_unknown_wire_type() {
  // Wire type 5 is not a valid `WireType`; craft the tag byte directly.
  let tag_byte = (5u8 << 3) | 1;
  let err = skip("t", &[tag_byte, 0, 0]).expect_err("unknown wire type must error");
  assert!(
    matches!(err, DecodeError::UnknownWireType(_)),
    "got {err:?}"
  );
}

#[test]
fn decode_error_variants_carry_their_context() {
  let e = DecodeError::missing_field("Alive", "node");
  assert!(e.is_missing_field());
  assert!(!e.to_string().is_empty());
  match &e {
    DecodeError::MissingField(info) => {
      assert_eq!(info.ty(), "Alive");
      assert_eq!(info.field(), "node");
      assert!(!info.to_string().is_empty());
    }
    other => panic!("expected MissingField, got {other:?}"),
  }

  let e = DecodeError::duplicate_field("Alive", "node", 2);
  assert!(e.is_duplicate_field());
  match &e {
    DecodeError::DuplicateField(info) => {
      assert_eq!((info.ty(), info.field(), info.tag()), ("Alive", "node", 2));
      assert!(!info.to_string().is_empty());
    }
    other => panic!("expected DuplicateField, got {other:?}"),
  }

  let e = DecodeError::unknown_wire_type("Alive", 9, 3);
  assert!(e.is_unknown_wire_type());
  match &e {
    DecodeError::UnknownWireType(info) => {
      assert_eq!((info.ty(), info.value(), info.tag()), ("Alive", 9, 3));
      assert!(!info.to_string().is_empty());
    }
    other => panic!("expected UnknownWireType, got {other:?}"),
  }

  let e = DecodeError::unknown_tag("Alive", 7);
  assert!(e.is_unknown_tag());
  match &e {
    DecodeError::UnknownTag(info) => {
      assert_eq!((info.ty(), info.tag()), ("Alive", 7));
      assert!(!info.to_string().is_empty());
    }
    other => panic!("expected UnknownTag, got {other:?}"),
  }

  let underflow = DecodeError::buffer_underflow();
  assert!(underflow.is_buffer_underflow());
  assert!(!underflow.to_string().is_empty());
  assert_eq!(underflow.clone(), underflow);
}

#[test]
fn decode_error_from_truncated_varint() {
  // A continuation-bit byte with nothing after it is an underflow; the varint
  // decoder's error routes through `DecodeError::from`.
  let err = u64::decode(&[0x80]).expect_err("truncated varint must fail");
  assert!(err.is_buffer_underflow(), "got {err:?}");
}

#[test]
fn encode_error_variants_and_update() {
  let e = EncodeError::insufficient_buffer(10, 4);
  assert!(!e.to_string().is_empty());
  match &e {
    EncodeError::InsufficientBuffer(cap) => {
      assert_eq!((cap.required(), cap.remaining()), (10, 4));
      assert!(!cap.to_string().is_empty());
    }
    other => panic!("expected InsufficientBuffer, got {other:?}"),
  }

  // `update` rewrites the capacity pair on an InsufficientBuffer …
  match EncodeError::insufficient_buffer(1, 1).update(99, 8) {
    EncodeError::InsufficientBuffer(cap) => assert_eq!((cap.required(), cap.remaining()), (99, 8)),
    other => panic!("expected InsufficientBuffer, got {other:?}"),
  }
  // … and is a no-op on any other variant.
  assert!(matches!(
    EncodeError::TooLarge.update(1, 2),
    EncodeError::TooLarge
  ));
  assert!(!EncodeError::TooLarge.to_string().is_empty());
  assert_eq!(EncodeError::custom("nope").to_string(), "nope");

  let cap = InsufficientBufferCapacity::new(5, 2);
  assert_eq!((cap.required(), cap.remaining()), (5, 2));
  assert_eq!(cap, InsufficientBufferCapacity::new(5, 2));
}

#[test]
fn encode_error_from_varint_no_room() {
  // The varint of a maximal u64 needs ten bytes; an empty buffer routes varing's
  // error through `EncodeError::from`.
  let err = u64::MAX
    .encode(&mut [])
    .expect_err("no room for the varint");
  assert!(
    matches!(err, EncodeError::InsufficientBuffer(_)),
    "got {err:?}"
  );
}

// ─── primitive decode error paths ────────────────────────────────────────────

#[test]
fn ip_and_socket_addr_reject_unknown_tag() {
  // The discriminant byte selects V4 (0) / V6 (1); anything else is unknown.
  let err = IpAddr::decode(&[7, 0, 0, 0, 0]).expect_err("unknown IpAddr tag");
  assert!(err.is_unknown_tag(), "got {err:?}");

  let err = SocketAddr::decode(&[9, 0, 0, 0, 0, 0, 0]).expect_err("unknown SocketAddr tag");
  assert!(err.is_unknown_tag(), "got {err:?}");
}

#[test]
fn address_decoders_reject_truncated_buffers() {
  // Each fixed-width address decoder underflows on a short buffer.
  assert!(
    Ipv4Addr::decode(&[1, 2, 3])
      .unwrap_err()
      .is_buffer_underflow()
  );
  assert!(
    Ipv6Addr::decode(&[0u8; 8])
      .unwrap_err()
      .is_buffer_underflow()
  );
  assert!(IpAddr::decode(&[]).unwrap_err().is_buffer_underflow());
  assert!(
    SocketAddrV4::decode(&[1, 2, 3])
      .unwrap_err()
      .is_buffer_underflow()
  );
  assert!(
    SocketAddrV6::decode(&[0u8; 10])
      .unwrap_err()
      .is_buffer_underflow()
  );
  assert!(SocketAddr::decode(&[]).unwrap_err().is_buffer_underflow());
}

#[test]
fn address_encoders_reject_undersized_buffers() {
  // Exercise the per-variant insufficient-buffer guards on the encode side.
  assert!(matches!(
    Ipv4Addr::LOCALHOST.encode(&mut [0u8; 3]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    Ipv6Addr::LOCALHOST.encode(&mut [0u8; 8]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    IpAddr::V4(Ipv4Addr::LOCALHOST).encode(&mut [0u8; 2]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    IpAddr::V6(Ipv6Addr::LOCALHOST).encode(&mut [0u8; 2]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1).encode(&mut [0u8; 3]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0, 0).encode(&mut [0u8; 8]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    SocketAddr::from((Ipv4Addr::LOCALHOST, 1)).encode(&mut [0u8; 2]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  assert!(matches!(
    SocketAddr::from((Ipv6Addr::LOCALHOST, 1)).encode(&mut [0u8; 2]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
}

#[test]
fn socket_addr_v6_rejects_scope_and_flowinfo() {
  // The compact `[16B IP][2B port]` layout cannot carry flowinfo/scope_id;
  // a scoped address is rejected rather than silently flattened to scope 0.
  let scoped = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7946, 0, 7);
  let err = scoped
    .encode(&mut [0u8; 32])
    .expect_err("scoped SocketAddrV6 must be rejected");
  assert!(matches!(err, EncodeError::Custom(_)), "got {err:?}");

  let flow = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7946, 0x1234, 0);
  assert!(matches!(
    flow.encode(&mut [0u8; 32]),
    Err(EncodeError::Custom(_))
  ));

  // The SocketAddr::V6 wrapper forwards to the inner encoder, so a scoped
  // address routed through it is rejected too.
  assert!(matches!(
    SocketAddr::V6(scoped).encode(&mut [0u8; 32]),
    Err(EncodeError::Custom(_))
  ));
}

#[test]
fn char_decode_rejects_invalid_scalar_value() {
  // 0xD800 is a surrogate; `char::from_u32` rejects it.
  let surrogate = 0xD800u32;
  let mut buf = std::vec![0u8; surrogate.encoded_len()];
  surrogate.encode(&mut buf).expect("encode u32");
  let err = char::decode(&buf).expect_err("surrogate is not a valid char");
  assert!(matches!(err, DecodeError::Custom(_)), "got {err:?}");
}

#[test]
fn char_decode_rejects_empty_buffer() {
  assert!(char::decode(&[]).unwrap_err().is_buffer_underflow());
}

#[test]
fn u8_and_bool_decode_reject_empty_buffer() {
  assert!(u8::decode(&[]).unwrap_err().is_buffer_underflow());
  assert!(bool::decode(&[]).unwrap_err().is_buffer_underflow());
}

#[test]
fn bool_decodes_any_nonzero_as_true() {
  // The decoder maps any nonzero byte to `true`, byte 0 to `false`.
  assert!(!bool::decode(&[0]).unwrap().1);
  assert!(bool::decode(&[1]).unwrap().1);
  assert!(bool::decode(&[0xFF]).unwrap().1);
}

#[test]
fn duration_decode_rejects_malformed_bytes() {
  // A lone continuation byte cannot form a valid duration varint pair.
  let err = Duration::decode(&[0x80]).expect_err("malformed duration");
  assert!(matches!(err, DecodeError::Custom(_)), "got {err:?}");
}

// ─── Node decode/encode error paths ──────────────────────────────────────────

/// A `Node` whose id+addr fields are both `u32` so the wire layout is small and
/// easy to hand-craft. `u32` is varint-typed, so each field is `[tag][varint]`.
type SmallNode = Node<u32, u32>;

fn small_node_bytes() -> Vec<u8> {
  Node::new(7u32, 9u32)
    .encode_to_vec()
    .expect("encode SmallNode")
}

#[test]
fn node_decode_rejects_duplicate_id_field() {
  // Concatenate the id field onto a full encoding so tag 1 appears twice.
  let full = small_node_bytes();
  let id_field_len = full.len() - (1 + 9u32.encoded_len()); // strip the addr field
  let mut dup = full.clone();
  dup.extend_from_slice(&full[..id_field_len]);
  let err = SmallNode::decode(&dup).expect_err("duplicate id must fail");
  assert!(err.is_duplicate_field(), "got {err:?}");
}

#[test]
fn node_decode_rejects_duplicate_addr_field() {
  let full = small_node_bytes();
  let id_field_len = full.len() - (1 + 9u32.encoded_len());
  let mut dup = full.clone();
  // Append the addr field (the tail after the id field) a second time.
  dup.extend_from_slice(&full[id_field_len..]);
  let err = SmallNode::decode(&dup).expect_err("duplicate addr must fail");
  assert!(err.is_duplicate_field(), "got {err:?}");
}

#[test]
fn node_decode_rejects_missing_fields() {
  // Only the id field present (truncate the addr field off the tail).
  let full = small_node_bytes();
  let id_field_len = full.len() - (1 + 9u32.encoded_len());
  let err = SmallNode::decode(&full[..id_field_len]).expect_err("missing addr must fail");
  assert!(err.is_missing_field(), "got {err:?}");

  // Empty buffer ⇒ both fields missing ⇒ MissingField (id reported first).
  let err = SmallNode::decode(&[]).expect_err("empty must fail");
  assert!(err.is_missing_field(), "got {err:?}");
}

#[test]
fn node_decode_skips_unknown_tag() {
  // Prepend an unrelated byte-wire-type field (tag 5) the decoder must skip,
  // then the genuine fields. The Node still decodes to its real values.
  let mut buf = std::vec![merge(WireType::Byte, 5), 0xAB];
  buf.extend_from_slice(&small_node_bytes());
  let (_, node) = SmallNode::decode(&buf).expect("unknown tag is skipped");
  assert_eq!(node, Node::new(7u32, 9u32));
}

#[test]
fn node_encode_rejects_undersized_buffers() {
  let node = Node::new(7u32, 9u32);
  // Zero-length buffer fails at the very first guard.
  assert!(matches!(
    node.encode(&mut []),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  // A buffer big enough for the id field but not the addr field fails at the
  // mid-encode guard.
  let need = node.encoded_len();
  assert!(matches!(
    node.encode(&mut std::vec![0u8; need - 1]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
}

// ─── tuple decode/encode error paths ─────────────────────────────────────────

fn small_tuple_bytes() -> Vec<u8> {
  (7u32, 9u32).encode_to_vec().expect("encode tuple")
}

#[test]
fn tuple_decode_rejects_duplicate_fields() {
  let full = small_tuple_bytes();
  let a_field_len = full.len() - (1 + 9u32.encoded_len());
  // Duplicate the A field (tag 1).
  let mut dup_a = full.clone();
  dup_a.extend_from_slice(&full[..a_field_len]);
  assert!(
    <(u32, u32)>::decode(&dup_a)
      .unwrap_err()
      .is_duplicate_field()
  );
  // Duplicate the B field (tag 2).
  let mut dup_b = full.clone();
  dup_b.extend_from_slice(&full[a_field_len..]);
  assert!(
    <(u32, u32)>::decode(&dup_b)
      .unwrap_err()
      .is_duplicate_field()
  );
}

#[test]
fn tuple_decode_rejects_missing_fields() {
  let full = small_tuple_bytes();
  let a_field_len = full.len() - (1 + 9u32.encoded_len());
  // Only the A field present.
  assert!(
    <(u32, u32)>::decode(&full[..a_field_len])
      .unwrap_err()
      .is_missing_field()
  );
  // Empty buffer ⇒ both missing.
  assert!(<(u32, u32)>::decode(&[]).unwrap_err().is_missing_field());
}

#[test]
fn tuple_decode_skips_unknown_tag() {
  let mut buf = std::vec![merge(WireType::Byte, 7), 0xCD];
  buf.extend_from_slice(&small_tuple_bytes());
  let (_, t) = <(u32, u32)>::decode(&buf).expect("unknown tag skipped");
  assert_eq!(t, (7u32, 9u32));
}

#[test]
fn tuple_encode_rejects_undersized_buffers() {
  let t = (7u32, 9u32);
  assert!(matches!(
    t.encode(&mut []),
    Err(EncodeError::InsufficientBuffer(_))
  ));
  let need = t.encoded_len();
  assert!(matches!(
    t.encode(&mut std::vec![0u8; need - 1]),
    Err(EncodeError::InsufficientBuffer(_))
  ));
}

#[test]
fn tuple_encode_with_length_delimited_roundtrips_and_guards() {
  use super::TupleEncoder;
  let enc = TupleEncoder::new(&7u32, &9u32);
  let len = enc.encoded_len_with_length_delimited();
  let mut buf = std::vec![0u8; len];
  let written = enc
    .encode_with_length_delimited(&mut buf)
    .expect("length-delimited encode");
  assert_eq!(written, len);
  let (read, decoded) =
    <(u32, u32)>::decode_length_delimited(&buf).expect("length-delimited decode");
  assert_eq!(read, len);
  assert_eq!(decoded, (7u32, 9u32));
}
