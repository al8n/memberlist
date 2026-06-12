use super::*;

#[test]
fn id_roundtrip_ascii() {
  let original = SmolStr::new("node-a");
  let bytes = id_to_bytes(&original);
  let decoded = id_from_bytes(&bytes).unwrap();
  assert_eq!(original, decoded);
}

#[test]
fn id_roundtrip_unicode() {
  let original = SmolStr::new("ノード-α");
  let bytes = id_to_bytes(&original);
  let decoded = id_from_bytes(&bytes).unwrap();
  assert_eq!(original, decoded);
}

#[test]
fn id_rejects_invalid_utf8() {
  let invalid = Bytes::from_static(&[0xff, 0xfe, 0xfd]);
  assert!(matches!(
    id_from_bytes(&invalid),
    Err(ConvertError::IdNotUtf8)
  ));
}

#[test]
fn ipv4_roundtrip() {
  let original: SocketAddr = "192.168.1.42:7946".parse().unwrap();
  let bytes = socket_addr_to_bytes(&original).unwrap();
  assert_eq!(bytes.len(), ADDR_V4_LEN);
  assert_eq!(bytes[0], VERSION_TAG_V4);
  let decoded = socket_addr_from_bytes(&bytes).unwrap();
  assert_eq!(original, decoded);
}

#[test]
fn ipv6_roundtrip() {
  let original: SocketAddr = "[fe80::1]:7946".parse().unwrap();
  let bytes = socket_addr_to_bytes(&original).unwrap();
  assert_eq!(bytes.len(), ADDR_V6_LEN);
  assert_eq!(bytes[0], VERSION_TAG_V6);
  let decoded = socket_addr_from_bytes(&bytes).unwrap();
  assert_eq!(original, decoded);
}

#[test]
fn addr_rejects_unknown_version() {
  let invalid = Bytes::from_static(&[0x42, 0, 0, 0, 0, 0, 0]);
  assert!(matches!(
    socket_addr_from_bytes(&invalid),
    Err(ConvertError::AddrUnknownVersion(0x42))
  ));
}

#[test]
fn addr_rejects_truncated_v4() {
  // Version tag + only 3 bytes of address (not 6).
  let invalid = Bytes::from_static(&[VERSION_TAG_V4, 1, 2, 3]);
  assert!(matches!(
    socket_addr_from_bytes(&invalid),
    Err(ConvertError::AddrLengthMismatch(_))
  ));
}

#[test]
fn addr_rejects_empty() {
  let empty = Bytes::new();
  assert!(matches!(
    socket_addr_from_bytes(&empty),
    Err(ConvertError::AddrEmpty(0))
  ));
}

#[test]
fn ipv4_known_byte_layout() {
  // 127.0.0.1:7946 → [0, 127, 0, 0, 1, 0x1f, 0x0a]
  let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  let bytes = socket_addr_to_bytes(&addr).unwrap();
  assert_eq!(&bytes[..], &[VERSION_TAG_V4, 127, 0, 0, 1, 0x1f, 0x0a]);
}

#[test]
fn ipv6_scoped_addr_rejected() {
  // This public helper must reject a scoped link-local address (the
  // compact wire layout cannot carry scope_id) instead of silently
  // flattening it to scope 0 — same contract as the `Data` encoder.
  // Unscoped IPv6 still round-trips.
  use core::net::{Ipv6Addr, SocketAddrV6};

  let scoped = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
    7946,
    0x1234,
    7,
  ));
  assert!(
    matches!(
      socket_addr_to_bytes(&scoped),
      Err(ConvertError::AddrScopedV6)
    ),
    "scoped IPv6 must be rejected, not silently canonicalized"
  );

  let unscoped: SocketAddr = "[2001:db8::1]:7946".parse().unwrap();
  let bytes = socket_addr_to_bytes(&unscoped).unwrap();
  assert_eq!(socket_addr_from_bytes(&bytes).unwrap(), unscoped);
}

#[test]
fn addr_length_mismatch_info_accessors() {
  // A truncated v4 buffer surfaces the version, expected, and actual
  // byte counts on the mismatch payload.
  let invalid = Bytes::from_static(&[VERSION_TAG_V4, 1, 2, 3]);
  match socket_addr_from_bytes(&invalid) {
    Err(ConvertError::AddrLengthMismatch(info)) => {
      assert_eq!(info.version(), VERSION_TAG_V4);
      assert_eq!(info.expected(), ADDR_V4_LEN);
      assert_eq!(info.actual(), 4);
      assert!(!info.to_string().is_empty());
    }
    other => panic!("expected AddrLengthMismatch, got {other:?}"),
  }
  // The display impl on the error wrapper also renders.
  let direct = AddrLengthMismatchInfo::new(VERSION_TAG_V6, ADDR_V6_LEN, 5);
  assert_eq!(
    (direct.version(), direct.expected(), direct.actual()),
    (VERSION_TAG_V6, ADDR_V6_LEN, 5)
  );
}

#[test]
fn addr_rejects_truncated_v6() {
  // Version tag v6 but only a handful of the 19 required bytes.
  let invalid = Bytes::from_static(&[VERSION_TAG_V6, 1, 2, 3, 4, 5]);
  match socket_addr_from_bytes(&invalid) {
    Err(ConvertError::AddrLengthMismatch(info)) => {
      assert_eq!(info.version(), VERSION_TAG_V6);
      assert_eq!(info.expected(), ADDR_V6_LEN);
      assert_eq!(info.actual(), 6);
    }
    other => panic!("expected AddrLengthMismatch for v6, got {other:?}"),
  }
}

#[test]
fn convert_error_variants_render_nonempty() {
  // Every ConvertError variant must have a non-empty Display string.
  assert!(!ConvertError::IdNotUtf8.to_string().is_empty());
  assert!(!ConvertError::AddrEmpty(0).to_string().is_empty());
  assert!(!ConvertError::AddrUnknownVersion(9).to_string().is_empty());
  assert!(!ConvertError::AddrScopedV6.to_string().is_empty());
  assert!(
    !ConvertError::AddrLengthMismatch(AddrLengthMismatchInfo::new(0, 7, 3))
      .to_string()
      .is_empty()
  );
}
