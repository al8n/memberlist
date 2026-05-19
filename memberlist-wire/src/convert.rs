//! Boundary helpers between buffa-generated wire fields (raw [`Bytes`])
//! and the typical application types `SmolStr` (for node ids) and
//! [`std::net::SocketAddr`] (for resolved addresses).
//!
//! `memberlist-machine` stays generic over `I, A`, but practically every
//! consumer uses `I = SmolStr` and `A = SocketAddr`. These helpers do
//! that conversion at the wire/state boundary so the state machine
//! continues to deal in typed values.
//!
//! # Wire layouts
//!
//! - **`id`**: raw UTF-8 bytes. `SmolStr` is encoded by writing its
//!   bytes directly; decoding is a `str::from_utf8` check followed by
//!   `SmolStr::new`.
//!
//! - **`addr`**: a tagged compact binary form mirroring the legacy
//!   `memberlist_proto::data::primitives` encoding so applications that
//!   resolve addresses outside the codec see the same bytes on the wire:
//!
//!   ```text
//!   [VERSION_TAG:1][IP_BYTES:4|16][PORT:u16 big-endian]
//!   ```
//!
//!   `VERSION_TAG` is `0` for IPv4 and `1` for IPv6. This is six bytes
//!   for v4 and eighteen for v6.
//!
//! # Errors
//!
//! [`ConvertError`] carries enough context for diagnostics without
//! leaking raw byte slices into the error type.

use core::str;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use bytes::Bytes;
use smol_str::SmolStr;

/// Errors surfaced when translating between [`Bytes`] wire fields and
/// the corresponding application types.
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
  /// An `id` field contained bytes that were not valid UTF-8.
  #[error("id is not valid UTF-8")]
  IdNotUtf8,
  /// An `addr` field was too short to contain a version tag.
  #[error("addr buffer too short: expected at least 1 byte, got {0}")]
  AddrEmpty(usize),
  /// An `addr` field carried an unknown version tag.
  #[error("unknown addr version tag: {0}")]
  AddrUnknownVersion(u8),
  /// A `SocketAddrV6` carried a nonzero `flowinfo`/`scope_id`, which the
  /// compact wire layout cannot represent. Rejected rather than silently
  /// canonicalized to scope 0 (which would gossip an undialable
  /// link-local peer) — same contract as the `Data` encoder.
  #[error("SocketAddrV6 flowinfo/scope_id cannot be represented on the wire")]
  AddrScopedV6,
  /// An `addr` field's length didn't match its declared version.
  #[error("addr length mismatch: expected {expected} bytes for version {version}, got {actual}")]
  AddrLengthMismatch {
    /// The version tag found.
    version: u8,
    /// Bytes required for that version (6 for v4, 18 for v6).
    expected: usize,
    /// Bytes the caller supplied.
    actual: usize,
  },
}

/// `1 (version) + 4 (ipv4) + 2 (port)`.
const ADDR_V4_LEN: usize = 1 + 4 + 2;
/// `1 (version) + 16 (ipv6) + 2 (port)`.
const ADDR_V6_LEN: usize = 1 + 16 + 2;
const VERSION_TAG_V4: u8 = 0;
const VERSION_TAG_V6: u8 = 1;

// ── id: SmolStr ↔ Bytes ──────────────────────────────────────────────

/// Decode a UTF-8 node id from a wire `id` field.
pub fn id_from_bytes(b: &Bytes) -> Result<SmolStr, ConvertError> {
  let s = str::from_utf8(b.as_ref()).map_err(|_| ConvertError::IdNotUtf8)?;
  Ok(SmolStr::new(s))
}

/// Encode a [`SmolStr`] node id into a wire `id` field.
pub fn id_to_bytes(s: &SmolStr) -> Bytes {
  Bytes::copy_from_slice(s.as_bytes())
}

// ── addr: SocketAddr ↔ Bytes ─────────────────────────────────────────

/// Decode a tagged-binary [`SocketAddr`] from a wire `addr` field.
pub fn socket_addr_from_bytes(b: &Bytes) -> Result<SocketAddr, ConvertError> {
  let buf = b.as_ref();
  if buf.is_empty() {
    return Err(ConvertError::AddrEmpty(0));
  }
  match buf[0] {
    VERSION_TAG_V4 => {
      if buf.len() != ADDR_V4_LEN {
        return Err(ConvertError::AddrLengthMismatch {
          version: VERSION_TAG_V4,
          expected: ADDR_V4_LEN,
          actual: buf.len(),
        });
      }
      let mut octets = [0u8; 4];
      octets.copy_from_slice(&buf[1..5]);
      let port = u16::from_be_bytes([buf[5], buf[6]]);
      Ok(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::from(octets),
        port,
      )))
    }
    VERSION_TAG_V6 => {
      if buf.len() != ADDR_V6_LEN {
        return Err(ConvertError::AddrLengthMismatch {
          version: VERSION_TAG_V6,
          expected: ADDR_V6_LEN,
          actual: buf.len(),
        });
      }
      let mut octets = [0u8; 16];
      octets.copy_from_slice(&buf[1..17]);
      let port = u16::from_be_bytes([buf[17], buf[18]]);
      Ok(SocketAddr::V6(SocketAddrV6::new(
        Ipv6Addr::from(octets),
        port,
        0,
        0,
      )))
    }
    other => Err(ConvertError::AddrUnknownVersion(other)),
  }
}

/// Encode a [`SocketAddr`] into a wire `addr` field.
///
/// Fallible because the compact layout cannot carry IPv6
/// `flowinfo`/`scope_id`; a scoped `SocketAddrV6` is rejected rather
/// than silently flattened to scope 0 (same contract as the `Data`
/// encoder. `socket_addr_from_bytes` correctly yields
/// scope 0 (the wire never carried it).
pub fn socket_addr_to_bytes(addr: &SocketAddr) -> Result<Bytes, ConvertError> {
  match addr {
    SocketAddr::V4(v4) => {
      let mut buf = Vec::with_capacity(ADDR_V4_LEN);
      buf.push(VERSION_TAG_V4);
      buf.extend_from_slice(&v4.ip().octets());
      buf.extend_from_slice(&v4.port().to_be_bytes());
      Ok(Bytes::from(buf))
    }
    SocketAddr::V6(v6) => {
      if v6.flowinfo() != 0 || v6.scope_id() != 0 {
        return Err(ConvertError::AddrScopedV6);
      }
      let mut buf = Vec::with_capacity(ADDR_V6_LEN);
      buf.push(VERSION_TAG_V6);
      buf.extend_from_slice(&v6.ip().octets());
      buf.extend_from_slice(&v6.port().to_be_bytes());
      Ok(Bytes::from(buf))
    }
  }
}

#[cfg(test)]
mod tests {
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
      Err(ConvertError::AddrLengthMismatch { .. })
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
    use std::net::{Ipv6Addr, SocketAddrV6};

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
}
