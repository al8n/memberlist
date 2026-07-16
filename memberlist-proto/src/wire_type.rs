//! Protobuf-like wire type encoding helpers.
//!
//! The `WireType` enum and the `merge`/`split`/`skip` tag helpers mirror the
//! implementation in `memberlist-proto::utils` so that the `data` module can
//! perform protobuf-like field encoding without depending on `memberlist-proto`.

use derive_more::IsVariant;

use crate::data::DecodeError;
use core::fmt;

/// A wire type used in Protobuf-like encoding/decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IsVariant)]
#[repr(u8)]
#[non_exhaustive]
pub enum WireType {
  /// A byte wire type.
  Byte = 0,
  /// A varint wire type.
  Varint = 1,
  /// A length-delimited wire type.
  LengthDelimited = 2,
  /// Fixed 32-bit wire type.
  Fixed32 = 3,
  /// Fixed 64-bit wire type.
  Fixed64 = 4,
}

impl WireType {
  /// Returns the [`WireType`] as a `&'static str`.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Byte => "byte",
      Self::Varint => "varint",
      Self::LengthDelimited => "length-delimited",
      Self::Fixed32 => "fixed32",
      Self::Fixed64 => "fixed64",
    }
  }
}

impl fmt::Display for WireType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl TryFrom<u8> for WireType {
  type Error = u8;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Byte,
      1 => Self::Varint,
      2 => Self::LengthDelimited,
      3 => Self::Fixed32,
      4 => Self::Fixed64,
      _ => return Err(value),
    })
  }
}

/// Merge wire type and tag into a byte.
#[inline]
pub const fn merge(ty: WireType, tag: u8) -> u8 {
  (ty as u8) << 3 | tag
}

/// Split a byte into wire type and tag.
#[inline]
pub const fn split(val: u8) -> (u8, u8) {
  let wire_type = val >> 3;
  let tag = val & 0b111;
  (wire_type, tag)
}

/// Skip a field in the buffer.
pub fn skip(ty: &'static str, src: &[u8]) -> Result<usize, DecodeError> {
  let buf_len = src.len();
  if buf_len == 0 {
    return Ok(0);
  }

  let mut offset = 0;
  let (wire_type, tag) = split(src[offset]);

  let wire_type =
    WireType::try_from(wire_type).map_err(|v| DecodeError::unknown_wire_type(ty, v, tag))?;
  offset += 1;
  let src = &src[offset..];
  match wire_type {
    WireType::Varint => match varing::decode_u64_varint(src) {
      // varing rejects a truncated varint, so `bytes_read <= src.len()` and
      // `offset + bytes_read <= buf_len` already holds; no clamp is needed.
      Ok((bytes_read, _)) => Ok(offset + bytes_read.get()),
      Err(e) => Err(e.into()),
    },
    WireType::LengthDelimited => {
      // Skip length-delimited field by reading the length and skipping the payload
      if src.is_empty() {
        return Err(DecodeError::buffer_underflow());
      }

      match varing::decode_u32_varint(src) {
        // header + declared payload may run past the buffer, or overflow usize
        // on 32-bit. A field that is not fully present is a truncated frame and
        // must be rejected rather than clamped-and-consumed (fail closed).
        Ok((bytes_read, length)) => offset
          .checked_add(bytes_read.get())
          .and_then(|n| n.checked_add(length as usize))
          .filter(|&end| end <= buf_len)
          .ok_or(DecodeError::BufferUnderflow),
        Err(e) => Err(e.into()),
      }
    }
    // A fixed-width field whose bytes are not all present is a truncated frame.
    WireType::Byte => (offset < buf_len)
      .then_some(offset + 1)
      .ok_or(DecodeError::BufferUnderflow),
    WireType::Fixed32 => (offset + 4 <= buf_len)
      .then_some(offset + 4)
      .ok_or(DecodeError::BufferUnderflow),
    WireType::Fixed64 => (offset + 8 <= buf_len)
      .then_some(offset + 8)
      .ok_or(DecodeError::BufferUnderflow),
  }
}
