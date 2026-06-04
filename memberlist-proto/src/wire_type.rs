//! Protobuf-like wire type encoding helpers.
//!
//! The `WireType` enum and the `merge`/`split`/`skip` tag helpers mirror the
//! implementation in `memberlist-proto::utils` so that the `data` module can
//! perform protobuf-like field encoding without depending on `memberlist-proto`.

use crate::data::DecodeError;

/// A wire type used in Protobuf-like encoding/decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl core::fmt::Display for WireType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
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
      Ok((bytes_read, _)) => Ok((offset + bytes_read.get()).min(buf_len)),
      Err(e) => Err(e.into()),
    },
    WireType::LengthDelimited => {
      // Skip length-delimited field by reading the length and skipping the payload
      if src.is_empty() {
        return Err(DecodeError::buffer_underflow());
      }

      match varing::decode_u32_varint(src) {
        Ok((bytes_read, length)) => Ok((offset + bytes_read.get() + length as usize).min(buf_len)),
        Err(e) => Err(e.into()),
      }
    }
    WireType::Byte => Ok((offset + 1).min(buf_len)),
    WireType::Fixed32 => Ok((offset + 4).min(buf_len)),
    WireType::Fixed64 => Ok((offset + 8).min(buf_len)),
  }
}
