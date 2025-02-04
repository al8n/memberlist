use std::borrow::Cow;

pub use nodecraft::Node;
pub use smallvec_wrapper::*;

pub use ack::*;
pub use alive::*;
pub use bad_state::*;
pub use cidr_policy::*;
pub use data::*;
pub(crate) use epoch::*;
pub use err::*;
pub use label::*;
pub use message::*;
pub use meta::*;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use metrics_label::MetricLabels;
pub use ping::*;
pub use push_pull_state::*;
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use secret::*;
pub use server::*;
pub use version::*;

use length_delimited::{
  decode_u32_varint, decode_u64_varint, encode_u32_varint, encoded_u32_varint_len,
  InsufficientBuffer, LengthDelimitedEncoder, Varint,
};

mod ack;
mod alive;
mod bad_state;
mod cidr_policy;
mod data;
mod epoch;
mod err;
mod label;
mod message;
mod meta;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
mod metrics_label;
mod ping;
mod push_pull_state;
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
mod secret;
mod server;
mod version;

const MAX_ENCODED_LEN_SIZE: usize = core::mem::size_of::<u32>();

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
}

impl WireType {
  /// Returns the [`WireType`] as a `&'static str`.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Byte => "byte",
      Self::Varint => "varint",
      Self::LengthDelimited => "length-delimited",
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
      _ => return Err(value),
    })
  }
}

#[inline]
const fn merge(ty: WireType, tag: u8) -> u8 {
  (ty as u8) << 3 | tag
}

#[inline]
const fn split(val: u8) -> (u8, u8) {
  let wire_type = val >> 3; // Shift right to get the wire type
  let tag = val & 0b111; // Mask with 0b111 to get last 3 bits
  (wire_type, tag)
}

#[inline]
const fn encoded_length_delimited_len(len: usize) -> usize {
  encoded_u32_varint_len(len as u32) + len
}

fn encode_length_delimited(src: &[u8], dst: &mut [u8]) -> Result<usize, InsufficientBuffer> {
  let len = src.len();

  if len > u32::MAX as usize {
    return Err(InsufficientBuffer::new());
  }

  let mut offset = 0;
  offset += Varint::encode(&(len as u32), &mut dst[offset..])?;

  if offset + len > dst.len() {
    return Err(InsufficientBuffer::new());
  }

  dst[offset..offset + len].copy_from_slice(src);
  offset += len;
  Ok(offset)
}

fn encoded_data_len<D: Data>(data: &D) -> usize {
  let len = data.encoded_len();
  match D::WIRE_TYPE {
    WireType::LengthDelimited => encoded_length_delimited_len(len),
    _ => len,
  }
}

fn encode_data<D: Data>(data: &D, dst: &mut [u8]) -> Result<usize, EncodeError> {
  match D::WIRE_TYPE {
    WireType::LengthDelimited => encode_length_delimited_data(data, dst),
    _ => data.encode(dst),
  }
}

fn encode_length_delimited_data<D: Data>(data: &D, dst: &mut [u8]) -> Result<usize, EncodeError> {
  let len = data.encoded_len();
  if len > u32::MAX as usize {
    return Err(EncodeError::TooLarge);
  }

  let mut offset = 0;
  offset += Varint::encode(&(len as u32), &mut dst[offset..])?;
  offset += data.encode(&mut dst[offset..])?;
  Ok(offset)
}

fn decode_varint<V>(wire_type: WireType, src: &[u8]) -> Result<(usize, V), DecodeError>
where
  V: Varint,
{
  // Verify that the wire type is `Varint` (1)
  if wire_type != WireType::Varint {
    return Err(DecodeError::new(format!(
      "invalid wire type: {} (expected {})",
      wire_type,
      WireType::Varint
    )));
  }

  // Decode the varint
  V::decode(src).map_err(|_| DecodeError::new("invalid varint"))
}

fn decode_length_delimited(wire_type: WireType, src: &[u8]) -> Result<(usize, &[u8]), DecodeError> {
  // Verify that the wire type is `LengthDelimited` (2)
  if wire_type != WireType::LengthDelimited {
    return Err(DecodeError::new(format!(
      "invalid wire type: {} (expected {})",
      wire_type,
      WireType::LengthDelimited
    )));
  }

  // Decode the length of the payload
  let (bytes_read, payload_len) =
    decode_u32_varint(src).map_err(|_| DecodeError::new("invalid length delimited varint"))?;

  // Ensure the buffer has enough bytes to read the payload
  let total_len = bytes_read + payload_len as usize;
  if total_len > src.len() {
    return Err(DecodeError::new("buffer underflow"));
  }

  let data = &src[bytes_read..total_len];
  Ok((total_len, data))
}

fn decode_data<D: Data>(src: &[u8]) -> Result<(usize, D), DecodeError> {
  match D::WIRE_TYPE {
    WireType::LengthDelimited => {
      let (bytes_read, data) = decode_length_delimited(D::WIRE_TYPE, src)?;
      let (_, value) = D::decode(data)?;
      Ok((bytes_read, value))
    }
    _ => {
      let (bytes_read, value) = D::decode(src)?;
      Ok((bytes_read, value))
    }
  }
}

fn skip(wire_type: WireType, src: &[u8]) -> Result<usize, DecodeError> {
  match wire_type {
    WireType::Varint => match decode_u64_varint(src) {
      Ok((bytes_read, _)) => Ok(bytes_read),
      Err(_) => Err(DecodeError::new("invalid varint")),
    },
    WireType::LengthDelimited => {
      // Skip length-delimited field by reading the length and skipping the payload
      if src.is_empty() {
        return Err(DecodeError::new("buffer underflow"));
      }

      match decode_u32_varint(src) {
        Ok((bytes_read, length)) => Ok(bytes_read + length as usize),
        Err(_) => Err(DecodeError::new("invalid varint")),
      }
    }
    WireType::Byte => Ok(1),
  }
}
