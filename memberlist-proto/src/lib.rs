//! Types used by the [`memberlist`](https://crates.io/crates/memberlist) crate.
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
#![deny(warnings)]
#![allow(clippy::type_complexity, clippy::double_parens, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use metrics_label::MetricLabels;
#[cfg(any(feature = "std", feature = "alloc"))]
pub use nodecraft::{
  CheapClone, Domain, HostAddr, Node, NodeId, ParseNodeIdError,
  hostaddr::{ParseDomainError, ParseHostAddrError},
};

#[cfg(any(feature = "arbitrary", test))]
mod arbitrary_impl;

/// Compression related types.
#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
pub mod compression;

/// Checksum related types.
#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
pub mod checksum;

/// Encryption related types.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub mod encryption;

#[cfg(feature = "metrics")]
mod metrics_label;
#[cfg(any(feature = "quickcheck", test))]
mod quickcheck_impl;
#[cfg(feature = "serde")]
mod serde_impl;

#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
pub use checksum::*;
#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
pub use compression::*;
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use encryption::*;

pub use ack::*;
pub use address::*;
pub use alive::*;
pub use bad_state::*;
pub use bytes;
pub use cidr_policy::*;
pub use data::*;
pub use err::*;
pub use label::*;
pub use meta::*;
pub use payload::*;
pub use ping::*;
pub use proto::*;
pub use push_pull::*;
pub use server::*;
pub use smallvec_wrapper::*;
pub use version::*;

mod ack;
mod address;
mod alive;
mod bad_state;
mod cidr_policy;
mod data;
mod err;
mod label;
mod meta;
mod payload;
mod ping;
mod proto;
mod push_pull;
mod server;
mod version;

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

use utils::*;

/// Utils for protobuf-like encoding/decoding
pub mod utils {
  use super::{DecodeError, WireType};

  /// Merge wire type and tag into a byte.
  #[inline]
  pub const fn merge(ty: WireType, tag: u8) -> u8 {
    (ty as u8) << 3 | tag
  }

  /// Split a byte into wire type and tag.
  #[inline]
  pub const fn split(val: u8) -> (u8, u8) {
    let wire_type = val >> 3; // Shift right to get the wire type
    let tag = val & 0b111; // Mask with 0b111 to get last 3 bits
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
          Ok((bytes_read, length)) => {
            Ok((offset + bytes_read.get() + length as usize).min(buf_len))
          }
          Err(e) => Err(e.into()),
        }
      }
      WireType::Byte => Ok((offset + 1).min(buf_len)),
      WireType::Fixed32 => Ok((offset + 4).min(buf_len)),
      WireType::Fixed64 => Ok((offset + 8).min(buf_len)),
    }
  }
}

#[cfg(debug_assertions)]
#[inline]
fn debug_assert_write_eq<T: ?Sized>(actual: usize, expected: usize) {
  debug_assert_eq!(
    actual,
    expected,
    "{}: expect writting {expected} bytes, but actual write {actual} bytes",
    core::any::type_name::<T>()
  );
}

#[cfg(debug_assertions)]
#[inline]
fn debug_assert_read_eq<T: ?Sized>(actual: usize, expected: usize) {
  debug_assert_eq!(
    actual,
    expected,
    "{}: expect reading {expected} bytes, but actual read {actual} bytes",
    core::any::type_name::<T>()
  );
}

#[inline]
fn check_encoded_message_size(required: usize) -> Result<(), EncodeError> {
  if required > u32::MAX as usize {
    return Err(EncodeError::TooLarge);
  }

  Ok(())
}
