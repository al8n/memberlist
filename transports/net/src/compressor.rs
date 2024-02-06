use bytes::Bytes;

use crate::COMPRESS_TAG;

/// Compress/Decompress errors.
#[derive(Debug, thiserror::Error)]
pub enum CompressorError {
  /// Compress errors
  #[error("compressor: {0}")]
  Compress(#[from] CompressError),
  /// Decompress errors
  #[error("compressor: {0}")]
  Decompress(#[from] DecompressError),
  /// Unknown compressor
  #[error("compressor: {0}")]
  UnknownCompressor(#[from] UnknownCompressor),
  /// Not enough bytes to decompress
  #[error("compressor: not enough bytes to decompress")]
  NotEnoughBytes,
}

/// Compressor for compress/decompress bytes for sending over the network.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[non_exhaustive]
pub enum Compressor {
  /// LZW decoder and encoder
  #[default]
  Lzw = { *COMPRESS_TAG.start() },
}

impl Compressor {
  /// The size of the compressor in bytes.
  pub const SIZE: usize = core::mem::size_of::<Self>();

  /// Returns true if the compressor is LZW.
  pub fn is_lzw(&self) -> bool {
    matches!(self, Self::Lzw)
  }
}

/// Unknown compressor
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnknownCompressor(u8);

impl core::fmt::Display for UnknownCompressor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown compressor {}", self.0)
  }
}

impl std::error::Error for UnknownCompressor {}

impl TryFrom<u8> for Compressor {
  type Error = UnknownCompressor;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      v if v == (Compressor::Lzw as u8) => Ok(Self::Lzw),
      _ => Err(UnknownCompressor(value)),
    }
  }
}

const LZW_LIT_WIDTH: u8 = 8;

/// Compress errors.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
  /// LZW compress errors
  #[error("{0}")]
  Lzw(#[from] weezl::LzwError),
}

/// Decompress errors.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
  /// LZW decompress errors
  #[error("{0}")]
  Lzw(#[from] weezl::LzwError),
}

impl Compressor {
  /// Decompresses the given buffer.
  pub fn decompress(&self, src: &[u8]) -> Result<Vec<u8>, DecompressError> {
    match self {
      Self::Lzw => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .decode(src)
        .map_err(DecompressError::Lzw),
    }
  }

  /// Compresses the given buffer.
  pub fn compress_into_bytes(&self, src: &[u8]) -> Result<Bytes, CompressError> {
    let mut buf = Vec::with_capacity(src.len());
    match self {
      Self::Lzw => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .into_vec(&mut buf)
        .encode_all(src)
        .status
        .map(|_| buf.into())
        .map_err(CompressError::Lzw),
    }
  }
}
