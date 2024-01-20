use bytes::Bytes;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(u8)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[non_exhaustive]
pub enum Compressor {
  #[default]
  Lzw = 0,
}

impl Compressor {
  pub const SIZE: usize = core::mem::size_of::<Self>();

  pub fn is_lzw(&self) -> bool {
    matches!(self, Self::Lzw)
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnknownCompressor(u8);

impl core::fmt::Display for UnknownCompressor {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "invalid compression algo {}", self.0)
  }
}

impl std::error::Error for UnknownCompressor {}

impl TryFrom<u8> for Compressor {
  type Error = UnknownCompressor;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Lzw),
      _ => Err(UnknownCompressor(value)),
    }
  }
}

const LZW_LIT_WIDTH: u8 = 8;

#[derive(Debug, thiserror::Error)]
pub enum CompressError {
  #[error("{0}")]
  Lzw(#[from] weezl::LzwError),
}

#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
  #[error("{0}")]
  Lzw(#[from] weezl::LzwError),
}

impl Compressor {
  pub(crate) fn decompress(&self, src: &[u8]) -> Result<Bytes, DecompressError> {
    match self {
      Self::Lzw => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .decode(src)
        .map(Into::into)
        .map_err(|e| DecompressError::Lzw(e)),
    }
  }

  pub(crate) fn compress(&self, src: &[u8]) -> Result<Bytes, CompressError> {
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
