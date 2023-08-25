use super::*;
use bytes::{Buf, Bytes};

#[derive(
  Debug,
  Default,
  Copy,
  Clone,
  PartialEq,
  Eq,
  Hash,
  Archive,
  Serialize,
  Deserialize,
  serde::Serialize,
  serde::Deserialize,
)]
#[repr(u8)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(u8), non_exhaustive)]
#[non_exhaustive]
pub enum CompressionAlgo {
  None = 0,
  #[default]
  Lzw = 1,
}

impl CompressionAlgo {
  pub const SIZE: usize = core::mem::size_of::<Self>();

  pub fn is_none(&self) -> bool {
    matches!(self, Self::None)
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidCompressionAlgo(u8);

impl core::fmt::Display for InvalidCompressionAlgo {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "invalid compression algo {}", self.0)
  }
}

impl std::error::Error for InvalidCompressionAlgo {}

impl TryFrom<u8> for CompressionAlgo {
  type Error = InvalidCompressionAlgo;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::None),
      1 => Ok(Self::Lzw),
      _ => Err(InvalidCompressionAlgo(value)),
    }
  }
}

impl ArchivedCompressionAlgo {
  pub fn is_none(&self) -> bool {
    match self {
      ArchivedCompressionAlgo::Lzw => false,
      ArchivedCompressionAlgo::None => true,
    }
  }
}

impl From<ArchivedCompressionAlgo> for CompressionAlgo {
  fn from(value: ArchivedCompressionAlgo) -> Self {
    match value {
      ArchivedCompressionAlgo::Lzw => Self::Lzw,
      ArchivedCompressionAlgo::None => Self::None,
    }
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Compress {
  algo: CompressionAlgo,
  buf: Bytes,
}

impl Compress {
  pub(crate) fn encode_slice(
    algo: CompressionAlgo,
    r1: u8,
    r2: u8,
    src: &[u8],
  ) -> Result<Vec<u8>, CompressError> {
    let mut buf = Vec::with_capacity(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE + src.len());
    buf[..ENCODE_HEADER_SIZE].copy_from_slice(&[
      MessageType::Compress as u8,
      0,
      r1,
      r2,
      0,
      0,
      0,
      0,
      algo as u8,
    ]);
    match algo {
      CompressionAlgo::Lzw => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .into_vec(&mut buf)
        .encode_all(src)
        .status
        .map(|_| {
          let len = buf.len();
          buf[ENCODE_META_SIZE..ENCODE_META_SIZE + MAX_MESSAGE_SIZE]
            .copy_from_slice(((len - ENCODE_HEADER_SIZE) as u32).to_be_bytes().as_slice());
          buf
        })
        .map_err(CompressError::Lzw),
      CompressionAlgo::None => unreachable!(),
    }
  }

  pub(crate) fn encode_slice_with_checksum(
    algo: CompressionAlgo,
    r1: u8,
    r2: u8,
    src: &[u8],
  ) -> Result<Vec<u8>, CompressError> {
    let mut buf =
      Vec::with_capacity(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE + src.len() + CHECKSUM_SIZE);
    buf[..ENCODE_HEADER_SIZE].copy_from_slice(&[
      MessageType::Compress as u8,
      MessageType::HasCrc as u8,
      r1,
      r2,
      0,
      0,
      0,
      0,
      algo as u8,
    ]);
    match algo {
      CompressionAlgo::Lzw => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .into_vec(&mut buf)
        .encode_all(src)
        .status
        .map(|_| {
          let len = buf.len();
          buf[ENCODE_META_SIZE..ENCODE_META_SIZE + MAX_MESSAGE_SIZE]
            .copy_from_slice(((len - ENCODE_HEADER_SIZE) as u32).to_be_bytes().as_slice());
          let cks = crc32fast::hash(&buf[ENCODE_HEADER_SIZE..]);
          buf.put_u32(cks);
          buf
        })
        .map_err(CompressError::Lzw),
      CompressionAlgo::None => unreachable!(),
    }
  }

  pub(crate) fn decompress(&self) -> Result<Bytes, DecompressError> {
    match self.algo {
      CompressionAlgo::Lzw => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .decode(&self.buf)
        .map(|buf| buf.into())
        .map_err(DecompressError::Lzw),
      CompressionAlgo::None => Ok(self.buf.clone()),
    }
  }

  pub(crate) fn decode_from_bytes(mut src: Bytes) -> Result<(EncodeHeader, Self), DecodeError> {
    let marker = src[1];
    let r1 = src[2];
    let r2 = src[3];
    let len = u32::from_be_bytes(
      src[ENCODE_META_SIZE..ENCODE_META_SIZE + MAX_MESSAGE_SIZE]
        .try_into()
        .unwrap(),
    );
    let algo = CompressionAlgo::try_from(src[ENCODE_HEADER_SIZE])?;
    match algo {
      CompressionAlgo::Lzw => {
        src.advance(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE);
        Ok((
          EncodeHeader {
            meta: EncodeMeta {
              ty: MessageType::Compress,
              marker,
              r1,
              r2,
            },
            len,
          },
          Self { algo, buf: src },
        ))
      }
      CompressionAlgo::None => {
        src.advance(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE);
        Ok((
          EncodeHeader {
            meta: EncodeMeta {
              ty: MessageType::Compress,
              marker,
              r1,
              r2,
            },
            len,
          },
          Self { algo, buf: src },
        ))
      }
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
