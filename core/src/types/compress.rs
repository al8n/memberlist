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
  const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

  pub(crate) fn encode_slice<C: Checksumer>(
    algo: CompressionAlgo,
    r1: u8,
    r2: u8,
    r3: u8,
    src: &[u8],
  ) -> Result<Message, CompressError> {
    let mut buf = Vec::with_capacity(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE + src.len());
    buf[..ENCODE_HEADER_SIZE].copy_from_slice(&[
      MessageType::Compress as u8,
      r1,
      r2,
      r3,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      algo as u8,
    ]);
    match algo {
      CompressionAlgo::Lzw => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
        .into_vec(&mut buf)
        .encode_all(&src)
        .status
        .map(|_| {
          let mut h = C::new();
          h.update(&buf);
          buf[ENCODE_META_SIZE..ENCODE_META_SIZE + MAX_MESSAGE_SIZE].copy_from_slice(
            ((buf.len() - ENCODE_HEADER_SIZE) as u32)
              .to_be_bytes()
              .as_slice(),
          );
          buf[ENCODE_META_SIZE + MAX_MESSAGE_SIZE..ENCODE_HEADER_SIZE]
            .copy_from_slice(h.finalize().to_be_bytes().as_slice());
          Message(MessageInner::Bytes(buf.into()))
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
        .map_err(|e| DecompressError::Lzw(e)),
      CompressionAlgo::None => Ok(self.buf.clone()),
    }
  }

  pub(crate) fn encode<C: Checksumer>(
    &self,
    r1: u8,
    r2: u8,
    r3: u8,
  ) -> Result<Message, CompressError> {
    Self::encode_slice::<C>(self.algo, r1, r2, r3, &self.buf)
  }

  pub(crate) fn decode_from_bytes<C: Checksumer>(
    mut src: Bytes,
  ) -> Result<(EncodeMeta, Self), DecodeError> {
    let r1 = src[1];
    let r2 = src[2];
    let r3 = src[3];
    let len = u32::from_be_bytes(
      (&src[ENCODE_META_SIZE..ENCODE_META_SIZE + MAX_MESSAGE_SIZE].try_into()).unwrap(),
    );
    let cks = u32::from_be_bytes(
      (&src[ENCODE_META_SIZE + MAX_MESSAGE_SIZE..ENCODE_HEADER_SIZE].try_into()).unwrap(),
    );
    let mut h = C::new();
    h.update(&src[ENCODE_HEADER_SIZE..]);
    if cks != h.finalize() {
      return Err(DecodeError::ChecksumMismatch);
    }
    let algo = CompressionAlgo::try_from(src[ENCODE_HEADER_SIZE])?;
    match algo {
      CompressionAlgo::Lzw => {
        src.advance(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE);
        Ok((
          EncodeMeta {
            ty: MessageType::Compress,
            r1,
            r2,
            r3,
          },
          Self { algo, buf: src },
        ))
      }
      CompressionAlgo::None => {
        src.advance(ENCODE_HEADER_SIZE + CompressionAlgo::SIZE);
        Ok((
          EncodeMeta {
            ty: MessageType::Compress,
            r1,
            r2,
            r3,
          },
          Self { algo, buf: src },
        ))
      }
    }
  }
}

const LZW_LIT_WIDTH: u8 = 8;
const PREALLOCATE_SIZE: usize = 128;

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

pub(crate) fn compress_payload(
  algo: CompressionAlgo,
  src: &[u8],
) -> Result<Vec<u8>, CompressError> {
  match algo {
    CompressionAlgo::Lzw => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .encode(src)
      .map_err(CompressError::Lzw),
    CompressionAlgo::None => unreachable!(),
  }
}
