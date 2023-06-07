use crate::checksum::Checksumer;

use super::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidCompressionAlgo(u8);

impl core::fmt::Display for InvalidCompressionAlgo {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "invalid compression algo {}", self)
  }
}

impl std::error::Error for InvalidCompressionAlgo {}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionAlgo {
  #[default]
  Lzw = 0,
  None = 1,
}

impl CompressionAlgo {
  pub fn is_none(&self) -> bool {
    matches!(self, Self::None)
  }
}

impl TryFrom<u8> for CompressionAlgo {
  type Error = InvalidCompressionAlgo;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Lzw),
      1 => Ok(Self::None),
      _ => Err(InvalidCompressionAlgo(value)),
    }
  }
}

pub(crate) struct CompressEncoder<T: BufMut, C: Checksumer> {
  buf: T,
  cks: C,
}

impl<T: BufMut, C: Checksumer> CompressEncoder<T, C> {
  #[inline]
  pub(crate) fn new(buf: T) -> Self {
    Self { buf, cks: C::new() }
  }

  #[inline]
  pub(crate) fn encoded_len(payload: &[u8]) -> usize {
    let length = if payload.is_empty() {
      0
    } else {
      // payload len + payload + tag
      encoded_u32_len(payload.len() as u32) + payload.len() + 1
    } + 1
      + 1; // seq_no + tag
    length + encoded_u32_len(length as u32) + CHECKSUM_SIZE
  }

  #[inline]
  pub(crate) fn encode_algo(&mut self, algo: CompressionAlgo) {
    self.buf.put_u8(1); // tag
    self.cks.update(&[algo as u8]);
    self.buf.put_u8(algo as u8);
  }

  #[inline]
  pub(crate) fn encode_payload(mut self, payload: &[u8]) -> T {
    if payload.is_empty() {
      self.buf.put_u32(self.cks.finalize());
      return self.buf;
    }
    self.buf.put_u8(2); // tag
    encode_u32_to_buf(&mut self.buf, payload.len() as u32);
    self.cks.update(payload);
    self.buf.put_slice(payload);
    self.buf.put_u32(self.cks.finalize());
    self.buf
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Compress {
  algo: CompressionAlgo,
  buf: Bytes,
}

impl Compress {
  #[inline]
  pub fn decode_len(buf: impl Buf) -> Result<u32, DecodeError> {
    decode_u32_from_buf(buf).map(|(x, _)| x).map_err(From::from)
  }

  #[inline]
  pub fn decode_from<C: Checksumer>(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut required = 0;
    let mut this = Self::default();
    let mut hasher = C::new();
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          if !buf.has_remaining() {
            return Err(DecodeError::Truncated(MessageType::Compress.as_err_str()));
          }
          this.algo = buf.get_u8().try_into()?;
          hasher.update(&[this.algo as u8]);
          required += 1;
        }
        2 => {
          if !buf.has_remaining() {
            return Err(DecodeError::Truncated(MessageType::Compress.as_err_str()));
          }
          let len = decode_u32_from_buf(&mut buf)?.0 as usize;
          if buf.remaining() < len {
            return Err(DecodeError::Truncated(MessageType::Compress.as_err_str()));
          }
          this.buf = buf.split_to(len);
          hasher.update(&this.buf);
        }
        _ => {}
      }
    }

    if required != 1 {
      return Err(DecodeError::Truncated(MessageType::Compress.as_err_str()));
    }

    if buf.remaining() < CHECKSUM_SIZE {
      return Err(DecodeError::Truncated(MessageType::Compress.as_err_str()));
    }

    let checksum = buf.get_u32();
    if hasher.finalize() != checksum {
      return Err(DecodeError::ChecksumMismatch);
    }

    Ok(this)
  }
}
