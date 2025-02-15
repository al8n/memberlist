pub use decoder::*;
pub use encoder::*;

mod decoder;
mod encoder;

const PAYLOAD_LEN_SIZE: usize = 4;
const MAX_MESSAGES_PER_BATCH: usize = 255;
const BATCH_OVERHEAD: usize = 2; // 1 byte for the batch message tag, 1 byte for the number of messages

const ENCRYPTED_MESSAGE_HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE; // 1 byte for the encryption tag, 1 byte algo, 4 bytes for the length
const COMPRESSED_MESSAGE_HEADER_SIZE: usize = 1 + 2 + PAYLOAD_LEN_SIZE; // 1 byte for the compression tag, 2 byte for the algo, 4 bytes for the uncompressed data length
const CHECKSUMED_MESSAGE_HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE; // 1 byte for the checksum tag, 1 byte for the algo, 4 bytes for the checksum

#[cfg(feature = "encryption")]
struct AeadBuffer<'a> {
  buf: &'a mut [u8],
  len: usize,
}

#[cfg(feature = "encryption")]
const _: () = {
  impl<'a> AeadBuffer<'a> {
    #[inline]
    const fn new(buf: &'a mut [u8]) -> Self {
      Self { buf, len: 0 }
    }
  }

  impl AsRef<[u8]> for AeadBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
      &self.buf[..self.len]
    }
  }

  impl AsMut<[u8]> for AeadBuffer<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
      &mut self.buf[..self.len]
    }
  }

  impl aead::Buffer for AeadBuffer<'_> {
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
      if self.len >= self.buf.len() {
        return Err(aead::Error);
      }

      self.buf[self.len..self.len + other.len()].copy_from_slice(other);
      self.len += other.len();
      Ok(())
    }

    fn truncate(&mut self, len: usize) {
      if len >= self.len {
        return;
      }

      self.buf[len..self.len].fill(0);
      self.len = len;
    }
  }
};
