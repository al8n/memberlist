pub use decoder::*;
pub use encoder::*;

mod decoder;
mod encoder;

const PAYLOAD_LEN_SIZE: usize = 4;
const MAX_MESSAGES_PER_BATCH: usize = 255;
/// - 1 byte for the batch message tag
/// - 1 byte for the number of messages
/// - 4 bytes for the total length of the messages in bytes
const BATCH_OVERHEAD: usize = 1 + 1 + PAYLOAD_LEN_SIZE;
/// - 1 byte for the label message tag
/// - 1 byte for the label length
const LABEL_OVERHEAD: usize = 1 + 1;

/// - 1 byte for the message tag
/// - 5 bytes for the max u32 varint encoded size
const MAX_PLAIN_MESSAGE_HEADER_SIZE: usize = 1 + 5;

const ENCRYPTED_MESSAGE_HEADER_SIZE: usize = 1 + 1 + PAYLOAD_LEN_SIZE; // 1 byte for the encryption tag, 1 byte algo, 4 bytes for the length
/// - 1 byte for the compression tag
/// - 2 byte for the algo
/// - 4 bytes for the uncompressed data length
/// - 4 bytes for the compressed data length
const COMPRESSED_MESSAGE_HEADER_SIZE: usize = 1 + 2 + PAYLOAD_LEN_SIZE + PAYLOAD_LEN_SIZE;
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
    const fn new(buf: &'a mut [u8], len: usize) -> Self {
      Self { buf, len }
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
