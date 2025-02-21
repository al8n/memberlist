pub use decoder::*;
pub use encoder::*;
pub use message::*;

mod decoder;
mod encoder;

mod message;

use core::future::Future;

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

const COMPOOUND_MESSAGE_TAG: u8 = 1;
const PING_MESSAGE_TAG: u8 = 2;
const INDIRECT_PING_MESSAGE_TAG: u8 = 3;
const ACK_MESSAGE_TAG: u8 = 4;
const SUSPECT_MESSAGE_TAG: u8 = 5;
const ALIVE_MESSAGE_TAG: u8 = 6;
const DEAD_MESSAGE_TAG: u8 = 7;
const PUSH_PULL_MESSAGE_TAG: u8 = 8;
const USER_DATA_MESSAGE_TAG: u8 = 9;
const NACK_MESSAGE_TAG: u8 = 10;
const ERROR_RESPONSE_MESSAGE_TAG: u8 = 11;
const LABELED_MESSAGE_TAG: u8 = 12;
const CHECKSUMED_MESSAGE_TAG: u8 = 13;
const COMPRESSED_MESSAGE_TAG: u8 = 14;
const ENCRYPTED_MESSAGE_TAG: u8 = 15;

#[inline]
const fn is_plain_message_tag(tag: u8) -> bool {
  matches!(
    tag,
    PING_MESSAGE_TAG
      | INDIRECT_PING_MESSAGE_TAG
      | ACK_MESSAGE_TAG
      | SUSPECT_MESSAGE_TAG
      | ALIVE_MESSAGE_TAG
      | DEAD_MESSAGE_TAG
      | PUSH_PULL_MESSAGE_TAG
      | USER_DATA_MESSAGE_TAG
      | NACK_MESSAGE_TAG
      | ERROR_RESPONSE_MESSAGE_TAG
  )
}

/// The reader used in the memberlist proto
pub trait ProtoReader: Send + Sync {
  /// Read the payload from the proto reader to the buffer
  ///
  /// Returns the number of bytes read
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send;

  /// Read exactly the payload from the proto reader to the buffer
  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Peek the payload from the proto reader to the buffer
  ///
  /// Returns the number of bytes peeked
  fn peek(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send;

  /// Peek exactly the payload from the proto reader to the buffer
  fn peek_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send;
}

impl<T: ProtoReader> ProtoReader for &mut T {
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send {
    (**self).read(buf)
  }

  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send {
    (**self).read_exact(buf)
  }

  fn peek(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send {
    (**self).peek(buf)
  }

  fn peek_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send {
    (**self).peek_exact(buf)
  }
}

const _: () = {
  use futures_util::io::AsyncRead;
  use peekable::future::AsyncPeekable;

  impl<T: AsyncRead + Send + Sync + Unpin> ProtoReader for AsyncPeekable<T> {
    async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
      AsyncPeekable::peek(self, buf).await
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
      futures_util::io::AsyncReadExt::read_exact(self, buf).await
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
      futures_util::io::AsyncReadExt::read(self, buf).await
    }

    async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
      AsyncPeekable::peek_exact(self, buf).await
    }
  }
};

/// The writer used in the memberlist proto
pub trait ProtoWriter: Send + Sync {
  /// Close the proto writer
  fn close(&mut self) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Write the payload to the proto writer
  fn write_all(&mut self, payload: &[u8]) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Flush the proto writer
  fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> + Send;
}

impl<W> ProtoWriter for W
where
  W: futures_util::AsyncWrite + Send + Sync + Unpin,
{
  async fn close(&mut self) -> std::io::Result<()> {
    futures_util::io::AsyncWriteExt::close(self).await
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    futures_util::io::AsyncWriteExt::write_all(self, payload).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    futures_util::io::AsyncWriteExt::flush(self).await
  }
}

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
