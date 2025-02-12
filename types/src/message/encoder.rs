use super::{Data, EncodeError, Message};

/// The encoder of a message
pub struct MessageEncoder<'a, I, A> {
  msgs: &'a [Message<I, A>],
  label: &'a str,
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  checksum: Option<super::ChecksumAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "deflate",
    feature = "gzip",
    feature = "snappy",
    feature = "lzw",
    feature = "zlib",
  ))]
  compress: Option<super::CompressAlgorithm>,
  #[cfg(feature = "encryption")]
  encrypt: Option<super::EncryptionAlgorithm>,
  #[allow(dead_code)]
  max_payload_size: usize,
}

impl<'a, I, A> MessageEncoder<'a, I, A> {
  /// Creates a new message encoder.
  #[inline]
  pub const fn new(max_payload_size: usize) -> Self {
    Self {
      msgs: &[],
      label: "",
      checksum: None,
      compress: None,
      encrypt: None,
      max_payload_size,
    }
  }

  /// Feeds the encoder with messages.
  pub const fn with_messages(&mut self, msgs: &'a [Message<I, A>]) -> &mut Self {
    self.msgs = msgs;
    self
  }

  /// Feeds the encoder with a label.
  pub const fn with_label(&mut self, label: &'a str) -> &mut Self {
    self.label = label;
    self
  }

  /// Feeds the encoder with a checksum algorithm.
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  pub const fn with_checksum(&mut self, checksum: Option<super::ChecksumAlgorithm>) -> &mut Self {
    self.checksum = checksum;
    self
  }

  /// Feeds the encoder with a compression algorithm.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "deflate",
    feature = "gzip",
    feature = "snappy",
    feature = "lzw",
    feature = "zlib",
  ))]
  pub const fn with_compression(
    &mut self,
    compress: Option<super::CompressAlgorithm>,
  ) -> &mut Self {
    self.compress = compress;
    self
  }

  /// Feeds the encoder with an encryption algorithm.
  #[cfg(feature = "encryption")]
  pub const fn with_encryption(
    &mut self,
    encrypt: Option<super::EncryptionAlgorithm>,
  ) -> &mut Self {
    self.encrypt = encrypt;
    self
  }
}

impl<I, A> MessageEncoder<'_, I, A>
where
  I: Data,
  A: Data,
{
  /// Encodes the messages.
  pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
    if self.msgs.len() > 1 {
      self.encode_batch()
    } else {
      self.encode_single()
    }
  }

  fn encode_single(&self) -> Result<Vec<u8>, EncodeError> {
    todo!()
  }

  fn encode_batch(&self) -> Result<Vec<u8>, EncodeError> {
    todo!()
  }
}
