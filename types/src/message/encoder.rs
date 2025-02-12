use super::{Data, EncodeError, Label, Message};

/// The errors may occur during encoding.

#[derive(Debug, thiserror::Error)]
pub enum EncoderError {
  #[error(transparent)]
  Encode(#[from] EncodeError),
  #[cfg(feature = "encryption")]
  #[error(transparent)]
  Encrypt(#[from] crate::EncryptionError),
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  #[error(transparent)]
  Compress(#[from] crate::CompressionError),
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[error(transparent)]
  Checksum(#[from] crate::ChecksumError),
}


/// The encoder of a message
pub struct MessageEncoder<'a, I, A> {
  msgs: &'a [Message<I, A>],
  label: &'a Label,
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
    feature = "snappy",
  ))]
  compress: Option<super::CompressAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  min_compression_size: usize,
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
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      checksum: None,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      compress: None,
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))]
      min_compression_size: 512,
      #[cfg(feature = "encryption")]
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
  pub const fn with_label(&mut self, label: &'a Label) -> &mut Self {
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
    feature = "snappy",
  ))]
  pub const fn with_compression(
    &mut self,
    compress: Option<super::CompressAlgorithm>,
  ) -> &mut Self {
    self.compress = compress;
    self
  }

  /// Feeds the encoder with a minimum compression size.
  /// 
  /// If the payload size is less than this value, the encoder will not compress the payload.
  /// 
  /// Default is 512 bytes. The minimum value is 32 bytes.
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  pub const fn with_min_compression_size(&mut self, min_compression_size: usize) -> &mut Self {
    self.min_compression_size = if min_compression_size < 32 {
      32
    } else {
      min_compression_size
    };
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
  pub fn encode(&self) -> Result<Vec<u8>, EncoderError> {
    if self.msgs.len() > 1 {
      self.encode_batch()
    } else {
      self.encode_single()
    }
  }

  fn encode_single(&self) -> Result<Vec<u8>, EncoderError> {
    if self.msgs.is_empty() {
      return Ok(Vec::new());
    }

    // 1. check if we need to compress the payload
    // 2. check if we need to add a checksum
    // 3. check if we need to encrypt the payload
    // 4. check if we need to add a label
    let msg = &self.msgs[0];
    let mut encoded_len = msg.encoded_len();

    #[cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))]
    let mut compress_algo = self.compress;

    #[cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))]
    if let Some(compress) = &mut compress_algo {
      if encoded_len >= self.min_compression_size {
        let compressed = compress.max_compress_len(encoded_len)?;
        // If the compressed size is larger than the original size,
        // we will not compress the payload.
        if compressed >= encoded_len {
          compress_algo = None;
        } else {
          encoded_len = 1 // an extra byte will be added to store the compression message tag
            + 1 // an extra byte will be added to store the compression algorithm
            + compressed;
        }
      }
    }

    // Now let's check if we need to add a checksum
    #[cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))]
    if let Some(checksum) = self.checksum {
      encoded_len += 1 // an extra byte will be added to store the checksum message tag
        + 1 // an extra byte will be added to store the checksum algorithm
        + checksum.output_size();
    }

    // Now let's check if we need to encrypt the payload
    #[cfg(feature = "encryption")]
    if let Some(algo) = self.encrypt {
      encoded_len += 1 // an extra byte will be added to store the encryption message tag
        + algo.encrypt_overhead()
        + algo.encrypted_len(encoded_len);
    }

    // Now let's check if we need to add a label
    if !self.label.is_empty() {
      encoded_len += self.label.encoded_overhead() + self.label.len();
    }

    // Now the encoded length is large enough for encoding/encrypting/compressing/labling
    let mut buf = vec![0u8; encoded_len];
    let mut offset = 0;

    // Do the encoding/encrypting/compressing/labling
    // 1. add the label
    // 2. write the encryption overhead
    // 3. encode the message after the encryption overhead
    if !self.label.is_empty() {
      buf[offset] = super::LABELED_TAG;
      offset += 1;
      buf[offset] = self.label.len() as u8; // label length can never be larger than 253
      offset += 1;
      buf[offset..offset + self.label.len()].copy_from_slice(self.label.as_bytes());
    }

    #[cfg(feature = "encryption")]
    if let Some(algo) = self.encrypt {
      buf[offset] = super::ENCRYPTED_MESSAGE_TAG;
      offset += 1;
      let nonce = super::EncryptionAlgorithm::random_nonce();
      buf[offset..nonce.len()].copy_from_slice(&nonce);
      offset += nonce.len();
    }



    todo!()
  }

  fn encode_batch(&self) -> Result<Vec<u8>, EncoderError> {
    todo!()
  }
}
