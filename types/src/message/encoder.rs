use super::{Data, EncodeError, Label, Message};

const PAYLOAD_LEN_SIZE: usize = 4;

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
  encrypt: Option<(super::EncryptionAlgorithm, super::SecretKey)>,
  #[allow(dead_code)]
  max_payload_size: usize,
}

impl<'a, I, A> MessageEncoder<'a, I, A> {
  /// Creates a new message encoder.
  #[inline]
  pub const fn new(max_payload_size: usize) -> Self {
    Self {
      msgs: &[],
      label: Label::EMPTY,
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
    encrypt: Option<(super::EncryptionAlgorithm, super::SecretKey)>,
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
    let original_encoded_len = encoded_len;

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
    let max_compressed_output_size = if let Some(compress) = &mut compress_algo {
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

        compressed
      } else {
        0
      }
    } else {
      0
    };

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
        + 4 // length of the total checksumed message
        + checksum.output_size();
    }

    // Now let's check if we need to encrypt the payload
    #[cfg(feature = "encryption")]
    if let Some((algo, _)) = &self.encrypt {
      encoded_len += 1 // an extra byte will be added to store the encryption message tag
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
      buf[offset] = super::LABELED_MESSAGE_TAG;
      offset += 1;
      buf[offset] = self.label.len() as u8; // label length can never be larger than 253
      offset += 1;
      buf[offset..offset + self.label.len()].copy_from_slice(self.label.as_bytes());
    }

    #[cfg(feature = "encryption")]
    let encrypted_len_offset = if let Some((algo, _)) = &self.encrypt {
      buf[offset] = super::ENCRYPTED_MESSAGE_TAG; // Add the encryption message tag
      offset += 1;
      buf[offset] = algo.as_u8(); // Add the encryption algorithm
      offset += 1;

      let encrypted_len_offset = offset;
      offset += PAYLOAD_LEN_SIZE; // 4 bytes to store the encrypted length
      encrypted_len_offset
    } else {
      offset
    };

    #[cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))]
    let checksum_len_offset = if let Some(checksum) = self.checksum {
      buf[offset] = super::CHECKSUMED_MESSAGE_TAG; // Add the checksum message tag
      offset += 1; // Add the checksum algorithm
      buf[offset] = checksum.as_u8();
      offset += 1;
      let checksum_len_offset = offset;
      offset += PAYLOAD_LEN_SIZE; // Add the length of the total checksumed message
      checksum_len_offset
    } else {
      offset
    };

    let mut data_written;
    cfg_if::cfg_if! {
      if #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
      ))] {
        if let Some(compress) = compress_algo {
          let mut encoded_buf = EncodeBuffer::new();
          encoded_buf.resize(original_encoded_len, 0);

          let start_offset = offset;
          let written = msg.encode(&mut encoded_buf)?;
          #[cfg(debug_assertions)]
          super::debug_assert_write_eq(written, original_encoded_len);

          buf[offset] = super::COMPRESSED_MESSAGE_TAG; // Add the compression message tag
          offset += 1;
          buf[offset..offset+2].copy_from_slice(&compress.encode_to_u16().to_be_bytes()); // Add the compression algorithm
          offset += 2;
          // add the original length of the compressed message, this is
          // useful for pre-allocating the buffer when decompressing
          buf[offset..offset + PAYLOAD_LEN_SIZE].copy_from_slice(&(written as u32).to_be_bytes());
          offset += PAYLOAD_LEN_SIZE; // Add the length of the total compressed message

          // compress to the buffer
          let compressed_len = compress.compress_to(&encoded_buf, &mut buf[offset..])?;
          #[cfg(debug_assertions)]
          debug_assert!(compressed_len <= max_compressed_output_size, "compress algo: {compress}, compressed_len: {}, max_compressed_output_size: {}", compressed_len, max_compressed_output_size);
          offset += compressed_len;
          data_written = offset - start_offset;
        } else {
          let written = msg.encode(&mut buf[offset..])?;
          #[cfg(debug_assertions)]
          super::debug_assert_write_eq(written, original_encoded_len);
          offset += written;
          data_written = written;
        }
      } else {
        let written = msg.encode(&mut buf[offset..])?;
        #[cfg(debug_assertions)]
        super::debug_assert_write_eq(written, original_encoded_len);
        offset += written;
        data_written = written;
      }
    };

    cfg_if::cfg_if! {
      if #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))] {
        if let Some(checksum) = self.checksum {
          let checksumed = checksum.checksum(&buf[checksum_len_offset + PAYLOAD_LEN_SIZE..offset])?;
          buf[checksum_len_offset..checksum_len_offset + PAYLOAD_LEN_SIZE].copy_from_slice(&(data_written as u32).to_be_bytes());
          let output_size = checksum.output_size();
          let checksumed_bytes = checksumed.to_be_bytes();
          buf[offset..offset + output_size].copy_from_slice(&checksumed_bytes[..output_size]);
          offset += output_size;
          data_written += output_size;
        } else {
          // we do not need to add a checksum, do nothing
        }
      } else {
        // we do not need to add a checksum, do nothing
      }
    }

    #[cfg(feature = "encryption")]
    if let Some((algo, pk)) = self.encrypt {
      let mut sub_offset = encrypted_len_offset;
      let nonce = super::EncryptionAlgorithm::random_nonce();
      let nonce_size = nonce.len();

      buf[sub_offset..sub_offset + PAYLOAD_LEN_SIZE]
        .copy_from_slice(&(data_written as u32).to_be_bytes());
      sub_offset += PAYLOAD_LEN_SIZE;

      buf[sub_offset..sub_offset + nonce_size].copy_from_slice(&nonce);
      sub_offset += nonce_size;

      let suffix_len = algo.encrypted_suffix_len(offset - sub_offset);
      algo.encrypt(
        pk,
        nonce,
        self.label.as_bytes(),
        &mut EncryptionBuffer::new(&mut buf[sub_offset..]),
      )?;
      offset += suffix_len;
    }

    buf.truncate(offset);
    Ok(buf)
  }

  fn encode_batch(&self) -> Result<Vec<u8>, EncoderError> {
    todo!()
  }
}

// 1500 bytes typically can hold the maximum size of a UDP packet
smallvec_wrapper::smallvec_wrapper!(
  EncodeBuffer<T>([T; 1500]);
);

#[cfg(feature = "encryption")]
struct EncryptionBuffer<'a> {
  buf: &'a mut [u8],
  len: usize,
}

#[cfg(feature = "encryption")]
const _: () = {
  impl<'a> EncryptionBuffer<'a> {
    #[inline]
    const fn new(buf: &'a mut [u8]) -> Self {
      Self { buf, len: 0 }
    }
  }

  impl AsRef<[u8]> for EncryptionBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
      &self.buf[..self.len]
    }
  }

  impl AsMut<[u8]> for EncryptionBuffer<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
      &mut self.buf[..self.len]
    }
  }

  impl aead::Buffer for EncryptionBuffer<'_> {
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
