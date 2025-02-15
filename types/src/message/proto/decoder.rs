use core::marker::PhantomData;

use crate::Label;

#[cfg(feature = "encryption")]
use crate::{message::ENCRYPTED_MESSAGE_TAG, EncryptionAlgorithm, EncryptionError, SecretKey};

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "brotli",
  feature = "snappy",
))]
use crate::{message::COMPRESSED_MESSAGE_TAG, CompressAlgorithm, CompressionError};

#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3",
))]
use crate::{message::CHECKSUMED_MESSAGE_TAG, ChecksumAlgorithm, ChecksumError};

/// A decoder for decoding messages.
pub struct ProtoDecoder<'a, I, A> {
  buf: &'a [u8],
  label: &'a Label,
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  checksum: Option<crate::ChecksumAlgorithm>,
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  compress: Option<crate::CompressAlgorithm>,
  #[cfg(feature = "encryption")]
  encrypt: Option<(EncryptionAlgorithm, &'a [SecretKey])>,

  _m: PhantomData<(I, A)>,
}

impl<'a, I, A> ProtoDecoder<'a, I, A> {
  /// Creates a new messages encoder.
  #[inline]
  pub const fn new(buf: &'a [u8]) -> Self {
    Self {
      buf,
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
      #[cfg(feature = "encryption")]
      encrypt: None,

      _m: PhantomData,
    }
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
  pub const fn with_checksum(&mut self, checksum: Option<crate::ChecksumAlgorithm>) -> &mut Self {
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
    compress: Option<crate::CompressAlgorithm>,
  ) -> &mut Self {
    self.compress = compress;
    self
  }

  /// Feeds the encoder with an encryption algorithm.
  #[cfg(feature = "encryption")]
  pub const fn with_encryption(
    &mut self,
    encrypt: Option<(EncryptionAlgorithm, &'a [SecretKey])>,
  ) -> &mut Self {
    self.encrypt = encrypt;
    self
  }
}
