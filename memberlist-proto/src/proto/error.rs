use crate::{DecodeError, EncodeError, Label, ParseLabelError};

/// An error that can occur during encoding.
#[derive(Debug, thiserror::Error)]
pub enum ProtoDecoderError {
  /// Decoding error
  #[error(transparent)]
  Decode(#[from] DecodeError),
  /// Returned when failed to parse the label.
  #[error(transparent)]
  Label(#[from] ParseLabelError),
  /// The label does not match the expected label
  #[error("The label {actual} does not match the expected label {expected}")]
  LabelMismatch {
    /// The actual label
    actual: Label,
    /// The expected label
    expected: Label,
  },
  /// Required label is missing
  #[error("label not found")]
  LabelNotFound,
  /// unexpected double packet label header
  #[error("unexpected double packet label header")]
  UnexpectedLabel,

  /// The offload task is canceled
  #[error("The offload task is canceled")]
  Offload,

  /// Returned when the encryption feature is disabled.
  #[error(
    "Receive an encrypted message while the encryption feature is disabled on the running node"
  )]
  EncryptionDisabled,

  /// Returned when the checksum feature is disabled.
  #[error(
    "Receive a checksumed message while the checksum feature is disabled on the running node"
  )]
  ChecksumDisabled,

  /// Returned when the compression feature is disabled.
  #[error(
    "Receive a compressed message while the compression feature is disabled on the running node"
  )]
  CompressionDisabled,

  /// Encryption error
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[error(transparent)]
  Encryption(#[from] crate::EncryptionError),
  /// Not encrypted
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[error("The message is not encrypted")]
  NotEncrypted,
  /// No installed key
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[error("key not found for decryption")]
  SecretKeyNotFound,
  /// No installed keys could decrypt the message
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[error("no installed keys could decrypt the message")]
  NoInstalledKeys,

  /// Compression error
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "snappy",
      feature = "brotli",
    )))
  )]
  #[error(transparent)]
  Compression(#[from] crate::CompressionError),

  /// Checksum error
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash64",
      feature = "xxhash32",
      feature = "xxhash3",
      feature = "murmur3",
    )))
  )]
  #[error(transparent)]
  Checksum(#[from] crate::ChecksumError),
}

impl From<ProtoDecoderError> for std::io::Error {
  fn from(value: ProtoDecoderError) -> Self {
    use std::io::ErrorKind;

    match value {
      ProtoDecoderError::Decode(e) => Self::new(ErrorKind::InvalidData, e),
      ProtoDecoderError::Label(e) => Self::new(ErrorKind::InvalidData, e),
      ProtoDecoderError::LabelMismatch { .. } => Self::new(ErrorKind::InvalidData, value),
      ProtoDecoderError::Offload => Self::other(value),
      ProtoDecoderError::EncryptionDisabled => Self::other(value),
      ProtoDecoderError::ChecksumDisabled => Self::other(value),
      ProtoDecoderError::CompressionDisabled => Self::other(value),
      ProtoDecoderError::UnexpectedLabel => Self::new(ErrorKind::InvalidInput, value),
      ProtoDecoderError::LabelNotFound => Self::new(ErrorKind::InvalidInput, value),
      #[cfg(feature = "encryption")]
      ProtoDecoderError::Encryption(e) => Self::new(ErrorKind::InvalidData, e),
      #[cfg(feature = "encryption")]
      ProtoDecoderError::NotEncrypted => Self::new(ErrorKind::InvalidData, value),
      #[cfg(feature = "encryption")]
      ProtoDecoderError::SecretKeyNotFound => Self::new(ErrorKind::InvalidData, value),
      #[cfg(feature = "encryption")]
      ProtoDecoderError::NoInstalledKeys => Self::new(ErrorKind::InvalidData, value),
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "snappy",
        feature = "brotli",
      ))]
      ProtoDecoderError::Compression(e) => Self::new(ErrorKind::InvalidData, e),
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      ProtoDecoderError::Checksum(e) => Self::new(ErrorKind::InvalidData, e),
    }
  }
}

impl ProtoDecoderError {
  /// Creates a new label mismatch error.
  #[inline]
  pub(crate) const fn label_mismatch(expected: Label, actual: Label) -> Self {
    Self::LabelMismatch { expected, actual }
  }

  #[inline]
  pub(crate) const fn double_label() -> Self {
    Self::UnexpectedLabel
  }
}

/// The errors may occur during encoding.
#[derive(Debug, thiserror::Error)]
pub enum ProtoEncoderError {
  #[error(transparent)]
  Encode(#[from] EncodeError),
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[error(transparent)]
  Encrypt(#[from] crate::EncryptionError),
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "brotli",
    feature = "snappy",
  ))]
  #[cfg_attr(
    docsrs,
    feature(any(
      feature = "zstd",
      feature = "lz4",
      feature = "brotli",
      feature = "snappy",
    ))
  )]
  #[error(transparent)]
  Compress(#[from] crate::CompressionError),
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[cfg_attr(
    docsrs,
    feature(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3",
    ))
  )]
  #[error(transparent)]
  Checksum(#[from] crate::ChecksumError),
}
