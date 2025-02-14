use super::CompressAlgorithm;

/// Compress errors.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
  /// Lz4 compress errors
  #[error(transparent)]
  #[cfg(feature = "lz4")]
  #[cfg_attr(docsrs, doc(cfg(feature = "lz4")))]
  Lz4(#[from] lz4_flex::block::CompressError),
  /// Brotli compress errors
  #[error(transparent)]
  #[cfg(feature = "brotli")]
  #[cfg_attr(docsrs, doc(cfg(feature = "brotli")))]
  Brotli(std::io::Error),
  /// Snappy compress errors
  #[error(transparent)]
  #[cfg(feature = "snappy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "snappy")))]
  Snappy(#[from] snap::Error),
  /// Zstd compress errors
  #[error(transparent)]
  #[cfg(feature = "zstd")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
  Zstd(std::io::Error),
}

/// Decompress errors.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
  /// Brotli decompress errors
  #[error(transparent)]
  #[cfg(feature = "brotli")]
  #[cfg_attr(docsrs, doc(cfg(feature = "brotli")))]
  Brotli(std::io::Error),
  /// LZ4 decompress errors
  #[error(transparent)]
  #[cfg(feature = "lz4")]
  #[cfg_attr(docsrs, doc(cfg(feature = "lz4")))]
  Lz4(#[from] lz4_flex::block::DecompressError),
  /// Snappy decompress errors
  #[error(transparent)]
  #[cfg(feature = "snappy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "snappy")))]
  Snappy(#[from] snap::Error),
  /// Zstd decompress errors
  #[error(transparent)]
  #[cfg(feature = "zstd")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
  Zstd(#[from] std::io::Error),
}

/// Compress/Decompress errors.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
  /// Compress errors
  #[error(transparent)]
  Compress(#[from] CompressError),
  /// Decompress errors
  #[error(transparent)]
  Decompress(#[from] DecompressError),
  /// Disabled
  #[error("the {algo} is supported but the feature {feature} is disabled")]
  Disabled {
    /// The algorithm want to use
    algo: CompressAlgorithm,
    /// The feature that is disabled
    feature: &'static str,
  },
  /// Unknown compressor
  #[error("unknown compress algorithm {0}")]
  UnknownAlgorithm(CompressAlgorithm),
}

impl CompressionError {
  #[cfg(not(all(
    feature = "brotli",
    feature = "lz4",
    feature = "snappy",
    feature = "zstd"
  )))]
  #[inline]
  const fn disabled(algo: CompressAlgorithm, feature: &'static str) -> Self {
    Self::Disabled { algo, feature }
  }
}
