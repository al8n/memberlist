use memberlist_types::ChecksumAlgorithm;

/// Checksum error
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ChecksumError {
  /// The checksum algorithm is supported, but the required feature is disabled
  #[error("the {algo} is supported but the feature {feature} is disabled")]
  Disabled {
    /// The algorithm want to use
    algo: ChecksumAlgorithm,
    /// The feature that is disabled
    feature: &'static str,
  },
  /// Unknown checksum algorithm
  #[error("unknown checksum algorithm: {0}")]
  UnknownChecksumAlgorithm(ChecksumAlgorithm),
  /// Checksum mismatch
  #[error("checksum mismatch")]
  Mismatch,
}

impl ChecksumError {
  #[inline]
  const fn disabled(algo: ChecksumAlgorithm, feature: &'static str) -> Self {
    Self::Disabled { algo, feature }
  }
}

pub(crate) fn checksum(algo: ChecksumAlgorithm, data: &[u8]) -> Result<u64, ChecksumError> {
  Ok(match algo {
    ChecksumAlgorithm::Crc32 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "crc32")] {
          crc32fast::hash(data) as u64
        } else {
          return Err(ChecksumError::disabled(algo, "crc32"));
        }
      }
    }
    ChecksumAlgorithm::XxHash32 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "xxhash32")] {
          xxhash_rust::xxh32::xxh32(data, 0) as u64
        } else {
          return Err(ChecksumError::disabled(algo, "xxhash32"));
        }
      }
    }
    ChecksumAlgorithm::XxHash64 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "xxhash64")] {
          xxhash_rust::xxh64::xxh64(data, 0)
        } else {
          return Err(ChecksumError::disabled(algo, "xxhash64"));
        }
      }
    }
    ChecksumAlgorithm::XxHash3 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "xxhash3")] {
          xxhash_rust::xxh3::xxh3_64(data)
        } else {
          return Err(ChecksumError::disabled(algo, "xxhash3"));
        }
      }
    }
    ChecksumAlgorithm::Murmur3 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "murmur3")] {
          use core::hash::Hasher as _;

          let mut hasher = hash32::Murmur3Hasher::default();
          hasher.write(data);
          hasher.finish()
        } else {
          return Err(ChecksumError::disabled(algo, "murmur3"));
        }
      }
    }
    algo => return Err(ChecksumError::UnknownChecksumAlgorithm(algo)),
  })
}
