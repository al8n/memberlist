use std::{borrow::Cow, str::FromStr};

const CRC32_TAG: u8 = 1;
const XXHASH32_TAG: u8 = 2;
const XXHASH64_TAG: u8 = 3;
const XXHASH3_TAG: u8 = 4;
const MURMUR3_TAG: u8 = 5;

/// An error type for parsing checksum algorithm from str.
#[derive(Debug, thiserror::Error)]
#[error("unknown checksum algorithm {0}")]
pub struct ParseChecksumAlgorithmError(String);

impl FromStr for ChecksumAlgorithm {
  type Err = ParseChecksumAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s {
      "crc32" | "Crc32" | "CRC32" => Self::Crc32,
      "xxhash32" | "XxHash32" | "XXHASH32" | "xxh32" => Self::XxHash32,
      "xxhash64" | "XxHash64" | "XXHASH64" | "xxh64" => Self::XxHash64,
      "xxhash3" | "XxHash3" | "XXHASH3" | "xxh3" => Self::XxHash3,
      "murmur3" | "Murmur3" | "MURMUR3" | "MurMur3" => Self::Murmur3,
      val if val.starts_with("unknown") => {
        let val = val.trim_start_matches("unknown(").trim_end_matches(')');
        Self::Unknown(
          val
            .parse::<u8>()
            .map_err(|_| ParseChecksumAlgorithmError(val.to_string()))?,
        )
      }
      val => return Err(ParseChecksumAlgorithmError(val.to_string())),
    })
  }
}

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
  UnknownAlgorithm(ChecksumAlgorithm),
  /// Checksum mismatch
  #[error("checksum mismatch")]
  Mismatch,
}

impl ChecksumError {
  #[cfg(not(all(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3"
  )))]
  #[inline]
  const fn disabled(algo: ChecksumAlgorithm, feature: &'static str) -> Self {
    Self::Disabled { algo, feature }
  }
}

/// The algorithm used to checksum the message.
#[derive(
  Debug, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::IsVariant,
)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
  /// CRC32 IEEE
  #[default]
  #[display("crc32")]
  Crc32,
  /// XXHash32
  #[display("xxhash32")]
  XxHash32,
  /// XXHash64
  #[display("xxhash64")]
  XxHash64,
  /// XXHash3
  #[display("xxhash3")]
  XxHash3,
  /// Murmur3
  #[display("murmur3")]
  Murmur3,
  /// Unknwon checksum algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

#[cfg(feature = "quickcheck")]
const _: () = {
  impl ChecksumAlgorithm {
    const MAX: Self = Self::Murmur3;
    const MIN: Self = Self::Crc32;
  }

  impl quickcheck::Arbitrary for ChecksumAlgorithm {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      let val = (u8::arbitrary(g) % Self::MAX.as_u8()) + Self::MIN.as_u8();
      match val {
        CRC32_TAG => Self::Crc32,
        XXHASH32_TAG => Self::XxHash32,
        XXHASH64_TAG => Self::XxHash64,
        XXHASH3_TAG => Self::XxHash3,
        MURMUR3_TAG => Self::Murmur3,
        _ => unreachable!(),
      }
    }
  }
};

impl ChecksumAlgorithm {
  /// Returns the checksum algorithm as a `u8`.
  #[inline]
  pub const fn as_u8(&self) -> u8 {
    match self {
      Self::Crc32 => CRC32_TAG,
      Self::XxHash32 => XXHASH32_TAG,
      Self::XxHash64 => XXHASH64_TAG,
      Self::XxHash3 => XXHASH3_TAG,
      Self::Murmur3 => MURMUR3_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Returns the checksum algorithm as a `&'static str`.
  #[inline]
  pub fn as_str(&self) -> Cow<'static, str> {
    let val = match self {
      Self::Crc32 => "crc32",
      Self::XxHash32 => "xxhash32",
      Self::XxHash64 => "xxhash64",
      Self::XxHash3 => "xxhash3",
      Self::Murmur3 => "murmur3",
      Self::Unknown(e) => return Cow::Owned(format!("unknown({})", e)),
    };
    Cow::Borrowed(val)
  }

  /// Returns the output size of the checksum algorithm.
  #[inline]
  pub const fn output_size(&self) -> usize {
    match self {
      Self::Crc32 => 4,
      Self::XxHash32 => 4,
      Self::XxHash64 => 8,
      Self::XxHash3 => 8,
      Self::Murmur3 => 4,
      Self::Unknown(_) => 0,
    }
  }
}

impl From<u8> for ChecksumAlgorithm {
  fn from(value: u8) -> Self {
    match value {
      CRC32_TAG => Self::Crc32,
      XXHASH32_TAG => Self::XxHash32,
      XXHASH64_TAG => Self::XxHash64,
      XXHASH3_TAG => Self::XxHash3,
      MURMUR3_TAG => Self::Murmur3,
      _ => Self::Unknown(value),
    }
  }
}

impl From<ChecksumAlgorithm> for u8 {
  fn from(value: ChecksumAlgorithm) -> Self {
    value.as_u8()
  }
}

impl ChecksumAlgorithm {
  /// Calculate the checksum of the data using the specified algorithm.
  pub fn checksum(&self, data: &[u8]) -> Result<u64, ChecksumError> {
    Ok(match self {
      Self::Crc32 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "crc32")] {
            crc32fast::hash(data) as u64
          } else {
            return Err(ChecksumError::disabled(*self, "crc32"));
          }
        }
      }
      Self::XxHash32 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "xxhash32")] {
            xxhash_rust::xxh32::xxh32(data, 0) as u64
          } else {
            return Err(ChecksumError::disabled(*self, "xxhash32"));
          }
        }
      }
      Self::XxHash64 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "xxhash64")] {
            xxhash_rust::xxh64::xxh64(data, 0)
          } else {
            return Err(ChecksumError::disabled(*self, "xxhash64"));
          }
        }
      }
      Self::XxHash3 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "xxhash3")] {
            xxhash_rust::xxh3::xxh3_64(data)
          } else {
            return Err(ChecksumError::disabled(*self, "xxhash3"));
          }
        }
      }
      Self::Murmur3 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "murmur3")] {
            use core::hash::Hasher as _;

            let mut hasher = hash32::Murmur3Hasher::default();
            hasher.write(data);
            hasher.finish()
          } else {
            return Err(ChecksumError::disabled(*self, "murmur3"));
          }
        }
      }
      algo => return Err(ChecksumError::UnknownAlgorithm(*algo)),
    })
  }
}

#[test]
fn test_checksum_algorithm_from_str() {
  assert_eq!(
    "crc32".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::Crc32
  );
  assert_eq!(
    "xxhash32".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::XxHash32
  );
  assert_eq!(
    "xxhash64".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::XxHash64
  );
  assert_eq!(
    "xxhash3".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::XxHash3
  );
  assert_eq!(
    "murmur3".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::Murmur3
  );
  assert_eq!(
    "unknown(33)".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::Unknown(33)
  );
  assert!("unknown".parse::<ChecksumAlgorithm>().is_err());
}
