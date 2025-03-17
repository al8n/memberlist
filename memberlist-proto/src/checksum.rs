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
      "crc32" | "Crc32" | "CRC32" => {
        #[cfg(not(feature = "crc32"))]
        return Err(ParseChecksumAlgorithmError(
          "feature `crc32` is disabled".to_string(),
        ));

        #[cfg(feature = "crc32")]
        Self::Crc32
      }
      "xxhash32" | "XxHash32" | "XXHASH32" | "xxh32" => {
        #[cfg(not(feature = "xxhash32"))]
        return Err(ParseChecksumAlgorithmError(
          "feature `xxhash32` is disabled".to_string(),
        ));

        #[cfg(feature = "xxhash32")]
        Self::XxHash32
      }
      "xxhash64" | "XxHash64" | "XXHASH64" | "xxh64" => {
        #[cfg(not(feature = "xxhash64"))]
        return Err(ParseChecksumAlgorithmError(
          "feature `xxhash64` is disabled".to_string(),
        ));

        #[cfg(feature = "xxhash64")]
        Self::XxHash64
      }
      "xxhash3" | "XxHash3" | "XXHASH3" | "xxh3" => {
        #[cfg(not(feature = "xxhash3"))]
        return Err(ParseChecksumAlgorithmError(
          "feature `xxhash3` is disabled".to_string(),
        ));

        #[cfg(feature = "xxhash3")]
        Self::XxHash3
      }
      "murmur3" | "Murmur3" | "MURMUR3" | "MurMur3" => {
        #[cfg(not(feature = "murmur3"))]
        return Err(ParseChecksumAlgorithmError(
          "feature `murmur3` is disabled".to_string(),
        ));

        #[cfg(feature = "murmur3")]
        Self::Murmur3
      }
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
  pub(crate) const fn disabled(algo: ChecksumAlgorithm, feature: &'static str) -> Self {
    Self::Disabled { algo, feature }
  }
}

/// The algorithm used to checksum the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::IsVariant)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
  /// CRC32 IEEE
  #[display("crc32")]
  #[cfg(feature = "crc32")]
  #[cfg_attr(docsrs, doc(cfg(feature = "crc32")))]
  Crc32,
  /// XXHash32
  #[display("xxhash32")]
  #[cfg(feature = "xxhash32")]
  #[cfg_attr(docsrs, doc(cfg(feature = "xxhash32")))]
  XxHash32,
  /// XXHash64
  #[display("xxhash64")]
  #[cfg(feature = "xxhash64")]
  #[cfg_attr(docsrs, doc(cfg(feature = "xxhash64")))]
  XxHash64,
  /// XXHash3
  #[display("xxhash3")]
  #[cfg(feature = "xxhash3")]
  #[cfg_attr(docsrs, doc(cfg(feature = "xxhash3")))]
  XxHash3,
  /// Murmur3
  #[display("murmur3")]
  #[cfg(feature = "murmur3")]
  #[cfg_attr(docsrs, doc(cfg(feature = "murmur3")))]
  Murmur3,
  /// Unknwon checksum algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl ChecksumAlgorithm {
  #[inline]
  pub(crate) const fn unknown_or_disabled(&self) -> Option<ChecksumError> {
    match *self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => None,
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => None,
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => None,
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => None,
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => None,
      Self::Unknown(val) => Some({
        #[cfg(not(all(
          feature = "crc32",
          feature = "xxhash32",
          feature = "xxhash64",
          feature = "xxhash3",
          feature = "murmur3"
        )))]
        match val {
          CRC32_TAG => ChecksumError::disabled(Self::Unknown(val), "crc32"),
          XXHASH32_TAG => ChecksumError::disabled(Self::Unknown(val), "xxhash32"),
          XXHASH64_TAG => ChecksumError::disabled(Self::Unknown(val), "xxhash64"),
          XXHASH3_TAG => ChecksumError::disabled(Self::Unknown(val), "xxhash3"),
          MURMUR3_TAG => ChecksumError::disabled(Self::Unknown(val), "murmur3"),
          _ => ChecksumError::UnknownAlgorithm(Self::Unknown(val)),
        }

        #[cfg(all(
          feature = "crc32",
          feature = "xxhash32",
          feature = "xxhash64",
          feature = "xxhash3",
          feature = "murmur3"
        ))]
        ChecksumError::UnknownAlgorithm(Self::Unknown(val))
      }),
    }
  }
}

impl Default for ChecksumAlgorithm {
  fn default() -> Self {
    cfg_if::cfg_if! {
      if #[cfg(feature = "crc32")] {
        Self::Crc32
      } else if #[cfg(feature = "xxhash32")] {
        Self::XxHash32
      } else if #[cfg(feature = "xxhash64")] {
        Self::XxHash64
      } else if #[cfg(feature = "xxhash3")] {
        Self::XxHash3
      } else if #[cfg(feature = "murmur3")] {
        Self::Murmur3
      } else {
        Self::Unknown(255)
      }
    }
  }
}

#[cfg(any(feature = "quickcheck", test))]
const _: () = {
  use quickcheck::Arbitrary;

  impl ChecksumAlgorithm {
    const MAX: Self = Self::Murmur3;
    const MIN: Self = Self::Crc32;
  }

  impl Arbitrary for ChecksumAlgorithm {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      let val = (u8::arbitrary(g) % Self::MAX.as_u8()) + Self::MIN.as_u8();
      match val {
        #[cfg(feature = "crc32")]
        CRC32_TAG => Self::Crc32,
        #[cfg(feature = "xxhash32")]
        XXHASH32_TAG => Self::XxHash32,
        #[cfg(feature = "xxhash64")]
        XXHASH64_TAG => Self::XxHash64,
        #[cfg(feature = "xxhash3")]
        XXHASH3_TAG => Self::XxHash3,
        #[cfg(feature = "murmur3")]
        MURMUR3_TAG => Self::Murmur3,
        _ => Self::Unknown(u8::MAX),
      }
    }
  }
};

impl ChecksumAlgorithm {
  /// Returns the checksum algorithm as a `u8`.
  #[inline]
  pub const fn as_u8(&self) -> u8 {
    match self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => CRC32_TAG,
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => XXHASH32_TAG,
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => XXHASH64_TAG,
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => XXHASH3_TAG,
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => MURMUR3_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Returns the checksum algorithm as a `&'static str`.
  #[inline]
  pub fn as_str(&self) -> Cow<'static, str> {
    let val = match self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => "crc32",
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => "xxhash32",
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => "xxhash64",
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => "xxhash3",
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => "murmur3",
      Self::Unknown(e) => return Cow::Owned(format!("unknown({})", e)),
    };
    Cow::Borrowed(val)
  }

  /// Returns the output size of the checksum algorithm.
  #[inline]
  pub const fn output_size(&self) -> usize {
    match self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => 4,
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => 4,
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => 8,
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => 8,
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => 4,
      Self::Unknown(_) => 0,
    }
  }
}

impl From<u8> for ChecksumAlgorithm {
  fn from(value: u8) -> Self {
    match value {
      #[cfg(feature = "crc32")]
      CRC32_TAG => Self::Crc32,
      #[cfg(feature = "xxhash32")]
      XXHASH32_TAG => Self::XxHash32,
      #[cfg(feature = "xxhash64")]
      XXHASH64_TAG => Self::XxHash64,
      #[cfg(feature = "xxhash3")]
      XXHASH3_TAG => Self::XxHash3,
      #[cfg(feature = "murmur3")]
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
      #[cfg(feature = "crc32")]
      Self::Crc32 => crc32fast::hash(data) as u64,
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => xxhash_rust::xxh32::xxh32(data, 0) as u64,
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => xxhash_rust::xxh64::xxh64(data, 0),
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => xxhash_rust::xxh3::xxh3_64(data),
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => {
        use core::hash::Hasher as _;

        let mut hasher = hash32::Murmur3Hasher::default();
        hasher.write(data);
        hasher.finish()
      }
      algo => return Err(ChecksumError::UnknownAlgorithm(*algo)),
    })
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  impl Serialize for ChecksumAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: Serializer,
    {
      if serializer.is_human_readable() {
        serializer.serialize_str(self.as_str().as_ref())
      } else {
        serializer.serialize_u8(self.as_u8())
      }
    }
  }

  impl<'de> Deserialize<'de> for ChecksumAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        <&str>::deserialize(deserializer).and_then(|s| {
          s.parse::<ChecksumAlgorithm>()
            .map_err(serde::de::Error::custom)
        })
      } else {
        let v = u8::deserialize(deserializer)?;
        Ok(Self::from(v))
      }
    }
  }
};

#[test]
fn test_checksum_algorithm_from_str() {
  #[cfg(feature = "crc32")]
  assert_eq!(
    "crc32".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::Crc32
  );
  #[cfg(feature = "xxhash32")]
  assert_eq!(
    "xxhash32".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::XxHash32
  );
  #[cfg(feature = "xxhash64")]
  assert_eq!(
    "xxhash64".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::XxHash64
  );
  #[cfg(feature = "xxhash3")]
  assert_eq!(
    "xxhash3".parse::<ChecksumAlgorithm>().unwrap(),
    ChecksumAlgorithm::XxHash3
  );
  #[cfg(feature = "murmur3")]
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

#[cfg(all(test, feature = "serde"))]
#[quickcheck_macros::quickcheck]
fn checksum_algorithm_serde(algo: ChecksumAlgorithm) -> bool {
  use bincode::config::standard;

  let Ok(serialized) = serde_json::to_string(&algo) else {
    return false;
  };
  let Ok(deserialized) = serde_json::from_str(&serialized) else {
    return false;
  };
  if algo != deserialized {
    return false;
  }

  let Ok(serialized) = bincode::serde::encode_to_vec(algo, standard()) else {
    return false;
  };

  let Ok((deserialized, _)) = bincode::serde::decode_from_slice(&serialized, standard()) else {
    return false;
  };

  algo == deserialized
}
