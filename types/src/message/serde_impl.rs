use super::{
  BrotliAlgorithm, BrotliQuality, BrotliWindow, ChecksumAlgorithm, CompressAlgorithm,
  EncryptionAlgorithm, ZstdAlgorithm,
};

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

impl Serialize for EncryptionAlgorithm {
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

impl<'de> Deserialize<'de> for EncryptionAlgorithm {
  fn deserialize<D>(deserializer: D) -> Result<EncryptionAlgorithm, D::Error>
  where
    D: Deserializer<'de>,
  {
    if deserializer.is_human_readable() {
      <&str>::deserialize(deserializer).and_then(|s| s.parse().map_err(serde::de::Error::custom))
    } else {
      u8::deserialize(deserializer).map(EncryptionAlgorithm::from)
    }
  }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BrotliAlgorithmHelper {
  quality: BrotliQuality,
  window: BrotliWindow,
}

impl From<BrotliAlgorithm> for BrotliAlgorithmHelper {
  fn from(algo: BrotliAlgorithm) -> Self {
    Self {
      quality: algo.quality(),
      window: algo.window(),
    }
  }
}

impl From<BrotliAlgorithmHelper> for BrotliAlgorithm {
  fn from(helper: BrotliAlgorithmHelper) -> Self {
    Self::with_quality_and_window(helper.quality, helper.window)
  }
}

impl Serialize for BrotliAlgorithm {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    if serializer.is_human_readable() {
      BrotliAlgorithmHelper::from(*self).serialize(serializer)
    } else {
      serializer.serialize_u8(self.encode())
    }
  }
}

impl<'de> Deserialize<'de> for BrotliAlgorithm {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    if deserializer.is_human_readable() {
      BrotliAlgorithmHelper::deserialize(deserializer).map(Into::into)
    } else {
      u8::deserialize(deserializer).map(Self::decode)
    }
  }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum CompressAlgorithmHelper {
  /// Brotli
  Brotli(BrotliAlgorithm),
  /// Deflate
  Deflate,
  /// Gzip
  Gzip,
  /// LAW
  Lzw,
  /// LZ4
  Lz4,
  /// Snappy
  Snappy,
  /// Zlib
  Zlib,
  /// Zstd
  Zstd(ZstdAlgorithm),
  /// Unknwon compressioned algorithm
  Unknown(u8),
}

impl From<CompressAlgorithm> for CompressAlgorithmHelper {
  fn from(algo: CompressAlgorithm) -> Self {
    match algo {
      CompressAlgorithm::Brotli(algo) => Self::Brotli(algo),
      CompressAlgorithm::Deflate => Self::Deflate,
      CompressAlgorithm::Gzip => Self::Gzip,
      CompressAlgorithm::Lzw => Self::Lzw,
      CompressAlgorithm::Lz4 => Self::Lz4,
      CompressAlgorithm::Snappy => Self::Snappy,
      CompressAlgorithm::Zlib => Self::Zlib,
      CompressAlgorithm::Zstd(algo) => Self::Zstd(algo),
      CompressAlgorithm::Unknown(v) => Self::Unknown(v),
    }
  }
}

impl From<CompressAlgorithmHelper> for CompressAlgorithm {
  fn from(helper: CompressAlgorithmHelper) -> Self {
    match helper {
      CompressAlgorithmHelper::Brotli(algo) => Self::Brotli(algo),
      CompressAlgorithmHelper::Deflate => Self::Deflate,
      CompressAlgorithmHelper::Gzip => Self::Gzip,
      CompressAlgorithmHelper::Lzw => Self::Lzw,
      CompressAlgorithmHelper::Lz4 => Self::Lz4,
      CompressAlgorithmHelper::Snappy => Self::Snappy,
      CompressAlgorithmHelper::Zlib => Self::Zlib,
      CompressAlgorithmHelper::Zstd(algo) => Self::Zstd(algo),
      CompressAlgorithmHelper::Unknown(v) => Self::Unknown(v),
    }
  }
}

impl Serialize for CompressAlgorithm {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    if serializer.is_human_readable() {
      CompressAlgorithmHelper::from(*self).serialize(serializer)
    } else {
      serializer.serialize_u16(self.encode_to_u16())
    }
  }
}

impl<'de> Deserialize<'de> for CompressAlgorithm {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: Deserializer<'de>,
  {
    if deserializer.is_human_readable() {
      CompressAlgorithmHelper::deserialize(deserializer).map(Into::into)
    } else {
      u16::deserialize(deserializer).map(Self::decode_from_u16)
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[quickcheck_macros::quickcheck]
  fn checksum_algorithm_serde(algo: ChecksumAlgorithm) -> bool {
    let Ok(serialized) = serde_json::to_string(&algo) else {
      return false;
    };
    let Ok(deserialized) = serde_json::from_str(&serialized) else {
      return false;
    };
    if algo != deserialized {
      return false;
    }

    let Ok(serialized) = bincode::serialize(&algo) else {
      return false;
    };

    let Ok(deserialized) = bincode::deserialize(&serialized) else {
      return false;
    };

    algo == deserialized
  }

  #[quickcheck_macros::quickcheck]
  fn encryption_algorithm_serde(algo: EncryptionAlgorithm) -> bool {
    let Ok(serialized) = serde_json::to_string(&algo) else {
      return false;
    };
    let Ok(deserialized) = serde_json::from_str(&serialized) else {
      return false;
    };
    if algo != deserialized {
      return false;
    }

    let Ok(serialized) = bincode::serialize(&algo) else {
      return false;
    };

    let Ok(deserialized) = bincode::deserialize(&serialized) else {
      return false;
    };

    algo == deserialized
  }

  #[quickcheck_macros::quickcheck]
  fn compress_algorithm_serde(algo: CompressAlgorithm) -> bool {
    let Ok(serialized) = serde_json::to_string(&algo) else {
      return false;
    };
    let Ok(deserialized) = serde_json::from_str(&serialized) else {
      return false;
    };
    if algo != deserialized {
      return false;
    }

    let Ok(serialized) = bincode::serialize(&algo) else {
      return false;
    };

    let Ok(deserialized) = bincode::deserialize(&serialized) else {
      return false;
    };

    algo == deserialized
  }
}
