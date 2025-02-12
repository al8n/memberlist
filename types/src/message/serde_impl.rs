use super::{ChecksumAlgorithm, EncryptionAlgorithm};

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
}
