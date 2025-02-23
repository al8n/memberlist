#[cfg(feature = "encryption")]
const _: () = {
  use super::SecretKey;
  use base64::Engine;
  use serde::{Deserialize, Serialize};

  impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: serde::Serializer,
    {
      if serializer.is_human_readable() {
        base64::engine::general_purpose::STANDARD
          .encode(self)
          .serialize(serializer)
      } else {
        serializer.serialize_bytes(self)
      }
    }
  }

  impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: serde::Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        <&str as Deserialize<'de>>::deserialize(deserializer)
          .and_then(|val| Self::try_from(val).map_err(<D::Error as serde::de::Error>::custom))
      } else {
        <&[u8] as Deserialize<'de>>::deserialize(deserializer)
          .and_then(|e| Self::try_from(e).map_err(<D::Error as serde::de::Error>::custom))
      }
    }
  }
};
