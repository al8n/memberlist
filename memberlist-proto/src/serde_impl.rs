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
      enum Buf {
        Aes128([u8; 24]),
        Aes192([u8; 32]),
        Aes256([u8; 44]),
      }

      impl AsMut<[u8]> for Buf {
        fn as_mut(&mut self) -> &mut [u8] {
          match self {
            Buf::Aes128(buf) => buf,
            Buf::Aes192(buf) => buf,
            Buf::Aes256(buf) => buf,
          }
        }
      }

      if serializer.is_human_readable() {
        let mut buf = match self {
          Self::Aes128(_) => Buf::Aes128([0; 24]),
          Self::Aes192(_) => Buf::Aes192([0; 32]),
          Self::Aes256(_) => Buf::Aes256([0; 44]),
        };

        base64::engine::general_purpose::STANDARD
          .encode_slice(self.as_ref(), buf.as_mut())
          .map_err(serde::ser::Error::custom)?;

        serializer.serialize_str(core::str::from_utf8(buf.as_mut()).unwrap())
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
