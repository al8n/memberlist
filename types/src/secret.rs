/// The key used while attempting to encrypt/decrypt a message
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretKey {
  /// secret key for AES128
  Aes128([u8; 16]),
  /// secret key for AES192
  Aes192([u8; 24]),
  /// secret key for AES256
  Aes256([u8; 32]),
}

#[cfg(feature = "serde")]
const _: () = {
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
      macro_rules! parse {
        ($key:ident) => {{
          match $key.len() {
            16 => Ok(Self::Aes128($key.try_into().unwrap())),
            24 => Ok(Self::Aes192($key.try_into().unwrap())),
            32 => Ok(Self::Aes256($key.try_into().unwrap())),
            _ => Err(<D::Error as serde::de::Error>::custom(
              "invalid secret key length",
            )),
          }
        }};
      }

      if deserializer.is_human_readable() {
        <String as Deserialize<'de>>::deserialize(deserializer).and_then(|val| {
          base64::engine::general_purpose::STANDARD
            .decode(val)
            .map_err(serde::de::Error::custom)
            .and_then(|key| parse!(key))
        })
      } else {
        <Vec<u8> as Deserialize<'de>>::deserialize(deserializer).and_then(|val| parse!(val))
      }
    }
  }
};

impl core::hash::Hash for SecretKey {
  fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl core::borrow::Borrow<[u8]> for SecretKey {
  fn borrow(&self) -> &[u8] {
    self.as_ref()
  }
}

impl PartialEq<[u8]> for SecretKey {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref() == other
  }
}

impl core::ops::Deref for SecretKey {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl core::ops::DerefMut for SecretKey {
  fn deref_mut(&mut self) -> &mut Self::Target {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl From<[u8; 16]> for SecretKey {
  fn from(k: [u8; 16]) -> Self {
    Self::Aes128(k)
  }
}

impl From<[u8; 24]> for SecretKey {
  fn from(k: [u8; 24]) -> Self {
    Self::Aes192(k)
  }
}

impl From<[u8; 32]> for SecretKey {
  fn from(k: [u8; 32]) -> Self {
    Self::Aes256(k)
  }
}

impl TryFrom<&[u8]> for SecretKey {
  type Error = String;

  fn try_from(k: &[u8]) -> Result<Self, Self::Error> {
    match k.len() {
      16 => Ok(Self::Aes128(k.try_into().unwrap())),
      24 => Ok(Self::Aes192(k.try_into().unwrap())),
      32 => Ok(Self::Aes256(k.try_into().unwrap())),
      x => Err(format!(
        "invalid key size: {}, secret key size must be 16, 24 or 32 bytes",
        x
      )),
    }
  }
}

impl AsRef<[u8]> for SecretKey {
  fn as_ref(&self) -> &[u8] {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl AsMut<[u8]> for SecretKey {
  fn as_mut(&mut self) -> &mut [u8] {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

smallvec_wrapper::smallvec_wrapper!(
  /// A collection of secret keys, you can just treat it as a `Vec<SecretKey>`.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  #[repr(transparent)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  pub SecretKeys([SecretKey; 3]);
);

#[cfg(test)]
mod tests {
  use std::ops::{Deref, DerefMut};

  use super::*;

  #[test]
  fn test_secret_key() {
    let mut key = SecretKey::from([0; 16]);
    assert_eq!(key.deref(), &[0; 16]);
    assert_eq!(key.deref_mut(), &mut [0; 16]);
    assert_eq!(key.as_ref(), &[0; 16]);
    assert_eq!(key.as_mut(), &mut [0; 16]);
    assert_eq!(key.len(), 16);
    assert!(!key.is_empty());
    assert_eq!(key.to_vec(), vec![0; 16]);

    let mut key = SecretKey::from([0; 24]);
    assert_eq!(key.deref(), &[0; 24]);
    assert_eq!(key.deref_mut(), &mut [0; 24]);
    assert_eq!(key.as_ref(), &[0; 24]);
    assert_eq!(key.as_mut(), &mut [0; 24]);
    assert_eq!(key.len(), 24);
    assert!(!key.is_empty());
    assert_eq!(key.to_vec(), vec![0; 24]);

    let mut key = SecretKey::from([0; 32]);
    assert_eq!(key.deref(), &[0; 32]);
    assert_eq!(key.deref_mut(), &mut [0; 32]);
    assert_eq!(key.as_ref(), &[0; 32]);
    assert_eq!(key.as_mut(), &mut [0; 32]);
    assert_eq!(key.len(), 32);
    assert!(!key.is_empty());
    assert_eq!(key.to_vec(), vec![0; 32]);

    let mut key = SecretKey::from([0; 16]);
    assert_eq!(key.as_ref(), &[0; 16]);
    assert_eq!(key.as_mut(), &mut [0; 16]);

    let mut key = SecretKey::from([0; 24]);
    assert_eq!(key.as_ref(), &[0; 24]);
    assert_eq!(key.as_mut(), &mut [0; 24]);

    let mut key = SecretKey::from([0; 32]);
    assert_eq!(key.as_ref(), &[0; 32]);
    assert_eq!(key.as_mut(), &mut [0; 32]);

    let key = SecretKey::Aes128([0; 16]);
    assert_eq!(key.to_vec(), vec![0; 16]);

    let key = SecretKey::Aes192([0; 24]);
    assert_eq!(key.to_vec(), vec![0; 24]);

    let key = SecretKey::Aes256([0; 32]);
    assert_eq!(key.to_vec(), vec![0; 32]);
  }

  #[test]
  fn test_try_from() {
    assert!(SecretKey::try_from([0; 15].as_slice()).is_err());
    assert!(SecretKey::try_from([0; 16].as_slice()).is_ok());
    assert!(SecretKey::try_from([0; 23].as_slice()).is_err());
    assert!(SecretKey::try_from([0; 24].as_slice()).is_ok());
    assert!(SecretKey::try_from([0; 31].as_slice()).is_err());
    assert!(SecretKey::try_from([0; 32].as_slice()).is_ok());
  }
}
