use std::str::FromStr;

use base64::{engine::general_purpose::STANDARD as b64, Engine as _};

/// Parse error for [`SecretKey`] from bytes slice
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("invalid key length({0}) - must be 16, 24, or 32 bytes for AES-128/192/256")]
pub struct InvalidKeyLength(usize);

/// Parse error for [`SecretKey`] from a base64 string
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseSecretKeyError {
  /// Invalid base64 string
  #[cfg_attr(feature = "std", error(transparent))]
  #[cfg_attr(not(feature = "std"), error("{0}"))]
  Base64(#[cfg_attr(feature = "std", from)] base64::DecodeError),
  /// Invalid key length
  #[error(transparent)]
  InvalidKeyLength(#[from] InvalidKeyLength),
}

#[cfg(not(feature = "std"))]
impl From<base64::DecodeError> for ParseSecretKeyError {
  fn from(e: base64::DecodeError) -> Self {
    Self::InvalidBase64(e)
  }
}

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

impl SecretKey {
  /// Returns the base64 encoded secret key
  pub fn to_base64(&self) -> String {
    b64.encode(self)
  }
}

impl TryFrom<&str> for SecretKey {
  type Error = ParseSecretKeyError;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    s.parse()
  }
}

impl FromStr for SecretKey {
  type Err = ParseSecretKeyError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let bytes = b64.decode(s)?;

    match bytes.len() {
      16 => {
        let mut key = [0u8; 16];
        key.copy_from_slice(&bytes);
        Ok(SecretKey::Aes128(key))
      }
      24 => {
        let mut key = [0u8; 24];
        key.copy_from_slice(&bytes);
        Ok(SecretKey::Aes192(key))
      }
      32 => {
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(SecretKey::Aes256(key))
      }
      v => Err(ParseSecretKeyError::InvalidKeyLength(InvalidKeyLength(v))),
    }
  }
}

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
  type Error = InvalidKeyLength;

  fn try_from(k: &[u8]) -> Result<Self, Self::Error> {
    match k.len() {
      16 => Ok(Self::Aes128(k.try_into().unwrap())),
      24 => Ok(Self::Aes192(k.try_into().unwrap())),
      32 => Ok(Self::Aes256(k.try_into().unwrap())),
      val => Err(InvalidKeyLength(val)),
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
  // #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  // #[cfg_attr(feature = "serde", serde(transparent))]
  pub SecretKeys([SecretKey; 3]);
);

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
            val => Err(<D::Error as serde::de::Error>::custom(InvalidKeyLength(
              val,
            ))),
          }
        }};
      }

      if deserializer.is_human_readable() {
        <&str as Deserialize<'de>>::deserialize(deserializer)
          .and_then(|val| Self::try_from(val).map_err(<D::Error as serde::de::Error>::custom))
      } else {
        <&[u8] as Deserialize<'de>>::deserialize(deserializer).and_then(|val| parse!(val))
      }
    }
  }
};

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a> Arbitrary<'a> for SecretKey {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      match u.int_in_range(0..=2)? {
        0 => Ok(SecretKey::Aes128(u.arbitrary()?)),
        1 => Ok(SecretKey::Aes192(u.arbitrary()?)),
        2 => Ok(SecretKey::Aes256(u.arbitrary()?)),
        _ => unreachable!(),
      }
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for SecretKey {
    fn arbitrary(g: &mut Gen) -> Self {
      macro_rules! gen {
        ($lit:literal) => {{
          let mut buf = [0; $lit];
          for i in 0..$lit {
            buf[i] = u8::arbitrary(g);
          }
          buf
        }};
      }

      match u8::arbitrary(g) % 3 {
        0 => SecretKey::Aes128(gen!(16)),
        1 => SecretKey::Aes192(gen!(24)),
        2 => SecretKey::Aes256(gen!(32)),
        _ => unreachable!(),
      }
    }
  }
};
