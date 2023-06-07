use super::DecodeError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum InvalidName {
  #[error("name too large, max length of name is 65536 but got {0}")]
  TooLarge(usize),
  #[error("{0}")]
  Utf8(#[from] core::str::Utf8Error),
}

#[derive(Clone)]
pub struct Name(Bytes);

impl Default for Name {
  fn default() -> Self {
    Self::new()
  }
}

impl Name {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = u16::MAX as usize;

  #[inline]
  pub fn new() -> Self {
    Self(Bytes::new())
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    core::mem::size_of::<u16>() + self.0.len()
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u16(self.0.len() as u16);
    buf.put(self.0.as_ref());
  }

  #[inline]
  pub(crate) fn decode_len(mut buf: impl Buf) -> Result<usize, DecodeError> {
    if buf.remaining() < core::mem::size_of::<u16>() {
      return Err(DecodeError::Corrupted);
    }
    Ok(buf.get_u16() as usize)
  }

  #[inline]
  pub(crate) fn decode_from(buf: Bytes) -> Result<Self, DecodeError> {
    if buf.remaining() == 0 {
      return Ok(Self::new());
    }
    match core::str::from_utf8(buf.as_ref()) {
      Ok(_) => Ok(Self(buf)),
      Err(e) => Err(DecodeError::InvalidName(InvalidName::Utf8(e))),
    }
  }
}

impl Serialize for Name {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
      serializer.serialize_str(self.as_str())
    } else {
      serializer.serialize_bytes(self.as_bytes())
    }
  }
}

impl<'de> Deserialize<'de> for Name {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    if deserializer.is_human_readable() {
      String::deserialize(deserializer)
        .and_then(|n| Name::try_from(n).map_err(serde::de::Error::custom))
    } else {
      Bytes::deserialize(deserializer)
        .and_then(|n| Name::try_from(n).map_err(serde::de::Error::custom))
    }
  }
}

impl AsRef<str> for Name {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Name {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    self.as_str().partial_cmp(other.as_str())
  }
}

impl core::cmp::Ord for Name {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl core::cmp::PartialEq for Name {
  fn eq(&self, other: &Self) -> bool {
    self.as_str() == other.as_str()
  }
}

impl core::cmp::PartialEq<str> for Name {
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&str> for Name {
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::PartialEq<String> for Name {
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&String> for Name {
  fn eq(&self, other: &&String) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::Eq for Name {}

impl core::hash::Hash for Name {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_str().hash(state)
  }
}

impl Name {
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  #[inline]
  pub fn as_str(&self) -> &str {
    core::str::from_utf8(&self.0).unwrap()
  }
}

impl TryFrom<&str> for Name {
  type Error = InvalidName;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl TryFrom<String> for Name {
  type Error = InvalidName;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s.into()))
  }
}

impl TryFrom<Bytes> for Name {
  type Error = InvalidName;

  fn try_from(s: Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s))
  }
}

impl TryFrom<&Bytes> for Name {
  type Error = InvalidName;

  fn try_from(s: &Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s.clone()))
  }
}

impl core::fmt::Debug for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl core::fmt::Display for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}
