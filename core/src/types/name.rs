use std::io::{self, Error, ErrorKind};

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use super::DecodeError;

#[derive(Debug, thiserror::Error)]
pub enum InvalidName {
  #[error("name too large, max length of name is 65536 but got {0}")]
  TooLarge(usize),
  #[error("{0}")]
  Utf8(#[from] core::str::Utf8Error),
}

#[derive(Clone)]
pub struct Name(SmolStr);

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
    Self(SmolStr::new(""))
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
  pub(crate) fn from_bytes(s: &[u8]) -> Result<Self, InvalidName> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    match core::str::from_utf8(s) {
      Ok(s) => Ok(Self(s.into())),
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  pub(crate) fn from_array<const N: usize>(s: [u8; N]) -> Result<Self, InvalidName> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    match core::str::from_utf8(&s) {
      Ok(s) => Ok(Self(SmolStr::new_inline(s))),
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    core::mem::size_of::<u16>() + self.0.len()
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u16(self.0.len() as u16);
    buf.put(self.0.as_bytes());
  }

  #[inline]
  pub(crate) fn decode_from(buf: &mut impl Buf) -> Result<Self, DecodeError> {
    let len = buf.get_u16() as usize;
    if len == 0 {
      return Ok(Self::new());
    }

    if len > Self::MAX_SIZE {
      return Err(DecodeError::InvalidName(InvalidName::TooLarge(len)));
    }
    if len > buf.remaining() {
      return Err(DecodeError::Truncated("name"));
    }

    let s = &buf.chunk()[..len];
    Self::from_bytes(s).map_err(From::from)
  }

  #[cfg(feature = "async")]
  #[inline]
  pub(crate) async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> io::Result<Self> {
    use futures_util::io::AsyncReadExt;

    let mut len_buf = [0; 2];
    r.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 {
      return Ok(Self::new());
    }

    if len <= 23 {
      map_inlined!(match Self.len() {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18,
        19, 20, 21, 22, 23
      } => r)
    } else {
      let mut buf = vec![0; len];
      r.read_exact(&mut buf).await?;
      Self::from_bytes(&buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }
  }
}

impl Serialize for Name {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(self.as_str())
  }
}

impl<'de> Deserialize<'de> for Name {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    SmolStr::deserialize(deserializer)
      .and_then(|n| Name::try_from(n).map_err(|e| serde::de::Error::custom(e)))
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
  pub fn as_bytes(&self) -> &[u8] {
    self.0.as_bytes()
  }

  pub fn as_str(&self) -> &str {
    self.0.as_str()
  }
}

impl TryFrom<&str> for Name {
  type Error = InvalidName;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s.into()))
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

impl TryFrom<SmolStr> for Name {
  type Error = InvalidName;

  fn try_from(s: SmolStr) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s))
  }
}

impl TryFrom<&SmolStr> for Name {
  type Error = InvalidName;

  fn try_from(s: &SmolStr) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidName::TooLarge(s.len()));
    }
    Ok(Self(s.clone()))
  }
}

impl core::fmt::Debug for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl core::fmt::Display for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}
