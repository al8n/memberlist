use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Invalid label error.
#[derive(Debug, thiserror::Error)]
pub enum InvalidLabel {
  /// The label is too large.
  #[error("the size of label must between [0-255] bytes, got {0}")]
  TooLarge(usize),
  /// The label is not valid utf8.
  #[error("{0}")]
  Utf8(#[from] core::str::Utf8Error),
}

/// General approach is to prefix all packets and streams with the same structure:
///
/// Encode:
/// ```text
///   magic type byte (244): u8
///   length of label name:  u8 (because labels can't be longer than 253 bytes)
///   label name:            bytes (max 253 bytes)
/// ```
#[derive(Clone)]
pub struct Label(Bytes);

impl Label {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = u8::MAX as usize - 2;

  /// Create an empty label.
  #[inline]
  pub const fn empty() -> Label {
    Label(Bytes::new())
  }

  /// The size of the label in bytes.
  #[inline]
  pub fn label_overhead(&self) -> usize {
    if self.is_empty() {
      0
    } else {
      2 + self.len()
    }
  }

  /// Create a label from a static str.
  #[inline]
  pub const fn from_static(s: &'static str) -> Self {
    Self(Bytes::from_static(s.as_bytes()))
  }

  /// Returns the label as a byte slice.
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  /// Returns the str of the label.
  #[inline]
  pub fn as_str(&self) -> &str {
    core::str::from_utf8(&self.0).unwrap()
  }

  /// Returns true if the label is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns the length of the label in bytes.
  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use serde::{Deserialize, Serialize};

  impl Serialize for Label {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
      if serializer.is_human_readable() {
        serializer.serialize_str(self.as_str())
      } else {
        serializer.serialize_bytes(self.as_bytes())
      }
    }
  }

  impl<'de> Deserialize<'de> for Label {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: serde::Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        String::deserialize(deserializer)
          .and_then(|n| Label::try_from(n).map_err(serde::de::Error::custom))
      } else {
        Bytes::deserialize(deserializer)
          .and_then(|n| Label::try_from(n).map_err(serde::de::Error::custom))
      }
    }
  }
};

impl AsRef<str> for Label {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Label {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl core::cmp::Ord for Label {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl core::cmp::PartialEq for Label {
  fn eq(&self, other: &Self) -> bool {
    self.as_str() == other.as_str()
  }
}

impl core::cmp::PartialEq<str> for Label {
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&str> for Label {
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::PartialEq<String> for Label {
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&String> for Label {
  fn eq(&self, other: &&String) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::Eq for Label {}

impl core::hash::Hash for Label {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_str().hash(state)
  }
}

impl TryFrom<&str> for Label {
  type Error = InvalidLabel;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidLabel::TooLarge(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl TryFrom<String> for Label {
  type Error = InvalidLabel;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidLabel::TooLarge(s.len()));
    }
    Ok(Self(s.into()))
  }
}

impl TryFrom<Bytes> for Label {
  type Error = InvalidLabel;

  fn try_from(s: Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidLabel::TooLarge(s.len()));
    }
    match core::str::from_utf8(s.as_ref()) {
      Ok(_) => Ok(Self(s)),
      Err(e) => Err(InvalidLabel::Utf8(e)),
    }
  }
}

impl TryFrom<&Bytes> for Label {
  type Error = InvalidLabel;

  fn try_from(s: &Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidLabel::TooLarge(s.len()));
    }
    match core::str::from_utf8(s.as_ref()) {
      Ok(_) => Ok(Self(s.clone())),
      Err(e) => Err(InvalidLabel::Utf8(e)),
    }
  }
}

impl core::fmt::Debug for Label {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl core::fmt::Display for Label {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

pub(crate) trait LabelAsyncIOExt: AsyncWrite + AsyncRead {
  fn add_label_header(&mut self, label: &Label) -> impl Future<Output = io::Result<()>> + Send {
    async move {
      let mut buf = BytesMut::with_capacity(2 + label.len());
      buf.put_u8(LABEL_TAG);
      buf.put_u8(label.len() as u8);
      buf.put_slice(label.as_bytes());

      self.write_all(&buf).await
    }
  }

  fn remove_label_header(&mut self) -> impl Future<Output = io::Result<Option<Label>>> + Send {
    async move {
      let mut buf = [0u8; 1];
      self.read_exact(&mut buf).await?;

      let len = buf[0] as usize;
      let mut buf = vec![0u8; len];
      self.read_exact(&mut buf).await?;
      let buf: Bytes = buf.into();
      Label::try_from(buf).map(Some).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
  }
}

impl<T: AsyncWrite + AsyncRead> LabelAsyncIOExt for T {}



/// Read and remove a label from the given bytes.
pub fn remove_label_from_bytes(buf: &mut Bytes) -> std::io::Result<Option<Label>> {
  if buf.is_empty() {
    return Ok(None);
  }

  let len = buf[0] as usize;
  if len > buf.len() {
    return Err(std::io::Error::new(
      std::io::ErrorKind::InvalidData,
      "not enough bytes to read label",
    ));
  }
  buf.advance(1);
  let label = buf.split_to(len);
  Label::try_from(label).map(Some).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
