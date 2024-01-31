use bytes::{Buf, BufMut, Bytes, BytesMut};
use cheap_clone::CheapClone;

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

impl CheapClone for Label {}

impl Label {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = u8::MAX as usize - 2;

  /// The tag for a label when encoding/decoding.
  pub const TAG: u8 = 127;

  /// Create an empty label.
  #[inline]
  pub const fn empty() -> Label {
    Label(Bytes::new())
  }

  /// The encoded overhead of a label.
  #[inline]
  pub fn encoded_overhead(&self) -> usize {
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

impl TryFrom<BytesMut> for Label {
  type Error = InvalidLabel;

  fn try_from(s: BytesMut) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(InvalidLabel::TooLarge(s.len()));
    }
    match core::str::from_utf8(s.as_ref()) {
      Ok(_) => Ok(Self(s.freeze())),
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

/// Label error.
#[derive(Debug, thiserror::Error)]
pub enum LabelError {
  /// Invalid label.
  #[error("{0}")]
  InvalidLabel(#[from] InvalidLabel),
  /// Not enough bytes to decode label.
  #[error("not enough bytes to decode label")]
  NotEnoughBytes,
  /// Label mismatch.
  #[error("label mismatch: expected {expected}, got {got}")]
  LabelMismatch {
    /// Expected label.
    expected: Label,
    /// Got label.
    got: Label,
  },

  /// Unexpected double label header
  #[error("unexpected double label header, inbound label check is disabled, but got double label header: local={local}, remote={remote}")]
  Duplicate {
    /// The local label.
    local: Label,
    /// The remote label.
    remote: Label,
  },
}

impl LabelError {
  /// Creates a new `LabelError::LabelMismatch`.
  pub fn mismatch(expected: Label, got: Label) -> Self {
    Self::LabelMismatch { expected, got }
  }

  /// Creates a new `LabelError::Duplicate`.
  pub fn duplicate(local: Label, remote: Label) -> Self {
    Self::Duplicate { local, remote }
  }
}

/// Label extension for [`Buf`] types.
pub trait LabelBufExt: Buf + sealed::Splitable + TryInto<Label, Error = InvalidLabel> {
  fn remove_label_header(&mut self) -> Result<Option<Label>, LabelError>
  where
    Self: Sized,
  {
    if self.remaining() < 1 {
      return Ok(None);
    }

    let data = self.chunk();
    if data[0] != Label::TAG {
      return Ok(None);
    }
    self.advance(1);
    let len = self.get_u8() as usize;
    if len > self.remaining() {
      return Err(LabelError::NotEnoughBytes);
    }
    let label = self.split_to(len);
    Self::try_into(label).map(Some).map_err(Into::into)
  }
}

impl<T: Buf + sealed::Splitable + TryInto<Label, Error = InvalidLabel>> LabelBufExt for T {}

/// Label extension for [`BufMut`] types.
pub trait LabelBufMutExt: BufMut {
  fn add_label_header(&mut self, label: &Label) {
    self.put_u8(Label::TAG);
    self.put_u8(label.len() as u8);
    self.put_slice(label.as_bytes());
  }
}

impl<T: BufMut> LabelBufMutExt for T {}

mod sealed {
  use bytes::{Bytes, BytesMut};

  pub trait Splitable {
    fn split_to(&mut self, len: usize) -> Self;
  }

  impl Splitable for BytesMut {
    fn split_to(&mut self, len: usize) -> Self {
      self.split_to(len)
    }
  }

  impl Splitable for Bytes {
    fn split_to(&mut self, len: usize) -> Self {
      self.split_to(len)
    }
  }
}
