use super::{DecodeError, EncodeError, MessageType};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum InvalidLabel {
  #[error("the size of label must between [0-255] bytes, got {0}")]
  InvalidSize(usize),
  #[error("{0}")]
  Utf8(#[from] core::str::Utf8Error),
}

#[inline]
fn make_label_header(label: &[u8], src: &[u8]) -> Bytes {
  let mut dst = BytesMut::with_capacity(2 + src.len() + label.len());
  dst.put_u8(MessageType::HasLabel as u8);
  dst.put_u8(label.len() as u8);
  dst.put_slice(label);
  dst.put_slice(src);
  dst.freeze()
}

#[derive(Clone)]
pub struct Label(Bytes);

impl Label {
  #[inline]
  pub(crate) const fn empty() -> Label {
    Label(Bytes::new())
  }

  /// Rrefixes outgoing packets with the correct header if
  /// the label is not empty.
  pub fn add_label_header_to_packet(src: &[u8], label: &[u8]) -> Result<Bytes, EncodeError> {
    if !label.is_empty() {
      if label.len() > Self::MAX_SIZE {
        return Err(EncodeError::InvalidLabel(InvalidLabel::InvalidSize(
          label.len(),
        )));
      }
      Ok(make_label_header(label, src))
    } else {
      Ok(Bytes::copy_from_slice(src))
    }
  }

  pub fn remove_label_header_from(mut buf: BytesMut) -> Result<(BytesMut, Label), DecodeError> {
    #[allow(clippy::declare_interior_mutable_const)]
    const EMPTY_LABEL: Label = Label::empty();

    if buf.is_empty() {
      return Ok((buf, EMPTY_LABEL));
    }

    if buf[0] != MessageType::HasLabel as u8 {
      return Ok((buf, EMPTY_LABEL));
    }

    if buf.len() < 2 {
      return Err(DecodeError::Truncated("label"));
    }

    let label_size = buf[1] as usize;
    if label_size < 1 {
      return Err(DecodeError::InvalidLabel(InvalidLabel::InvalidSize(0)));
    }

    if buf.len() < 2 + label_size {
      return Err(DecodeError::Truncated("label"));
    }

    buf.advance(2);
    let label = buf.split_to(label_size);
    Label::from_bytes(label.freeze())
      .map(|label| (buf, label))
      .map_err(From::from)
  }

  pub fn remove_label_header_from_packet(mut buf: Bytes) -> Result<(Bytes, Bytes), DecodeError> {
    #[allow(clippy::declare_interior_mutable_const)]
    const EMPTY_BYTES: Bytes = Bytes::new();

    if buf.is_empty() {
      return Ok((buf, EMPTY_BYTES));
    }

    if buf[0] != MessageType::HasLabel as u8 {
      return Ok((buf, EMPTY_BYTES));
    }

    if buf.len() < 2 {
      return Err(DecodeError::Truncated("label"));
    }

    let label_size = buf[1] as usize;
    if label_size < 1 {
      return Err(DecodeError::InvalidLabel(InvalidLabel::InvalidSize(0)));
    }

    if buf.len() < 2 + label_size {
      return Err(DecodeError::Truncated("label"));
    }

    buf.advance(2);
    let label = buf.split_to(label_size);
    Ok((buf, label))
  }

  #[inline]
  pub(crate) fn label_overhead(&self) -> usize {
    if self.is_empty() {
      0
    } else {
      2 + self.len()
    }
  }

  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = u8::MAX as usize;

  #[inline]
  pub const fn from_static(s: &'static str) -> Self {
    Self(Bytes::from_static(s.as_bytes()))
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
  pub(crate) fn from_bytes(s: Bytes) -> Result<Self, InvalidLabel> {
    if s.len() > Self::MAX_SIZE || s.is_empty() {
      return Err(InvalidLabel::InvalidSize(s.len()));
    }
    match core::str::from_utf8(&s) {
      Ok(_) => Ok(Self(s)),
      Err(e) => Err(e.into()),
    }
  }
}

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

impl AsRef<str> for Label {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Label {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    self.as_str().partial_cmp(other.as_str())
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

impl Label {
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  #[inline]
  pub fn as_str(&self) -> &str {
    core::str::from_utf8(&self.0).unwrap()
  }
}

impl TryFrom<&str> for Label {
  type Error = InvalidLabel;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE || s.is_empty() {
      return Err(InvalidLabel::InvalidSize(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl TryFrom<String> for Label {
  type Error = InvalidLabel;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE || s.is_empty() {
      return Err(InvalidLabel::InvalidSize(s.len()));
    }
    Ok(Self(s.into()))
  }
}

impl TryFrom<Bytes> for Label {
  type Error = InvalidLabel;

  fn try_from(s: Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE || s.is_empty() {
      return Err(InvalidLabel::InvalidSize(s.len()));
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
    if s.len() > Self::MAX_SIZE || s.is_empty() {
      return Err(InvalidLabel::InvalidSize(s.len()));
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
