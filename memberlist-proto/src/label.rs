use nodecraft::CheapClone;
use smol_str::SmolStr;

use super::{Data, DataRef, DecodeError, EncodeError};

/// Parse label error.
#[derive(Debug, thiserror::Error)]
pub enum ParseLabelError {
  /// The label is too large.
  #[error("the size of label must between [0-253] bytes, got {0}")]
  TooLarge(usize),
  /// The label is not valid utf8.
  #[error(transparent)]
  Utf8(#[from] core::str::Utf8Error),
}

/// General approach is to prefix all packets and streams with the same structure:
///
/// Encode:
/// ```text
///   magic type byte (127): u8
///   length of label name:  u8 (because labels can't be longer than 253 bytes)
///   label name:            bytes (max 253 bytes)
/// ```
#[derive(Clone, Default, derive_more::Display)]
#[display("{_0}")]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct Label(pub(crate) SmolStr);

impl CheapClone for Label {}

impl Label {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = u8::MAX as usize - 2;

  /// An empty label.
  pub const EMPTY: &Label = &Label(SmolStr::new_inline(""));

  /// Create an empty label.
  #[inline]
  pub const fn empty() -> Label {
    Label(SmolStr::new_inline(""))
  }

  /// The encoded overhead of a label.
  #[inline]
  pub fn encoded_overhead(&self) -> usize {
    if self.is_empty() { 0 } else { 2 + self.len() }
  }

  /// Create a label from a static str.
  #[inline]
  pub fn from_static(s: &'static str) -> Result<Self, ParseLabelError> {
    Self::try_from(s)
  }

  /// Returns the label as a byte slice.
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    self.0.as_bytes()
  }

  /// Returns the str of the label.
  #[inline]
  pub fn as_str(&self) -> &str {
    self.0.as_str()
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

impl AsRef<str> for Label {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::borrow::Borrow<str> for Label {
  fn borrow(&self) -> &str {
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

impl core::str::FromStr for Label {
  type Err = ParseLabelError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    s.try_into()
  }
}

impl TryFrom<&str> for Label {
  type Error = ParseLabelError;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(ParseLabelError::TooLarge(s.len()));
    }
    Ok(Self(SmolStr::new(s)))
  }
}

impl TryFrom<&String> for Label {
  type Error = ParseLabelError;

  fn try_from(s: &String) -> Result<Self, Self::Error> {
    s.as_str().try_into()
  }
}

impl TryFrom<String> for Label {
  type Error = ParseLabelError;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(ParseLabelError::TooLarge(s.len()));
    }
    Ok(Self(s.into()))
  }
}

impl TryFrom<Vec<u8>> for Label {
  type Error = ParseLabelError;

  fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
    String::from_utf8(s)
      .map_err(|e| e.utf8_error().into())
      .and_then(Self::try_from)
  }
}

impl TryFrom<&[u8]> for Label {
  type Error = ParseLabelError;

  fn try_from(s: &[u8]) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(ParseLabelError::TooLarge(s.len()));
    }
    match core::str::from_utf8(s) {
      Ok(s) => Ok(Self(SmolStr::new(s))),
      Err(e) => Err(ParseLabelError::Utf8(e)),
    }
  }
}

impl core::fmt::Debug for Label {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

/// Label error.
#[derive(Debug, thiserror::Error)]
pub enum LabelError {
  /// Invalid label.
  #[error(transparent)]
  ParseLabelError(#[from] ParseLabelError),
  /// Not enough data to decode label.
  #[error("not enough data to decode label")]
  BufferUnderflow,
  /// Label mismatch.
  #[error("label mismatch: expected {expected}, got {got}")]
  LabelMismatch {
    /// Expected label.
    expected: Label,
    /// Got label.
    got: Label,
  },

  /// Unexpected double label header
  #[error(
    "unexpected double label header, inbound label check is disabled, but got double label header: local={local}, remote={remote}"
  )]
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

impl<'a> DataRef<'a, Label> for &'a str {
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let len = buf.len();
    if len > Label::MAX_SIZE {
      return Err(DecodeError::custom(
        ParseLabelError::TooLarge(len).to_string(),
      ));
    }

    Ok((
      len,
      core::str::from_utf8(buf).map_err(|e| DecodeError::custom(e.to_string()))?,
    ))
  }
}

impl Data for Label {
  type Ref<'a> = &'a str;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(Self(SmolStr::new(val)))
  }

  fn encoded_len(&self) -> usize {
    self.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let len = self.len();
    if len > buf.len() {
      return Err(EncodeError::insufficient_buffer(len, buf.len()));
    }
    buf[..len].copy_from_slice(self.as_bytes());
    Ok(len)
  }
}

#[cfg(test)]
mod tests {
  use core::hash::{Hash, Hasher};

  use super::*;

  #[test]
  fn test_try_from_string() {
    let label = Label::try_from("hello".to_string()).unwrap();
    assert_eq!(label, "hello");

    assert!(Label::try_from("a".repeat(256)).is_err());
  }

  #[test]
  fn test_debug_and_hash() {
    let label = Label::from_static("hello").unwrap();
    assert_eq!(format!("{:?}", label), "hello");

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    label.hash(&mut hasher);
    let h1 = hasher.finish();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    "hello".hash(&mut hasher);
    let h2 = hasher.finish();
    assert_eq!(h1, h2);
    assert_eq!(label.as_ref(), "hello");
  }
}
