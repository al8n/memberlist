use core::marker::PhantomData;

use bytes::{Buf, BufMut, Bytes};
use nodecraft::CheapClone;
use smol_str::SmolStr;

use super::{merge, skip, split, Data, DataRef, DecodeError, EncodeError, WireType};

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
///   magic type byte (244): u8
///   length of label name:  u8 (because labels can't be longer than 253 bytes)
///   label name:            bytes (max 253 bytes)
/// ```
#[derive(Clone, derive_more::Display)]
#[display("{_0}")]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct Label(pub(crate) SmolStr);

impl CheapClone for Label {}

impl Label {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = u8::MAX as usize - 2;

  /// The tag for a label when encoding/decoding.
  pub const TAG: u8 = 244;

  /// Create an empty label.
  #[inline]
  pub const fn empty() -> Label {
    Label(SmolStr::new_inline(""))
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
pub trait LabelBufExt: Buf + sealed::Splitable + TryInto<Label, Error = ParseLabelError> {
  /// Remove the label prefix from the buffer.
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
      return Err(LabelError::BufferUnderflow);
    }
    let label = self.split_to(len);
    Self::try_into(label).map(Some).map_err(Into::into)
  }
}

impl<T: Buf + sealed::Splitable + TryInto<Label, Error = ParseLabelError>> LabelBufExt for T {}

/// Label extension for [`BufMut`] types.
pub trait LabelBufMutExt: BufMut {
  /// Add label prefix to the buffer.
  fn add_label_header(&mut self, label: &Label) {
    if label.is_empty() {
      return;
    }
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

/// A [`Message`](super::Message) with a [`Label`].
#[viewit::viewit(
  vis_all = "",
  setters(prefix = "with", style = "move"),
  getters(style = "move")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LabeledMessage<I, A> {
  /// The label of the message.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(doc = "Returns the label of the message.", inline,)
    ),
    setter(attrs(doc = "Sets the label of the message.", inline,))
  )]
  label: Label,
  /// The message.
  #[viewit(
    getter(skip),
    setter(attrs(doc = "Sets the payload of the message.", inline,))
  )]
  #[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary_impl::bytes))]
  payload: Bytes,
  #[viewit(getter(skip), setter(skip))]
  _m: PhantomData<(I, A)>,
}

const LABEL_TAG: u8 = 1;
const MESSAGE_TAG: u8 = 2;

impl<I, A> LabeledMessage<I, A> {
  /// Creates a new `CompressedMessage`.
  #[inline]
  pub const fn new(label: Label, payload: Bytes) -> Self {
    Self {
      label,
      payload,
      _m: PhantomData,
    }
  }

  #[inline]
  const fn payload_byte() -> u8 {
    merge(WireType::LengthDelimited, MESSAGE_TAG)
  }

  #[inline]
  const fn label_byte() -> u8 {
    merge(WireType::LengthDelimited, LABEL_TAG)
  }
}

/// A reference type for `LabeledMessage`.
#[viewit::viewit(vis_all = "", setters(skip), getters(style = "move"))]
#[derive(Debug, Clone, Copy)]
pub struct LabeledMessageRef<'a, I, A> {
  /// The algorithm used to compression the message.
  #[viewit(getter(const, attrs(doc = "Returns the label of the message.", inline,)))]
  label: &'a str,
  /// The message.
  #[viewit(getter(const, attrs(doc = "Returns the payload of the message.", inline,)))]
  payload: &'a [u8],
  #[viewit(getter(skip))]
  _m: PhantomData<(I, A)>,
}

impl<'a, I, A> LabeledMessageRef<'a, I, A> {
  /// Creates a new `LabeledMessageRef`.
  #[inline]
  pub(crate) const fn new(label: &'a str, payload: &'a [u8]) -> Self {
    Self {
      label,
      payload,
      _m: PhantomData,
    }
  }
}

impl<I, A> Data for LabeledMessage<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = LabeledMessageRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(LabeledMessage::new(
      Label(SmolStr::new(val.label)),
      Bytes::copy_from_slice(val.payload),
    ))
  }

  fn encoded_len(&self) -> usize {
    1 + self.label.encoded_len_with_length_delimited()
      + 1
      + self.payload.encoded_len_with_length_delimited()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let buf_len = buf.len();
    let mut offset = 0;
    if buf_len == 0 {
      return Err(EncodeError::insufficient_buffer(
        self.encoded_len(),
        buf_len,
      ));
    }

    buf[offset] = Self::label_byte();
    offset += 1;
    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }
    offset += self.label.encode_length_delimited(&mut buf[offset..])?;
    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }

    buf[offset] = Self::payload_byte();
    offset += 1;
    offset += self
      .payload
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), buf_len))?;

    Ok(offset)
  }
}

impl<'a, I, A> DataRef<'a, LabeledMessage<I, A>> for LabeledMessageRef<'a, I::Ref<'a>, A::Ref<'a>>
where
  I: Data,
  A: Data,
{
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let buf_len = buf.len();

    if buf_len == 0 {
      return Err(DecodeError::buffer_underflow());
    }

    let mut message = None;
    let mut label = None;

    while offset < buf_len {
      let b = buf[offset];
      offset += 1;

      match b {
        b if b == LabeledMessage::<I, A>::label_byte() => {
          if label.is_some() {
            return Err(DecodeError::duplicate_field(
              "LabeledMessage",
              "label",
              LABEL_TAG,
            ));
          }
          let (bytes_read, val) =
            <&str as DataRef<Label>>::decode_length_delimited(&buf[offset..])?;
          offset += bytes_read;
          label = Some(val);
        }
        b if b == LabeledMessage::<I, A>::payload_byte() => {
          if message.is_some() {
            return Err(DecodeError::duplicate_field(
              "LabeledMessage",
              "payload",
              MESSAGE_TAG,
            ));
          }

          let (bytes_read, payload) =
            <&[u8] as DataRef<Bytes>>::decode_length_delimited(&buf[offset..])?;
          offset += bytes_read;
          message = Some(payload);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
          offset += skip(wire_type, &buf[offset..])?;
        }
      }
    }

    let message = message.ok_or(DecodeError::missing_field("LabeledMessage", "payload"))?;
    let label = label.ok_or(DecodeError::missing_field("LabeledMessage", "label"))?;
    Ok((offset, LabeledMessageRef::new(label, message)))
  }
}

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for Label {
    fn arbitrary(g: &mut Gen) -> Self {
      let mut s = String::new();
      while s.len() < 253 {
        let c = char::arbitrary(g);
        let char_len = c.len_utf8();

        if s.len() + char_len > 253 {
          break;
        }
        s.push(c);
      }

      Label(s.into())
    }
  }

  impl<I, A> Arbitrary for LabeledMessage<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::new(
        Arbitrary::arbitrary(g),
        <Vec<u8> as Arbitrary>::arbitrary(g).into(),
      )
    }
  }
};

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
