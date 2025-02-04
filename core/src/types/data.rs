use std::borrow::Cow;

use length_delimited::InsufficientBuffer;

use super::WireType;

/// The memberlist data can be transmitted through the network.
pub trait Data: Send + Sync + 'static {
  /// The wire type of the data.
  const WIRE_TYPE: WireType = WireType::LengthDelimited;

  /// Returns the encoded length of the data only considering the data itself, (e.g. no length prefix, no wire type).
  fn encoded_len(&self) -> usize;

  /// Encodes the data to a buffer without the wire type and length prefix.
  ///
  /// An error will be returned if the buffer does not have sufficient capacity.
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>;

  /// Decodes an instance of the message from the `src`, the `src` should not contain the wire type or length prefix.
  fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized;
}

/// A data encoding error
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
  /// Returned when the encoded buffer is too small to hold the bytes format of the types.
  #[error(transparent)]
  InsufficientBuffer(#[from] InsufficientBuffer),
  /// Returned when the data in encoded format is larger than the maximum allowed size.
  #[error("encoded data is too large, the maximum allowed size is {MAX} bytes", MAX = u32::MAX)]
  TooLarge,
  /// A custom encoding error.
  #[error("{0}")]
  Custom(Cow<'static, str>),
}

impl EncodeError {
  /// Creates a custom encoding error.
  pub fn custom<T>(value: T) -> Self
  where
    T: Into<Cow<'static, str>>,
  {
    Self::Custom(value.into())
  }

  #[inline]
  pub(crate) fn with_information(self, required: u64, available: u64) -> Self {
    match self {
      Self::InsufficientBuffer(_) => {
        Self::InsufficientBuffer(InsufficientBuffer::with_information(required, available))
      }
      e => e,
    }
  }
}

impl From<length_delimited::EncodeError> for EncodeError {
  #[inline]
  fn from(value: length_delimited::EncodeError) -> Self {
    match value {
      length_delimited::EncodeError::Underflow => {
        Self::InsufficientBuffer(InsufficientBuffer::new())
      }
    }
  }
}

impl From<Cow<'static, str>> for EncodeError {
  fn from(value: Cow<'static, str>) -> Self {
    Self::Custom(value)
  }
}

/// A message decoding error.
///
/// `DecodeError` indicates that the input buffer does not contain a valid
/// Protobuf message. The error details should be considered 'best effort': in
/// general it is not possible to exactly pinpoint why data is malformed.
#[derive(Clone, PartialEq, Eq)]
pub struct DecodeError {
  inner: Box<Inner>,
}

#[derive(Clone, PartialEq, Eq)]
struct Inner {
  /// A 'best effort' root cause description.
  description: Cow<'static, str>,
  /// A stack of (message, field) name pairs, which identify the specific
  /// message type and field where decoding failed. The stack contains an
  /// entry per level of nesting.
  stack: Vec<(&'static str, &'static str)>,
}

impl DecodeError {
  /// Creates a new `DecodeError` with a 'best effort' root cause description.
  ///
  /// Meant to be used only by `Message` implementations.
  #[doc(hidden)]
  #[cold]
  pub fn new(description: impl Into<Cow<'static, str>>) -> Self {
    Self {
      inner: Box::new(Inner {
        description: description.into(),
        stack: Vec::new(),
      }),
    }
  }

  /// Pushes a (message, field) name location pair on to the location stack.
  ///
  /// Meant to be used only by `Message` implementations.
  #[doc(hidden)]
  pub fn push(&mut self, message: &'static str, field: &'static str) {
    self.inner.stack.push((message, field));
  }
}

impl core::fmt::Debug for DecodeError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("DecodeError")
      .field("description", &self.inner.description)
      .field("stack", &self.inner.stack)
      .finish()
  }
}

impl core::fmt::Display for DecodeError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("failed to decode memberlist message: ")?;
    for &(message, field) in &self.inner.stack {
      write!(f, "{}.{}: ", message, field)?;
    }
    f.write_str(&self.inner.description)
  }
}

impl core::error::Error for DecodeError {}
