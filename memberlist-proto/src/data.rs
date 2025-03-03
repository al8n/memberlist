use std::borrow::Cow;

use const_varint::{decode_u32_varint, encode_u32_varint_to, encoded_u32_varint_len};

use super::WireType;

pub use tuple::TupleEncoder;

#[cfg(any(feature = "std", feature = "alloc"))]
mod bytes;
#[cfg(any(feature = "std", feature = "alloc"))]
mod nodecraft;
mod primitives;
#[cfg(any(feature = "std", feature = "alloc"))]
mod string;

mod tuple;

/// The reference type of the data.
pub trait DataRef<'a, D>
where
  D: Data + ?Sized,
  Self: Copy + core::fmt::Debug + Send + Sync,
{
  /// Decodes the reference type from a buffer.
  ///
  /// The entire buffer will be consumed.
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized;

  /// Decodes a length-delimited reference instance of the message from the buffer.
  fn decode_length_delimited(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    if D::WIRE_TYPE != WireType::LengthDelimited {
      return Self::decode(src);
    }

    let (mut offset, len) = decode_u32_varint(src)?;
    let len = len as usize;
    if len + offset > src.len() {
      return Err(DecodeError::buffer_underflow());
    }

    let src = &src[offset..offset + len];
    let (bytes_read, value) = Self::decode(src)?;

    #[cfg(debug_assertions)]
    super::debug_assert_read_eq::<Self>(bytes_read, len);

    offset += bytes_read;
    Ok((offset, value))
  }
}

/// The memberlist data can be transmitted through the network.
pub trait Data: core::fmt::Debug + Send + Sync {
  /// The wire type of the data.
  const WIRE_TYPE: WireType = WireType::LengthDelimited;

  /// The reference type of the data.
  type Ref<'a>: DataRef<'a, Self>;

  /// Converts the reference type to the owned type.
  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized;

  /// Returns the encoded length of the data only considering the data itself, (e.g. no length prefix, no wire type).
  fn encoded_len(&self) -> usize;

  /// Returns the encoded length of the data including the length delimited.
  fn encoded_len_with_length_delimited(&self) -> usize {
    let len = self.encoded_len();
    match Self::WIRE_TYPE {
      WireType::LengthDelimited => encoded_u32_varint_len(len as u32) + len,
      _ => len,
    }
  }

  /// Encodes the message to a buffer.
  ///
  /// An error will be returned if the buffer does not have sufficient capacity.
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>;

  /// Encodes the message into a vec.
  #[cfg(any(feature = "std", feature = "alloc"))]
  fn encode_to_vec(&self) -> Result<std::vec::Vec<u8>, EncodeError> {
    let len = self.encoded_len();
    let mut vec = std::vec![0; len];
    self.encode(&mut vec).map(|_| vec)
  }

  /// Encodes the message into a [`Bytes`](::bytes::Bytes).
  #[cfg(any(feature = "std", feature = "alloc"))]
  fn encode_to_bytes(&self) -> Result<::bytes::Bytes, EncodeError> {
    self.encode_to_vec().map(Into::into)
  }

  /// Encodes the message with a length-delimiter to a buffer.
  ///
  /// An error will be returned if the buffer does not have sufficient capacity.
  fn encode_length_delimited(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if Self::WIRE_TYPE != WireType::LengthDelimited {
      return self.encode(buf);
    }

    let len = self.encoded_len();
    if len > u32::MAX as usize {
      return Err(EncodeError::TooLarge);
    }

    let mut offset = 0;
    offset += encode_u32_varint_to(len as u32, buf)?;
    offset += self.encode(&mut buf[offset..])?;

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len_with_length_delimited());

    Ok(offset)
  }

  /// Encodes the message with a length-delimiter into a vec.
  #[cfg(any(feature = "std", feature = "alloc"))]
  fn encode_length_delimited_to_vec(&self) -> Result<std::vec::Vec<u8>, EncodeError> {
    let len = self.encoded_len_with_length_delimited();
    let mut vec = std::vec![0; len];
    self.encode_length_delimited(&mut vec).map(|_| vec)
  }

  /// Encodes the message with a length-delimiter into a [`Bytes`](::bytes::Bytes).
  #[cfg(any(feature = "std", feature = "alloc"))]
  fn encode_length_delimited_to_bytes(&self) -> Result<::bytes::Bytes, EncodeError> {
    self.encode_length_delimited_to_vec().map(Into::into)
  }

  /// Decodes an instance of the message from a buffer.
  ///
  /// The entire buffer will be consumed.
  fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    <Self::Ref<'_> as DataRef<Self>>::decode(src)
      .and_then(|(bytes_read, value)| Self::from_ref(value).map(|val| (bytes_read, val)))
  }

  /// Decodes a length-delimited instance of the message from the buffer.
  fn decode_length_delimited(buf: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    <Self::Ref<'_> as DataRef<Self>>::decode_length_delimited(buf)
      .and_then(|(bytes_read, value)| Self::from_ref(value).map(|val| (bytes_read, val)))
  }
}

/// A data encoding error
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
  /// Returned when the encoded buffer is too small to hold the bytes format of the types.
  #[error("insufficient buffer capacity, required: {required}, remaining: {remaining}")]
  InsufficientBuffer {
    /// The required buffer capacity.
    required: usize,
    /// The remaining buffer capacity.
    remaining: usize,
  },
  /// Returned when the data in encoded format is larger than the maximum allowed size.
  #[error("encoded data is too large, the maximum allowed size is {MAX} bytes", MAX = u32::MAX)]
  TooLarge,
  /// A custom encoding error.
  #[error("{0}")]
  Custom(Cow<'static, str>),
}

impl EncodeError {
  /// Creates an insufficient buffer error.
  #[inline]
  pub const fn insufficient_buffer(required: usize, remaining: usize) -> Self {
    Self::InsufficientBuffer {
      required,
      remaining,
    }
  }

  /// Creates a custom encoding error.
  pub fn custom<T>(value: T) -> Self
  where
    T: Into<Cow<'static, str>>,
  {
    Self::Custom(value.into())
  }

  /// Update the error with the required and remaining buffer capacity.
  pub fn update(mut self, required: usize, remaining: usize) -> Self {
    match self {
      Self::InsufficientBuffer {
        required: ref mut r,
        remaining: ref mut rem,
      } => {
        *r = required;
        *rem = remaining;
        self
      }
      _ => self,
    }
  }
}

impl From<const_varint::EncodeError> for EncodeError {
  #[inline]
  fn from(value: const_varint::EncodeError) -> Self {
    match value {
      const_varint::EncodeError::Underflow {
        required,
        remaining,
      } => Self::InsufficientBuffer {
        required,
        remaining,
      },
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
/// message. The error details should be considered 'best effort': in
/// general it is not possible to exactly pinpoint why data is malformed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, derive_more::IsVariant)]
pub enum DecodeError {
  /// Returned when the buffer does not have enough data to decode the message.
  #[error("buffer underflow")]
  BufferUnderflow,

  /// Returned when the buffer does not contain the required field.
  #[error("missing {field} in {ty}")]
  MissingField {
    /// The type of the message.
    ty: &'static str,
    /// The name of the field.
    field: &'static str,
  },

  /// Returned when the buffer contains duplicate fields for the same tag in a message.
  #[error("duplicate field {field} with tag {tag} in {ty}")]
  DuplicateField {
    /// The type of the message.
    ty: &'static str,
    /// The name of the field.
    field: &'static str,
    /// The tag of the field.
    tag: u8,
  },

  /// Returned when there is a unknown wire type.
  #[error("unknown wire type value {value} with tag {tag} when decoding {ty}")]
  UnknownWireType {
    /// The type of the message.
    ty: &'static str,
    /// The unknown wire type value.
    value: u8,
    /// The tag of the field.
    tag: u8,
  },

  /// Returned when finding a unknown tag.
  #[error("unknown tag {tag} when decoding {ty}")]
  UnknownTag {
    /// The type of the message.
    ty: &'static str,
    /// The unknown tag value.
    tag: u8,
  },

  /// Returned when fail to decode the length-delimited
  #[error("length-delimited overflow the maximum value of u32")]
  LengthDelimitedOverflow,

  /// A custom decoding error.
  #[error("{0}")]
  Custom(Cow<'static, str>),
}

impl From<const_varint::DecodeError> for DecodeError {
  #[inline]
  fn from(e: const_varint::DecodeError) -> Self {
    match e {
      const_varint::DecodeError::Underflow => Self::BufferUnderflow,
      const_varint::DecodeError::Overflow => Self::LengthDelimitedOverflow,
    }
  }
}

impl DecodeError {
  /// Creates a new buffer underflow decoding error.
  #[inline]
  pub const fn buffer_underflow() -> Self {
    Self::BufferUnderflow
  }

  /// Creates a new missing field decoding error.
  #[inline]
  pub const fn missing_field(ty: &'static str, field: &'static str) -> Self {
    Self::MissingField { ty, field }
  }

  /// Creates a new duplicate field decoding error.
  #[inline]
  pub const fn duplicate_field(ty: &'static str, field: &'static str, tag: u8) -> Self {
    Self::DuplicateField { ty, field, tag }
  }

  /// Creates a new unknown wire type decoding error.
  #[inline]
  pub const fn unknown_wire_type(ty: &'static str, value: u8, tag: u8) -> Self {
    Self::UnknownWireType { ty, value, tag }
  }

  /// Creates a new unknown tag decoding error.
  #[inline]
  pub const fn unknown_tag(ty: &'static str, tag: u8) -> Self {
    Self::UnknownTag { ty, tag }
  }

  /// Creates a custom decoding error.
  #[inline]
  pub fn custom<T>(value: T) -> Self
  where
    T: Into<Cow<'static, str>>,
  {
    Self::Custom(value.into())
  }
}
