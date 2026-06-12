//! The `Data` / `DataRef` traits and their primitive implementations.
//!
//! This is a verbatim copy of `memberlist-proto::data` + the supporting
//! helpers from `memberlist-proto::lib` (`check_encoded_message_size`,
//! `debug_assert_write_eq`, `debug_assert_read_eq`).  Only the
//! `use super::WireType` import has been repointed to `crate::wire_type`.
//!
//! `memberlist-proto` stays frozen and is NOT a dependency of this crate.
use std::borrow::Cow;

use varing::{decode_u32_varint, encode_u32_varint_to, encoded_u32_varint_len};

use crate::wire_type::WireType;

pub use tuple::TupleEncoder;

#[cfg(any(feature = "std", feature = "alloc"))]
mod bytes;
#[cfg(any(feature = "std", feature = "alloc"))]
mod node;
mod primitives;
#[cfg(any(feature = "std", feature = "alloc"))]
mod string;

mod tuple;

#[cfg(all(test, feature = "std"))]
mod tests;

// ─── helpers (inlined from memberlist-proto::lib) ────────────────────────────

#[cfg(debug_assertions)]
#[inline]
pub(crate) fn debug_assert_write_eq<T: ?Sized>(actual: usize, expected: usize) {
  debug_assert_eq!(
    actual,
    expected,
    "{}: expect writting {expected} bytes, but actual write {actual} bytes",
    core::any::type_name::<T>()
  );
}

#[cfg(debug_assertions)]
#[inline]
pub(crate) fn debug_assert_read_eq<T: ?Sized>(actual: usize, expected: usize) {
  debug_assert_eq!(
    actual,
    expected,
    "{}: expect reading {expected} bytes, but actual read {actual} bytes",
    core::any::type_name::<T>()
  );
}

// Only the `std`/`alloc`-gated data submodules (`bytes`, `string`) call this, so
// it carries the same gate to stay live-code under every feature combination.
#[cfg(any(feature = "std", feature = "alloc"))]
#[inline]
pub(crate) fn check_encoded_message_size(required: usize) -> Result<(), EncodeError> {
  if required > u32::MAX as usize {
    return Err(EncodeError::TooLarge);
  }

  Ok(())
}

// ─── DataRef ─────────────────────────────────────────────────────────────────

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

    let (offset, len) = decode_u32_varint(src)?;
    let mut offset = offset.get();
    let len = len as usize;
    if len + offset > src.len() {
      return Err(DecodeError::buffer_underflow());
    }

    let src = &src[offset..offset + len];
    let (bytes_read, value) = Self::decode(src)?;

    #[cfg(debug_assertions)]
    debug_assert_read_eq::<Self>(bytes_read, len);

    offset += bytes_read;
    Ok((offset, value))
  }
}

// ─── Data ────────────────────────────────────────────────────────────────────

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
      WireType::LengthDelimited => encoded_u32_varint_len(len as u32).get() + len,
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
    offset += encode_u32_varint_to(len as u32, buf)?.get();
    offset += self.encode(&mut buf[offset..])?;

    #[cfg(debug_assertions)]
    debug_assert_write_eq::<Self>(offset, self.encoded_len_with_length_delimited());

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

// ─── EncodeError ─────────────────────────────────────────────────────────────

/// The `(required, remaining)` capacity pair carried by
/// [`EncodeError::InsufficientBuffer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsufficientBufferCapacity {
  required: usize,
  remaining: usize,
}

impl InsufficientBufferCapacity {
  /// Construct an insufficient-buffer-capacity payload.
  #[inline(always)]
  pub const fn new(required: usize, remaining: usize) -> Self {
    Self {
      required,
      remaining,
    }
  }

  /// The buffer capacity required to encode the value.
  #[inline(always)]
  pub const fn required(&self) -> usize {
    self.required
  }

  /// The buffer capacity remaining at the time of the error.
  #[inline(always)]
  pub const fn remaining(&self) -> usize {
    self.remaining
  }

  /// Replace the `required` capacity in place (kept for
  /// [`EncodeError::update`]; not part of the accessor-only surface
  /// callers should drive directly).
  #[inline(always)]
  pub(crate) fn set_required(&mut self, required: usize) {
    self.required = required;
  }

  /// Replace the `remaining` capacity in place (kept for
  /// [`EncodeError::update`]; not part of the accessor-only surface
  /// callers should drive directly).
  #[inline(always)]
  pub(crate) fn set_remaining(&mut self, remaining: usize) {
    self.remaining = remaining;
  }
}

impl core::fmt::Display for InsufficientBufferCapacity {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "required: {}, remaining: {}",
      self.required, self.remaining
    )
  }
}

/// A data encoding error
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
  /// Returned when the encoded buffer is too small to hold the bytes format of the types.
  #[error("insufficient buffer capacity, {0}")]
  InsufficientBuffer(InsufficientBufferCapacity),
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
    Self::InsufficientBuffer(InsufficientBufferCapacity::new(required, remaining))
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
      Self::InsufficientBuffer(ref mut cap) => {
        cap.set_required(required);
        cap.set_remaining(remaining);
        self
      }
      _ => self,
    }
  }
}

impl From<varing::EncodeError> for EncodeError {
  #[inline]
  fn from(value: varing::EncodeError) -> Self {
    match value {
      varing::EncodeError::InsufficientSpace(err) => Self::InsufficientBuffer(
        InsufficientBufferCapacity::new(err.requested().get(), err.available()),
      ),
      varing::EncodeError::Other(e) => EncodeError::custom(e),
      _ => EncodeError::custom("unknown encoding error"),
    }
  }
}

impl From<varing::ConstEncodeError> for EncodeError {
  #[inline]
  fn from(value: varing::ConstEncodeError) -> Self {
    match value {
      varing::ConstEncodeError::InsufficientSpace(err) => Self::InsufficientBuffer(
        InsufficientBufferCapacity::new(err.requested().get(), err.available()),
      ),
      varing::ConstEncodeError::Other(e) => EncodeError::custom(e),
      _ => EncodeError::custom("unknown encoding error"),
    }
  }
}

impl From<Cow<'static, str>> for EncodeError {
  fn from(value: Cow<'static, str>) -> Self {
    Self::Custom(value)
  }
}

// ─── DecodeError ─────────────────────────────────────────────────────────────

/// Payload for [`DecodeError::MissingField`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissingFieldInfo {
  ty: &'static str,
  field: &'static str,
}

impl MissingFieldInfo {
  /// Construct a missing-field payload.
  #[inline(always)]
  pub const fn new(ty: &'static str, field: &'static str) -> Self {
    Self { ty, field }
  }

  /// The type of the message.
  #[inline(always)]
  pub const fn ty(&self) -> &'static str {
    self.ty
  }

  /// The name of the missing field.
  #[inline(always)]
  pub const fn field(&self) -> &'static str {
    self.field
  }
}

impl core::fmt::Display for MissingFieldInfo {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "missing {} in {}", self.field, self.ty)
  }
}

/// Payload for [`DecodeError::DuplicateField`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuplicateFieldInfo {
  ty: &'static str,
  field: &'static str,
  tag: u8,
}

impl DuplicateFieldInfo {
  /// Construct a duplicate-field payload.
  #[inline(always)]
  pub const fn new(ty: &'static str, field: &'static str, tag: u8) -> Self {
    Self { ty, field, tag }
  }

  /// The type of the message.
  #[inline(always)]
  pub const fn ty(&self) -> &'static str {
    self.ty
  }

  /// The name of the duplicate field.
  #[inline(always)]
  pub const fn field(&self) -> &'static str {
    self.field
  }

  /// The wire tag of the field.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    self.tag
  }
}

impl core::fmt::Display for DuplicateFieldInfo {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "duplicate field {} with tag {} in {}",
      self.field, self.tag, self.ty
    )
  }
}

/// Payload for [`DecodeError::UnknownWireType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownWireTypeInfo {
  ty: &'static str,
  value: u8,
  tag: u8,
}

impl UnknownWireTypeInfo {
  /// Construct an unknown-wire-type payload.
  #[inline(always)]
  pub const fn new(ty: &'static str, value: u8, tag: u8) -> Self {
    Self { ty, value, tag }
  }

  /// The type of the message being decoded.
  #[inline(always)]
  pub const fn ty(&self) -> &'static str {
    self.ty
  }

  /// The unknown wire-type value encountered.
  #[inline(always)]
  pub const fn value(&self) -> u8 {
    self.value
  }

  /// The field tag associated with the unknown wire type.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    self.tag
  }
}

impl core::fmt::Display for UnknownWireTypeInfo {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "unknown wire type value {} with tag {} when decoding {}",
      self.value, self.tag, self.ty
    )
  }
}

/// Payload for [`DecodeError::UnknownTag`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownTagInfo {
  ty: &'static str,
  tag: u8,
}

impl UnknownTagInfo {
  /// Construct an unknown-tag payload.
  #[inline(always)]
  pub const fn new(ty: &'static str, tag: u8) -> Self {
    Self { ty, tag }
  }

  /// The type of the message being decoded.
  #[inline(always)]
  pub const fn ty(&self) -> &'static str {
    self.ty
  }

  /// The unknown tag value encountered.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    self.tag
  }
}

impl core::fmt::Display for UnknownTagInfo {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "unknown tag {} when decoding {}", self.tag, self.ty)
  }
}

/// A message decoding error.
///
/// `DecodeError` indicates that the input buffer does not contain a valid
/// message. The error details should be considered 'best effort': in
/// general it is not possible to exactly pinpoint why data is malformed.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, derive_more::IsVariant)]
pub enum DecodeError {
  /// Returned when the buffer does not have enough data to decode the message.
  #[error("buffer underflow")]
  BufferUnderflow,

  /// Returned when the buffer does not contain the required field.
  #[error("{0}")]
  MissingField(MissingFieldInfo),

  /// Returned when the buffer contains duplicate fields for the same tag in a message.
  #[error("{0}")]
  DuplicateField(DuplicateFieldInfo),

  /// Returned when there is a unknown wire type.
  #[error("{0}")]
  UnknownWireType(UnknownWireTypeInfo),

  /// Returned when finding a unknown tag.
  #[error("{0}")]
  UnknownTag(UnknownTagInfo),

  /// Returned when fail to decode the length-delimited
  #[error("length-delimited overflow the maximum value of u32")]
  LengthDelimitedOverflow,

  /// A custom decoding error.
  #[error("{0}")]
  Custom(Cow<'static, str>),
}

impl From<varing::DecodeError> for DecodeError {
  #[inline]
  fn from(e: varing::DecodeError) -> Self {
    match e {
      varing::DecodeError::Overflow => Self::LengthDelimitedOverflow,
      varing::DecodeError::InsufficientData { .. } => Self::buffer_underflow(),
      varing::DecodeError::Other(cow) => Self::Custom(cow),
      _ => Self::custom("unknown decoding error"),
    }
  }
}

impl From<varing::ConstDecodeError> for DecodeError {
  #[inline]
  fn from(e: varing::ConstDecodeError) -> Self {
    match e {
      varing::ConstDecodeError::Overflow => Self::LengthDelimitedOverflow,
      varing::ConstDecodeError::InsufficientData { .. } => Self::buffer_underflow(),
      varing::ConstDecodeError::Other(cow) => Self::custom(cow),
      _ => Self::custom("unknown decoding error"),
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
    Self::MissingField(MissingFieldInfo::new(ty, field))
  }

  /// Creates a new duplicate field decoding error.
  #[inline]
  pub const fn duplicate_field(ty: &'static str, field: &'static str, tag: u8) -> Self {
    Self::DuplicateField(DuplicateFieldInfo::new(ty, field, tag))
  }

  /// Creates a new unknown wire type decoding error.
  #[inline]
  pub const fn unknown_wire_type(ty: &'static str, value: u8, tag: u8) -> Self {
    Self::UnknownWireType(UnknownWireTypeInfo::new(ty, value, tag))
  }

  /// Creates a new unknown tag decoding error.
  #[inline]
  pub const fn unknown_tag(ty: &'static str, tag: u8) -> Self {
    Self::UnknownTag(UnknownTagInfo::new(ty, tag))
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
