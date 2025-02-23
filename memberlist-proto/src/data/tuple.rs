use const_varint::{encode_u32_varint_to, encoded_u32_varint_len};

use crate::utils::{merge, skip};

use super::{Data, DataRef, DecodeError, EncodeError};

const A_TAG: u8 = 1;
const B_TAG: u8 = 2;

impl<'a, A, B> DataRef<'a, (A, B)> for (A::Ref<'a>, B::Ref<'a>)
where
  A: Data,
  B: Data,
{
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let buf_len = buf.len();
    let mut a = None;
    let mut b = None;

    while offset < buf_len {
      match buf[offset] {
        byte if byte == merge(A::WIRE_TYPE, A_TAG) => {
          if a.is_some() {
            return Err(DecodeError::duplicate_field("tuple(2)", "0", A_TAG));
          }

          offset += 1;
          let (bytes_read, value) = A::Ref::decode_length_delimited(&buf[offset..])?;
          offset += bytes_read;
          a = Some(value);
        }
        byte if byte == merge(B::WIRE_TYPE, B_TAG) => {
          if b.is_some() {
            return Err(DecodeError::duplicate_field("tuple(2)", "1", B_TAG));
          }

          offset += 1;
          let (bytes_read, value) = B::Ref::decode_length_delimited(&buf[offset..])?;
          offset += bytes_read;
          b = Some(value);
        }
        _ => offset += skip("tuple(2)", &buf[offset..])?,
      }
    }

    let a = a.ok_or_else(|| DecodeError::missing_field("tuple(2)", "0"))?;
    let b = b.ok_or_else(|| DecodeError::missing_field("tuple(2)", "1"))?;

    Ok((offset, (a, b)))
  }
}

impl<A, B> Data for (A, B)
where
  A: Data,
  B: Data,
{
  type Ref<'a> = (A::Ref<'a>, B::Ref<'a>);

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    let (a, b) = val;
    Ok((A::from_ref(a)?, B::from_ref(b)?))
  }

  fn encoded_len(&self) -> usize {
    TupleEncoder::new(&self.0, &self.1).encoded_len()
  }

  fn encoded_len_with_length_delimited(&self) -> usize {
    TupleEncoder::new(&self.0, &self.1).encoded_len_with_length_delimited()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    TupleEncoder::new(&self.0, &self.1).encode(buf)
  }

  fn encode_length_delimited(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    TupleEncoder::new(&self.0, &self.1).encode_with_length_delimited(buf)
  }
}

/// The encoder for a tuple of two data types.
pub struct TupleEncoder<'a, A, B> {
  a: &'a A,
  b: &'a B,
}

impl<'a, A, B> TupleEncoder<'a, A, B> {
  /// Create a new tuple encoder.
  pub fn new(a: &'a A, b: &'a B) -> Self {
    Self { a, b }
  }
}

impl<A, B> TupleEncoder<'_, A, B>
where
  A: Data,
  B: Data,
{
  /// Returns the encoded length of the data only considering the data itself, (e.g. no length prefix, no wire type).
  pub fn encoded_len(&self) -> usize {
    1 + self.a.encoded_len_with_length_delimited() + 1 + self.b.encoded_len_with_length_delimited()
  }

  /// Returns the encoded length of the data including the length delimited.
  pub fn encoded_len_with_length_delimited(&self) -> usize {
    let len = self.encoded_len();
    encoded_u32_varint_len(len as u32) + len
  }

  /// Encodes the message to a buffer.
  ///
  /// An error will be returned if the buffer does not have sufficient capacity.
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut offset = 0;
    let buf_len = buf.len();

    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), 0));
    }

    buf[offset] = merge(A::WIRE_TYPE, A_TAG);
    offset += 1;

    offset += self
      .a
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), buf_len))?;

    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }

    buf[offset] = merge(B::WIRE_TYPE, B_TAG);
    offset += 1;

    offset += self
      .b
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), buf_len))?;

    Ok(offset)
  }

  /// Encodes the message with a length-delimiter to a buffer.
  ///
  /// An error will be returned if the buffer does not have sufficient capacity.
  pub fn encode_with_length_delimited(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let len = self.encoded_len();
    if len > u32::MAX as usize {
      return Err(EncodeError::TooLarge);
    }

    let mut offset = 0;
    offset += encode_u32_varint_to(len as u32, buf)?;
    offset += self.encode(&mut buf[offset..])?;

    #[cfg(debug_assertions)]
    super::super::debug_assert_write_eq::<Self>(offset, self.encoded_len_with_length_delimited());

    Ok(offset)
  }
}
