use smol_str::SmolStr;

use super::{Data, DataRef, DecodeError, EncodeError, WireType, merge, skip};

/// Error response from the remote peer
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct ErrorResponse {
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the msg of the error response")
    ),
    setter(attrs(doc = "Sets the msg of the error response (Builder pattern)"))
  )]
  message: SmolStr,
}

impl ErrorResponse {
  const MESSAGE_TAG: u8 = 1;
  const MESSAGE_BYTE: u8 = merge(WireType::LengthDelimited, Self::MESSAGE_TAG);

  /// Create a new error response
  pub fn new(message: impl Into<SmolStr>) -> Self {
    Self {
      message: message.into(),
    }
  }

  /// Returns the msg of the error response
  pub fn set_message(&mut self, msg: impl Into<SmolStr>) -> &mut Self {
    self.message = msg.into();
    self
  }
}

impl Data for ErrorResponse {
  type Ref<'a> = ErrorResponseRef<'a>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(Self {
      message: val.message.into(),
    })
  }

  fn encoded_len(&self) -> usize {
    1 + self.message.encoded_len_with_length_delimited()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut offset = 0;
    if buf.is_empty() {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), 0));
    }

    buf[offset] = Self::MESSAGE_BYTE;
    offset += 1;
    offset += self.message.encode_length_delimited(&mut buf[offset..])?;
    #[cfg(debug_assertions)]
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
    Ok(offset)
  }
}

impl core::fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.message)
  }
}

impl std::error::Error for ErrorResponse {}

impl From<ErrorResponse> for SmolStr {
  fn from(err: ErrorResponse) -> Self {
    err.message
  }
}

impl From<SmolStr> for ErrorResponse {
  fn from(msg: SmolStr) -> Self {
    Self { message: msg }
  }
}

/// Reference type of error response from the remote peer
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ErrorResponseRef<'a> {
  message: &'a str,
}

impl<'a> ErrorResponseRef<'a> {
  /// Returns message of the error response
  #[inline]
  pub const fn message(&self) -> &'a str {
    self.message
  }
}

impl<'a> DataRef<'a, ErrorResponse> for ErrorResponseRef<'a> {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let mut msg = None;

    while offset < src.len() {
      match src[offset] {
        ErrorResponse::MESSAGE_BYTE => {
          if msg.is_some() {
            return Err(DecodeError::duplicate_field(
              "ErrorResponse",
              "msg",
              ErrorResponse::MESSAGE_TAG,
            ));
          }
          offset += 1;

          let (bytes_read, value) =
            <&str as DataRef<SmolStr>>::decode_length_delimited(&src[offset..])?;
          offset += bytes_read;
          msg = Some(value);
        }
        _ => offset += skip("ErrorResponse", &src[offset..])?,
      }
    }

    Ok((
      offset,
      Self {
        message: msg.unwrap_or_default(),
      },
    ))
  }
}

#[cfg(test)]
mod tests {
  use arbitrary::{Arbitrary, Unstructured};

  use super::*;

  #[test]
  fn test_access() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);

    let mut data = Unstructured::new(&data);
    let err = ErrorResponse::arbitrary(&mut data).unwrap();
    assert_eq!(err.message(), &err.message);
  }
}
