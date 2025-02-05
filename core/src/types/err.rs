use length_delimited::{encoded_u32_varint_len, InsufficientBuffer, LengthDelimitedEncoder};
use smol_str::SmolStr;

use super::{decode_length_delimited, merge, skip, split, DecodeError, WireType};

/// Error response from the remote peer
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

  /// Returns the encoded length of the error response
  #[inline]
  pub fn encoded_len(&self) -> usize {
    if self.message.is_empty() {
      return 0;
    }

    let len = self.message.len();
    1 + encoded_u32_varint_len(len as u32) + len
  }

  /// Encodes the error response into the buffer
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, InsufficientBuffer> {
    let msg_len = self.message.len();
    if msg_len == 0 {
      return Ok(0);
    }

    let len = buf.len();
    let mut offset = 0;
    buf[offset] = Self::MESSAGE_BYTE;
    offset += 1;

    offset += (msg_len as u32)
      .encode(&mut buf[offset..])
      .map_err(|_| InsufficientBuffer::with_information(self.encoded_len() as u64, len as u64))?;

    if offset + msg_len > len {
      return Err(InsufficientBuffer::with_information(
        self.encoded_len() as u64,
        len as u64,
      ));
    }

    buf[offset..offset + msg_len].copy_from_slice(self.message.as_bytes());
    offset += msg_len;
    Ok(offset)
  }

  /// Decodes the error response from the buffer
  pub fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError> {
    let mut offset = 0;
    let mut msg = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        Self::MESSAGE_BYTE => {
          let (bytes_read, value) =
            decode_length_delimited(WireType::LengthDelimited, &src[offset..])?;
          offset += bytes_read;

          match core::str::from_utf8(value) {
            Ok(value) => {
              msg = Some(SmolStr::new(value));
            }
            Err(e) => {
              return Err(DecodeError::new(e.to_string()));
            }
          }
        }
        _ => {
          let (wire_type, _) = split(src[offset]);
          let wire_type = WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      Self {
        message: msg.unwrap_or_default(),
      },
    ))
  }

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

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a> Arbitrary<'a> for ErrorResponse {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      let message = u.arbitrary::<String>()?;
      Ok(Self::new(message))
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for ErrorResponse {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::new(String::arbitrary(g))
    }
  }
};

#[cfg(test)]
mod tests {
  use arbitrary::{Arbitrary, Unstructured};

  use super::*;

  #[quickcheck_macros::quickcheck]
  fn fuzzy_error_message_encode_decode(err: ErrorResponse) -> bool {
    let mut buf = vec![0; err.encoded_len()];
    let Ok(written) = err.encode(&mut buf) else {
      return false;
    };
    match ErrorResponse::decode(&buf) {
      Ok((readed, decoded)) => err == decoded && written == readed,
      Err(e) => {
        println!("Error: {e}");
        false
      }
    }
  }

  #[test]
  fn test_access() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);

    let mut data = Unstructured::new(&data);
    let err = ErrorResponse::arbitrary(&mut data).unwrap();
    assert_eq!(err.message(), &err.message);
  }
}
