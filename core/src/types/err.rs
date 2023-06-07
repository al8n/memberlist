use super::*;

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct ErrorResponse {
  err: String,
}

impl core::fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.err)
  }
}

impl std::error::Error for ErrorResponse {}

impl ErrorResponse {
  #[inline]
  pub fn new(err: impl std::error::Error) -> Self {
    Self {
      err: err.to_string(),
    }
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    let basic_len = if self.err.is_empty() {
      0
    } else {
      // err len + err + err tag
      encoded_u32_len(self.err.len() as u32) + self.err.len() + 1
    };
    basic_len + encoded_u32_len(basic_len as u32)
  }

  #[inline]
  pub fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    if self.err.is_empty() {
      return;
    }
    buf.put_u8(1); // tag
    encode_u32_to_buf(&mut buf, self.err.len() as u32);
    buf.put_slice(self.err.as_bytes());
  }

  #[inline]
  pub fn decode_len(buf: impl Buf) -> Result<usize, DecodeError> {
    decode_u32_from_buf(buf)
      .map(|(x, _)| x as usize)
      .map_err(From::from)
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut required = 0;
    let mut this = Self { err: String::new() };
    while buf.has_remaining() {
      #[allow(clippy::single_match)]
      match buf.get_u8() {
        1 => {
          let len = decode_u32_from_buf(&mut buf)?.0 as usize;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated(
              MessageType::ErrorResponse.as_err_str(),
            ));
          }
          this.err = match String::from_utf8(buf.split_to(len).to_vec()) {
            Ok(s) => s,
            Err(e) => return Err(DecodeError::InvalidErrorResponse(e)),
          };
          required += 1;
        }
        _ => {}
      }
    }

    if required != 1 {
      return Err(DecodeError::Truncated(
        MessageType::ErrorResponse.as_err_str(),
      ));
    }
    Ok(this)
  }
}
