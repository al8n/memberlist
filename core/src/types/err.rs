use super::*;

#[viewit::viewit]
#[derive(Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct ErrorResponse {
  err: String,
}

impl<E: std::error::Error> From<E> for ErrorResponse {
  fn from(err: E) -> Self {
    Self {
      err: err.to_string(),
    }
  }
}

impl ErrorResponse {
  #[inline]
  pub fn encoded_len(&self) -> usize {
    LENGTH_SIZE + self.err.as_bytes().len()
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u32(self.encoded_len() as u32);
    buf.put_slice(self.err.as_bytes());
  }

  #[inline]
  pub fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    if buf.remaining() < LENGTH_SIZE {
      return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
      return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
    }
    let seq_no = buf.get_u32();
    if len == 0 {
      Ok(Self { err: String::new() })
    } else {
      let err = buf.copy_to_bytes(len);
      String::from_utf8(err.into())
        .map(|err| Self { err })
        .map_err(|e| DecodeError::other(e))
    }
  }

  #[cfg(feature = "async")]
  #[inline]
  pub async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> Result<Self, std::io::Error> {
    use futures_util::io::AsyncReadExt;
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;

    if len == 0 {
      Ok(Self { err: String::new() })
    } else {
      let mut payload = vec![0u8; len];
      r.read_exact(&mut payload).await?;
      String::from_utf8(payload)
        .map(|err| Self { err })
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.utf8_error()))
    }
  }
}
