use super::*;

pub(crate) struct AckResponseEncoder<T: BufMut> {
  buf: T,
}

impl<T: BufMut> AckResponseEncoder<T> {
  #[inline]
  pub(crate) fn new(buf: T) -> Self {
    Self { buf }
  }

  #[inline]
  pub(crate) fn encoded_len(payload: &[u8]) -> usize {
    LENGTH_SIZE
    + core::mem::size_of::<u32>() // seq_no
    + core::mem::size_of::<u32>() // payload len
    + payload.len()
  }

  #[inline]
  pub(crate) fn encode_seq_no(&mut self, seq_no: u32) {
    self.buf.put_u32(seq_no);
  }

  #[inline]
  pub(crate) fn encode_payload(&mut self, payload: &[u8]) {
    self.buf.put_u32(payload.len() as u32);
    self.buf.put_slice(payload);
  }
}

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AckResponse {
  seq_no: u32,
  payload: Bytes,
}

impl AckResponse {
  pub fn new(seq_no: u32, payload: Bytes) -> Self {
    Self { seq_no, payload }
  }

  #[inline]
  pub fn empty(seq_no: u32) -> Self {
    Self {
      seq_no,
      payload: Bytes::new(),
    }
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    let length = self.payload.len() + encoded_u32_len(self.seq_no) as usize;
    length + encoded_u32_len(length as u32)
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, buf: &mut BytesMut) {
    encode_u32_to_buf(buf, self.encoded_len() as u32);
    encode_u32_to_buf(buf, self.seq_no);
    buf.put_slice(&self.payload);
  }

  #[inline]
  pub fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    let (len, _) = decode_u32_from_buf(buf)?;
    let len = len as usize;

    let (seq_no, readed) = decode_u32_from_buf(buf)?;

    if buf.remaining() + readed < len {
      return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
    }

    if len - readed == 0 {
      Ok(Self {
        seq_no,
        payload: Bytes::new(),
      })
    } else {
      Ok(Self {
        seq_no,
        payload: buf.copy_to_bytes(len - readed),
      })
    }
  }

  #[cfg(feature = "async")]
  #[inline]
  pub async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> Result<Self, std::io::Error> {
    use futures_util::io::AsyncReadExt;

    let (len, _) = decode_u32_from_reader(r).await?;
    let len = len as usize;
    let (seq_no, readed) = decode_u32_from_reader(r).await?;
    if len < readed {
      return Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "invalid length",
      ));
    }

    if len - readed == 0 {
      Ok(Self {
        seq_no,
        payload: Bytes::new(),
      })
    } else {
      let mut payload = vec![0u8; len as usize - readed];
      r.read_exact(&mut payload).await?;
      Ok(Self {
        seq_no,
        payload: Bytes::from(payload),
      })
    }
  }
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct NackResponse {
  seq_no: u32,
}

impl NackResponse {}
