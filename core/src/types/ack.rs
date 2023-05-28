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
    LENGTH_SIZE
    + core::mem::size_of::<u32>() // seq_no
    + self.payload.len()
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
    buf.put_u32(self.seq_no);
    buf.put_slice(&self.payload);
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
    if len - core::mem::size_of::<u32>() == 0 {
      Ok(Self {
        seq_no,
        payload: Bytes::new(),
      })
    } else {
      let payload = buf.copy_to_bytes(len - core::mem::size_of::<u32>());
      Ok(Self { seq_no, payload })
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

    let mut seq_no = [0u8; 4];
    r.read_exact(&mut seq_no).await?;
    let seq_no = u32::from_be_bytes(seq_no);

    if len - core::mem::size_of::<u32>() == 0 {
      Ok(Self {
        seq_no,
        payload: Bytes::new(),
      })
    } else {
      let mut payload = vec![0u8; len - core::mem::size_of::<u32>()];
      r.read_exact(&mut payload).await?;
      Ok(Self {
        seq_no,
        payload: Bytes::from(payload),
      })
    }
  }
}
