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
    let length = if self.payload.is_empty() {
      0
    } else {
      // payload len + payload + tag
      encoded_u32_len(self.payload.len() as u32) + self.payload.len() + 1
    } + encoded_u32_len(self.seq_no)
      + 1; // seq_no + tag
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
    buf.put_u8(1); // tag
    encode_u32_to_buf(buf, self.seq_no);
    if !self.payload.is_empty() {
      buf.put_u8(1); // tag
      encode_u32_to_buf(buf, self.payload.len() as u32);
      buf.put_slice(&self.payload);
    }
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut this = Self::default();
    let mut required = 0;
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          this.seq_no = decode_u32_from_buf(buf)?.0;
          required += 1;
        }
        2 => {
          let payload_len = decode_u32_from_buf(buf)?.0 as usize;
          if buf.remaining() < payload_len {
            return Err(DecodeError::Truncated(
              MessageType::AckResponse.as_err_str(),
            ));
          }
          this.payload = buf.split_to(payload_len);
          required += 1;
        }
        _ => {}
      }
    }

    if required != 2 {
      return Err(DecodeError::Truncated(
        MessageType::AckResponse.as_err_str(),
      ));
    }
    Ok(this)
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
