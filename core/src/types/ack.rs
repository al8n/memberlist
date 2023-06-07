use crate::checksum::Checksumer;

use super::*;

pub(crate) struct AckResponseEncoder<T: BufMut, C: Checksumer> {
  buf: T,
  cks: C,
}

impl<T: BufMut, C: Checksumer> AckResponseEncoder<T, C> {
  #[inline]
  pub(crate) fn new(buf: T) -> Self {
    Self { buf, cks: C::new() }
  }

  #[inline]
  pub(crate) fn encoded_len(seq_no: u32, payload: &[u8]) -> usize {
    let length = if payload.is_empty() {
      0
    } else {
      // payload len + payload + tag
      encoded_u32_len(payload.len() as u32) + payload.len() + 1
    } + encoded_u32_len(seq_no)
      + 1; // seq_no + tag
    length + encoded_u32_len(length as u32) + CHECKSUM_SIZE
  }

  #[inline]
  pub(crate) fn encode_seq_no(&mut self, seq_no: u32) {
    self.buf.put_u8(1); // tag
    self.cks.update(seq_no.to_be_bytes().as_ref());
    encode_u32_to_buf(&mut self.buf, seq_no);
  }

  #[inline]
  pub(crate) fn encode_payload(mut self, payload: &[u8]) -> T {
    if payload.is_empty() {
      self.buf.put_u32(self.cks.finalize());
      return self.buf;
    }
    self.buf.put_u8(2); // tag
    encode_u32_to_buf(&mut self.buf, payload.len() as u32);
    self.cks.update(payload);
    self.buf.put_slice(payload);
    self.buf.put_u32(self.cks.finalize());
    self.buf
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
  #[inline]
  pub fn empty(seq_no: u32) -> Self {
    Self {
      seq_no,
      payload: Bytes::new(),
    }
  }

  #[inline]
  pub fn decode_len(buf: impl Buf) -> Result<usize, DecodeError> {
    decode_u32_from_buf(buf)
      .map(|(len, _)| len as usize)
      .map_err(From::from)
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
    length + encoded_u32_len(length as u32) + CHECKSUM_SIZE
  }

  #[inline]
  pub fn encode_to<C: Checksumer>(&self, mut buf: &mut BytesMut) {
    let mut cks = C::new();
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    buf.put_u8(1); // tag
    cks.update(self.seq_no.to_be_bytes().as_ref());
    encode_u32_to_buf(&mut buf, self.seq_no);
    if !self.payload.is_empty() {
      buf.put_u8(2); // tag
      encode_u32_to_buf(&mut buf, self.payload.len() as u32);
      buf.put_slice(&self.payload);
      cks.update(&self.payload);
    }
    buf.put_u32(cks.finalize());
  }

  #[inline]
  pub fn decode_from<C: Checksumer>(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut hasher = C::new();
    let mut this = Self::default();
    let mut required = 0;
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          this.seq_no = decode_u32_from_buf(&mut buf)?.0;
          hasher.update(this.seq_no.to_be_bytes().as_ref());
          required += 1;
        }
        2 => {
          let payload_len = decode_u32_from_buf(&mut buf)?.0 as usize;
          hasher.update(payload_len.to_be_bytes().as_ref());
          if buf.remaining() < payload_len {
            return Err(DecodeError::Truncated(
              MessageType::AckResponse.as_err_str(),
            ));
          }
          this.payload = buf.split_to(payload_len);
          hasher.update(&this.payload);
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

    if buf.remaining() < CHECKSUM_SIZE {
      return Err(DecodeError::Truncated(
        MessageType::AckResponse.as_err_str(),
      ));
    }
    let cks = buf.get_u32();
    if cks != hasher.finalize() {
      return Err(DecodeError::ChecksumMismatch);
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

impl NackResponse {
  #[inline]
  pub fn new(seq_no: u32) -> Self {
    Self { seq_no }
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    let length = encoded_u32_len(self.seq_no) + 1; // seq_no + tag
    length + encoded_u32_len(length as u32) + CHECKSUM_SIZE
  }

  #[inline]
  pub fn encode_to<C: Checksumer>(&self, mut buf: &mut BytesMut) {
    let mut cks = C::new();
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    buf.put_u8(1); // tag
    cks.update(self.seq_no.to_be_bytes().as_ref());
    encode_u32_to_buf(&mut buf, self.seq_no);
    buf.put_u32(cks.finalize());
  }

  #[inline]
  pub fn decode_from<C: Checksumer>(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut hasher = C::new();
    let mut seq_no = 0;
    let mut required = 0;
    while buf.has_remaining() {
      #[allow(clippy::single_match)]
      match buf.get_u8() {
        1 => {
          seq_no = decode_u32_from_buf(&mut buf)?.0;
          hasher.update(seq_no.to_be_bytes().as_ref());
          required += 1;
        }
        _ => {}
      }
    }

    if required != 1 {
      return Err(DecodeError::Truncated(
        MessageType::NackResponse.as_err_str(),
      ));
    }

    if buf.remaining() < CHECKSUM_SIZE {
      return Err(DecodeError::Truncated(
        MessageType::NackResponse.as_err_str(),
      ));
    }
    let cks = buf.get_u32();
    if cks != hasher.finalize() {
      return Err(DecodeError::ChecksumMismatch);
    }
    Ok(Self { seq_no })
  }
}
