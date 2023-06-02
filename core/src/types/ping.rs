use super::*;

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Ping {
  seq_no: u32,
  /// Source node, used for a direct reply
  source: NodeId,

  /// NodeId is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  target: Option<NodeId>,
}

impl Ping {
  #[inline]
  pub const fn new(seq_no: u32, source: NodeId) -> Self {
    Self {
      seq_no,
      target: None,
      source,
    }
  }

  pub fn with_target(mut self, target: NodeId) -> Self {
    self.target = Some(target);
    self
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    let basic = encoded_u32_len(self.seq_no) + 1 // seq_no + tag
    + if let Some(t) = &self.target {
      t.encoded_len() + 1 // target + tag
    } else { 0 }
    + self.source.encoded_len() + 1; // source + tag
    basic + encoded_u32_len(basic as u32)
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);

    buf.put_u8(1); // seq_no tag
    encode_u32_to_buf(&mut buf, self.seq_no);

    buf.put_u8(2); // source tag
    self.source.encode_to(buf);

    if let Some(target) = &self.target {
      buf.put_u8(3); // target tag
      target.encode_to(buf);
    }
  }

  #[inline]
  pub(crate) fn decode_len(buf: impl Buf) -> Result<usize, DecodeError> {
    decode_u32_from_buf(buf)
      .map(|(len, _)| len as usize)
      .map_err(From::from)
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut this = Self::default();
    let mut required = 0;
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          this.seq_no = decode_u32_from_buf(&mut buf)?.0;
          required += 1;
        }
        2 => {
          let len = NodeId::decode_len(&mut buf)?;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
          }
          this.source = NodeId::decode_from(buf.split_to(len))?;
          required += 1;
        }
        3 => {
          let len = NodeId::decode_len(&mut buf)?;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
          }
          this.target = Some(NodeId::decode_from(buf.split_to(len))?);
        }
        _ => {}
      }
    }

    if required != 2 {
      return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
    }
    Ok(this)
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IndirectPing {
  /// true if we'd like a nack back
  nack: bool,
  ping: Ping,
}

impl IndirectPing {
  #[inline]
  pub const fn new(nack: bool, ping: Ping) -> Self {
    Self { nack, ping }
  }

  #[inline]
  pub fn with_target(mut self, target: NodeId) -> Self {
    self.ping.target = Some(target);
    self
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    let basic = 1 + 1 // nack + tag
    + self.ping.encoded_len() + 1; // ping + 1
    basic + encoded_u32_len(basic as u32)
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    buf.put_u8(1); // nack tag
    buf.put_u8(self.nack as u8);
    buf.put_u8(2); // ping tag
    self.ping.encode_to(buf);
  }

  #[inline]
  pub fn decode_len(mut buf: impl Buf) -> Result<usize, DecodeError> {
    decode_u32_from_buf(buf)
      .map(|(len, _)| len as usize)
      .map_err(From::from)
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut this = Self::default();
    let mut required = 0;
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          if !buf.has_remaining() {
            return Err(DecodeError::Truncated(
              MessageType::IndirectPing.as_err_str(),
            ));
          }
          match buf.get_u8() {
            0 => this.nack = false,
            1 => this.nack = true,
            x => return Err(DecodeError::UnknownMarkBit(x)),
          }
          required += 1;
        }
        2 => {
          let len = Ping::decode_len(&mut buf)?;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated(
              MessageType::IndirectPing.as_err_str(),
            ));
          }
          this.ping = Ping::decode_from(buf.split_to(len))?;
          required += 1;
        }
        _ => {}
      }
    }

    if required != 2 {
      return Err(DecodeError::Truncated(
        MessageType::IndirectPing.as_err_str(),
      ));
    }

    Ok(this)
  }
}
