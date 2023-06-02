use super::*;

#[viewit::viewit]
#[derive(Debug, Clone)]
pub(crate) struct Alive {
  incarnation: u32,
  // The versions of the protocol/delegate that are being spoken, order:
  // pmin, pmax, pcur, dmin, dmax, dcur
  vsn: [u8; VSN_SIZE],
  meta: Bytes,
  node: NodeId,
}

impl Default for Alive {
  fn default() -> Self {
    Self {
      incarnation: 0,
      node: NodeId::default(),
      meta: Bytes::new(),
      vsn: VSN_EMPTY,
    }
  }
}

impl Alive {
  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    let basic = encoded_u32_len(self.incarnation) + 1 // incarnation + tag
      + VSN_SIZE + 1 // vsn + tag
      + if self.meta.is_empty() { 0 } else {
        let len = self.meta.len();
        encoded_u32_len(len as u32) + len + 1 // meta_size + meta + tag
      }
      + self.node.encoded_len() + 1; // node + node tag
    basic + encoded_u32_len(basic as u32)
  }

  #[inline]
  pub(crate) fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub(crate) fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    buf.put_u8(1); // incarnation tag
    encode_u32_to_buf(&mut buf, self.incarnation);
    buf.put_u8(2); // vsn tag
    buf.put_slice(&self.vsn);
    buf.put_u8(3); // node tag
    self.node.encode_to(&mut buf);
    buf.put_u8(4); // meta tag
    encode_u32_to_buf(&mut buf, self.meta.len() as u32); // meta len
    buf.put_slice(&self.meta);
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut this = Self::default();
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          this.incarnation = decode_u32_from_buf(&mut buf)?.0;
        }
        2 => {
          if buf.remaining() < VSN_SIZE {
            return Err(DecodeError::Truncated(MessageType::Alive.as_err_str()));
          }
          let mut vsn = [0; VSN_SIZE];
          buf.copy_to_slice(&mut vsn);
          this.vsn = vsn;
        }
        3 => {
          let node_len = NodeId::decode_len(&mut buf)?;
          let node = NodeId::decode_from(buf.split_to(node_len))?;
          this.node = node;
        }
        4 => {
          let meta_len = decode_u32_from_buf(&mut buf)?.0 as usize;
          if buf.remaining() < meta_len {
            return Err(DecodeError::Truncated(MessageType::Alive.as_err_str()));
          }
          this.meta = buf.split_to(meta_len);
        }
        _ => {}
      }
    }
    Ok(this)
  }
}
