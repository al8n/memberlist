use super::*;

#[viewit::viewit]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct PushPullHeader {
  nodes: u32,
  user_state_len: u32, // Encodes the byte lengh of user state
  join: bool,          // Is this a join request or a anti-entropy run
}

impl PushPullHeader {
  #[inline]
  pub fn encoded_len(&self) -> usize {
    encoded_u32_len(self.nodes) + 1 // nodes + tag
    + encoded_u32_len(self.user_state_len) + 1 // user_state_len + tag
    + 1 + 1 // join + tag
  }

  #[inline]
  pub fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    buf.put_u8(1); // nodes tag
    encode_u32_to_buf(&mut buf, self.nodes);
    buf.put_u8(2); // user_state_len tag
    encode_u32_to_buf(&mut buf, self.user_state_len);
    buf.put_u8(3); // join tag
    buf.put_u8(self.join as u8);
  }

  #[inline]
  pub fn decode_len(buf: impl Buf) -> Result<u32, DecodeError> {
    decode_u32_from_buf(buf).map(|x| x.0).map_err(From::from)
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut required = 0;
    let mut this = Self {
      nodes: 0,
      user_state_len: 0,
      join: false,
    };
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          this.nodes = decode_u32_from_buf(&mut buf)?.0;
          required += 1;
        }
        2 => {
          this.user_state_len = decode_u32_from_buf(&mut buf)?.0;
          required += 1;
        }
        3 => {
          if !buf.has_remaining() {
            return Err(DecodeError::Truncated(MessageType::PushPull.as_err_str()));
          }
          this.join = match buf.get_u8() {
            0 => false,
            1 => true,
            x => return Err(DecodeError::UnknownMarkBit(x)),
          };
          required += 1;
        }
        _ => {}
      }
    }
    if required != 3 {
      return Err(DecodeError::Truncated(MessageType::PushPull.as_err_str()));
    }
    Ok(this)
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PushNodeState {
  node: NodeId,
  meta: Bytes,
  incarnation: u32,
  state: NodeState,
  /// - 0: encryption algorithm
  /// - 1: compression algorithm
  /// - 2: delegate version
  vsn: [u8; VSN_SIZE],
}

impl PushNodeState {
  #[inline]
  pub fn encoded_len(&self) -> usize {
    // vsn + tag
    let encoded_node_len = self.node.encoded_len();
    encoded_node_len + encoded_u32_len(encoded_node_len as u32) + 1 // node + tag
    + encoded_u32_len(self.incarnation) + 1 // incarnation + tag
    + if self.meta.is_empty() {
      0
    } else {
      let len = encoded_u32_len(self.meta.len() as u32);
      len + 1 + self.meta.len()
    }
    + 1 + 1 // state + tag
    + VSN_SIZE + 1
  }

  #[inline]
  pub fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    buf.put_u8(1); // node tag
    self.node.encode_to(buf);
    buf.put_u8(2); // incarnation tag
    encode_u32_to_buf(&mut buf, self.incarnation);
    if !self.meta.is_empty() {
      buf.put_u8(3); // meta tag
      encode_u32_to_buf(&mut buf, self.meta.len() as u32);
      buf.put_slice(&self.meta);
    }
    buf.put_u8(4); // state tag
    buf.put_u8(self.state as u8); // state
    buf.put_u8(5); // vsn tag
    buf.put_slice(&self.vsn);
  }

  #[inline]
  pub fn decode_len(buf: impl Buf) -> Result<usize, DecodeError> {
    decode_u32_from_buf(buf)
      .map(|x| x.0 as usize)
      .map_err(From::from)
  }

  #[inline]
  pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut node = None;
    let mut incarnation = None;
    let mut meta = Bytes::new();
    let mut state = None;
    let mut vsn = None;
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          let len = decode_u32_from_buf(&mut buf)?.0 as usize;
          if buf.remaining() < len {
            return Err(DecodeError::Truncated(MessageType::PushPull.as_err_str()));
          }
          node = Some(NodeId::decode_from(buf.split_to(len))?);
        }
        2 => {
          incarnation = Some(decode_u32_from_buf(&mut buf)?.0);
        }
        3 => {
          let len = decode_u32_from_buf(&mut buf)?.0 as usize;
          if buf.remaining() < len {
            return Err(DecodeError::Truncated(MessageType::PushPull.as_err_str()));
          }
          meta = buf.split_to(len);
        }
        4 => {
          if !buf.has_remaining() {
            return Err(DecodeError::Truncated(MessageType::PushPull.as_err_str()));
          }
          state = Some(NodeState::try_from(buf.get_u8())?);
        }
        5 => {
          if buf.remaining() < VSN_SIZE {
            return Err(DecodeError::Truncated(MessageType::PushPull.as_err_str()));
          }
          let mut vbuf = [0; VSN_SIZE];
          buf.copy_to_slice(&mut vbuf);
          vsn = Some(vbuf);
        }
        _ => {}
      }
    }

    Ok(Self {
      node: node.ok_or_else(|| DecodeError::Truncated(MessageType::PushPull.as_err_str()))?,
      incarnation: incarnation
        .ok_or_else(|| DecodeError::Truncated(MessageType::PushPull.as_err_str()))?,
      meta,
      state: state.ok_or_else(|| DecodeError::Truncated(MessageType::PushPull.as_err_str()))?,
      vsn: vsn.ok_or_else(|| DecodeError::Truncated(MessageType::PushPull.as_err_str()))?,
    })
  }
}
