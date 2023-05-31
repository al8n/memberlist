use super::*;

#[viewit::viewit]
pub(crate) struct PushPullHeader {
  nodes: u32,
  user_state_len: u32, // Encodes the byte lengh of user state
  join: bool,          // Is this a join request or a anti-entropy run
}

impl PushPullHeader {
  #[inline]
  pub fn encoded_len(&self) -> usize {
    let basic = encoded_u32_len(self.nodes) + 1 // nodes + tag
    + encoded_u32_len(self.user_state_len) + 1 // user_state_len + tag
    + 1 + 1; // join + tag
    basic + encoded_u32_len(basic as u32)
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
    buf.put_u8(1); // nodes tag
    encode_u32_to_buf(buf, self.nodes);
    buf.put_u8(2); // user_state_len tag
    encode_u32_to_buf(buf, self.user_state_len);
    buf.put_u8(3); // join tag
    buf.put_u8(self.join as u8);
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
      match required {
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
  vsn: [u8; VSN_SIZE],
}

impl PushNodeState {
  #[inline]
  pub const fn pmin(&self) -> u8 {
    self.vsn[0]
  }
  #[inline]
  pub const fn pmax(&self) -> u8 {
    self.vsn[1]
  }
  #[inline]
  pub const fn pcur(&self) -> u8 {
    self.vsn[2]
  }
  #[inline]
  pub const fn dmin(&self) -> u8 {
    self.vsn[3]
  }
  #[inline]
  pub const fn dmax(&self) -> u8 {
    self.vsn[4]
  }
  #[inline]
  pub const fn dcur(&self) -> u8 {
    self.vsn[5]
  }
}

impl Default for PushNodeState {
  fn default() -> Self {
    Self {
      node: NodeId::default(),
      meta: Bytes::default(),
      incarnation: 0,
      state: NodeState::default(),
      vsn: VSN_EMPTY,
    }
  }
}
