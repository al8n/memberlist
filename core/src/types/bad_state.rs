use super::*;

macro_rules! bad_bail {
  ($name: ident) => {
    #[viewit::viewit]
    #[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
    pub(crate) struct $name {
      incarnation: u32,
      node: NodeId,
      from: NodeId,
    }

    impl $name {
      #[inline]
      pub(crate) fn encoded_len(&self) -> usize {
        let basic = encoded_u32_len(self.incarnation) + 1 // incarnation + tag
        + self.node.encoded_len() + 1 // node + node tag
        + self.from.encoded_len() + 1; // from + from tag
        basic + encoded_u32_len(basic as u32)
      }

      #[inline]
      pub(crate) fn encode_to_msg(&self) -> Message {
        let basic_encoded_len = self.encoded_len() + MessageType::SIZE;
        let encoded_len = basic_encoded_len + encoded_u32_len(basic_encoded_len as u32);
        let mut buf = BytesMut::with_capacity(encoded_len);
        buf.put_u8(MessageType::$name as u8);
        encode_u32_to_buf(&mut buf, encoded_len as u32);
        self.encode_to(&mut buf);
        Message(buf)
      }

      #[inline]
      pub(crate) fn encode_to(&self, mut buf: &mut BytesMut) {
        encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
        buf.put_u8(1); // incarnation tag
        encode_u32_to_buf(&mut buf, self.incarnation);
        buf.put_u8(2); // node tag
        self.node.encode_to(buf);
        buf.put_u8(3); // from tag
        self.from.encode_to(buf);
      }

      #[inline]
      pub(crate) fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
        let mut this = Self::default();
        let mut required = 0;
        while buf.has_remaining() {
          match buf.get_u8() {
            1 => {
              this.incarnation = decode_u32_from_buf(&mut buf)?.0;
              required += 1;
            }
            2 => {
              let node_len = NodeId::decode_len(&mut buf)?;
              this.node = NodeId::decode_from(buf.split_to(node_len))?;
              required += 1;
            }
            3 => {
              let from_len = NodeId::decode_len(&mut buf)?;
              this.from = NodeId::decode_from(buf.split_to(from_len))?;
              required += 1;
            }
            _ => {}
          }
        }

        if required != 3 {
          return Err(DecodeError::Truncated(MessageType::$name.as_err_str()));
        }
        Ok(this)
      }
    }
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);

impl Dead {
  #[inline]
  pub(crate) fn dead_self(&self) -> bool {
    self.node == self.from
  }
}
