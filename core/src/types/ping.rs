use super::*;

macro_rules! bail_ping {
  ($name: ident) => {
    #[viewit::viewit(getters(skip), setters(skip))]
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub(crate) struct $name {
      seq_no: u32,
      /// Source node, used for a direct reply
      source: NodeId,

      /// `NodeId` is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      target: NodeId,
    }

    impl $name {
      #[inline]
      pub fn encoded_len(&self) -> usize {
        let basic = encoded_u32_len(self.seq_no) + 1 // seq_no + tag
        + self.target.encoded_len() + 1 // target + tag
        + self.source.encoded_len() + 1; // source + tag
        basic + encoded_u32_len(basic as u32)
      }

      #[inline]
      pub fn encode_to(&self, mut buf: &mut BytesMut) {
        encode_u32_to_buf(&mut buf, self.encoded_len() as u32);

        buf.put_u8(1); // seq_no tag
        encode_u32_to_buf(&mut buf, self.seq_no);

        buf.put_u8(2); // source tag
        self.source.encode_to(buf);

        buf.put_u8(3); // target tag
        self.target.encode_to(buf);
      }

      #[inline]
      pub(crate) fn decode_len(buf: impl Buf) -> Result<usize, DecodeError> {
        decode_u32_from_buf(buf)
          .map(|(len, _)| len as usize)
          .map_err(From::from)
      }

      #[inline]
      pub fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
        let mut seq_no = None;
        let mut target = None;
        let mut source = None;
        while buf.has_remaining() {
          match buf.get_u8() {
            1 => {
              seq_no = Some(decode_u32_from_buf(&mut buf)?.0);
            }
            2 => {
              let len = NodeId::decode_len(&mut buf)?;
              if len > buf.remaining() {
                return Err(DecodeError::Truncated(MessageType::$name.as_err_str()));
              }
              source = Some(NodeId::decode_from(buf.split_to(len))?);
            }
            3 => {
              let len = NodeId::decode_len(&mut buf)?;
              if len > buf.remaining() {
                return Err(DecodeError::Truncated(MessageType::$name.as_err_str()));
              }
              target = Some(NodeId::decode_from(buf.split_to(len))?);
            }
            _ => {}
          }
        }

        Ok(Self {
          seq_no: seq_no.ok_or_else(|| DecodeError::Truncated(MessageType::$name.as_err_str()))?,
          source: source.ok_or_else(|| DecodeError::Truncated(MessageType::$name.as_err_str()))?,
          target: target.ok_or_else(|| DecodeError::Truncated(MessageType::$name.as_err_str()))?,
        })
      }
    }
  };
}

bail_ping!(Ping);
bail_ping!(IndirectPing);

impl From<Ping> for IndirectPing {
  fn from(ping: Ping) -> Self {
    Self {
      seq_no: ping.seq_no,
      source: ping.source,
      target: ping.target,
    }
  }
}

impl From<IndirectPing> for Ping {
  fn from(ping: IndirectPing) -> Self {
    Self {
      seq_no: ping.seq_no,
      source: ping.source,
      target: ping.target,
    }
  }
}
