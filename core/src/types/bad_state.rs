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
        LENGTH_SIZE
        + core::mem::size_of::<u32>() // incarnation
        + self.node.encoded_len() // node data
        + self.from.encoded_len() // from data
      }

      #[inline]
      pub(crate) fn encode_to_msg(&self) -> Message {
        let encoded_len = self.encoded_len();
        let mut buf = BytesMut::with_capacity(encoded_len + MessageType::SIZE + LENGTH_SIZE);
        buf.put_u8(MessageType::$name as u8);
        buf.put_u32(encoded_len as u32);
        self.encode_to(&mut buf);
        Message(buf)
      }

      #[inline]
      pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
        buf.put_u32(self.encoded_len() as u32);
        buf.put_u32(self.incarnation);
        self.node.encode_to(buf);
        self.from.encode_to(buf);
      }

      #[inline]
      pub(crate) fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
        if buf.remaining() < LENGTH_SIZE {
          return Err(DecodeError::Truncated(MessageType::$name.as_err_str()));
        }

        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
          return Err(DecodeError::Truncated(MessageType::$name.as_err_str()));
        }
        let incarnation = buf.get_u32();
        let node = NodeId::decode_from(buf)?;
        let from_len = buf.get_u32() as usize;
        let from = NodeId::decode_from(buf)?;
        Ok(Self {
          incarnation,
          node,
          from,
        })
      }

      #[inline]
      pub(crate) async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Self> {
        use futures_util::io::AsyncReadExt;
        let mut len_buf = [0u8; LENGTH_SIZE];
        r.read_exact(&mut len_buf).await?;
        let mut incarnation = [0u8; core::mem::size_of::<u32>()];
        r.read_exact(&mut incarnation).await?;
        let node = NodeId::decode_from_reader(r).await?;
        let from = NodeId::decode_from_reader(r).await?;
        Ok(Self {
          incarnation: u32::from_be_bytes(incarnation),
          node,
          from,
        })
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
