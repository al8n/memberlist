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
    LENGTH_SIZE
    + core::mem::size_of::<u32>() // incarnation
    + self.node.encoded_len() // node data
    + self.meta.len() // meta data
    + self.vsn.len() // vsn data
  }

  #[inline]
  pub(crate) fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u32(self.encoded_len() as u32);
    buf.put_slice(&self.vsn);
    buf.put_u32(self.incarnation);
    self.node.encode_to(buf);
    buf.put_slice(&self.meta);
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    if buf.remaining() < LENGTH_SIZE {
      return Err(DecodeError::Truncated(MessageType::Alive.as_err_str()));
    }

    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
      return Err(DecodeError::Truncated(MessageType::Alive.as_err_str()));
    }

    let mut vsn = [0; VSN_SIZE];
    buf.copy_to_slice(&mut vsn);

    let incarnation = buf.get_u32();
    let node = NodeId::decode_from(&mut buf)?;
    let node_len = node.encoded_len();
    let readed = VSN_SIZE + core::mem::size_of::<u32>() + node_len;
    if readed > len {
      return Err(DecodeError::Truncated(MessageType::Alive.as_err_str()));
    }

    let meta = buf.copy_to_bytes(len - readed);
    Ok(Self {
      vsn,
      incarnation,
      node,
      meta,
    })
  }

  #[cfg(feature = "async")]
  #[inline]
  pub(crate) async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> std::io::Result<Self> {
    use futures_util::io::AsyncReadExt;

    let mut len = [0; LENGTH_SIZE];
    r.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;

    let mut vsn = [0; VSN_SIZE];
    r.read_exact(&mut vsn).await?;

    let mut incarnation = [0; 4];
    r.read_exact(&mut incarnation).await?;
    let incarnation = u32::from_be_bytes(incarnation);

    let node = NodeId::decode_from_reader(r).await?;
    let node_len = node.encoded_len();
    let readed = VSN_SIZE + core::mem::size_of::<u32>() + node_len;
    if readed > len {
      return Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        DecodeError::Truncated(MessageType::Alive.as_err_str()),
      ));
    }

    if len - readed == 0 {
      return Ok(Self {
        vsn,
        incarnation,
        node,
        meta: Bytes::new(),
      });
    }

    let mut meta = vec![0; len - readed];
    r.read_exact(&mut meta).await?;
    Ok(Self {
      vsn,
      incarnation,
      node,
      meta: Bytes::from(meta),
    })
  }
}
