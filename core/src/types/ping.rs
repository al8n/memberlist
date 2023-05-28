use super::*;

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Ping {
  seq_no: u32,
  /// NodeId is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  target: Option<NodeId>,

  /// Source node, used for a direct reply
  source: NodeId,
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
    LENGTH_SIZE
    + core::mem::size_of::<u32>() // seq_no
    + match &self.target {
      Some(target) => 1 + target.encoded_len(),
      None => 1,
    }
    + self.source.encoded_len()
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u32(self.encoded_len() as u32);
    buf.put_u32(self.seq_no);
    match &self.target {
      Some(target) => {
        buf.put_u8(1);
        target.encode_to(buf);
      }
      None => {
        buf.put_u8(0);
      }
    }
    self.source.encode_to(buf);
  }

  #[inline]
  pub fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    if buf.remaining() < LENGTH_SIZE {
      return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
      return Err(DecodeError::Truncated(MessageType::Ping.as_err_str()));
    }
    let seq_no = buf.get_u32();
    let target = match buf.get_u8() {
      0 => None,
      1 => Some(NodeId::decode_from(buf)?),
      b => return Err(DecodeError::UnknownMarkBit(b)),
    };
    let source = NodeId::decode_from(buf)?;
    Ok(Self {
      seq_no,
      target,
      source,
    })
  }

  #[cfg(feature = "async")]
  #[inline]
  pub async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> Result<Self, std::io::Error> {
    use futures_util::io::AsyncReadExt;
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;

    let mut seq_no = [0u8; 4];
    r.read_exact(&mut seq_no).await?;
    let seq_no = u32::from_be_bytes(seq_no);

    let mut mark = [0u8; 1];
    r.read_exact(&mut mark).await?;

    let target = match mark[0] {
      0 => None,
      1 => NodeId::decode_from_reader(r).await.map(Some)?,
      b => {
        return Err(Error::new(
          ErrorKind::InvalidData,
          DecodeError::UnknownMarkBit(b),
        ))
      }
    };

    let source = NodeId::decode_from_reader(r).await?;

    Ok(Self {
      seq_no,
      target,
      source,
    })
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
    self.ping.with_target(target);
    self
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    1 + self.ping.encoded_len()
  }

  #[inline]
  pub fn encode(&self) -> Bytes {
    let mut buf = BytesMut::with_capacity(self.encoded_len());
    self.encode_to(&mut buf);
    buf.freeze()
  }

  #[inline]
  pub fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u32(self.encoded_len() as u32);
    buf.put_u8(self.nack as u8);
    self.ping.encode_to(buf);
  }

  #[inline]
  pub fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    if buf.remaining() < LENGTH_SIZE {
      return Err(DecodeError::Truncated(
        MessageType::IndirectPing.as_err_str(),
      ));
    }
    let len = buf.get_u32() as usize;

    if buf.remaining() < len {
      return Err(DecodeError::Truncated(
        MessageType::IndirectPing.as_err_str(),
      ));
    }

    let nack = match buf.get_u8() {
      0 => false,
      1 => true,
      b => return Err(DecodeError::UnknownMarkBit(b)),
    };

    Ping::decode_from(buf).map(|ping| Self { nack, ping })
  }

  #[cfg(feature = "async")]
  #[inline]
  pub async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> std::io::Result<Self> {
    use futures_util::io::AsyncReadExt;
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;

    let mut nack = [0u8; 1];
    r.read_exact(&mut nack).await?;

    Ping::decode_from_reader(r).await.and_then(|ping| {
      let nack = match nack[0] {
        0 => false,
        1 => true,
        b => {
          return Err(Error::new(
            ErrorKind::InvalidData,
            DecodeError::UnknownMarkBit(b),
          ))
        }
      };
      Ok(Self { nack, ping })
    })
  }
}
