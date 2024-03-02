use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use transformable::{utils::*, Transformable};

const MAX_INLINED_BYTES: usize = 64;

/// Ack response is sent for a ping
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(feature = "rkyv", archive_attr(derive(Debug, PartialEq, Eq, Hash)))]
pub struct Ack {
  #[viewit(
    getter(const, attrs(doc = "Returns the sequence number of the ack")),
    setter(
      const,
      attrs(doc = "Sets the sequence number of the ack (Builder pattern)")
    )
  )]
  sequence_number: u32,
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the payload of the ack")),
    setter(attrs(doc = "Sets the payload of the ack (Builder pattern)"))
  )]
  payload: Bytes,
}

impl Ack {
  /// Create a new ack response with the given sequence number and empty payload.
  #[inline]
  pub const fn new(sequence_number: u32) -> Self {
    Self {
      sequence_number,
      payload: Bytes::new(),
    }
  }

  /// Sets the sequence number of the ack
  #[inline]
  pub fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the payload of the ack
  #[inline]
  pub fn set_payload(&mut self, payload: Bytes) -> &mut Self {
    self.payload = payload;
    self
  }

  /// Consumes the [`Ack`] and returns the sequence number and payload
  #[inline]
  pub fn into_components(self) -> (u32, Bytes) {
    (self.sequence_number, self.payload)
  }
}

/// Error that can occur when transforming an ack response.
#[derive(Debug, thiserror::Error)]
pub enum AckTransformError {
  /// The buffer did not contain enough bytes to encode an ack response.
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// The buffer did not contain enough bytes to decode an ack response.
  #[error("the buffer did not contain enough bytes to decode Ack")]
  NotEnoughBytes,
  /// Varint decoding error
  #[error("fail to decode sequence number: {0}")]
  DecodeVarint(#[from] DecodeVarintError),
  /// Varint encoding error
  #[error("fail to encode sequence number: {0}")]
  EncodeVarint(#[from] EncodeVarintError),
}

impl Transformable for Ack {
  type Error = AckTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if encoded_len > dst.len() {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(dst, encoded_len as u32);
    offset += core::mem::size_of::<u32>();
    NetworkEndian::write_u32(&mut dst[offset..], self.sequence_number);
    offset += core::mem::size_of::<u32>();

    let payload_size = self.payload.len();
    if !self.payload.is_empty() {
      dst[offset..offset + payload_size].copy_from_slice(&self.payload);
      offset += payload_size;
    }

    debug_assert_eq!(
      offset, encoded_len,
      "expect bytes written ({encoded_len}) not match actual bytes writtend ({offset})"
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    core::mem::size_of::<u32>() + core::mem::size_of::<u32>() + self.payload.len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let mut offset = 0;
    if core::mem::size_of::<u32>() > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let total_len = NetworkEndian::read_u32(&src[offset..]);
    offset += core::mem::size_of::<u32>();
    let sequence_number = NetworkEndian::read_u32(&src[offset..]);
    offset += core::mem::size_of::<u32>();

    if total_len as usize == core::mem::size_of::<u32>() {
      return Ok((
        offset,
        Self {
          sequence_number,
          payload: Bytes::new(),
        },
      ));
    }

    if total_len as usize - core::mem::size_of::<u32>() > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let payload = Bytes::copy_from_slice(&src[offset..total_len as usize]);
    Ok((
      total_len as usize,
      Self {
        sequence_number,
        payload,
      },
    ))
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf)?;
    let total_len = NetworkEndian::read_u32(&buf) as usize;
    let sequence_number = NetworkEndian::read_u32(&buf[core::mem::size_of::<u32>()..]);

    if total_len == 2 * core::mem::size_of::<u32>() {
      return Ok((
        total_len,
        Self {
          sequence_number,
          payload: Bytes::new(),
        },
      ));
    }

    let payload_len = total_len - core::mem::size_of::<u32>();
    if payload_len <= MAX_INLINED_BYTES {
      let mut buf = [0; MAX_INLINED_BYTES];
      reader.read_exact(&mut buf[..payload_len])?;
      let payload = Bytes::copy_from_slice(&buf[..payload_len]);
      Ok((
        total_len,
        Self {
          sequence_number,
          payload,
        },
      ))
    } else {
      let mut payload = vec![0; payload_len];
      reader.read_exact(&mut payload)?;
      Ok((
        total_len,
        Self {
          sequence_number,
          payload: payload.into(),
        },
      ))
    }
  }

  async fn decode_from_async_reader<R: futures::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    use futures::AsyncReadExt;

    let mut buf = [0; 8];
    reader.read_exact(&mut buf).await?;

    let total_len = NetworkEndian::read_u32(&buf) as usize;
    let sequence_number = NetworkEndian::read_u32(&buf[core::mem::size_of::<u32>()..]);

    if total_len == 2 * core::mem::size_of::<u32>() {
      return Ok((
        total_len,
        Self {
          sequence_number,
          payload: Bytes::new(),
        },
      ));
    }

    let payload_len = total_len - core::mem::size_of::<u32>();
    if payload_len <= MAX_INLINED_BYTES {
      let mut buf = [0; MAX_INLINED_BYTES];
      reader.read_exact(&mut buf[..payload_len]).await?;
      let payload = Bytes::copy_from_slice(&buf[..payload_len]);
      Ok((
        total_len,
        Self {
          sequence_number,
          payload,
        },
      ))
    } else {
      let mut payload = vec![0; payload_len];
      reader.read_exact(&mut payload).await?;
      Ok((
        total_len,
        Self {
          sequence_number,
          payload: payload.into(),
        },
      ))
    }
  }
}

/// Nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(derive(Debug, Clone, PartialEq, Eq, Hash), repr(transparent))
)]
#[repr(transparent)]
pub struct Nack {
  #[viewit(
    getter(const, attrs(doc = "Returns the sequence number of the nack")),
    setter(
      const,
      attrs(doc = "Sets the sequence number of the nack (Builder pattern)")
    )
  )]
  sequence_number: u32,
}

impl Nack {
  /// Create a new nack response with the given sequence number.
  #[inline]
  pub const fn new(sequence_number: u32) -> Self {
    Self { sequence_number }
  }

  /// Sets the sequence number of the nack response
  #[inline]
  pub fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }
}

impl Transformable for Nack {
  type Error = <u32 as Transformable>::Error;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    <u32 as Transformable>::encode(&self.sequence_number, dst)
  }

  fn encoded_len(&self) -> usize {
    <u32 as Transformable>::encoded_len(&self.sequence_number)
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let (n, sequence_number) = <u32 as Transformable>::decode(src)?;
    Ok((n, Self { sequence_number }))
  }

  fn encode_to_vec(&self) -> Result<Vec<u8>, Self::Error> {
    <u32 as Transformable>::encode_to_vec(&self.sequence_number)
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<usize> {
    <u32 as Transformable>::encode_to_async_writer(&self.sequence_number, writer).await
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
    <u32 as Transformable>::encode_to_writer(&self.sequence_number, writer)
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <u32 as Transformable>::decode_from_reader(reader)
      .map(|(n, sequence_number)| (n, Self { sequence_number }))
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <u32 as Transformable>::decode_from_async_reader(reader)
      .await
      .map(|(n, sequence_number)| (n, Self { sequence_number }))
  }
}

#[cfg(test)]
const _: () = {
  use rand::random;

  impl Ack {
    /// Create a new ack response with the given sequence number and random payload.
    #[inline]
    pub fn random(payload_size: usize) -> Self {
      let sequence_number = random();
      let payload = (0..payload_size)
        .map(|_| random())
        .collect::<Vec<_>>()
        .into();
      Self {
        sequence_number,
        payload,
      }
    }
  }

  impl Nack {
    /// Create a new nack response with the given sequence number.
    #[inline]
    pub fn random() -> Self {
      Self {
        sequence_number: random(),
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ack_response_encode_decode() {
    for i in 0..100 {
      // Generate and test 100 random instances
      let ack_response = Ack::random(i);
      let mut buf = vec![0; ack_response.encoded_len()];
      let encoded = ack_response.encode(&mut buf).unwrap();
      assert_eq!(encoded, buf.len());
      let (read, decoded) = Ack::decode(&buf).unwrap();
      assert_eq!(read, buf.len());
      assert_eq!(ack_response.sequence_number, decoded.sequence_number);
      assert_eq!(ack_response.payload, decoded.payload);
    }
  }

  #[test]
  fn test_nack_response_encode_decode() {
    for _ in 0..100 {
      // Generate and test 100 random instances
      let nack_response = Nack::random();
      let mut buf = vec![0; nack_response.encoded_len()];
      let encoded = nack_response.encode(&mut buf).unwrap();
      assert_eq!(encoded, buf.len());
      let (read, decoded) = Nack::decode(&buf).unwrap();
      assert_eq!(read, buf.len());
      assert_eq!(nack_response.sequence_number, decoded.sequence_number);
    }
  }
}
