use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use transformable::{utils::*, Transformable};

/// Ack response is sent for a ping
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(feature = "rkyv", archive_attr(derive(Debug, PartialEq, Eq, Hash)))]
pub struct Ack {
  seq_no: u32,
  payload: Bytes,
}

impl Ack {
  /// Create a new ack response with the given sequence number and empty payload.
  #[inline]
  pub const fn new(seq_no: u32) -> Self {
    Self {
      seq_no,
      payload: Bytes::new(),
    }
  }
}

/// Error that can occur when transforming an ack response.
#[derive(Debug, thiserror::Error)]
pub enum AckTransformError {
  #[error("encode buffer too small")]
  BufferTooSmall,
  #[error("the buffer did not contain enough bytes to decode Ack")]
  NotEnoughBytes,
  #[error("fail to decode sequence number: {0}")]
  DecodeVarint(#[from] DecodeVarintError),
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

    NetworkEndian::write_u32(dst, self.seq_no);
    let mut offset = core::mem::size_of::<u32>();

    if !self.payload.is_empty() {
      dst[offset] = 1;
      offset += 1;
      let payload_len = self.payload.len() as u32;
      NetworkEndian::write_u32(&mut dst[offset..], payload_len);
      offset += core::mem::size_of::<u32>();
      dst[offset..offset + payload_len as usize].copy_from_slice(&self.payload);
      offset += payload_len as usize;
    } else {
      dst[offset] = 0;
      offset += 1;
    }
    debug_assert_eq!(
      offset, encoded_len,
      "expect bytes written ({encoded_len}) not match actual bytes writtend ({offset})"
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    core::mem::size_of::<u32>()
      + 1
      + if self.payload.is_empty() {
        0
      } else {
        core::mem::size_of::<u32>() + self.payload.len()
      }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let mut offset = 0;
    if core::mem::size_of::<u32>() + 1 > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let seq_no = NetworkEndian::read_u32(&src[offset..]);
    offset += core::mem::size_of::<u32>();

    let is_payload_empty = src[offset] == 0;
    offset += 1;

    if is_payload_empty {
      Ok((offset, Self::new(seq_no)))
    } else {
      if offset + core::mem::size_of::<u32>() > src.len() {
        return Err(Self::Error::NotEnoughBytes);
      }
      let payload_len = NetworkEndian::read_u32(&src[offset..]);
      offset += core::mem::size_of::<u32>();
      if offset + payload_len as usize > src.len() {
        return Err(Self::Error::NotEnoughBytes);
      }
      let payload = Bytes::copy_from_slice(&src[offset..offset + payload_len as usize]);
      offset += payload_len as usize;
      Ok((offset, Self { seq_no, payload }))
    }
  }
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
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
  seq_no: u32,
}

impl Nack {
  /// Create a new nack response with the given sequence number.
  #[inline]
  pub const fn new(seq_no: u32) -> Self {
    Self { seq_no }
  }
}

impl Transformable for Nack {
  type Error = <u32 as Transformable>::Error;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    <u32 as Transformable>::encode(&self.seq_no, dst)
  }

  fn encoded_len(&self) -> usize {
    <u32 as Transformable>::encoded_len(&self.seq_no)
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let (n, seq_no) = <u32 as Transformable>::decode(src)?;
    Ok((n, Self { seq_no }))
  }

  fn encode_to_vec(&self) -> Result<Vec<u8>, Self::Error> {
    <u32 as Transformable>::encode_to_vec(&self.seq_no)
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<usize> {
    <u32 as Transformable>::encode_to_async_writer(&self.seq_no, writer).await
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
    <u32 as Transformable>::encode_to_writer(&self.seq_no, writer)
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <u32 as Transformable>::decode_from_reader(reader).map(|(n, seq_no)| (n, Self { seq_no }))
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    <u32 as Transformable>::decode_from_async_reader(reader)
      .await
      .map(|(n, seq_no)| (n, Self { seq_no }))
  }
}

#[cfg(test)]
const _: () = {
  use rand::random;

  impl Ack {
    /// Create a new ack response with the given sequence number and random payload.
    #[inline]
    pub fn random(payload_size: usize) -> Self {
      let seq_no = random();
      let payload = (0..payload_size)
        .map(|_| random())
        .collect::<Vec<_>>()
        .into();
      Self { seq_no, payload }
    }
  }

  impl Nack {
    /// Create a new nack response with the given sequence number.
    #[inline]
    pub fn random() -> Self {
      Self { seq_no: random() }
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
      assert_eq!(ack_response.seq_no, decoded.seq_no);
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
      assert_eq!(nack_response.seq_no, decoded.seq_no);
    }
  }
}
