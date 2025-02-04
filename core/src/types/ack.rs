use bytes::Bytes;
use length_delimited::{encoded_u32_varint_len, InsufficientBuffer, Varint};

use super::{decode_length_delimited, decode_varint, merge, skip, split, DecodeError, WireType};

/// Ack response is sent for a ping
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ack {
  /// The sequence number of the ack
  #[viewit(
    getter(const, attrs(doc = "Returns the sequence number of the ack")),
    setter(
      const,
      attrs(doc = "Sets the sequence number of the ack (Builder pattern)")
    )
  )]
  sequence_number: u32,
  /// The payload of the ack
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the payload of the ack")),
    setter(attrs(doc = "Sets the payload of the ack (Builder pattern)"))
  )]
  payload: Bytes,
}

impl Ack {
  const SEQUENCE_NUMBER_TAG: u8 = 1;
  const SEQUENCE_NUMBER_BYTE: u8 = merge(WireType::Varint, Self::SEQUENCE_NUMBER_TAG);
  const PAYLOAD_TAG: u8 = 2;
  const PAYLOAD_BYTE: u8 = merge(WireType::LengthDelimited, Self::PAYLOAD_TAG);

  /// Returns the encoded length of the ack message
  #[inline]
  pub fn encoded_len(&self) -> usize {
    let sequence_number_len = 1 + encoded_u32_varint_len(self.sequence_number);
    let payload_len = 1 + encoded_u32_varint_len(self.payload.len() as u32) + self.payload.len();

    sequence_number_len + payload_len
  }

  /// Encodes the ack response into the given buffer
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, InsufficientBuffer> {
    let len = buf.len();
    if len < 2 {
      return Err(InsufficientBuffer::with_information(
        self.encoded_len() as u64,
        len as u64,
      ));
    }

    let mut offset = 0;
    buf[offset] = Self::SEQUENCE_NUMBER_BYTE;
    offset += 1;
    offset += self
      .sequence_number
      .encode(&mut buf[offset..])
      .map_err(|_| InsufficientBuffer::with_information(self.encoded_len() as u64, len as u64))?;

    if offset + 1 >= len {
      return Err(InsufficientBuffer::with_information(
        self.encoded_len() as u64,
        len as u64,
      ));
    }

    buf[offset] = Self::PAYLOAD_BYTE;
    offset += 1;

    let payload_len = self.payload.len();
    offset += (payload_len as u32)
      .encode(&mut buf[offset..])
      .map_err(|_| InsufficientBuffer::new())?;
    if offset + payload_len > len {
      return Err(InsufficientBuffer::with_information(
        self.encoded_len() as u64,
        len as u64,
      ));
    }

    buf[offset..offset + payload_len].copy_from_slice(&self.payload);
    offset += payload_len;
    Ok(offset)
  }

  /// Decodes the whole ack response from the given buffer
  #[inline]
  pub fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError> {
    let mut offset = 0;
    let mut sequence_number = None;
    let mut payload = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        Self::SEQUENCE_NUMBER_BYTE => {
          let (bytes_read, value) = decode_varint::<u32>(WireType::Varint, &src[offset..])?;
          offset += bytes_read;
          sequence_number = Some(value);
        }
        Self::PAYLOAD_BYTE => {
          let (readed, data) = decode_length_delimited(WireType::LengthDelimited, &src[offset..])?;
          offset += readed;
          payload = Some(Bytes::copy_from_slice(data));
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      Self {
        sequence_number: sequence_number.unwrap_or(0),
        payload: payload.unwrap_or_default(),
      },
    ))
  }

  /// Decodes the sequence number from the given buffer
  #[inline]
  pub fn decode_sequence_number(src: &[u8]) -> Result<(usize, u32), DecodeError> {
    let mut offset = 0;
    let mut sequence_number = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let (wire_type, tag) = split(src[offset]);
      offset += 1;

      let wire_type = WireType::try_from(wire_type)
        .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
      match tag {
        Self::SEQUENCE_NUMBER_TAG => {
          let (bytes_read, value) = decode_varint::<u32>(wire_type, &src[offset..])?;
          offset += bytes_read;
          sequence_number = Some(value);
        }
        _ => {
          // Skip unknown fields
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    // Ensure the sequence_number was found
    Ok((offset, sequence_number.unwrap_or(0)))
  }

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

/// Nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
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
  const SEQUENCE_NUMBER_TAG: u8 = 1;
  const SEQUENCE_NUMBER_BYTE: u8 = merge(WireType::Varint, Self::SEQUENCE_NUMBER_TAG);

  /// Returns the encoded length of the nack message
  #[inline]
  pub const fn encoded_len(&self) -> usize {
    1 + encoded_u32_varint_len(self.sequence_number)
  }

  /// Encodes the nack response into the given buffer
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, InsufficientBuffer> {
    let len = buf.len();
    if len < 1 {
      return Err(InsufficientBuffer::with_information(
        self.encoded_len() as u64,
        len as u64,
      ));
    }

    let mut offset = 0;
    buf[offset] = Self::SEQUENCE_NUMBER_BYTE;
    offset += 1;
    offset += self
      .sequence_number
      .encode(&mut buf[offset..])
      .map_err(|_| InsufficientBuffer::with_information(self.encoded_len() as u64, len as u64))?;
    Ok(offset)
  }

  /// Decodes the whole nack response from the given buffer
  #[inline]
  pub fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError> {
    let mut offset = 0;
    let mut sequence_number = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        Self::SEQUENCE_NUMBER_BYTE => {
          let (bytes_read, value) = decode_varint::<u32>(WireType::Varint, &src[offset..])?;
          offset += bytes_read;
          sequence_number = Some(value);
        }
        _ => {
          let (wire_type, _) = split(src[offset]);
          let wire_type = WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      Self {
        sequence_number: sequence_number.unwrap_or(0),
      },
    ))
  }

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

  #[test]
  fn test_access() {
    let mut ack = Ack::random(100);
    ack.set_payload(Bytes::from_static(b"hello world"));
    ack.set_sequence_number(100);
    assert_eq!(ack.sequence_number(), 100);
    assert_eq!(ack.payload(), &Bytes::from_static(b"hello world"));

    let mut nack = Nack::random();
    nack.set_sequence_number(100);
    assert_eq!(nack.sequence_number(), 100);
  }
}
