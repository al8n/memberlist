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
    if self.payload.is_empty() {
      return sequence_number_len;
    }

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

    if self.payload.is_empty() {
      return Ok(offset);
    }

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

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a> Arbitrary<'a> for Ack {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self {
        sequence_number: u.arbitrary()?,
        payload: u.arbitrary::<Vec<u8>>()?.into(),
      })
    }
  }

  impl<'a> Arbitrary<'a> for Nack {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self {
        sequence_number: u.arbitrary()?,
      })
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for Ack {
    fn arbitrary(g: &mut Gen) -> Self {
      Self {
        sequence_number: u32::arbitrary(g),
        payload: Vec::<u8>::arbitrary(g).into(),
      }
    }
  }

  impl Arbitrary for Nack {
    fn arbitrary(g: &mut Gen) -> Self {
      Self {
        sequence_number: u32::arbitrary(g),
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;
  use arbitrary::{Arbitrary, Unstructured};
  use quickcheck_macros::quickcheck;

  #[quickcheck]
  fn fuzzy_ack_encode_decode(ack: Ack) -> bool {
    let mut buf = vec![0; ack.encoded_len()];
    let Ok(written) = ack.encode(&mut buf) else {
      return false;
    };
    let Ok((readed, decoded)) = Ack::decode(&buf) else {
      return false;
    };
    ack == decoded && written == readed
  }

  #[quickcheck]
  fn fuzzy_nack_encode_decode(nack: Nack) -> bool {
    let mut buf = vec![0; nack.encoded_len()];
    let Ok(written) = nack.encode(&mut buf) else {
      return false;
    };
    let Ok((readed, decoded)) = Nack::decode(&buf) else {
      return false;
    };
    nack == decoded && written == readed
  }

  #[test]
  fn test_access() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);
    let mut data = Unstructured::new(&data);
    let mut ack = Ack::arbitrary(&mut data).unwrap();
    ack.set_payload(Bytes::from_static(b"hello world"));
    ack.set_sequence_number(100);
    assert_eq!(ack.sequence_number(), 100);
    assert_eq!(ack.payload(), &Bytes::from_static(b"hello world"));

    let mut nack = Nack::arbitrary(&mut data).unwrap();
    nack.set_sequence_number(100);
    assert_eq!(nack.sequence_number(), 100);
  }
}
