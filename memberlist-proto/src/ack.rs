use bytes::Bytes;

use super::{Data, DataRef, DecodeError, EncodeError, WireType, merge, skip, split};

/// Ack response is sent for a ping
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
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
  #[cfg_attr(any(feature = "arbitrary", test), arbitrary(with = crate::arbitrary_impl::bytes))]
  payload: Bytes,
}

impl Ack {
  const SEQUENCE_NUMBER_TAG: u8 = 1;
  const SEQUENCE_NUMBER_BYTE: u8 = merge(WireType::Varint, Self::SEQUENCE_NUMBER_TAG);
  const PAYLOAD_TAG: u8 = 2;
  const PAYLOAD_BYTE: u8 = merge(WireType::LengthDelimited, Self::PAYLOAD_TAG);

  /// Decodes the sequence number from the given buffer
  #[inline]
  pub fn decode_sequence_number(src: &[u8]) -> Result<(usize, u32), DecodeError> {
    let mut offset = 0;
    let mut sequence_number = None;

    while offset < src.len() {
      match src[offset] {
        Self::SEQUENCE_NUMBER_BYTE => {
          offset += 1;
          let (bytes_read, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
          offset += bytes_read;
          sequence_number = Some(value);
        }
        b => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
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

impl Data for Ack {
  type Ref<'a> = AckRef<'a>;

  #[inline]
  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(Self {
      sequence_number: val.sequence_number,
      payload: Bytes::copy_from_slice(val.payload),
    })
  }

  fn encoded_len(&self) -> usize {
    let mut len = 1 + self.sequence_number.encoded_len();
    let payload_len = self.payload.len();
    if payload_len != 0 {
      len += 1 + self.payload.encoded_len_with_length_delimited();
    }
    len
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    macro_rules! bail {
      ($offset:expr, $remaining:ident) => {
        if $offset >= $remaining {
          return Err(EncodeError::insufficient_buffer(
            self.encoded_len(),
            $remaining,
          ));
        }
      };
    }

    let len = buf.len();
    let mut offset = 0;
    bail!(offset, len);
    buf[offset] = Self::SEQUENCE_NUMBER_BYTE;
    offset += 1;
    offset += self
      .sequence_number
      .encode(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    let payload_len = self.payload.len();
    if payload_len != 0 {
      bail!(offset, len);
      buf[offset] = Self::PAYLOAD_BYTE;
      offset += 1;
      offset += self
        .payload
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(self.encoded_len(), len))?;
    }

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
    Ok(offset)
  }
}

/// The reference to an [`Ack`] message
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct AckRef<'a> {
  sequence_number: u32,
  payload: &'a [u8],
}

impl<'a> AckRef<'a> {
  /// Create a new ack reference with the given sequence number and payload
  #[inline]
  pub const fn new(sequence_number: u32, payload: &'a [u8]) -> Self {
    Self {
      sequence_number,
      payload,
    }
  }

  /// Returns the sequence number of the ack
  #[inline]
  pub const fn sequence_number(&self) -> u32 {
    self.sequence_number
  }

  /// Returns the payload of the ack
  #[inline]
  pub const fn payload(&self) -> &'a [u8] {
    self.payload
  }
}

impl<'a> DataRef<'a, Ack> for AckRef<'a> {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let mut sequence_number = None;
    let mut payload = None;

    while offset < src.len() {
      // Parse the tag and wire type
      match src[offset] {
        Ack::SEQUENCE_NUMBER_BYTE => {
          if sequence_number.is_some() {
            return Err(DecodeError::duplicate_field(
              "Ack",
              "sequence_number",
              Ack::SEQUENCE_NUMBER_TAG,
            ));
          }
          offset += 1;
          let (bytes_read, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
          offset += bytes_read;
          sequence_number = Some(value);
        }
        Ack::PAYLOAD_BYTE => {
          if payload.is_some() {
            return Err(DecodeError::duplicate_field(
              "Ack",
              "payload",
              Ack::PAYLOAD_TAG,
            ));
          }
          offset += 1;
          let (readed, data) = <&[u8] as DataRef<Bytes>>::decode_length_delimited(&src[offset..])?;
          offset += readed;
          payload = Some(data);
        }
        b => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      AckRef {
        sequence_number: sequence_number.unwrap_or(0),
        payload: payload.unwrap_or_default(),
      },
    ))
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
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
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

impl<'a> DataRef<'a, Self> for Nack {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut sequence_number = None;
    let mut offset = 0;
    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        Self::SEQUENCE_NUMBER_BYTE => {
          if sequence_number.is_some() {
            return Err(DecodeError::duplicate_field(
              "Nack",
              "sequence_number",
              Self::SEQUENCE_NUMBER_TAG,
            ));
          }

          let (bytes_read, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
          offset += bytes_read;
          sequence_number = Some(value);
        }
        _ => {
          let (wire_type, _) = split(src[offset]);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
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
}

impl Data for Nack {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    1 + self.sequence_number.encoded_len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut offset = 0;
    if buf.is_empty() {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), 0));
    }
    buf[offset] = Self::SEQUENCE_NUMBER_BYTE;
    offset += 1;
    offset += self.sequence_number.encode(&mut buf[offset..])?;
    #[cfg(debug_assertions)]
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
    Ok(offset)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use arbitrary::{Arbitrary, Unstructured};

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
