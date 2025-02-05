use crate::types::encode_data;

use super::{
  decode_data, decode_length_delimited, decode_varint, encode_length_delimited, encoded_data_len,
  encoded_length_delimited_len, merge, skip, split, DecodeError, DelegateVersion, EncodeError,
  Meta, ProtocolVersion, WireType,
};

use length_delimited::{encoded_u32_varint_len, InsufficientBuffer, LengthDelimitedEncoder};
use nodecraft::{CheapClone, Node};

/// Alive message
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Alive<I, A> {
  /// The incarnation of the alive message
  #[viewit(
    getter(const, attrs(doc = "Returns the incarnation of the alive message")),
    setter(
      const,
      attrs(doc = "Sets the incarnation of the alive message (Builder pattern)")
    )
  )]
  incarnation: u32,
  /// The meta of the alive message
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the meta of the alive message")
    ),
    setter(attrs(doc = "Sets the meta of the alive message (Builder pattern)"))
  )]
  meta: Meta,
  /// The node of the alive message
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the node of the alive message")
    ),
    setter(attrs(doc = "Sets the node of the alive message (Builder pattern)"))
  )]
  node: Node<I, A>,
  /// The protocol version of the alive message is speaking
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the protocol version of the alive message is speaking")
    ),
    setter(
      const,
      attrs(doc = "Sets the protocol version of the alive message is speaking (Builder pattern)")
    )
  )]
  protocol_version: ProtocolVersion,
  /// The delegate version of the alive message is speaking
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the delegate version of the alive message is speaking")
    ),
    setter(
      const,
      attrs(doc = "Sets the delegate version of the alive message is speaking (Builder pattern)")
    )
  )]
  delegate_version: DelegateVersion,
}

const INCARNATION_TAG: u8 = 1;
const META_TAG: u8 = 2;
const ID_TAG: u8 = 3;
const ADDR_TAG: u8 = 4;
const PROTOCOL_VERSION_TAG: u8 = 5;
const DELEGATE_VERSION_TAG: u8 = 6;

const INCARNATION_BYTE: u8 = merge(WireType::Varint, INCARNATION_TAG);
const META_BYTE: u8 = merge(WireType::LengthDelimited, META_TAG);
const PROTOCOL_VERSION_BYTE: u8 = merge(WireType::Byte, PROTOCOL_VERSION_TAG);
const DELEGATE_VERSION_BYTE: u8 = merge(WireType::Byte, DELEGATE_VERSION_TAG);

impl<I, A> Alive<I, A> {
  #[inline]
  const fn id_byte() -> u8
  where
    I: super::Data,
  {
    merge(I::WIRE_TYPE, ID_TAG)
  }

  #[inline]
  const fn addr_byte() -> u8
  where
    A: super::Data,
  {
    merge(A::WIRE_TYPE, ADDR_TAG)
  }

  /// Returns the encoded length of the alive message.
  #[inline]
  pub fn encoded_len(&self) -> usize
  where
    I: super::Data,
    A: super::Data,
  {
    let mut len = 1 + encoded_u32_varint_len(self.incarnation);

    if !self.meta.is_empty() {
      len += 1 + encoded_length_delimited_len(self.meta.len());
    }
    len += 1 + encoded_data_len(self.node.id());
    len += 1 + encoded_data_len(self.node.address());
    len += 1 + 1;
    len += 1 + 1;
    len
  }

  /// Encodes the alive message into the buffer.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>
  where
    I: super::Data,
    A: super::Data,
  {
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(
            InsufficientBuffer::with_information($this.encoded_len() as u64, $len as u64).into(),
          );
        }
      };
    }

    let len = buf.len();
    let mut offset = 0;

    bail!(self(offset, len));
    buf[offset] = INCARNATION_BYTE;
    offset += 1;
    offset += self
      .incarnation
      .encode(&mut buf[offset..])
      .map_err(|_| InsufficientBuffer::with_information(self.encoded_len() as u64, len as u64))?;

    if !self.meta.is_empty() {
      bail!(self(offset, len));
      buf[offset] = META_BYTE;
      offset += 1;
      offset += encode_length_delimited(self.meta.as_bytes(), &mut buf[offset..])
        .map_err(|_| InsufficientBuffer::with_information(self.encoded_len() as u64, len as u64))?;
    }

    bail!(self(offset, len));
    buf[offset] = Self::id_byte();
    offset += 1;
    offset += encode_data(self.node.id(), &mut buf[offset..])
      .map_err(|e| e.with_information(self.encoded_len() as u64, len as u64))?;

    bail!(self(offset, len));
    buf[offset] = Self::addr_byte();
    offset += 1;

    offset += encode_data(self.node.address(), &mut buf[offset..])
      .map_err(|e| e.with_information(self.encoded_len() as u64, len as u64))?;

    bail!(self(offset + 4, len));
    buf[offset] = PROTOCOL_VERSION_BYTE;
    buf[offset] = self.protocol_version.into();
    buf[offset] = DELEGATE_VERSION_BYTE;
    buf[offset] = self.delegate_version.into();
    offset += 1;

    Ok(offset)
  }

  #[inline]
  pub fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    I: super::Data,
    A: super::Data,
  {
    let mut offset = 0;
    let mut incarnation = None;
    let mut meta = None;
    let mut id = None;
    let mut addr = None;
    let mut protocol_version = None;
    let mut delegate_version = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        INCARNATION_BYTE => {
          let (bytes_read, value) = decode_varint::<u32>(WireType::Varint, &src[offset..])?;
          offset += bytes_read;
          incarnation = Some(value);
        }
        META_BYTE => {
          let (readed, data) = decode_length_delimited(WireType::LengthDelimited, &src[offset..])?;
          offset += readed;
          meta = Some(Meta::try_from(data).map_err(|e| DecodeError::new(e.to_string()))?);
        }
        DELEGATE_VERSION_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          delegate_version = Some(src[offset].into());
          offset += 1;
        }
        PROTOCOL_VERSION_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          protocol_version = Some(src[offset].into());
          offset += 1;
        }
        b if b == Self::id_byte() => {
          let (readed, data) = decode_data::<I>(&src[offset..])?;
          offset += readed;
          id = Some(data);
        }
        b if b == Self::addr_byte() => {
          let (readed, data) = decode_data::<A>(&src[offset..])?;
          offset += readed;
          addr = Some(data);
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
        incarnation: incarnation.ok_or_else(|| DecodeError::new("missing incarnation"))?,
        meta: meta.unwrap_or_default(),
        node: Node::new(
          id.ok_or_else(|| DecodeError::new("missing node id"))?,
          addr.ok_or_else(|| DecodeError::new("missing node address"))?,
        ),
        protocol_version: protocol_version.unwrap_or_default(),
        delegate_version: delegate_version.unwrap_or_default(),
      },
    ))
  }
}

impl<I, A> Alive<I, A> {
  /// Construct a new alive message with the given incarnation, meta, node, protocol version and delegate version.
  #[inline]
  pub const fn new(incarnation: u32, node: Node<I, A>) -> Self {
    Self {
      incarnation,
      meta: Meta::empty(),
      node,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
    }
  }

  /// Sets the incarnation of the alive message.
  #[inline]
  pub fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the meta of the alive message.
  #[inline]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the node of the alive message.
  #[inline]
  pub fn set_node(&mut self, node: Node<I, A>) -> &mut Self {
    self.node = node;
    self
  }

  /// Set the protocol version of the alive message is speaking.
  #[inline]
  pub fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Set the delegate version of the alive message is speaking.
  #[inline]
  pub fn set_delegate_version(&mut self, delegate_version: DelegateVersion) -> &mut Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for Alive<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      incarnation: self.incarnation,
      meta: self.meta.clone(),
      node: self.node.cheap_clone(),
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a, I, A> Arbitrary<'a> for Alive<I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self {
        incarnation: u.arbitrary()?,
        meta: u.arbitrary()?,
        node: {
          let id = u.arbitrary()?;
          let addr = u.arbitrary()?;
          Node::new(id, addr)
        },
        protocol_version: u.arbitrary()?,
        delegate_version: u.arbitrary()?,
      })
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::Arbitrary;

  impl<I, A> Arbitrary for Alive<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      Self {
        incarnation: u32::arbitrary(g),
        meta: Meta::arbitrary(g),
        node: Node::new(I::arbitrary(g), A::arbitrary(g)),
        protocol_version: ProtocolVersion::arbitrary(g),
        delegate_version: DelegateVersion::arbitrary(g),
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use arbitrary::{Arbitrary, Unstructured};

  use super::*;

  // #[test]
  // fn test_encode_decode() {
  //   for i in 0..100 {
  //     let alive = Alive::random(i);
  //     let mut buf = vec![0; alive.encoded_len()];
  //     let encoded_len = alive.encode(&mut buf).unwrap();
  //     assert_eq!(encoded_len, alive.encoded_len());
  //     let (decoded_len, decoded) = Alive::decode(&buf).unwrap();
  //     assert_eq!(decoded_len, encoded_len);
  //     assert_eq!(decoded, alive);
  //   }
  // }

  #[test]
  fn test_access() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);
    let mut data = Unstructured::new(&data);

    let mut alive = Alive::<String, SocketAddr>::arbitrary(&mut data).unwrap();
    alive.set_incarnation(1);
    assert_eq!(alive.incarnation(), 1);
    alive.set_meta(Meta::empty());
    assert_eq!(alive.meta(), &Meta::empty());
    alive.set_node(Node::new("a".into(), "127.0.0.1:8081".parse().unwrap()));
    assert_eq!(alive.node().id(), "a");
    alive.set_protocol_version(ProtocolVersion::V1);
    assert_eq!(alive.protocol_version(), ProtocolVersion::V1);
    alive.set_delegate_version(DelegateVersion::V1);
    assert_eq!(alive.delegate_version(), DelegateVersion::V1);
  }
}
