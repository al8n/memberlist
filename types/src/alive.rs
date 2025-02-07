use super::{
  merge, skip, split, Data, DecodeError, DelegateVersion, EncodeError, Meta, ProtocolVersion,
  WireType,
};

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
}

impl<I, A> Data for Alive<I, A>
where
  I: Data,
  A: Data,
{
  fn encoded_len(&self) -> usize {
    let mut len = 1 + self.incarnation.encoded_len();
    len += 1 + self.meta.encoded_len_with_length_delimited();
    len += 1 + self.node.id().encoded_len_with_length_delimited();
    len += 1 + self.node.address().encoded_len_with_length_delimited();
    len += 1 + 1;
    len += 1 + 1;
    len
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          println!("{}", $offset);
          return Err(EncodeError::insufficient_buffer($this.encoded_len(), $len).into());
        }
      };
    }

    let len = buf.len();
    let mut offset = 0;

    bail!(self(offset, len));
    buf[offset] = INCARNATION_BYTE;
    offset += 1;
    offset += self.incarnation.encode(&mut buf[offset..])?;

    bail!(self(offset, len));
    buf[offset] = META_BYTE;
    offset += 1;
    offset += self
      .meta
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = Self::id_byte();
    offset += 1;
    offset += self
      .node
      .id()
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = Self::addr_byte();
    offset += 1;

    offset += self
      .node
      .address()
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = PROTOCOL_VERSION_BYTE;
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = self.protocol_version.into();
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = DELEGATE_VERSION_BYTE;
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = self.delegate_version.into();
    offset += 1;

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq(offset, self.encoded_len());

    Ok(offset)
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
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
          let (bytes_read, value) = u32::decode(&src[offset..])?;
          offset += bytes_read;
          incarnation = Some(value);
        }
        META_BYTE => {
          let (readed, data) = Meta::decode_length_delimited(&src[offset..])?;
          offset += readed;
          meta = Some(data);
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
          let (readed, data) = I::decode_length_delimited(&src[offset..])?;
          offset += readed;
          id = Some(data);
        }
        b if b == Self::addr_byte() => {
          let (readed, data) = A::decode_length_delimited(&src[offset..])?;
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
