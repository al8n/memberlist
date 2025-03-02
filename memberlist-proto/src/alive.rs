use super::{
  Data, DataRef, DecodeError, DelegateVersion, EncodeError, Meta, ProtocolVersion, WireType, merge,
  skip, split,
};

use nodecraft::{CheapClone, Node};

/// Alive message
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
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
const NODE_TAG: u8 = 3;
const PROTOCOL_VERSION_TAG: u8 = 4;
const DELEGATE_VERSION_TAG: u8 = 5;

const INCARNATION_BYTE: u8 = merge(WireType::Varint, INCARNATION_TAG);
const META_BYTE: u8 = merge(WireType::LengthDelimited, META_TAG);
const PROTOCOL_VERSION_BYTE: u8 = merge(WireType::Byte, PROTOCOL_VERSION_TAG);
const DELEGATE_VERSION_BYTE: u8 = merge(WireType::Byte, DELEGATE_VERSION_TAG);

impl<I, A> Alive<I, A> {
  #[inline]
  const fn node_byte() -> u8
  where
    I: super::Data,
    A: super::Data,
  {
    merge(WireType::LengthDelimited, NODE_TAG)
  }
}

impl<I, A> Data for Alive<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = AliveRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Meta::from_ref(val.meta)
      .and_then(|meta| Node::<I, A>::from_ref(val.node).map(|node| (meta, node)))
      .map(|(meta, node)| Self {
        incarnation: val.incarnation,
        meta,
        node,
        protocol_version: val.protocol_version,
        delegate_version: val.delegate_version,
      })
  }

  fn encoded_len(&self) -> usize {
    let mut len = 1 + self.incarnation.encoded_len();
    let meta_len = self.meta.len();
    if meta_len != 0 {
      len += 1 + self.meta.encoded_len_with_length_delimited();
    }
    len += 1 + self.node.encoded_len_with_length_delimited();
    len += 1 + 1;
    len += 1 + 1;
    len
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
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

    let meta_len = self.meta.len();
    if meta_len != 0 {
      bail!(self(offset, len));
      buf[offset] = META_BYTE;
      offset += 1;
      offset += self
        .meta
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(self.encoded_len(), len))?;
    }

    bail!(self(offset, len));
    buf[offset] = Self::node_byte();
    offset += 1;
    offset += self
      .node
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
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len());

    Ok(offset)
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

/// The reference type of [`Alive`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct AliveRef<'a, I, A> {
  incarnation: u32,
  meta: &'a [u8],
  node: Node<I, A>,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl<'a, I, A> AliveRef<'a, I, A> {
  /// Returns the incarnation of the alive message.
  #[inline]
  pub const fn incarnation(&self) -> u32 {
    self.incarnation
  }

  /// Returns the meta of the alive message.
  #[inline]
  pub const fn meta(&self) -> &'a [u8] {
    self.meta
  }

  /// Returns the node of the alive message.
  #[inline]
  pub const fn node(&self) -> &Node<I, A> {
    &self.node
  }

  /// Returns the protocol version of the alive message is speaking.
  #[inline]
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  /// Returns the delegate version of the alive message is speaking.
  #[inline]
  pub const fn delegate_version(&self) -> DelegateVersion {
    self.delegate_version
  }
}

impl<'a, I, A> DataRef<'a, Alive<I, A>> for AliveRef<'a, I::Ref<'a>, A::Ref<'a>>
where
  I: Data,
  A: Data,
{
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let mut incarnation = None;
    let mut meta = None;
    let mut node = None;
    let mut protocol_version = None;
    let mut delegate_version = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        INCARNATION_BYTE => {
          if incarnation.is_some() {
            return Err(DecodeError::duplicate_field(
              "Alive",
              "incarnation",
              INCARNATION_TAG,
            ));
          }
          let (bytes_read, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
          offset += bytes_read;
          incarnation = Some(value);
        }
        META_BYTE => {
          if meta.is_some() {
            return Err(DecodeError::duplicate_field("Alive", "meta", META_TAG));
          }

          let (readed, data) = <&[u8] as DataRef<Meta>>::decode_length_delimited(&src[offset..])?;
          offset += readed;
          meta = Some(data);
        }
        DELEGATE_VERSION_BYTE => {
          if delegate_version.is_some() {
            return Err(DecodeError::duplicate_field(
              "Alive",
              "delegate_version",
              DELEGATE_VERSION_TAG,
            ));
          }

          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          delegate_version = Some(src[offset].into());
          offset += 1;
        }
        PROTOCOL_VERSION_BYTE => {
          if protocol_version.is_some() {
            return Err(DecodeError::duplicate_field(
              "Alive",
              "protocol_version",
              PROTOCOL_VERSION_TAG,
            ));
          }

          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          protocol_version = Some(src[offset].into());
          offset += 1;
        }
        b if b == Alive::<I, A>::node_byte() => {
          if node.is_some() {
            return Err(DecodeError::duplicate_field("Alive", "node", NODE_TAG));
          }

          let (readed, data) =
            <Node<I::Ref<'_>, A::Ref<'_>> as DataRef<Node<I, A>>>::decode_length_delimited(
              &src[offset..],
            )?;

          offset += readed;
          node = Some(data);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      Self {
        incarnation: incarnation
          .ok_or_else(|| DecodeError::missing_field("Alive", "incarnation"))?,
        meta: meta.unwrap_or_default(),
        node: node.ok_or_else(|| DecodeError::missing_field("Alive", "node"))?,
        protocol_version: protocol_version.unwrap_or_default(),
        delegate_version: delegate_version.unwrap_or_default(),
      },
    ))
  }
}

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
