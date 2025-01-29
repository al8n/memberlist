use crate::MetaError;

use super::{
  version::{UnknownDelegateVersion, UnknownProtocolVersion},
  DelegateVersion, Meta, ProtocolVersion, MAX_ENCODED_LEN_SIZE,
};

use byteorder::{ByteOrder, NetworkEndian};
use nodecraft::{CheapClone, Node, NodeTransformError};
use transformable::Transformable;

/// Alive message
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", rkyv(compare(PartialEq)))]
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

/// Alive transform error.
#[derive(thiserror::Error)]
pub enum AliveTransformError<I: Transformable, A: Transformable> {
  /// Node transform error.
  #[error("node transform error: {0}")]
  Node(#[from] NodeTransformError<I, A>),
  /// Meta transform error.
  #[error("meta transform error: {0}")]
  Meta(#[from] MetaError),
  /// Message too large.
  #[error("encoded message too large, max 4294967295 got {0}")]
  TooLarge(u64),
  /// Encode buffer too small.
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// The buffer did not contain enough bytes to decode Alive.
  #[error("the buffer did not contain enough bytes to decode Alive")]
  NotEnoughBytes,
  /// Invalid protocol version.
  #[error("unknown protocol version: {0}")]
  UnknownProtocolVersion(#[from] UnknownProtocolVersion),
  /// Invalid delegate version.
  #[error("unknown delegate version: {0}")]
  UnknownDelegateVersion(#[from] UnknownDelegateVersion),
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for AliveTransformError<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<I, A> Transformable for Alive<I, A>
where
  I: Transformable + 'static,
  A: Transformable + 'static,
{
  type Error = AliveTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if encoded_len as u64 > u32::MAX as u64 {
      return Err(Self::Error::TooLarge(encoded_len as u64));
    }

    if encoded_len > dst.len() {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += MAX_ENCODED_LEN_SIZE;

    NetworkEndian::write_u32(&mut dst[offset..], self.incarnation);
    offset += core::mem::size_of::<u32>();

    offset += self
      .meta
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Meta)?;

    offset += self
      .node
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Node)?;

    dst[offset] = self.protocol_version as u8;
    offset += 1;

    dst[offset] = self.delegate_version as u8;
    offset += 1;

    debug_assert_eq!(
      offset, encoded_len,
      "expect bytes written ({encoded_len}) not match actual bytes written ({offset})"
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MAX_ENCODED_LEN_SIZE
      + core::mem::size_of::<u32>() // incarnation
      + self.meta.encoded_len()
      + self.node.encoded_len()
      + 1 // protocol_version
      + 1 // delegate_version
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let mut offset = 0;
    if core::mem::size_of::<u32>() > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let encoded_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    offset += MAX_ENCODED_LEN_SIZE;
    if encoded_len > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let incarnation = NetworkEndian::read_u32(&src[offset..]);
    offset += core::mem::size_of::<u32>();

    let (meta_len, meta) = Meta::decode(&src[offset..]).map_err(Self::Error::Meta)?;
    offset += meta_len;

    let (node_len, node) = Node::decode(&src[offset..]).map_err(Self::Error::Node)?;
    offset += node_len;

    if 1 + offset > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }
    let protocol_version =
      ProtocolVersion::try_from(src[offset]).map_err(Self::Error::UnknownProtocolVersion)?;
    offset += 1;

    if 1 + offset > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }
    let delegate_version =
      DelegateVersion::try_from(src[offset]).map_err(Self::Error::UnknownDelegateVersion)?;
    offset += 1;

    Ok((
      offset,
      Self {
        incarnation,
        meta,
        node,
        protocol_version,
        delegate_version,
      },
    ))
  }
}

#[cfg(feature = "rkyv")]
const _: () = {
  use core::fmt::Debug;
  use rkyv::Archive;

  impl<I: Debug + Archive, A: Debug + Archive> core::fmt::Debug for ArchivedAlive<I, A>
  where
    I::Archived: Debug,
    A::Archived: Debug,
  {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("Alive")
        .field("incarnation", &self.incarnation)
        .field("meta", &self.meta)
        .field("node", &self.node)
        .field("protocol_version", &self.protocol_version)
        .field("delegate_version", &self.delegate_version)
        .finish()
    }
  }

  impl<I: Archive, A: Archive> PartialEq for ArchivedAlive<I, A>
  where
    I::Archived: PartialEq,
    A::Archived: PartialEq,
  {
    fn eq(&self, other: &Self) -> bool {
      self.incarnation == other.incarnation
        && self.meta == other.meta
        && self.node == other.node
        && self.protocol_version == other.protocol_version
        && self.delegate_version == other.delegate_version
    }
  }

  impl<I: Archive, A: Archive> Eq for ArchivedAlive<I, A>
  where
    I::Archived: Eq,
    A::Archived: Eq,
  {
  }

  impl<I: Archive, A: Archive> core::hash::Hash for ArchivedAlive<I, A>
  where
    I::Archived: core::hash::Hash,
    A::Archived: core::hash::Hash,
  {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
      self.incarnation.hash(state);
      self.meta.hash(state);
      self.node.hash(state);
      self.protocol_version.hash(state);
      self.delegate_version.hash(state);
    }
  }
};

#[cfg(test)]
const _: () = {
  use std::net::SocketAddr;

  use rand::{distr::Alphanumeric, random, rng, Rng};
  use smol_str::SmolStr;

  impl Alive<SmolStr, SocketAddr> {
    pub(crate) fn random(size: usize) -> Self {
      let id = rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let id = String::from_utf8(id).unwrap().into();
      Self {
        incarnation: random(),
        meta: (0..size)
          .map(|_| random::<u8>())
          .collect::<Vec<_>>()
          .try_into()
          .unwrap(),
        node: Node::new(
          id,
          format!("127.0.0.1:{}", rng().random_range(0..65535))
            .parse()
            .unwrap(),
        ),
        protocol_version: ProtocolVersion::V1,
        delegate_version: DelegateVersion::V1,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_encode_decode() {
    for i in 0..100 {
      let alive = Alive::random(i);
      let mut buf = vec![0; alive.encoded_len()];
      let encoded_len = alive.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, alive.encoded_len());
      let (decoded_len, decoded) = Alive::decode(&buf).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, alive);
    }
  }

  #[test]
  fn test_access() {
    let mut alive = Alive::random(16);
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
