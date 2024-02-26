use crate::{
  version::{UnknownDelegateVersion, UnknownProtocolVersion},
  DelegateVersion, ProtocolVersion,
};

use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use nodecraft::{CheapClone, Node};
use transformable::Transformable;

use super::MAX_ENCODED_LEN_SIZE;

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
pub struct Alive<I, A> {
  incarnation: u32,
  meta: Bytes,
  node: Node<I, A>,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
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
pub enum AliveTransformError<I: Transformable, A: Transformable> {
  /// Node transform error.
  Node(<Node<I, A> as Transformable>::Error),
  /// Message too large.
  TooLarge(u64),
  /// Encode buffer too small.
  BufferTooSmall,
  /// The buffer did not contain enough bytes to decode Alive.
  NotEnoughBytes,
  /// Invalid protocol version.
  UnknownProtocolVersion(UnknownProtocolVersion),
  /// Invalid delegate version.
  UnknownDelegateVersion(UnknownDelegateVersion),
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for AliveTransformError<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Node(err) => write!(f, "node transform error: {:?}", err),
      Self::TooLarge(val) => write!(f, "encoded message too large, max {} got {val}", u32::MAX),
      Self::BufferTooSmall => write!(f, "encode buffer too small"),
      Self::NotEnoughBytes => write!(f, "the buffer did not contain enough bytes to decode Alive"),
      Self::UnknownProtocolVersion(err) => write!(f, "invalid protocol version: {:?}", err),
      Self::UnknownDelegateVersion(err) => write!(f, "invalid delegate version: {:?}", err),
    }
  }
}

impl<I: Transformable, A: Transformable> core::fmt::Display for AliveTransformError<I, A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Node(err) => write!(f, "node transform error: {}", err),
      Self::TooLarge(val) => write!(f, "encoded message too large, max {} got {val}", u32::MAX),
      Self::BufferTooSmall => write!(f, "encode buffer too small"),
      Self::NotEnoughBytes => write!(f, "the buffer did not contain enough bytes to decode Alive"),
      Self::UnknownProtocolVersion(err) => write!(f, "invalid protocol version: {}", err),
      Self::UnknownDelegateVersion(err) => write!(f, "invalid delegate version: {}", err),
    }
  }
}

impl<I: Transformable, A: Transformable> std::error::Error for AliveTransformError<I, A> {}

impl<I: Transformable, A: Transformable> From<UnknownProtocolVersion>
  for AliveTransformError<I, A>
{
  fn from(err: UnknownProtocolVersion) -> Self {
    Self::UnknownProtocolVersion(err)
  }
}

impl<I: Transformable, A: Transformable> From<UnknownDelegateVersion>
  for AliveTransformError<I, A>
{
  fn from(err: UnknownDelegateVersion) -> Self {
    Self::UnknownDelegateVersion(err)
  }
}

impl<I: Transformable, A: Transformable> Transformable for Alive<I, A> {
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

    if !self.meta.is_empty() {
      dst[offset] = 1;
      offset += 1;
      let meta_len = self.meta.len() as u32;
      NetworkEndian::write_u32(&mut dst[offset..], meta_len);
      offset += core::mem::size_of::<u32>();
      dst[offset..offset + meta_len as usize].copy_from_slice(&self.meta);
      offset += meta_len as usize;
    } else {
      dst[offset] = 0;
      offset += 1;
    }

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
      + if self.meta.is_empty() { 1 } else { self.meta.len() + 1 + MAX_ENCODED_LEN_SIZE }
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

    let is_meta_empty = src[offset] == 0;
    offset += 1;

    let meta_len = if is_meta_empty {
      0
    } else {
      if core::mem::size_of::<u32>() > src.len() - offset {
        return Err(Self::Error::NotEnoughBytes);
      }
      let meta_len = NetworkEndian::read_u32(&src[offset..]) as usize;
      offset += core::mem::size_of::<u32>();
      meta_len
    };

    let meta = if meta_len > 0 {
      if meta_len > src.len() - offset {
        return Err(Self::Error::NotEnoughBytes);
      }
      let meta = Bytes::copy_from_slice(&src[offset..offset + meta_len]);
      offset += meta_len;
      meta
    } else {
      Bytes::new()
    };

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

  use rand::{distributions::Alphanumeric, random, thread_rng, Rng};
  use smol_str::SmolStr;

  impl Alive<SmolStr, SocketAddr> {
    fn random(size: usize) -> Self {
      let id = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let id = String::from_utf8(id).unwrap().into();
      Self {
        incarnation: random(),
        meta: (0..size).map(|_| random::<u8>()).collect::<Vec<_>>().into(),
        node: Node::new(
          id,
          format!("127.0.0.1:{}", thread_rng().gen_range(0..65535))
            .parse()
            .unwrap(),
        ),
        protocol_version: ProtocolVersion::V0,
        delegate_version: DelegateVersion::V0,
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
}
