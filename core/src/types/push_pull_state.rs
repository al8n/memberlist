use std::sync::Arc;

use crate::version::{UnknownDelegateVersion, UnknownProtocolVersion};

use super::*;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use nodecraft::{CheapClone, Node};
use transformable::Transformable;

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
pub struct PushServerState<I, A> {
  id: I,
  #[viewit(
    getter(const, rename = "address", style = "ref"),
    setter(rename = "with_address")
  )]
  addr: A,
  meta: Bytes,
  incarnation: u32,
  state: ServerState,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl<I: CheapClone, A: CheapClone> CheapClone for PushServerState<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      id: self.id.cheap_clone(),
      addr: self.addr.cheap_clone(),
      meta: self.meta.clone(),
      incarnation: self.incarnation,
      state: self.state,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}

impl<I: CheapClone, A: CheapClone> PushServerState<I, A> {
  /// Returns a [`Node`] with the same id and address as this [`PushServerState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

/// Transform errors for [`PushPull`].
pub enum PushPullTransformError<I: Transformable, A: Transformable> {
  /// Error transforming the id.
  Id(I::Error),
  /// Error transforming the address.
  Address(A::Error),
  /// The encode buffer is too small.
  BufferTooSmall,
  /// The encoded bytes is too large.
  TooLarge,
  /// Not enough bytes to decode.
  NotEnoughBytes,
  /// Invalid server state.
  UnknownServerState(UnknownServerState),
  /// Invalid protocol version.
  UnknownProtocolVersion(UnknownProtocolVersion),
  /// Invalid delegate version.
  UnknownDelegateVersion(UnknownDelegateVersion),
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for PushPullTransformError<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

impl<I: Transformable, A: Transformable> core::fmt::Display for PushPullTransformError<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Id(err) => write!(f, "id transforming error: {err}"),
      Self::Address(err) => write!(f, "address transforming error: {err}"),
      Self::BufferTooSmall => write!(f, "encode buffer is too small"),
      Self::TooLarge => write!(f, "the encoded bytes is too large"),
      Self::NotEnoughBytes => write!(f, "not enough bytes to decode"),
      Self::UnknownServerState(err) => write!(f, "{err}"),
      Self::UnknownProtocolVersion(err) => write!(f, "{err}"),
      Self::UnknownDelegateVersion(err) => write!(f, "{err}"),
    }
  }
}

impl<I: Transformable, A: Transformable> std::error::Error for PushPullTransformError<I, A> {}

impl<I: Transformable, A: Transformable> Transformable for PushServerState<I, A> {
  type Error = PushPullTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if encoded_len as u64 > u32::MAX as u64 {
      return Err(Self::Error::TooLarge);
    }

    if encoded_len > dst.len() {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(dst, encoded_len as u32);
    offset += MAX_ENCODED_LEN_SIZE;
    NetworkEndian::write_u32(&mut dst[offset..], self.incarnation);
    offset += core::mem::size_of::<u32>();
    dst[offset] = self.state as u8;
    offset += 1;
    dst[offset] = self.protocol_version as u8;
    offset += 1;
    dst[offset] = self.delegate_version as u8;
    offset += 1;

    if !self.meta.is_empty() {
      dst[offset] = 1;
      offset += 1;
      NetworkEndian::write_u32(&mut dst[offset..], self.meta.len() as u32);
      offset += core::mem::size_of::<u32>();
      dst[offset..offset + self.meta.len()].copy_from_slice(&self.meta);
      offset += self.meta.len();
    } else {
      dst[offset] = 0;
      offset += 1;
    }

    offset += self
      .id
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Id)?;
    offset += self
      .addr
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Address)?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect bytes written ({encoded_len}) not match actual bytes writtend ({offset})"
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    MAX_ENCODED_LEN_SIZE
    + core::mem::size_of::<u32>() // incarnation
    + 1 // server state
    + 1 // protocol version
    + 1 // delegate version
    + 1 + if self.meta.is_empty() {
      0
    } else {
      core::mem::size_of::<u32>() + self.meta.len()
    } + self.id.encoded_len() + self.addr.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < MAX_ENCODED_LEN_SIZE {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(src) as usize;
    offset += MAX_ENCODED_LEN_SIZE;
    if encoded_len > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let incarnation = NetworkEndian::read_u32(&src[offset..]);
    offset += core::mem::size_of::<u32>();
    let state = ServerState::try_from(src[offset]).map_err(Self::Error::UnknownServerState)?;
    offset += 1;
    let protocol_version =
      ProtocolVersion::try_from(src[offset]).map_err(Self::Error::UnknownProtocolVersion)?;
    offset += 1;
    let delegate_version =
      DelegateVersion::try_from(src[offset]).map_err(Self::Error::UnknownDelegateVersion)?;
    offset += 1;

    let (meta_len, meta) = if src[offset] == 1 {
      offset += 1;
      let meta_len = NetworkEndian::read_u32(&src[offset..]) as usize;
      offset += core::mem::size_of::<u32>();
      (
        meta_len,
        Bytes::copy_from_slice(&src[offset..offset + meta_len]),
      )
    } else {
      offset += 1;
      (0, Bytes::new())
    };

    offset += meta_len;
    let (id_len, id) = I::decode(&src[offset..]).map_err(Self::Error::Id)?;
    offset += id_len;
    let (addr_len, addr) = A::decode(&src[offset..]).map_err(Self::Error::Address)?;
    offset += addr_len;

    debug_assert_eq!(
      offset, encoded_len,
      "expect bytes read ({encoded_len}) not match actual bytes read ({offset})"
    );

    Ok((
      offset,
      Self {
        id,
        addr,
        meta,
        incarnation,
        state,
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

  impl<I: Debug + Archive, A: Debug + Archive> core::fmt::Debug for ArchivedPushServerState<I, A>
  where
    I::Archived: Debug,
    A::Archived: Debug,
  {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("PushServerState")
        .field("id", &self.id)
        .field("addr", &self.addr)
        .field("meta", &self.meta)
        .field("incarnation", &self.incarnation)
        .field("state", &self.state)
        .field("protocol_version", &self.protocol_version)
        .field("delegate_version", &self.delegate_version)
        .finish()
    }
  }

  impl<I: Archive, A: Archive> PartialEq for ArchivedPushServerState<I, A>
  where
    I::Archived: PartialEq,
    A::Archived: PartialEq,
  {
    fn eq(&self, other: &Self) -> bool {
      self.id == other.id
        && self.addr == other.addr
        && self.meta == other.meta
        && self.incarnation == other.incarnation
        && self.state == other.state
        && self.protocol_version == other.protocol_version
        && self.delegate_version == other.delegate_version
    }
  }

  impl<I: Archive, A: Archive> Eq for ArchivedPushServerState<I, A>
  where
    I::Archived: Eq,
    A::Archived: Eq,
  {
  }

  impl<I: Archive, A: Archive> core::hash::Hash for ArchivedPushServerState<I, A>
  where
    I::Archived: core::hash::Hash,
    A::Archived: core::hash::Hash,
  {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
      self.id.hash(state);
      self.addr.hash(state);
      self.meta.hash(state);
      self.incarnation.hash(state);
      self.state.hash(state);
      self.protocol_version.hash(state);
      self.delegate_version.hash(state);
    }
  }
};

#[viewit::viewit]
#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
pub struct PushPull<I, A> {
  join: bool,
  states: Arc<Vec<PushServerState<I, A>>>,
  user_data: Bytes,
}

impl<I, A> Clone for PushPull<I, A> {
  fn clone(&self) -> Self {
    Self {
      join: self.join,
      states: self.states.clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> CheapClone for PushPull<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      join: self.join,
      states: self.states.cheap_clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> PushPull<I, A> {
  /// Create a new [`PushPull`] message.
  #[inline]
  pub fn new(states: Vec<PushServerState<I, A>>, user_data: Bytes, join: bool) -> Self {
    Self {
      states: Arc::new(states),
      user_data,
      join,
    }
  }
}

impl<I: Transformable, A: Transformable> Transformable for PushPull<I, A> {
  type Error = PushPullTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();

    if encoded_len as u64 > u32::MAX as u64 {
      return Err(Self::Error::TooLarge);
    }

    if encoded_len > dst.len() {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(dst, encoded_len as u32);
    offset += MAX_ENCODED_LEN_SIZE;
    dst[offset] = self.join as u8;
    offset += 1;
    NetworkEndian::write_u32(&mut dst[offset..], self.states.len() as u32);
    offset += core::mem::size_of::<u32>();

    for state in self.states.iter() {
      offset += state.encode(&mut dst[offset..])?;
    }

    if !self.user_data.is_empty() {
      dst[offset] = 1;
      offset += 1;
      NetworkEndian::write_u32(&mut dst[offset..], self.user_data.len() as u32);
      offset += core::mem::size_of::<u32>();
      dst[offset..offset + self.user_data.len()].copy_from_slice(&self.user_data);
      offset += self.user_data.len();
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
    let mut encoded_len = MAX_ENCODED_LEN_SIZE;
    encoded_len += 1; // join
    encoded_len += core::mem::size_of::<u32>(); // # of nodes
    for state in self.states.iter() {
      encoded_len += state.encoded_len();
    }
    encoded_len += 1
      + if self.user_data.is_empty() {
        0
      } else {
        core::mem::size_of::<u32>() + self.user_data.len()
      };
    encoded_len
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < MAX_ENCODED_LEN_SIZE {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(src) as usize;
    offset += MAX_ENCODED_LEN_SIZE;
    if encoded_len > src.len() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let join = src[offset] == 1;
    offset += 1;

    let states_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    offset += core::mem::size_of::<u32>();

    let mut states = Vec::with_capacity(states_len);
    for _ in 0..states_len {
      let (state_len, state) = PushServerState::decode(&src[offset..])?;
      offset += state_len;
      states.push(state);
    }

    let (user_data_len, user_data) = if src[offset] == 1 {
      offset += 1;
      let user_data_len = NetworkEndian::read_u32(&src[offset..]) as usize;
      offset += core::mem::size_of::<u32>();
      (
        user_data_len,
        Bytes::copy_from_slice(&src[offset..offset + user_data_len]),
      )
    } else {
      offset += 1;
      (0, Bytes::new())
    };

    offset += user_data_len;

    debug_assert_eq!(
      offset, encoded_len,
      "expect bytes read ({encoded_len}) not match actual bytes read ({offset})"
    );

    Ok((
      offset,
      Self {
        join,
        states: Arc::new(states),
        user_data,
      },
    ))
  }
}

#[cfg(feature = "rkyv")]
const _: () = {
  use core::fmt::Debug;
  use rkyv::Archive;

  impl<I: Debug + Archive, A: Debug + Archive> core::fmt::Debug for ArchivedPushPull<I, A>
  where
    I::Archived: Debug,
    A::Archived: Debug,
  {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("PushPull")
        .field("join", &self.join)
        .field("states", &self.states)
        .field("user_data", &self.user_data)
        .finish()
    }
  }

  impl<I: Archive, A: Archive> PartialEq for ArchivedPushPull<I, A>
  where
    I::Archived: PartialEq,
    A::Archived: PartialEq,
  {
    fn eq(&self, other: &Self) -> bool {
      self.join == other.join && self.states == other.states && self.user_data == other.user_data
    }
  }

  impl<I: Archive, A: Archive> Eq for ArchivedPushPull<I, A>
  where
    I::Archived: Eq,
    A::Archived: Eq,
  {
  }

  impl<I: Archive, A: Archive> core::hash::Hash for ArchivedPushPull<I, A>
  where
    I::Archived: core::hash::Hash,
    A::Archived: core::hash::Hash,
  {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
      self.join.hash(state);
      self.states.hash(state);
      self.user_data.hash(state);
    }
  }
};

#[cfg(test)]
const _: () = {
  use std::net::SocketAddr;

  use rand::{distributions::Alphanumeric, random, thread_rng, Rng};
  use smol_str::SmolStr;

  impl PushServerState<SmolStr, SocketAddr> {
    fn generate(size: usize) -> Self {
      Self {
        id: String::from_utf8(
          thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .collect::<Vec<_>>(),
        )
        .unwrap()
        .into(),
        addr: SocketAddr::from(([127, 0, 0, 1], thread_rng().gen_range(0..65535))),
        meta: (0..size).map(|_| random::<u8>()).collect::<Vec<_>>().into(),
        incarnation: random(),
        state: ServerState::try_from(thread_rng().gen_range(0..=3)).unwrap(),
        protocol_version: ProtocolVersion::V0,
        delegate_version: DelegateVersion::V0,
      }
    }
  }

  impl PushPull<SmolStr, SocketAddr> {
    fn generate(size: usize) -> Self {
      let states = (0..size)
        .map(|_| PushServerState::generate(size))
        .collect::<Vec<_>>();
      let user_data = (0..size).map(|_| random::<u8>()).collect::<Vec<_>>().into();
      let join = random();
      Self {
        join,
        states: Arc::new(states),
        user_data,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_push_server_state() {
    for i in 0..100 {
      let state = PushServerState::generate(i);
      let mut buf = vec![0; state.encoded_len()];
      let encoded_len = state.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, state.encoded_len());
      let (readed, decoded) = PushServerState::decode(&buf).unwrap();
      assert_eq!(readed, encoded_len);
      assert_eq!(decoded, state);
    }
  }

  #[test]
  fn test_push_pull() {
    for i in 0..100 {
      let push_pull = PushPull::generate(i);
      let mut buf = vec![0; push_pull.encoded_len()];
      let encoded_len = push_pull.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, push_pull.encoded_len());
      let (readed, decoded) = PushPull::decode(&buf).unwrap();
      assert_eq!(readed, encoded_len);
      assert_eq!(decoded, push_pull);
    }
  }
}
