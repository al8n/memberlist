use std::sync::Arc;

use super::*;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use nodecraft::{CheapClone, Node};
use transformable::Transformable;

/// Push node state is the state push to the remote server.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
pub struct PushNodeState<I, A> {
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the id of the push node state")
    ),
    setter(attrs(doc = "Sets the id of the push node state (Builder pattern)"))
  )]
  id: I,
  #[viewit(
    getter(
      const,
      rename = "address",
      style = "ref",
      attrs(doc = "Returns the address of the push node state")
    ),
    setter(
      rename = "with_address",
      attrs(doc = "Sets the address of the push node state (Builder pattern)")
    )
  )]
  addr: A,
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the meta of the push node state")
    ),
    setter(attrs(doc = "Sets the meta of the push node state (Builder pattern)"))
  )]
  meta: Meta,
  #[viewit(
    getter(const, attrs(doc = "Returns the incarnation of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the incarnation of the push node state (Builder pattern)")
    )
  )]
  incarnation: u32,
  #[viewit(
    getter(const, attrs(doc = "Returns the state of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the state of the push node state (Builder pattern)")
    )
  )]
  state: State,
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the protocol version of the push node state is speaking")
    ),
    setter(
      const,
      attrs(
        doc = "Sets the protocol version of the push node state is speaking (Builder pattern)"
      )
    )
  )]
  protocol_version: ProtocolVersion,
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the delegate version of the push node state is speaking")
    ),
    setter(
      const,
      attrs(
        doc = "Sets the delegate version of the push node state is speaking (Builder pattern)"
      )
    )
  )]
  delegate_version: DelegateVersion,
}

impl<I, A> PushNodeState<I, A> {
  /// Construct a new push node state with the given id, address and state.
  #[inline]
  pub const fn new(incarnation: u32, id: I, addr: A, state: State) -> Self {
    Self {
      id,
      addr,
      meta: Meta::empty(),
      incarnation,
      state,
      protocol_version: ProtocolVersion::V0,
      delegate_version: DelegateVersion::V0,
    }
  }

  /// Sets the id of the push node state
  #[inline]
  pub fn set_id(&mut self, id: I) -> &mut Self {
    self.id = id;
    self
  }

  /// Sets the address of the push node state
  #[inline]
  pub fn set_address(&mut self, addr: A) -> &mut Self {
    self.addr = addr;
    self
  }

  /// Sets the meta of the push node state
  #[inline]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the incarnation of the push node state
  #[inline]
  pub fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the state of the push node state
  #[inline]
  pub fn set_state(&mut self, state: State) -> &mut Self {
    self.state = state;
    self
  }

  /// Sets the protocol version of the push node state
  #[inline]
  pub fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the delegate version of the push node state
  #[inline]
  pub fn set_delegate_version(&mut self, delegate_version: DelegateVersion) -> &mut Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for PushNodeState<I, A> {
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

impl<I: CheapClone, A: CheapClone> PushNodeState<I, A> {
  /// Returns a [`Node`] with the same id and address as this [`PushNodeState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

/// Transform errors for [`PushPull`].
#[derive(thiserror::Error)]
pub enum PushPullTransformError<I: Transformable, A: Transformable> {
  /// Error transforming the id.
  #[error("id transforming error: {0}")]
  Id(I::Error),
  /// Error transforming the address.
  #[error("address transforming error: {0}")]
  Address(A::Error),
  /// Error transforming the meta.
  #[error("meta transforming error: {0}")]
  Meta(#[from] MetaError),
  /// The encode buffer is too small.
  #[error("encode buffer is too small")]
  BufferTooSmall,
  /// The encoded bytes is too large.
  #[error("the encoded bytes is too large")]
  TooLarge,
  /// Not enough bytes to decode.
  #[error("not enough bytes to decode `PushPull`")]
  NotEnoughBytes,
  /// Invalid server state.
  #[error(transparent)]
  UnknownState(#[from] UnknownState),
  /// Invalid protocol version.
  #[error(transparent)]
  UnknownProtocolVersion(#[from] UnknownProtocolVersion),
  /// Invalid delegate version.
  #[error(transparent)]
  UnknownDelegateVersion(#[from] UnknownDelegateVersion),
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for PushPullTransformError<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

impl<I: Transformable, A: Transformable> Transformable for PushNodeState<I, A> {
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

    offset += self
      .meta
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Meta)?;

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
    + self.meta.encoded_len() + self.id.encoded_len() + self.addr.encoded_len()
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
    let state = State::try_from(src[offset]).map_err(Self::Error::UnknownState)?;
    offset += 1;
    let protocol_version =
      ProtocolVersion::try_from(src[offset]).map_err(Self::Error::UnknownProtocolVersion)?;
    offset += 1;
    let delegate_version =
      DelegateVersion::try_from(src[offset]).map_err(Self::Error::UnknownDelegateVersion)?;
    offset += 1;

    let (meta_len, meta) = Meta::decode(&src[offset..]).map_err(Self::Error::Meta)?;
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

  impl<I: Debug + Archive, A: Debug + Archive> core::fmt::Debug for ArchivedPushNodeState<I, A>
  where
    I::Archived: Debug,
    A::Archived: Debug,
  {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("PushNodeState")
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

  impl<I: Archive, A: Archive> PartialEq for ArchivedPushNodeState<I, A>
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

  impl<I: Archive, A: Archive> Eq for ArchivedPushNodeState<I, A>
  where
    I::Archived: Eq,
    A::Archived: Eq,
  {
  }

  impl<I: Archive, A: Archive> core::hash::Hash for ArchivedPushNodeState<I, A>
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

/// Push pull message.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
pub struct PushPull<I, A> {
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns whether the push pull message is a join message")
    ),
    setter(
      const,
      attrs(doc = "Sets whether the push pull message is a join message (Builder pattern)")
    )
  )]
  join: bool,
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the states of the push pull message")
    ),
    setter(attrs(doc = "Sets the states of the push pull message (Builder pattern)"))
  )]
  states: Arc<TinyVec<PushNodeState<I, A>>>,
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the user data of the push pull message")
    ),
    setter(attrs(doc = "Sets the user data of the push pull message (Builder pattern)"))
  )]
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
  pub fn new(join: bool, states: TinyVec<PushNodeState<I, A>>) -> Self {
    Self {
      states: Arc::new(states),
      user_data: Bytes::new(),
      join,
    }
  }

  /// Consumes the [`PushPull`] and returns the states and user data.
  #[inline]
  pub fn into_components(self) -> (bool, Bytes, Arc<TinyVec<PushNodeState<I, A>>>) {
    (self.join, self.user_data, self.states)
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

    let mut states = TinyVec::with_capacity(states_len);
    for _ in 0..states_len {
      let (state_len, state) = PushNodeState::decode(&src[offset..])?;
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

#[cfg(test)]
const _: () = {
  use std::net::SocketAddr;

  use rand::{distributions::Alphanumeric, random, thread_rng, Rng};
  use smol_str::SmolStr;

  impl PushNodeState<SmolStr, SocketAddr> {
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
        meta: (0..size)
          .map(|_| random::<u8>())
          .collect::<Vec<_>>()
          .try_into()
          .unwrap(),
        incarnation: random(),
        state: State::try_from(thread_rng().gen_range(0..=3)).unwrap(),
        protocol_version: ProtocolVersion::V0,
        delegate_version: DelegateVersion::V0,
      }
    }
  }

  impl PushPull<SmolStr, SocketAddr> {
    pub(crate) fn generate(size: usize) -> Self {
      let states = (0..size)
        .map(|_| PushNodeState::generate(size))
        .collect::<TinyVec<_>>();
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
  use std::net::SocketAddr;

  use super::*;

  #[test]
  fn test_push_server_state() {
    for i in 0..100 {
      let state = PushNodeState::generate(i);
      let mut buf = vec![0; state.encoded_len()];
      let encoded_len = state.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, state.encoded_len());
      let (readed, decoded) = PushNodeState::decode(&buf).unwrap();
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

  #[test]
  fn test_push_pull_clone_and_cheap_clone() {
    let push_pull = PushPull::generate(100);
    let cloned = push_pull.clone();
    let cheap_cloned = push_pull.cheap_clone();
    assert_eq!(cloned, push_pull);
    assert_eq!(cheap_cloned, push_pull);
    let cloned1 = format!("{:?}", cloned);
    let cheap_cloned1 = format!("{:?}", cheap_cloned);
    assert_eq!(cloned1, cheap_cloned1);
  }

  #[test]
  fn test_push_node_state() {
    let mut state = PushNodeState::generate(100);
    state.set_id("test".into());
    assert_eq!(state.id(), "test");
    state.set_address(SocketAddr::from(([127, 0, 0, 1], 8080)));
    assert_eq!(state.address(), &SocketAddr::from(([127, 0, 0, 1], 8080)));
    state.set_meta(Meta::try_from("test").unwrap());
    assert_eq!(state.meta(), &Meta::try_from("test").unwrap());
    state.set_incarnation(100);
    assert_eq!(state.incarnation(), 100);

    state.set_state(State::Alive);
    assert_eq!(state.state(), State::Alive);

    state.set_protocol_version(ProtocolVersion::V0);
    assert_eq!(state.protocol_version(), ProtocolVersion::V0);

    state.set_delegate_version(DelegateVersion::V0);
    assert_eq!(state.delegate_version(), DelegateVersion::V0);

    let _cloned = state.cheap_clone();
  }
}
