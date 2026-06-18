use super::*;

use bytes::Bytes;
use triomphe::Arc;

/// Push node state is the state push to the remote server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushNodeState<I, A> {
  /// The id of the push node state.
  id: I,
  /// The address of the push node state.
  addr: A,
  /// Metadata from the delegate for this push node state.
  meta: Meta,
  /// The incarnation of the push node state.
  incarnation: u32,
  /// The state of the push node state.
  state: State,
  /// The protocol version of the push node state is speaking.
  protocol_version: ProtocolVersion,
  /// The delegate version of the push node state is speaking.
  delegate_version: DelegateVersion,
}

impl<I, A> PushNodeState<I, A> {
  /// Construct a new push node state with the given id, address and state.
  #[inline(always)]
  pub const fn new(incarnation: u32, id: I, addr: A, state: State) -> Self {
    Self {
      id,
      addr,
      meta: Meta::empty(),
      incarnation,
      state,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
    }
  }

  /// Returns the id of the push node state.
  #[inline(always)]
  pub const fn id_ref(&self) -> &I {
    &self.id
  }

  /// Returns the address of the push node state.
  #[inline(always)]
  pub const fn address_ref(&self) -> &A {
    &self.addr
  }

  /// Returns the meta of the push node state.
  #[inline(always)]
  pub const fn meta_ref(&self) -> &Meta {
    &self.meta
  }

  /// Returns the incarnation of the push node state.
  #[inline(always)]
  pub const fn incarnation(&self) -> u32 {
    self.incarnation
  }

  /// Returns the state of the push node state.
  #[inline(always)]
  pub const fn state(&self) -> State {
    self.state
  }

  /// Returns the protocol version of the push node state is speaking.
  #[inline(always)]
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  /// Returns the delegate version of the push node state is speaking.
  #[inline(always)]
  pub const fn delegate_version(&self) -> DelegateVersion {
    self.delegate_version
  }

  /// Sets the id of the push node state
  #[inline(always)]
  pub fn set_id(&mut self, id: I) -> &mut Self {
    self.id = id;
    self
  }

  /// Sets the id of the push node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_id(mut self, id: I) -> Self {
    self.id = id;
    self
  }

  /// Sets the address of the push node state
  #[inline(always)]
  pub fn set_address(&mut self, addr: A) -> &mut Self {
    self.addr = addr;
    self
  }

  /// Sets the address of the push node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_address(mut self, addr: A) -> Self {
    self.addr = addr;
    self
  }

  /// Sets the meta of the push node state
  #[inline(always)]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the meta of the push node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_meta(mut self, meta: Meta) -> Self {
    self.meta = meta;
    self
  }

  /// Sets the incarnation of the push node state
  #[inline(always)]
  pub const fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the incarnation of the push node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_incarnation(mut self, incarnation: u32) -> Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the state of the push node state
  #[inline(always)]
  pub const fn set_state(&mut self, state: State) -> &mut Self {
    self.state = state;
    self
  }

  /// Sets the state of the push node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_state(mut self, state: State) -> Self {
    self.state = state;
    self
  }

  /// Sets the protocol version of the push node state
  #[inline(always)]
  pub const fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the protocol version of the push node state is speaking (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the delegate version of the push node state
  #[inline(always)]
  pub const fn set_delegate_version(&mut self, delegate_version: DelegateVersion) -> &mut Self {
    self.delegate_version = delegate_version;
    self
  }

  /// Sets the delegate version of the push node state is speaking (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_delegate_version(mut self, delegate_version: DelegateVersion) -> Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I, A> CheapClone for PushNodeState<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
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

impl<I, A> PushNodeState<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
  /// Returns a [`Node`] with the same id and address as this [`PushNodeState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

/// Push pull message.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PushPull<I, A> {
  /// Whether the push pull message is a join message.
  join: bool,
  /// The states of the push pull message.
  states: Arc<[PushNodeState<I, A>]>,
  /// The user data of the push pull message.
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
      states: self.states.clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> PushPull<I, A> {
  /// Create a new [`PushPull`] message.
  #[inline(always)]
  pub fn new(join: bool, states: impl Iterator<Item = PushNodeState<I, A>>) -> Self {
    Self {
      states: Arc::from_iter(states),
      user_data: Bytes::new(),
      join,
    }
  }

  /// Returns whether the push pull message is a join message.
  #[inline(always)]
  pub const fn join(&self) -> bool {
    self.join
  }

  /// Returns the states of the push pull message.
  #[inline(always)]
  pub fn states_slice(&self) -> &[PushNodeState<I, A>] {
    &self.states
  }

  /// Returns the user data of the push pull message as a byte slice.
  #[inline(always)]
  pub fn user_data(&self) -> &[u8] {
    self.user_data.as_ref()
  }

  /// Cheap-clones the user data of the push pull message as a `Bytes` handle.
  #[inline(always)]
  pub fn user_data_bytes(&self) -> Bytes {
    self.user_data.clone()
  }

  /// Sets whether the push pull message is a join message.
  #[inline(always)]
  pub const fn set_join(&mut self) -> &mut Self {
    self.join = true;
    self
  }

  /// Sets whether the push pull message is a join message (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub const fn with_join(mut self) -> Self {
    self.join = true;
    self
  }

  /// Assigns the raw join flag.
  #[inline(always)]
  pub const fn update_join(&mut self, val: bool) -> &mut Self {
    self.join = val;
    self
  }

  /// Assigns the raw join flag (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub const fn maybe_join(mut self, val: bool) -> Self {
    self.join = val;
    self
  }

  /// Clears the join flag.
  #[inline(always)]
  pub const fn clear_join(&mut self) -> &mut Self {
    self.join = false;
    self
  }

  /// Sets the states of the push pull message.
  #[inline(always)]
  pub fn set_states(&mut self, states: Arc<[PushNodeState<I, A>]>) -> &mut Self {
    self.states = states;
    self
  }

  /// Sets the states of the push pull message (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub fn with_states(mut self, states: Arc<[PushNodeState<I, A>]>) -> Self {
    self.states = states;
    self
  }

  /// Sets the user data of the push pull message.
  #[inline(always)]
  pub fn set_user_data(&mut self, user_data: Bytes) -> &mut Self {
    self.user_data = user_data;
    self
  }

  /// Sets the user data of the push pull message (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub fn with_user_data(mut self, user_data: Bytes) -> Self {
    self.user_data = user_data;
    self
  }

  /// Consumes the [`PushPull`] and returns the states and user data.
  #[inline(always)]
  pub fn into_components(self) -> (bool, Bytes, Arc<[PushNodeState<I, A>]>) {
    (self.join, self.user_data, self.states)
  }
}
