use super::*;
use core::fmt;

/// Represents a node in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeState<I, A> {
  /// The id of the node.
  id: I,
  /// The address of the node.
  addr: A,
  /// Metadata from the delegate for this node.
  meta: Meta,
  /// State of the node.
  state: State,
  /// The protocol version of the node is speaking.
  protocol_version: ProtocolVersion,
  /// The delegate version of the node is speaking.
  delegate_version: DelegateVersion,
}

impl<I, A> From<Alive<I, A>> for NodeState<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
  fn from(value: Alive<I, A>) -> Self {
    let (id, addr) = value.node.into_parts();
    Self {
      id,
      addr,
      meta: value.meta,
      state: State::Alive,
      protocol_version: value.protocol_version,
      delegate_version: value.delegate_version,
    }
  }
}

impl<I, A> NodeState<I, A> {
  /// Construct a new node with the given name, address and state.
  #[inline(always)]
  pub const fn new(id: I, addr: A, state: State) -> Self {
    Self {
      id,
      addr,
      meta: Meta::empty(),
      state,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
    }
  }

  /// Returns the id of the node.
  #[inline(always)]
  pub const fn id_ref(&self) -> &I {
    &self.id
  }

  /// Returns the address of the node.
  #[inline(always)]
  pub const fn address_ref(&self) -> &A {
    &self.addr
  }

  /// Returns the meta of the node.
  #[inline(always)]
  pub const fn meta_ref(&self) -> &Meta {
    &self.meta
  }

  /// Returns the state of the node.
  #[inline(always)]
  pub const fn state(&self) -> State {
    self.state
  }

  /// Returns the protocol version of the node is speaking.
  #[inline(always)]
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  /// Returns the delegate version of the node is speaking.
  #[inline(always)]
  pub const fn delegate_version(&self) -> DelegateVersion {
    self.delegate_version
  }

  /// Sets the id of the node state
  #[inline(always)]
  pub fn set_id(&mut self, id: I) -> &mut Self {
    self.id = id;
    self
  }

  /// Sets the id of the node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_id(mut self, id: I) -> Self {
    self.id = id;
    self
  }

  /// Sets the address of the node state
  #[inline(always)]
  pub fn set_address(&mut self, addr: A) -> &mut Self {
    self.addr = addr;
    self
  }

  /// Sets the address of the node state (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_address(mut self, addr: A) -> Self {
    self.addr = addr;
    self
  }

  /// Sets the metadata for the node.
  #[inline(always)]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the meta of the node (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_meta(mut self, meta: Meta) -> Self {
    self.meta = meta;
    self
  }

  /// Sets the state for the node.
  #[inline(always)]
  pub const fn set_state(&mut self, state: State) -> &mut Self {
    self.state = state;
    self
  }

  /// Sets the state of the node (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_state(mut self, state: State) -> Self {
    self.state = state;
    self
  }

  /// Set the protocol version of the alive message is speaking.
  #[inline(always)]
  pub const fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the protocol version of the node is speaking (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Set the delegate version of the alive message is speaking.
  #[inline(always)]
  pub const fn set_delegate_version(&mut self, delegate_version: DelegateVersion) -> &mut Self {
    self.delegate_version = delegate_version;
    self
  }

  /// Sets the delegate version of the node is speaking (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_delegate_version(mut self, delegate_version: DelegateVersion) -> Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I, A> CheapClone for NodeState<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
  fn cheap_clone(&self) -> Self {
    Self {
      id: self.id.cheap_clone(),
      addr: self.addr.cheap_clone(),
      meta: self.meta.cheap_clone(),
      state: self.state,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}

impl<I, A> NodeState<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
  /// Returns a [`Node`] with the same id and address as this [`NodeState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

impl<I, A> fmt::Display for NodeState<I, A>
where
  I: fmt::Display,
  A: fmt::Display,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}({})", self.id, self.addr)
  }
}
