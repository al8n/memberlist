use super::*;

/// Alive message
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Alive<I, A> {
  /// The incarnation of the alive message
  incarnation: u32,
  /// The meta of the alive message
  pub(super) meta: Meta,
  /// The node of the alive message
  pub(super) node: Node<I, A>,
  /// The protocol version of the alive message is speaking
  pub(super) protocol_version: ProtocolVersion,
  /// The delegate version of the alive message is speaking
  pub(super) delegate_version: DelegateVersion,
}

impl<I, A> Alive<I, A> {
  /// Construct a new alive message with the given incarnation, meta, node, protocol version and delegate version.
  #[inline(always)]
  pub const fn new(incarnation: u32, node: Node<I, A>) -> Self {
    Self {
      incarnation,
      meta: Meta::empty(),
      node,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
    }
  }

  /// Returns the incarnation of the alive message.
  #[inline(always)]
  pub const fn incarnation(&self) -> u32 {
    self.incarnation
  }

  /// Returns the meta of the alive message.
  #[inline(always)]
  pub const fn meta_ref(&self) -> &Meta {
    &self.meta
  }

  /// Returns the node of the alive message.
  #[inline(always)]
  pub const fn node_ref(&self) -> &Node<I, A> {
    &self.node
  }

  /// Returns the protocol version of the alive message is speaking.
  #[inline(always)]
  pub const fn protocol_version(&self) -> ProtocolVersion {
    self.protocol_version
  }

  /// Returns the delegate version of the alive message is speaking.
  #[inline(always)]
  pub const fn delegate_version(&self) -> DelegateVersion {
    self.delegate_version
  }

  /// Sets the incarnation of the alive message.
  #[inline(always)]
  pub const fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the incarnation of the alive message (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub const fn with_incarnation(mut self, incarnation: u32) -> Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the meta of the alive message.
  #[inline(always)]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the meta of the alive message (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub fn with_meta(mut self, meta: Meta) -> Self {
    self.meta = meta;
    self
  }

  /// Sets the node of the alive message.
  #[inline(always)]
  pub fn set_node(&mut self, node: Node<I, A>) -> &mut Self {
    self.node = node;
    self
  }

  /// Sets the node of the alive message (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub fn with_node(mut self, node: Node<I, A>) -> Self {
    self.node = node;
    self
  }

  /// Set the protocol version of the alive message is speaking.
  #[inline(always)]
  pub const fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the protocol version of the alive message is speaking (Builder pattern).
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

  /// Sets the delegate version of the alive message is speaking (Builder pattern).
  #[must_use]
  #[inline(always)]
  pub const fn with_delegate_version(mut self, delegate_version: DelegateVersion) -> Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I, A> CheapClone for Alive<I, A>
where
  I: CheapClone,
  A: CheapClone,
{
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
