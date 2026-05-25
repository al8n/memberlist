//! Generic owned message *shapes* (`Alive<I,A>`, `Suspect<I>`, …).
//!
//! These are the application-facing typed representations consumed by
//! `memberlist-machine`. They carry NO wire codec — the hand-rolled
//! `Data`/`DataRef` message impls from `memberlist-proto` are deliberately
//! left behind (frozen backup). [`crate::bridge`] bridges these to the
//! buffa-generated concrete codec in [`crate::messages`] using the
//! [`crate::data::Data`] trait for the `I`/`A` byte fields.

// ─── re-exports ──────────────────────────────────────────────────────────────

pub use crate::{CheapClone, Node};

// ─── Meta ────────────────────────────────────────────────────────────────────

use std::str::FromStr;

use bytes::{Bytes, BytesMut};

/// Invalid meta error.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
#[error("meta size {0} exceeds the wire ceiling Meta::MAX_SIZE")]
pub struct LargeMeta(usize);

/// The metadata of a node in the cluster.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Meta(Bytes);

impl Default for Meta {
  #[inline(always)]
  fn default() -> Self {
    Self::empty()
  }
}

impl CheapClone for Meta {}

impl Meta {
  /// Absolute upper bound on a `Meta` byte length, enforced at
  /// construction (`TryFrom` impls and `from_static*`). This is the
  /// wire-layer ceiling — a `Meta` larger than this cannot be
  /// represented on the wire and the constructors refuse to build
  /// one.
  ///
  /// Coordinators ([`memberlist-machine`]) can apply a TIGHTER
  /// per-endpoint cap via `EndpointConfig::with_meta_max_size` (the
  /// historical default mirrors Go memberlist at 512 bytes); this
  /// constant is the absolute hard ceiling above which no
  /// machine-level config can rise.
  ///
  /// The value is `u16::MAX` so a future length-prefixed wire
  /// encoding can fit any valid `Meta` in two bytes.
  pub const MAX_SIZE: usize = u16::MAX as usize;

  /// Create an empty meta.
  #[inline(always)]
  pub const fn empty() -> Meta {
    Meta(Bytes::new())
  }

  /// Create a meta from a static str.
  #[inline(always)]
  pub const fn from_static_str(s: &'static str) -> Result<Self, LargeMeta> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::from_static(s.as_bytes())))
  }

  /// Create a meta from a static bytes.
  #[inline(always)]
  pub const fn from_static(s: &'static [u8]) -> Result<Self, LargeMeta> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::from_static(s)))
  }

  /// Returns the meta as a byte slice.
  #[inline(always)]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  /// Returns true if the meta is empty.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns the length of the meta in bytes.
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.0.len()
  }
}

impl AsRef<[u8]> for Meta {
  fn as_ref(&self) -> &[u8] {
    self.as_bytes()
  }
}

impl core::ops::Deref for Meta {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_bytes()
  }
}

impl core::cmp::PartialEq<[u8]> for Meta {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_bytes().eq(other)
  }
}

impl core::cmp::PartialEq<&[u8]> for Meta {
  fn eq(&self, other: &&[u8]) -> bool {
    self.as_bytes().eq(*other)
  }
}

impl core::cmp::PartialEq<Bytes> for Meta {
  fn eq(&self, other: &Bytes) -> bool {
    self.as_bytes().eq(other.as_ref())
  }
}

impl core::cmp::PartialEq<Vec<u8>> for Meta {
  fn eq(&self, other: &Vec<u8>) -> bool {
    self.as_bytes().eq(other.as_slice())
  }
}

impl TryFrom<&str> for Meta {
  type Error = LargeMeta;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl FromStr for Meta {
  type Err = LargeMeta;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Meta::try_from(s)
  }
}

impl TryFrom<String> for Meta {
  type Error = LargeMeta;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    Meta::try_from(s.into_bytes())
  }
}

impl TryFrom<Bytes> for Meta {
  type Error = LargeMeta;

  fn try_from(s: Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(s))
  }
}

impl TryFrom<Vec<u8>> for Meta {
  type Error = LargeMeta;

  fn try_from(s: Vec<u8>) -> Result<Self, Self::Error> {
    Meta::try_from(Bytes::from(s))
  }
}

impl TryFrom<&[u8]> for Meta {
  type Error = LargeMeta;

  fn try_from(s: &[u8]) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::copy_from_slice(s)))
  }
}

impl TryFrom<&Bytes> for Meta {
  type Error = LargeMeta;

  fn try_from(s: &Bytes) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(s.clone()))
  }
}

impl TryFrom<BytesMut> for Meta {
  type Error = LargeMeta;

  fn try_from(s: BytesMut) -> Result<Self, Self::Error> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(s.freeze()))
  }
}

// ─── ProtocolVersion / DelegateVersion ───────────────────────────────────────

/// Delegate version
#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum DelegateVersion {
  /// Version 1
  #[default]
  #[display("v1")]
  V1,
  /// Unknown version (used for forwards and backwards compatibility)
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl From<u8> for DelegateVersion {
  fn from(v: u8) -> Self {
    match v {
      1 => Self::V1,
      val => Self::Unknown(val),
    }
  }
}

impl From<DelegateVersion> for u8 {
  fn from(v: DelegateVersion) -> Self {
    match v {
      DelegateVersion::V1 => 1,
      DelegateVersion::Unknown(val) => val,
    }
  }
}

/// Protocol version
#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum ProtocolVersion {
  /// Version 1
  #[default]
  #[display("v1")]
  V1,
  /// Unknown version (used for forwards and backwards compatibility)
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl From<u8> for ProtocolVersion {
  fn from(v: u8) -> Self {
    match v {
      1 => Self::V1,
      val => Self::Unknown(val),
    }
  }
}

impl From<ProtocolVersion> for u8 {
  fn from(v: ProtocolVersion) -> Self {
    match v {
      ProtocolVersion::V1 => 1,
      ProtocolVersion::Unknown(val) => val,
    }
  }
}

// ─── State / NodeState ───────────────────────────────────────────────────────

use std::borrow::Cow;

/// State for the memberlist
#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum State {
  /// Alive state
  #[default]
  #[display("alive")]
  Alive,
  /// Suspect state
  #[display("suspect")]
  Suspect,
  /// Dead state
  #[display("dead")]
  Dead,
  /// Left state
  #[display("left")]
  Left,
  /// Unknown state (used for forwards and backwards compatibility)
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl From<u8> for State {
  fn from(value: u8) -> Self {
    match value {
      0 => Self::Alive,
      1 => Self::Suspect,
      2 => Self::Dead,
      3 => Self::Left,
      val => Self::Unknown(val),
    }
  }
}

impl From<State> for u8 {
  fn from(value: State) -> Self {
    match value {
      State::Alive => 0,
      State::Suspect => 1,
      State::Dead => 2,
      State::Left => 3,
      State::Unknown(val) => val,
    }
  }
}

impl State {
  /// Returns the [`State`] as a str representation.
  #[inline(always)]
  pub fn as_str(&self) -> Cow<'static, str> {
    match self {
      Self::Alive => Cow::Borrowed("alive"),
      Self::Suspect => Cow::Borrowed("suspect"),
      Self::Dead => Cow::Borrowed("dead"),
      Self::Left => Cow::Borrowed("left"),
      Self::Unknown(val) => Cow::Owned(format!("unknown({val})")),
    }
  }
}

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

impl<I: CheapClone, A: CheapClone> From<Alive<I, A>> for NodeState<I, A> {
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

impl<I: CheapClone, A: CheapClone> CheapClone for NodeState<I, A> {
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

impl<I: CheapClone, A: CheapClone> NodeState<I, A> {
  /// Returns a [`Node`] with the same id and address as this [`NodeState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

impl<I: core::fmt::Display, A: core::fmt::Display> core::fmt::Display for NodeState<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}({})", self.id, self.addr)
  }
}

// ─── Alive<I,A> ──────────────────────────────────────────────────────────────

/// Alive message
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Alive<I, A> {
  /// The incarnation of the alive message
  incarnation: u32,
  /// The meta of the alive message
  meta: Meta,
  /// The node of the alive message
  node: Node<I, A>,
  /// The protocol version of the alive message is speaking
  protocol_version: ProtocolVersion,
  /// The delegate version of the alive message is speaking
  delegate_version: DelegateVersion,
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

// ─── Suspect<I> / Dead<I> ────────────────────────────────────────────────────

macro_rules! bad_bail_typed {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct $name<I> {
      /// The incarnation of the message.
      incarnation: u32,
      /// The node of the message.
      node: I,
      /// The source node of the message.
      from: I,
    }

    impl<I> $name<I> {
      /// Create a new message
      #[inline(always)]
      pub const fn new(incarnation: u32, node: I, from: I) -> Self {
        Self {
          incarnation,
          node,
          from,
        }
      }

      /// Returns the incarnation of the message.
      #[inline(always)]
      pub const fn incarnation(&self) -> u32 {
        self.incarnation
      }

      /// Returns the node of the message.
      #[inline(always)]
      pub const fn node_ref(&self) -> &I {
        &self.node
      }

      /// Returns the source node of the message.
      #[inline(always)]
      pub const fn from_ref(&self) -> &I {
        &self.from
      }

      /// Sets the incarnation of the message
      #[inline(always)]
      pub const fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
        self.incarnation = incarnation;
        self
      }

      /// Sets the incarnation of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub const fn with_incarnation(mut self, incarnation: u32) -> Self {
        self.incarnation = incarnation;
        self
      }

      /// Sets the source node of the message
      #[inline(always)]
      pub fn set_from(&mut self, source: I) -> &mut Self {
        self.from = source;
        self
      }

      /// Sets the source node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_from(mut self, source: I) -> Self {
        self.from = source;
        self
      }

      /// Sets the node which in this state
      #[inline(always)]
      pub fn set_node(&mut self, target: I) -> &mut Self {
        self.node = target;
        self
      }

      /// Sets the node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_node(mut self, target: I) -> Self {
        self.node = target;
        self
      }
    }
  };
}

bad_bail_typed!(
  /// Suspect message
  Suspect
);
bad_bail_typed!(
  /// Dead message
  Dead
);

// ─── Ack / Nack ──────────────────────────────────────────────────────────────

/// Ack response is sent for a ping
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ack {
  /// The sequence number of the ack
  sequence_number: u32,
  /// The payload of the ack
  payload: Bytes,
}

impl Ack {
  /// Create a new ack response with the given sequence number and empty payload.
  #[inline(always)]
  pub const fn new(sequence_number: u32) -> Self {
    Self {
      sequence_number,
      payload: Bytes::new(),
    }
  }

  /// Returns the sequence number of the ack
  #[inline(always)]
  pub const fn sequence_number(&self) -> u32 {
    self.sequence_number
  }

  /// Returns the payload of the ack as a byte slice.
  #[inline(always)]
  pub fn payload(&self) -> &[u8] {
    self.payload.as_ref()
  }

  /// Cheap-clones the payload of the ack as a `Bytes` handle.
  #[inline(always)]
  pub fn payload_bytes(&self) -> Bytes {
    self.payload.clone()
  }

  /// Sets the sequence number of the ack
  #[inline(always)]
  pub const fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the sequence number of the ack (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_sequence_number(mut self, sequence_number: u32) -> Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the payload of the ack
  #[inline(always)]
  pub fn set_payload(&mut self, payload: Bytes) -> &mut Self {
    self.payload = payload;
    self
  }

  /// Sets the payload of the ack (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_payload(mut self, payload: Bytes) -> Self {
    self.payload = payload;
    self
  }

  /// Consumes the [`Ack`] and returns the sequence number and payload
  #[inline(always)]
  pub fn into_components(self) -> (u32, Bytes) {
    (self.sequence_number, self.payload)
  }
}

/// Nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Nack {
  sequence_number: u32,
}

impl Nack {
  /// Create a new nack response with the given sequence number.
  #[inline(always)]
  pub const fn new(sequence_number: u32) -> Self {
    Self { sequence_number }
  }

  /// Returns the sequence number of the nack
  #[inline(always)]
  pub const fn sequence_number(&self) -> u32 {
    self.sequence_number
  }

  /// Sets the sequence number of the nack response
  #[inline(always)]
  pub const fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the sequence number of the nack response (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_sequence_number(mut self, sequence_number: u32) -> Self {
    self.sequence_number = sequence_number;
    self
  }
}

// ─── Ping<I,A> / IndirectPing<I,A> ───────────────────────────────────────────

macro_rules! bail_ping_typed {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct $name<I, A> {
      /// The sequence number of the ack
      sequence_number: u32,

      /// Source target, used for a direct reply
      source: Node<I, A>,

      /// [`Node`] is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      target: Node<I, A>,
    }

    impl<I, A> $name<I, A> {
      /// Create a new message
      #[inline(always)]
      pub const fn new(sequence_number: u32, source: Node<I, A>, target: Node<I, A>) -> Self {
        Self {
          sequence_number,
          source,
          target,
        }
      }

      /// Returns the sequence number of the message
      #[inline(always)]
      pub const fn sequence_number(&self) -> u32 {
        self.sequence_number
      }

      /// Returns the source node of the message
      #[inline(always)]
      pub const fn source_ref(&self) -> &Node<I, A> {
        &self.source
      }

      /// Returns the target node of the message
      #[inline(always)]
      pub const fn target_ref(&self) -> &Node<I, A> {
        &self.target
      }

      /// Sets the sequence number of the message
      #[inline(always)]
      pub const fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
        self.sequence_number = sequence_number;
        self
      }

      /// Sets the sequence number of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub const fn with_sequence_number(mut self, sequence_number: u32) -> Self {
        self.sequence_number = sequence_number;
        self
      }

      /// Sets the source node of the message
      #[inline(always)]
      pub fn set_source(&mut self, source: Node<I, A>) -> &mut Self {
        self.source = source;
        self
      }

      /// Sets the source node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_source(mut self, source: Node<I, A>) -> Self {
        self.source = source;
        self
      }

      /// Sets the target node of the message
      #[inline(always)]
      pub fn set_target(&mut self, target: Node<I, A>) -> &mut Self {
        self.target = target;
        self
      }

      /// Sets the target node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_target(mut self, target: Node<I, A>) -> Self {
        self.target = target;
        self
      }
    }

    impl<I: CheapClone, A: CheapClone> CheapClone for $name<I, A> {
      fn cheap_clone(&self) -> Self {
        Self {
          sequence_number: self.sequence_number,
          source: self.source.cheap_clone(),
          target: self.target.cheap_clone(),
        }
      }
    }
  };
}

bail_ping_typed!(
  #[doc = "Ping is sent to a target to check if it is alive"]
  Ping
);
bail_ping_typed!(
  #[doc = "IndirectPing is sent to a target to check if it is alive"]
  IndirectPing
);

impl<I, A> From<IndirectPing<I, A>> for Ping<I, A> {
  fn from(ping: IndirectPing<I, A>) -> Self {
    Self {
      sequence_number: ping.sequence_number,
      source: ping.source,
      target: ping.target,
    }
  }
}

// ─── ErrorResponse ───────────────────────────────────────────────────────────

use smol_str::SmolStr;

/// Error response from the remote peer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ErrorResponse {
  message: SmolStr,
}

impl ErrorResponse {
  /// Create a new error response
  #[inline(always)]
  pub fn new(message: impl Into<SmolStr>) -> Self {
    Self {
      message: message.into(),
    }
  }

  /// Returns the message of the error response as a string slice.
  #[inline(always)]
  pub fn message(&self) -> &str {
    self.message.as_str()
  }

  /// Sets the msg of the error response
  #[inline(always)]
  pub fn set_message(&mut self, msg: impl Into<SmolStr>) -> &mut Self {
    self.message = msg.into();
    self
  }

  /// Sets the msg of the error response (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_message(mut self, msg: impl Into<SmolStr>) -> Self {
    self.message = msg.into();
    self
  }
}

impl core::fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.message)
  }
}

impl std::error::Error for ErrorResponse {}

impl From<ErrorResponse> for SmolStr {
  fn from(err: ErrorResponse) -> Self {
    err.message
  }
}

impl From<SmolStr> for ErrorResponse {
  fn from(msg: SmolStr) -> Self {
    Self { message: msg }
  }
}

// ─── PushNodeState<I,A> ──────────────────────────────────────────────────────

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

// ─── PushPull<I,A> ───────────────────────────────────────────────────────────

use triomphe::Arc;

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

// ─── owned Message<I,A> enum ─────────────────────────────────────────────────

/// Tag constants for the owned `Message` enum variants.
pub mod message_tags {
  /// Ping message tag
  pub const PING: u8 = 2;
  /// IndirectPing message tag
  pub const INDIRECT_PING: u8 = 3;
  /// Ack message tag
  pub const ACK: u8 = 4;
  /// Suspect message tag
  pub const SUSPECT: u8 = 5;
  /// Alive message tag
  pub const ALIVE: u8 = 6;
  /// Dead message tag
  pub const DEAD: u8 = 7;
  /// PushPull message tag
  pub const PUSH_PULL: u8 = 8;
  /// UserData message tag
  pub const USER_DATA: u8 = 9;
  /// Nack message tag
  pub const NACK: u8 = 10;
  /// ErrorResponse message tag
  pub const ERROR_RESPONSE: u8 = 11;
}

/// Owned message enum — the application-facing representation of all
/// memberlist gossip messages. Carries NO wire codec. [`crate::bridge`]
/// bridges this to the buffa-generated concrete codec via [`crate::data::Data`].
#[derive(
  Debug,
  Clone,
  derive_more::From,
  derive_more::IsVariant,
  derive_more::Unwrap,
  derive_more::TryUnwrap,
  PartialEq,
  Eq,
  Hash,
)]
#[non_exhaustive]
pub enum Message<I, A> {
  /// Ping message
  Ping(Ping<I, A>),
  /// Indirect ping message
  IndirectPing(IndirectPing<I, A>),
  /// Ack response message
  Ack(Ack),
  /// Suspect message
  Suspect(Suspect<I>),
  /// Alive message
  Alive(Alive<I, A>),
  /// Dead message
  Dead(Dead<I>),
  /// PushPull message
  PushPull(PushPull<I, A>),
  /// User mesg, not handled by us
  UserData(Bytes),
  /// Nack response message
  Nack(Nack),
  /// Error response message
  ErrorResponse(ErrorResponse),
}

impl<I, A> Message<I, A> {
  /// Returns the wire tag byte for this message variant.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    match self {
      Self::Ping(_) => message_tags::PING,
      Self::IndirectPing(_) => message_tags::INDIRECT_PING,
      Self::Ack(_) => message_tags::ACK,
      Self::Suspect(_) => message_tags::SUSPECT,
      Self::Alive(_) => message_tags::ALIVE,
      Self::Dead(_) => message_tags::DEAD,
      Self::PushPull(_) => message_tags::PUSH_PULL,
      Self::UserData(_) => message_tags::USER_DATA,
      Self::Nack(_) => message_tags::NACK,
      Self::ErrorResponse(_) => message_tags::ERROR_RESPONSE,
    }
  }

  /// Construct a [`Message`] from a [`Ping`].
  #[inline(always)]
  pub const fn ping(val: Ping<I, A>) -> Self {
    Self::Ping(val)
  }

  /// Construct a [`Message`] from an [`IndirectPing`].
  #[inline(always)]
  pub const fn indirect_ping(val: IndirectPing<I, A>) -> Self {
    Self::IndirectPing(val)
  }

  /// Construct a [`Message`] from an [`Ack`].
  #[inline(always)]
  pub const fn ack(val: Ack) -> Self {
    Self::Ack(val)
  }

  /// Construct a [`Message`] from a [`Suspect`].
  #[inline(always)]
  pub const fn suspect(val: Suspect<I>) -> Self {
    Self::Suspect(val)
  }

  /// Construct a [`Message`] from an [`Alive`].
  #[inline(always)]
  pub const fn alive(val: Alive<I, A>) -> Self {
    Self::Alive(val)
  }

  /// Construct a [`Message`] from a [`Dead`].
  #[inline(always)]
  pub const fn dead(val: Dead<I>) -> Self {
    Self::Dead(val)
  }

  /// Construct a [`Message`] from a [`PushPull`].
  #[inline(always)]
  pub const fn push_pull(val: PushPull<I, A>) -> Self {
    Self::PushPull(val)
  }

  /// Construct a [`Message`] from user data bytes.
  #[inline(always)]
  pub const fn user_data(val: Bytes) -> Self {
    Self::UserData(val)
  }

  /// Construct a [`Message`] from a [`Nack`].
  #[inline(always)]
  pub const fn nack(val: Nack) -> Self {
    Self::Nack(val)
  }

  /// Construct a [`Message`] from an [`ErrorResponse`].
  #[inline(always)]
  pub const fn error_response(val: ErrorResponse) -> Self {
    Self::ErrorResponse(val)
  }
}
