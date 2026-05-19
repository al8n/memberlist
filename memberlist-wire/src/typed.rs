//! Generic owned message *shapes* (`Alive<I,A>`, `Suspect<I>`, …).
//!
//! These are the application-facing typed representations consumed by
//! `memberlist-machine`. They carry NO wire codec — the hand-rolled
//! `Data`/`DataRef` message impls from `memberlist-proto` are deliberately
//! left behind (frozen backup). [`crate::bridge`] bridges these to the
//! buffa-generated concrete codec in [`crate::messages`] using the
//! [`crate::data::Data`] trait for the `I`/`A` byte fields.

// ─── re-exports ──────────────────────────────────────────────────────────────

/// Re-export `Node` from `nodecraft`; NOT redefined here.
pub use nodecraft::{CheapClone, Node};

// ─── Meta ────────────────────────────────────────────────────────────────────

use std::str::FromStr;

use bytes::{Bytes, BytesMut};

/// Invalid meta error.
#[derive(Debug, thiserror::Error)]
#[error("the size of meta must between [0-512] bytes, got {0}")]
pub struct LargeMeta(usize);

/// The metadata of a node in the cluster.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Meta(Bytes);

impl Default for Meta {
  #[inline]
  fn default() -> Self {
    Self::empty()
  }
}

impl CheapClone for Meta {}

impl Meta {
  /// The maximum size of a name in bytes.
  pub const MAX_SIZE: usize = 512;

  /// Create an empty meta.
  #[inline]
  pub const fn empty() -> Meta {
    Meta(Bytes::new())
  }

  /// Create a meta from a static str.
  #[inline]
  pub const fn from_static_str(s: &'static str) -> Result<Self, LargeMeta> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::from_static(s.as_bytes())))
  }

  /// Create a meta from a static bytes.
  #[inline]
  pub const fn from_static(s: &'static [u8]) -> Result<Self, LargeMeta> {
    if s.len() > Self::MAX_SIZE {
      return Err(LargeMeta(s.len()));
    }
    Ok(Self(Bytes::from_static(s)))
  }

  /// Returns the meta as a byte slice.
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  /// Returns true if the meta is empty.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns the length of the meta in bytes.
  #[inline]
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
  #[inline]
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
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeState<I, A> {
  /// The id of the node.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the id of the node")),
    setter(attrs(doc = "Sets the id of the node (Builder pattern)"))
  )]
  id: I,
  /// The address of the node.
  #[viewit(
    getter(
      const,
      rename = "address",
      style = "ref",
      attrs(doc = "Returns the address of the node")
    ),
    setter(
      rename = "with_address",
      attrs(doc = "Sets the address of the node (Builder pattern)")
    )
  )]
  addr: A,
  /// Metadata from the delegate for this node.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the meta of the node")),
    setter(attrs(doc = "Sets the meta of the node (Builder pattern)"))
  )]
  meta: Meta,
  /// State of the node.
  #[viewit(
    getter(const, attrs(doc = "Returns the state of the node")),
    setter(const, attrs(doc = "Sets the state of the node (Builder pattern)"))
  )]
  state: State,
  /// The protocol version of the node is speaking.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the protocol version of the node is speaking")
    ),
    setter(
      const,
      attrs(doc = "Sets the protocol version of the node is speaking (Builder pattern)")
    )
  )]
  protocol_version: ProtocolVersion,
  /// The delegate version of the node is speaking.
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the delegate version of the node is speaking")
    ),
    setter(
      const,
      attrs(doc = "Sets the delegate version of the node is speaking (Builder pattern)")
    )
  )]
  delegate_version: DelegateVersion,
}

impl<I: CheapClone, A: CheapClone> From<Alive<I, A>> for NodeState<I, A> {
  fn from(value: Alive<I, A>) -> Self {
    let (id, addr) = value.node.into_components();
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
  #[inline]
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

  /// Sets the id of the node state
  #[inline]
  pub fn set_id(&mut self, id: I) -> &mut Self {
    self.id = id;
    self
  }

  /// Sets the address of the node state
  #[inline]
  pub fn set_address(&mut self, addr: A) -> &mut Self {
    self.addr = addr;
    self
  }

  /// Sets the metadata for the node.
  #[inline]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the state for the node.
  #[inline]
  pub fn set_state(&mut self, state: State) -> &mut Self {
    self.state = state;
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

// ─── Suspect<I> / Dead<I> ────────────────────────────────────────────────────

macro_rules! bad_bail_typed {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[viewit::viewit(
      getters(vis_all = "pub"),
      setters(vis_all = "pub", prefix = "with")
    )]
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct $name<I> {
      /// The incarnation of the message.
      #[viewit(
        getter(const, attrs(doc = "Returns the incarnation of the message")),
        setter(
          const,
          attrs(doc = "Sets the incarnation of the message (Builder pattern)")
        )
      )]
      incarnation: u32,
      /// The node of the message.
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the node of the message")),
        setter(attrs(doc = "Sets the node of the message (Builder pattern)"))
      )]
      node: I,
      /// The source node of the message.
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the source node of the message")),
        setter(attrs(doc = "Sets the source node of the message (Builder pattern)"))
      )]
      from: I,
    }

    impl<I> $name<I> {
      /// Create a new message
      #[inline]
      pub const fn new(incarnation: u32, node: I, from: I) -> Self {
        Self {
          incarnation,
          node,
          from,
        }
      }

      /// Sets the incarnation of the message
      #[inline]
      pub fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
        self.incarnation = incarnation;
        self
      }

      /// Sets the source node of the message
      #[inline]
      pub fn set_from(&mut self, source: I) -> &mut Self {
        self.from = source;
        self
      }

      /// Sets the node which in this state
      #[inline]
      pub fn set_node(&mut self, target: I) -> &mut Self {
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
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ack {
  /// The sequence number of the ack
  #[viewit(
    getter(const, attrs(doc = "Returns the sequence number of the ack")),
    setter(
      const,
      attrs(doc = "Sets the sequence number of the ack (Builder pattern)")
    )
  )]
  sequence_number: u32,
  /// The payload of the ack
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the payload of the ack")),
    setter(attrs(doc = "Sets the payload of the ack (Builder pattern)"))
  )]
  payload: Bytes,
}

impl Ack {
  /// Create a new ack response with the given sequence number and empty payload.
  #[inline]
  pub const fn new(sequence_number: u32) -> Self {
    Self {
      sequence_number,
      payload: Bytes::new(),
    }
  }

  /// Sets the sequence number of the ack
  #[inline]
  pub fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the payload of the ack
  #[inline]
  pub fn set_payload(&mut self, payload: Bytes) -> &mut Self {
    self.payload = payload;
    self
  }

  /// Consumes the [`Ack`] and returns the sequence number and payload
  #[inline]
  pub fn into_components(self) -> (u32, Bytes) {
    (self.sequence_number, self.payload)
  }
}

/// Nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Nack {
  #[viewit(
    getter(const, attrs(doc = "Returns the sequence number of the nack")),
    setter(
      const,
      attrs(doc = "Sets the sequence number of the nack (Builder pattern)")
    )
  )]
  sequence_number: u32,
}

impl Nack {
  /// Create a new nack response with the given sequence number.
  #[inline]
  pub const fn new(sequence_number: u32) -> Self {
    Self { sequence_number }
  }

  /// Sets the sequence number of the nack response
  #[inline]
  pub fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
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
    #[viewit::viewit(
      getters(vis_all = "pub"),
      setters(vis_all = "pub", prefix = "with")
    )]
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct $name<I, A> {
      /// The sequence number of the ack
      #[viewit(
        getter(const, attrs(doc = "Returns the sequence number of the ack")),
        setter(
          const,
          attrs(doc = "Sets the sequence number of the ack (Builder pattern)")
        )
      )]
      sequence_number: u32,

      /// Source target, used for a direct reply
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the source node of the ping message")),
        setter(attrs(doc = "Sets the source node of the ping message (Builder pattern)"))
      )]
      source: Node<I, A>,

      /// [`Node`] is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the target node of the ping message")),
        setter(attrs(doc = "Sets the target node of the ping message (Builder pattern)"))
      )]
      target: Node<I, A>,
    }

    impl<I, A> $name<I, A> {
      /// Create a new message
      #[inline]
      pub const fn new(sequence_number: u32, source: Node<I, A>, target: Node<I, A>) -> Self {
        Self {
          sequence_number,
          source,
          target,
        }
      }

      /// Sets the sequence number of the message
      #[inline]
      pub fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
        self.sequence_number = sequence_number;
        self
      }

      /// Sets the source node of the message
      #[inline]
      pub fn set_source(&mut self, source: Node<I, A>) -> &mut Self {
        self.source = source;
        self
      }

      /// Sets the target node of the message
      #[inline]
      pub fn set_target(&mut self, target: Node<I, A>) -> &mut Self {
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
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ErrorResponse {
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the msg of the error response")
    ),
    setter(attrs(doc = "Sets the msg of the error response (Builder pattern)"))
  )]
  message: SmolStr,
}

impl ErrorResponse {
  /// Create a new error response
  pub fn new(message: impl Into<SmolStr>) -> Self {
    Self {
      message: message.into(),
    }
  }

  /// Returns the msg of the error response
  pub fn set_message(&mut self, msg: impl Into<SmolStr>) -> &mut Self {
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
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushNodeState<I, A> {
  /// The id of the push node state.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the id of the push node state")
    ),
    setter(attrs(doc = "Sets the id of the push node state (Builder pattern)"))
  )]
  id: I,
  /// The address of the push node state.
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
  /// Metadata from the delegate for this push node state.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the meta of the push node state")
    ),
    setter(attrs(doc = "Sets the meta of the push node state (Builder pattern)"))
  )]
  meta: Meta,
  /// The incarnation of the push node state.
  #[viewit(
    getter(const, attrs(doc = "Returns the incarnation of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the incarnation of the push node state (Builder pattern)")
    )
  )]
  incarnation: u32,
  /// The state of the push node state.
  #[viewit(
    getter(const, attrs(doc = "Returns the state of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the state of the push node state (Builder pattern)")
    )
  )]
  state: State,
  /// The protocol version of the push node state is speaking.
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
  /// The delegate version of the push node state is speaking.
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
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
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

// ─── PushPull<I,A> ───────────────────────────────────────────────────────────

use triomphe::Arc;

/// Push pull message.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PushPull<I, A> {
  /// Whether the push pull message is a join message.
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
  /// The states of the push pull message.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the states of the push pull message")
    ),
    setter(attrs(doc = "Sets the states of the push pull message (Builder pattern)"))
  )]
  states: Arc<[PushNodeState<I, A>]>,
  /// The user data of the push pull message.
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
      states: self.states.clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> PushPull<I, A> {
  /// Create a new [`PushPull`] message.
  #[inline]
  pub fn new(join: bool, states: impl Iterator<Item = PushNodeState<I, A>>) -> Self {
    Self {
      states: Arc::from_iter(states),
      user_data: Bytes::new(),
      join,
    }
  }

  /// Consumes the [`PushPull`] and returns the states and user data.
  #[inline]
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
  #[inline]
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
  pub const fn ping(val: Ping<I, A>) -> Self {
    Self::Ping(val)
  }

  /// Construct a [`Message`] from an [`IndirectPing`].
  pub const fn indirect_ping(val: IndirectPing<I, A>) -> Self {
    Self::IndirectPing(val)
  }

  /// Construct a [`Message`] from an [`Ack`].
  pub const fn ack(val: Ack) -> Self {
    Self::Ack(val)
  }

  /// Construct a [`Message`] from a [`Suspect`].
  pub const fn suspect(val: Suspect<I>) -> Self {
    Self::Suspect(val)
  }

  /// Construct a [`Message`] from an [`Alive`].
  pub const fn alive(val: Alive<I, A>) -> Self {
    Self::Alive(val)
  }

  /// Construct a [`Message`] from a [`Dead`].
  pub const fn dead(val: Dead<I>) -> Self {
    Self::Dead(val)
  }

  /// Construct a [`Message`] from a [`PushPull`].
  pub const fn push_pull(val: PushPull<I, A>) -> Self {
    Self::PushPull(val)
  }

  /// Construct a [`Message`] from user data bytes.
  pub const fn user_data(val: Bytes) -> Self {
    Self::UserData(val)
  }

  /// Construct a [`Message`] from a [`Nack`].
  pub const fn nack(val: Nack) -> Self {
    Self::Nack(val)
  }

  /// Construct a [`Message`] from an [`ErrorResponse`].
  pub const fn error_response(val: ErrorResponse) -> Self {
    Self::ErrorResponse(val)
  }
}
