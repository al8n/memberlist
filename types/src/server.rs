use nodecraft::{CheapClone, Node};

use super::{DelegateVersion, Meta, ProtocolVersion};

/// State for the memberlist
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(derive(Debug, Clone, PartialEq, Eq, Hash))
)]
#[repr(u8)]
#[non_exhaustive]
pub enum State {
  /// Alive state
  #[default]
  Alive = 0,
  /// Suspect state
  Suspect = 1,
  /// Dead state
  Dead = 2,
  /// Left state
  Left = 3,
}

impl State {
  /// Returns the [`NodeStateState`] as a `&'static str`.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Alive => "alive",
      Self::Suspect => "suspect",
      Self::Dead => "dead",
      Self::Left => "left",
    }
  }

  /// Returns an array of the default state metrics.
  #[cfg(feature = "metrics")]
  #[cfg_attr(feature = "docs", doc(cfg(feature = "metrics")))]
  #[inline]
  pub const fn metrics_array() -> [(&'static str, usize); 4] {
    [("alive", 0), ("suspect", 0), ("dead", 0), ("left", 0)]
  }
}

impl core::fmt::Display for State {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl TryFrom<u8> for State {
  type Error = UnknownState;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      0 => Self::Alive,
      1 => Self::Suspect,
      2 => Self::Dead,
      3 => Self::Left,
      _ => return Err(UnknownState(value)),
    })
  }
}

/// Unknown server state.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("{0} is not a valid state")]
pub struct UnknownState(u8);

/// Represents a node in the cluster
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
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

impl<I: CheapClone, A: CheapClone> From<super::Alive<I, A>> for NodeState<I, A> {
  fn from(value: super::Alive<I, A>) -> Self {
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
      protocol_version: ProtocolVersion::V0,
      delegate_version: DelegateVersion::V0,
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

#[cfg(feature = "rkyv")]
const _: () = {
  use core::fmt::Debug;
  use rkyv::Archive;

  impl<I: Debug + Archive, A: Debug + Archive> core::fmt::Debug for ArchivedNodeState<I, A>
  where
    I::Archived: Debug,
    A::Archived: Debug,
  {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      f.debug_struct("NodeState")
        .field("id", &self.id)
        .field("addr", &self.addr)
        .field("meta", &self.meta)
        .field("state", &self.state)
        .field("protocol_version", &self.protocol_version)
        .field("delegate_version", &self.delegate_version)
        .finish()
    }
  }

  impl<I: Archive, A: Archive> PartialEq for ArchivedNodeState<I, A>
  where
    I::Archived: PartialEq,
    A::Archived: PartialEq,
  {
    fn eq(&self, other: &Self) -> bool {
      self.id == other.id
        && self.addr == other.addr
        && self.meta == other.meta
        && self.state == other.state
        && self.protocol_version == other.protocol_version
        && self.delegate_version == other.delegate_version
    }
  }

  impl<I: Archive, A: Archive> Eq for ArchivedNodeState<I, A>
  where
    I::Archived: Eq,
    A::Archived: Eq,
  {
  }

  impl<I: Archive, A: Archive> core::hash::Hash for ArchivedNodeState<I, A>
  where
    I::Archived: core::hash::Hash,
    A::Archived: core::hash::Hash,
  {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
      self.id.hash(state);
      self.addr.hash(state);
      self.meta.hash(state);
      self.state.hash(state);
      self.protocol_version.hash(state);
      self.delegate_version.hash(state);
    }
  }
};

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use smol_str::SmolStr;

  use super::*;

  #[test]
  fn test_state_try_from() {
    assert_eq!(State::try_from(0), Ok(State::Alive));
    assert_eq!(State::try_from(1), Ok(State::Suspect));
    assert_eq!(State::try_from(2), Ok(State::Dead));
    assert_eq!(State::try_from(3), Ok(State::Left));
    assert_eq!(State::try_from(4), Err(UnknownState(4)));
  }

  #[test]
  fn test_state_as_str() {
    assert_eq!(State::Alive.as_str(), "alive");
    assert_eq!(State::Suspect.as_str(), "suspect");
    assert_eq!(State::Dead.as_str(), "dead");
    assert_eq!(State::Left.as_str(), "left");
  }

  #[test]
  fn test_node_state_cheap_clone() {
    let node = NodeState::<_, SocketAddr>::new(
      SmolStr::from("a"),
      "127.0.0.1:8080".parse().unwrap(),
      State::Alive,
    );
    let node2 = node.cheap_clone();
    assert_eq!(node, node2);
  }

  #[test]
  fn test_access() {
    let mut node = NodeState::<_, SocketAddr>::new(
      SmolStr::from("a"),
      "127.0.0.1:8080".parse().unwrap(),
      State::Alive,
    );

    node.set_address("127.0.0.1:8081".parse().unwrap());
    assert_eq!(node.address(), &SocketAddr::from(([127, 0, 0, 1], 8081)));
    node.set_id(SmolStr::from("b"));
    assert_eq!(node.id(), &SmolStr::from("b"));
    node.set_meta(Meta::empty());
    assert_eq!(node.meta(), &Meta::empty());
    node.set_state(State::Dead);
    assert_eq!(node.state(), State::Dead);
    node.set_protocol_version(ProtocolVersion::V0);
    assert_eq!(node.protocol_version(), ProtocolVersion::V0);
    node.set_delegate_version(DelegateVersion::V0);
    assert_eq!(node.delegate_version(), DelegateVersion::V0);
    println!("{}", node);
  }
}
