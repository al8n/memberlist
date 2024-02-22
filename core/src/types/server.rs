use bytes::Bytes;
use nodecraft::{CheapClone, Node};

use super::{DelegateVersion, ProtocolVersion};

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
  #[default]
  Alive = 0,
  Suspect = 1,
  Dead = 2,
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

  #[cfg(feature = "metrics")]
  #[inline]
  pub(crate) const fn empty_metrics() -> [(&'static str, usize); 4] {
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct UnknownState(u8);

impl core::fmt::Display for UnknownState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{} is not a valid state", self.0)
  }
}

impl std::error::Error for UnknownState {}

/// Represents a node in the cluster
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub(crate)", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
pub struct NodeState<I, A> {
  #[viewit(getter(const, style = "ref"))]
  id: I,
  #[viewit(
    getter(const, rename = "address", style = "ref"),
    setter(rename = "with_address")
  )]
  addr: A,
  /// Metadata from the delegate for this node.
  #[viewit(getter(const, style = "ref"))]
  meta: Bytes,
  /// State of the node.
  state: State,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl<I, A> NodeState<I, A> {
  /// Construct a new node with the given name, address and state.
  #[inline]
  pub fn new(
    id: I,
    addr: A,
    state: State,
    protocol_version: ProtocolVersion,
    delegate_version: DelegateVersion,
  ) -> Self {
    Self {
      id,
      addr,
      meta: Bytes::new(),
      state,
      protocol_version,
      delegate_version,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for NodeState<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      id: self.id.cheap_clone(),
      addr: self.addr.cheap_clone(),
      meta: self.meta.clone(),
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
