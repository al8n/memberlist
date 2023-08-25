use super::*;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[repr(u8)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(u8), non_exhaustive)]
#[non_exhaustive]
pub enum NodeState {
  #[default]
  Alive = 0,
  Suspect = 1,
  Dead = 2,
  Left = 3,
}

impl NodeState {
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

impl core::fmt::Display for NodeState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl From<NodeState> for ArchivedNodeState {
  fn from(value: NodeState) -> Self {
    match value {
      NodeState::Alive => Self::Alive,
      NodeState::Suspect => Self::Suspect,
      NodeState::Dead => Self::Dead,
      NodeState::Left => Self::Left,
    }
  }
}

impl From<ArchivedNodeState> for NodeState {
  fn from(value: ArchivedNodeState) -> Self {
    match value {
      ArchivedNodeState::Alive => Self::Alive,
      ArchivedNodeState::Suspect => Self::Suspect,
      ArchivedNodeState::Dead => Self::Dead,
      ArchivedNodeState::Left => Self::Left,
    }
  }
}

/// Represents a node in the cluster, can be thought as an identifier for a node
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoteNode {
  pub name: Option<Name>,
  pub addr: IpAddr,
  pub port: Option<u16>,
}

/// Represents a node in the cluster
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
  #[viewit(getter(const, style = "ref"))]
  id: NodeId,
  /// Metadata from the delegate for this node.
  #[viewit(getter(const, style = "ref"))]
  meta: Bytes,
  /// State of the node.
  state: NodeState,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl Node {
  /// Construct a new node with the given name, address and state.
  #[inline]
  pub fn new(
    name: Name,
    addr: SocketAddr,
    state: NodeState,
    protocol_version: ProtocolVersion,
    delegate_version: DelegateVersion,
  ) -> Self {
    Self {
      id: NodeId { name, addr },
      meta: Bytes::new(),
      state,
      protocol_version,
      delegate_version,
    }
  }

  /// Return the node name
  #[inline]
  pub fn name(&self) -> &Name {
    &self.id.name
  }

  #[inline]
  pub fn address(&self) -> SocketAddr {
    self.id.addr()
  }
}

impl core::fmt::Display for Node {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}({})", self.id.name.as_ref(), self.id.addr)
  }
}
