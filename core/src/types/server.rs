use super::*;

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
pub enum ServerState {
  #[default]
  Alive = 0,
  Suspect = 1,
  Dead = 2,
  Left = 3,
}

impl ServerState {
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

impl core::fmt::Display for ServerState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

// /// Represents a node in the cluster, can be thought as an identifier for a node
// #[derive(Clone, Debug, Eq, PartialEq, Hash)]
// pub struct RemoteServer<I, A> {
//   pub name: Option<Name>,
//   pub addr: IpAddr,
//   pub port: Option<u16>,
// }

/// Represents a node in the cluster
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
pub struct Server<I, A> {
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
  state: ServerState,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl<I, A> Server<I, A> {
  /// Construct a new node with the given name, address and state.
  #[inline]
  pub fn new(
    id: I,
    addr: A,
    state: ServerState,
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

impl<I: core::fmt::Display, A: core::fmt::Display> core::fmt::Display for Server<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}({})", self.id, self.addr)
  }
}
