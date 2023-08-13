use rkyv::{Archive, Deserialize, Serialize, validation::validators::CheckDeserializeError, de::deserializers::{SharedDeserializeMap, SharedDeserializeMapError}};
use std::{net::SocketAddr, borrow::Borrow};

use super::{Name, DecodeError};

#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub struct NodeId {
  #[viewit(getter(const, style = "ref"))]
  name: Name,
  addr: SocketAddr,
}

impl Eq for NodeId {}

impl PartialEq for NodeId {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.addr == other.addr
  }
}

impl core::hash::Hash for NodeId {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.addr.hash(state);
  }
}

impl NodeId {
  #[inline]
  pub fn new(name: Name, addr: SocketAddr) -> Self {
    Self { name, addr }
  }
}

impl From<(Name, SocketAddr)> for NodeId {
  fn from(val: (Name, SocketAddr)) -> Self {
    Self {
      name: val.0,
      addr: val.1,
    }
  }
}

impl core::fmt::Display for NodeId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}({})", self.name.as_str(), self.addr)
  }
}

impl Borrow<SocketAddr> for NodeId {
  fn borrow(&self) -> &SocketAddr {
    &self.addr
  }
}

impl Borrow<SocketAddr> for ArchivedNodeId {
  fn borrow(&self) -> &SocketAddr {
    &self.addr.as_socket_addr()
  }
}

impl ArchivedNodeId {
  pub fn addr(&self) -> SocketAddr {
    self.addr.as_socket_addr()
  }

  pub fn name(&self) -> &str {
    self.name.borrow()
  }
}

pub enum CowNodeId<'a> {
  Owned(NodeId),
  Borrowed(&'a NodeId),
  Archived(&'a ArchivedNodeId)
}

impl<'a> CowNodeId<'a> {
  pub fn name(&self) -> &str {
    match self {
      Self::Owned(id) => id.name.as_str(),
      Self::Borrowed(id) => id.name.as_str(),
      Self::Archived(id) => id.name(),
    }
  }

  pub fn addr(&self) -> SocketAddr {
    match self {
      Self::Owned(id) => id.addr,
      Self::Borrowed(id) => id.addr,
      Self::Archived(id) => id.addr(),
    }
  }

  pub fn to_owned(&self) -> Result<NodeId, DecodeError> {
    match self {
      Self::Owned(id) => Ok(id.clone()),
      Self::Borrowed(id) => Ok(*id.clone()),
      Self::Archived(id) => (*id).deserialize(&mut SharedDeserializeMap::default()).map_err(DecodeError::Decode),
    }
  }
}

pub type InvalidNodeId = CheckDeserializeError<ArchivedNodeId, SharedDeserializeMapError>;

impl<'a> From<NodeId> for CowNodeId<'a> {
  fn from(value: NodeId) -> Self {
    Self::Owned(value)
  }
}

impl<'a> From<&'a NodeId> for CowNodeId<'a> {
  fn from(value: &'a NodeId) -> Self {
    Self::Borrowed(value)
  }
}

impl<'a> From<&'a ArchivedNodeId> for CowNodeId<'a> {
  fn from(value: &'a ArchivedNodeId) -> Self {
    Self::Archived(value)
  }
}

impl<'a> PartialEq<Self> for CowNodeId<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.addr() == other.addr()
  }
}

impl<'a> PartialEq<NodeId> for CowNodeId<'a> {
  fn eq(&self, other: &NodeId) -> bool {
    self.addr() == other.addr()
  }
}

impl<'a> PartialEq<ArchivedNodeId> for CowNodeId<'a> {
  fn eq(&self, other: &ArchivedNodeId) -> bool {
    self.addr() == other.addr()
  }
}


impl<'a> PartialEq<CowNodeId<'a>> for NodeId {
  fn eq(&self, other: &CowNodeId<'a>) -> bool {
    self.addr() == other.addr()
  }
}

impl<'a> PartialEq<CowNodeId<'a>> for ArchivedNodeId {
  fn eq(&self, other: &CowNodeId<'a>) -> bool {
    self.addr() == other.addr()
  }
}

impl<'a> core::hash::Hash for CowNodeId<'a> {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.addr().hash(state)
  }
}

impl<'a> core::fmt::Display for CowNodeId<'a> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      CowNodeId::Owned(id) => id.fmt(f),
      CowNodeId::Borrowed(id) => id.fmt(f),
      CowNodeId::Archived(id) => id.fmt(f),
    }
  }
}

impl core::fmt::Display for ArchivedNodeId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}({})", self.name(), self.addr())
  }
}

#[test]
fn test_id_borrow() {
  use std::collections::HashSet;

  let mut m = HashSet::new();
  let addr = "127.0.0.1:80".parse().unwrap();
  m.insert(NodeId::new(Name::from_static_unchecked("foo"), addr));
  assert!(m.contains(&addr));
}