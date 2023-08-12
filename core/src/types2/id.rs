use rkyv::{Archive, Deserialize, Serialize};
use std::net::SocketAddr;

use super::Name;

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
