use std::{net::SocketAddr, time::Instant};

use bytes::Bytes;

pub type SharedString = std::borrow::Cow<'static, str>;

#[viewit::viewit(vis_all = "")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Packet {
  /// The raw contents of the packet.
  #[viewit(getter(style = "ref"))]
  buf: Bytes,

  /// Address of the peer. This is an actual [`SocketAddr`] so we
  /// can expose some concrete details about incoming packets.
  from: SocketAddr,

  /// The time when the packet was received. This should be
  /// taken as close as possible to the actual receipt time to help make an
  /// accurate RTT measurement during probes.
  timestamp: Instant,
}

impl Packet {
  pub fn new(buf: Bytes, from: SocketAddr, timestamp: Instant) -> Self {
    Self {
      buf,
      from,
      timestamp,
    }
  }
}

#[viewit::viewit(vis_all = "", getters(vis_all = "pub"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
  #[viewit(getter(const))]
  addr: SocketAddr,
  /// The name of the node being addressed. This is optional but
  /// transports may require it.
  #[viewit(getter(const, style = "ref"))]
  name: String,
}

impl From<SocketAddr> for Address {
  fn from(addr: SocketAddr) -> Self {
    Self {
      addr,
      name: Default::default(),
    }
  }
}

impl core::fmt::Display for Address {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{} ({})", self.name, self.addr)
  }
}

/// An ID of a type of message that can be received
/// on network channels from other members.
///
/// The list of available message types.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub enum MessageType {
  Ping,
  IndirectPing,
  AckResp,
  Suspect,
  Alive,
  Dead,
  PushPull,
  Compound,
  /// User mesg, not handled by us
  User,
  Compress,
  Encrypt,
  NackResp,
  HasCrc,
  Err,
  /// HasLabel has a deliberately high value so that you can disambiguate
  /// it from the encryptionVersion header which is either 0/1 right now and
  /// also any of the existing [`MessageType`].
  HasLabel = 244,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum NodeStateType {
  Alive,
  Suspect,
  Dead,
  Left,
}

impl core::fmt::Display for NodeStateType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Alive => write!(f, "alive"),
      Self::Suspect => write!(f, "suspect"),
      Self::Dead => write!(f, "dead"),
      Self::Left => write!(f, "left"),
    }
  }
}

impl core::str::FromStr for NodeStateType {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.trim().to_lowercase().as_str() {
      "alive" => Ok(Self::Alive),
      "suspect" => Ok(Self::Suspect),
      "dead" => Ok(Self::Dead),
      "left" => Ok(Self::Left),
      _ => Err(format!("invalid node state type: {}", s)),
    }
  }
}

/// Represents a node in the cluster
#[viewit::viewit(vis_all = "", getters(vis_all = "pub"), setters(vis_all = "pub"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
  #[viewit(getter(
    style = "ref",
    result(converter(fn = "SharedString::as_ref"), type = "&str")
  ))]
  name: SharedString,
  addr: SocketAddr,
  /// Metadata from the delegate for this node.
  #[viewit(getter(const, style = "ref"))]
  meta: Bytes,
  /// State of the node.
  state: NodeStateType,
  /// Minimum protocol version this understands
  pmin: u8,
  /// Maximum protocol version this understands
  pmax: u8,
  /// Current version node is speaking
  pcur: u8,
  /// Min protocol version for the delegate to understand
  dmin: u8,
  /// Max protocol version for the delegate to understand
  dmax: u8,
  /// Current version delegate is speaking
  dcur: u8,
}

impl Node {
  /// Construct a new node with the given name, address and state.
  #[inline]
  pub const fn new(name: SharedString, addr: SocketAddr, state: NodeStateType) -> Self {
    Self {
      name,
      addr,
      meta: Bytes::new(),
      state,
      pmin: 0,
      pmax: 0,
      pcur: 0,
      dmin: 0,
      dmax: 0,
      dcur: 0,
    }
  }
}

impl core::fmt::Display for Node {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{} ({})", self.name, self.addr)
  }
}
