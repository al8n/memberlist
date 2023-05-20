#![forbid(unsafe_code)]

use bytes::Bytes;
use std::{net::SocketAddr, time::Instant};

pub use bytes;
pub use smol_str::SmolStr;

#[doc(hidden)]
pub mod hidden;

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
  name: smol_str::SmolStr,
}

impl Address {
  #[inline]
  pub fn new<T>(name: T, addr: SocketAddr) -> Self
  where
    T: AsRef<str>,
  {
    Self {
      name: smol_str::SmolStr::new(name),
      addr,
    }
  }
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
  Ping = 0,
  IndirectPing = 1,
  AckResponse = 2,
  Suspect = 3,
  Alive = 4,
  Dead = 5,
  PushPull = 6,
  Compound = 7,
  /// User mesg, not handled by us
  User = 8,
  Compress = 9,
  Encrypt = 10,
  NackResponse = 11,
  HasCrc = 12,
  ErrorResponse = 13,
  /// HasLabel has a deliberately high value so that you can disambiguate
  /// it from the encryptionVersion header which is either 0/1 right now and
  /// also any of the existing [`MessageType`].
  HasLabel = 244,
}

impl core::fmt::Display for MessageType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Ping => write!(f, "Ping"),
      Self::IndirectPing => write!(f, "IndirectPing"),
      Self::AckResponse => write!(f, "AckResponse"),
      Self::Suspect => write!(f, "Suspect"),
      Self::Alive => write!(f, "Alive"),
      Self::Dead => write!(f, "Dead"),
      Self::PushPull => write!(f, "PushPull"),
      Self::Compound => write!(f, "Compound"),
      Self::User => write!(f, "User"),
      Self::Compress => write!(f, "Compress"),
      Self::Encrypt => write!(f, "Encrypt"),
      Self::NackResponse => write!(f, "NackResponse"),
      Self::HasCrc => write!(f, "HasCrc"),
      Self::ErrorResponse => write!(f, "ErrorResponse"),
      Self::HasLabel => write!(f, "HasLabel"),
    }
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidMessageType(u8);

impl core::fmt::Display for InvalidMessageType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid message type: {}", self.0)
  }
}

impl std::error::Error for InvalidMessageType {}

impl TryFrom<u8> for MessageType {
  type Error = InvalidMessageType;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Ping),
      1 => Ok(Self::IndirectPing),
      2 => Ok(Self::AckResponse),
      3 => Ok(Self::Suspect),
      4 => Ok(Self::Alive),
      5 => Ok(Self::Dead),
      6 => Ok(Self::PushPull),
      7 => Ok(Self::Compound),
      8 => Ok(Self::User),
      9 => Ok(Self::Compress),
      10 => Ok(Self::Encrypt),
      11 => Ok(Self::NackResponse),
      12 => Ok(Self::HasCrc),
      13 => Ok(Self::ErrorResponse),
      244 => Ok(Self::HasLabel),
      _ => Err(InvalidMessageType(value)),
    }
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InvalidNodeState(u8);

impl core::fmt::Display for InvalidNodeState {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid node state: {}", self.0)
  }
}

impl std::error::Error for InvalidNodeState {}

#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum NodeState {
  #[default]
  Alive = 0,
  Suspect = 1,
  Dead = 2,
  Left = 3,
}

impl TryFrom<u8> for NodeState {
  type Error = InvalidNodeState;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Alive),
      1 => Ok(Self::Suspect),
      2 => Ok(Self::Dead),
      3 => Ok(Self::Left),
      _ => Err(InvalidNodeState(value)),
    }
  }
}

impl core::fmt::Display for NodeState {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Alive => write!(f, "alive"),
      Self::Suspect => write!(f, "suspect"),
      Self::Dead => write!(f, "dead"),
      Self::Left => write!(f, "left"),
    }
  }
}

impl core::str::FromStr for NodeState {
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
    result(converter(fn = "SmolStr::as_str"), type = "&str")
  ))]
  name: SmolStr,
  addr: SocketAddr,
  /// Metadata from the delegate for this node.
  #[viewit(getter(const, style = "ref"))]
  meta: Bytes,
  /// State of the node.
  state: NodeState,
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
  pub fn new<T>(name: T, addr: SocketAddr, state: NodeState) -> Self
  where
    T: AsRef<str>,
  {
    Self {
      name: SmolStr::new(name),
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
