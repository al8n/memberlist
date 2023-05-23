#![forbid(unsafe_code)]

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
  net::{IpAddr, SocketAddr},
  time::Instant,
};

pub use bytes;
pub use smol_str::SmolStr;

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
  name: Name,
}

impl Address {
  #[inline]
  pub fn new(name: Name, addr: SocketAddr) -> Self {
    Self { name, addr }
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

/// Represents a node in the cluster, can be thought as an identifier for a node
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoteNode {
  pub name: Option<Name>,
  pub addr: IpAddr,
  pub port: Option<u16>,
}

impl RemoteNode {
  /// Construct a new remote node identifier with the given ip addr.
  #[inline]
  pub const fn new(addr: IpAddr) -> Self {
    Self {
      name: None,
      addr,
      port: None,
    }
  }

  /// With the given name
  #[inline]
  pub fn with_name(mut self, name: Option<Name>) -> Self {
    self.name = name;
    self
  }

  /// With the given port
  #[inline]
  pub fn with_port(mut self, port: Option<u16>) -> Self {
    self.port = port;
    self
  }

  /// Return the node name
  #[inline]
  pub const fn name(&self) -> Option<&Name> {
    self.name.as_ref()
  }

  #[inline]
  pub const fn ip(&self) -> IpAddr {
    self.addr
  }

  #[inline]
  pub const fn port(&self) -> Option<u16> {
    self.port
  }
}

/// Represents a node in the cluster
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
  #[viewit(getter(const, style = "ref"))]
  full_address: Address,
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
  pub fn new(name: Name, addr: SocketAddr, state: NodeState) -> Self {
    Self {
      full_address: Address { addr, name },
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

  /// Return the node name
  #[inline]
  pub fn name(&self) -> &str {
    self.full_address.name.as_ref()
  }

  #[inline]
  pub const fn vsn(&self) -> [u8; 6] {
    [
      self.pcur, self.pmin, self.pmax, self.dcur, self.dmin, self.dmax,
    ]
  }

  #[inline]
  pub const fn address(&self) -> SocketAddr {
    self.full_address.addr()
  }
}

impl core::fmt::Display for Node {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{} ({})", self.full_address.name, self.full_address.addr)
  }
}

#[derive(Clone)]
pub struct Name(Bytes);

impl bytes::Buf for Name {
  fn remaining(&self) -> usize {
    self.0.remaining()
  }

  fn chunk(&self) -> &[u8] {
    self.0.chunk()
  }

  fn advance(&mut self, cnt: usize) {
    self.0.advance(cnt);
  }
}

impl Default for Name {
  fn default() -> Self {
    Self(Bytes::new())
  }
}

impl Name {
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  pub fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  pub fn clear(&mut self) {
    self.0.clear()
  }

  #[inline]
  #[doc(hidden)]
  pub fn bytes(&self) -> &Bytes {
    &self.0
  }

  #[inline]
  #[doc(hidden)]
  pub fn bytes_mut(&mut self) -> &mut Bytes {
    &mut self.0
  }
}

impl Serialize for Name {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(self.as_str())
  }
}

impl<'de> Deserialize<'de> for Name {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    String::deserialize(deserializer).map(Name::from)
  }
}

impl AsRef<str> for Name {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Name {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    self.as_str().partial_cmp(other.as_str())
  }
}

impl core::cmp::Ord for Name {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl core::cmp::PartialEq for Name {
  fn eq(&self, other: &Self) -> bool {
    self.as_str() == other.as_str()
  }
}

impl core::cmp::PartialEq<str> for Name {
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&str> for Name {
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::PartialEq<String> for Name {
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&String> for Name {
  fn eq(&self, other: &&String) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::Eq for Name {}

impl core::hash::Hash for Name {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_str().hash(state)
  }
}

impl Name {
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  fn as_str(&self) -> &str {
    // unwrap safe here, because there is no way to build a name with invalid utf8
    core::str::from_utf8(&self.0).unwrap()
  }
}

impl From<Name> for String {
  fn from(name: Name) -> Self {
    // unwrap safe here, because there is no way to build a name with invalid utf8
    String::from_utf8(name.0.to_vec()).unwrap()
  }
}

impl From<&str> for Name {
  fn from(s: &str) -> Self {
    Self(Bytes::copy_from_slice(s.as_bytes()))
  }
}

impl core::fmt::Debug for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    // unwrap safe here, because there is no way to build a name with invalid utf8
    write!(f, "{}", core::str::from_utf8(&self.0).unwrap())
  }
}

impl core::fmt::Display for Name {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    // unwrap safe here, because there is no way to build a name with invalid utf8
    write!(f, "{}", core::str::from_utf8(&self.0).unwrap())
  }
}

impl From<String> for Name {
  fn from(s: String) -> Self {
    Self(Bytes::from(s))
  }
}
