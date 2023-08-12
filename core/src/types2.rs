use std::{
  net::{IpAddr, SocketAddr},
  ops::{Deref, DerefMut},
};

use bytes::Bytes;
use rkyv::{
  ser::{
    serializers::{
      AllocScratchError, AllocSerializer, CompositeSerializerError, SharedSerializeMapError,
    },
    Serializer,
  },
  AlignedVec, Archive, Deserialize, Serialize,
};

mod name;
pub use name::*;

mod id;
pub use id::*;

mod ack;
pub(crate) use ack::*;

mod alive;
pub(crate) use alive::*;

mod compress;
pub use compress::*;

mod bad_state;
pub(crate) use bad_state::*;

mod err;
pub(crate) use err::*;

mod ping;
pub(crate) use ping::*;

mod push_pull_state;
pub(crate) use push_pull_state::*;

use crate::{checksum::Checksumer, DelegateVersion, ProtocolVersion};

mod label;
pub use label::*;

mod address;
pub use address::*;

mod packet;
pub use packet::*;

const ENCODE_META_SIZE: usize = 8;
const DEFAULT_ENCODE_PREALLOCATE_SIZE: usize = 128;

#[derive(Debug, Clone, Copy)]
pub struct DecodeU32Error;

impl core::fmt::Display for DecodeU32Error {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid length")
  }
}

impl std::error::Error for DecodeU32Error {}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
  #[error("truncated {0}")]
  Truncated(&'static str),
  #[error("corrupted message")]
  Corrupted,
  #[error("checksum mismatch")]
  ChecksumMismatch,
  #[error("unknown mark bit {0}")]
  UnknownMarkBit(u8),
  #[error("fail to decode length {0}")]
  Length(DecodeU32Error),
  #[error("invalid ip addr length {0}")]
  InvalidIpAddrLength(usize),
  #[error("invalid domain {0}")]
  InvalidDomain(#[from] InvalidDomain),
  // #[error("invalid name {0}")]
  // InvalidName(#[from] InvalidName),
  #[error("invalid string {0}")]
  InvalidErrorResponse(std::string::FromUtf8Error),
  #[error("invalid size {0}")]
  InvalidMessageSize(#[from] DecodeU32Error),
  // #[error("{0}")]
  // InvalidNodeState(#[from] InvalidNodeState),
  #[error("{0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  // #[error("{0}")]
  // InvalidCompressionAlgo(#[from] InvalidCompressionAlgo),
  #[error("{0}")]
  InvalidLabel(#[from] InvalidLabel),
  #[error("failed to read full push node state ({0} / {1})")]
  FailReadRemoteState(usize, usize),
  #[error("failed to read full user state ({0} / {1})")]
  FailReadUserState(usize, usize),
  #[error("mismatch message type, expected {expected}, got {got}")]
  MismatchMessageType {
    expected: &'static str,
    got: &'static str,
  },
  #[error("sequence number from ack ({ack}) doesn't match ping ({ping})")]
  MismatchSequenceNumber { ack: u32, ping: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
  #[error("{0}")]
  InvalidLabel(#[from] InvalidLabel),
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Message(pub(crate) AlignedVec);

impl Default for Message {
  fn default() -> Self {
    Self::new()
  }
}

impl Message {
  const PREFIX_SIZE: usize = 1;

  #[inline]
  pub fn new() -> Self {
    Self::new_with_type(MessageType::User)
  }

  //   #[doc(hidden)]
  //   #[inline]
  //   pub fn __from_bytes_mut(data: BytesMut) -> Self {
  //     Self(data)
  //   }

  #[inline]
  pub(crate) fn new_with_type(ty: MessageType) -> Self {
    let mut this = AlignedVec::with_capacity(Self::PREFIX_SIZE);
    this.push(ty as u8);
    Self(this)
  }

  //   pub(crate) fn compounds(mut msgs: Vec<Self>) -> Vec<BytesMut> {
  //     const MAX_MESSAGES: usize = 255;

  //     let mut bufs = Vec::with_capacity((msgs.len() + MAX_MESSAGES - 1) / MAX_MESSAGES);

  //     while msgs.len() > MAX_MESSAGES {
  //       bufs.push(Self::compound(msgs.drain(..MAX_MESSAGES).collect()));
  //     }

  //     if !msgs.is_empty() {
  //       bufs.push(Self::compound(msgs));
  //     }

  //     bufs
  //   }

  //   pub(crate) fn compound(msgs: Vec<Self>) -> BytesMut {
  //     let num_msgs = msgs.len();
  //     let total: usize = msgs.iter().map(|m| m.len()).sum();
  //     let mut buf = BytesMut::with_capacity(
  //       1
  //         + core::mem::size_of::<u8>()
  //         + num_msgs * core::mem::size_of::<u16>()
  //         + total,
  //     );
  //     // Write out the type
  //     buf.put_u8(MessageType::Compound as u8);
  //     // Write out the number of message
  //     buf.put_u8(num_msgs as u8);

  //     let mut compound = buf.split_off(num_msgs * 2);
  //     for msg in msgs {
  //       // Add the message length
  //       buf.put_u16(msg.len() as u16);
  //       // put msg into compound
  //       compound.put_slice(&msg.freeze());
  //     }

  //     buf.unsplit(compound);
  //     buf
  //   }

  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut this = AlignedVec::with_capacity(cap + Self::PREFIX_SIZE);
    this.push(MessageType::User as u8);
    Self(this)
  }

  #[inline]
  pub fn resize(&mut self, new_len: usize, val: u8) {
    self.0.resize(new_len + Self::PREFIX_SIZE, val);
  }

  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.0.reserve(additional);
  }

  #[inline]
  pub fn truncate(&mut self, len: usize) {
    self.0.resize(Self::PREFIX_SIZE + len, 0)
  }

  #[inline]
  pub fn put_slice(&mut self, buf: &[u8]) {
    self.0.extend_from_slice(buf)
  }

  #[inline]
  pub fn put_u8(&mut self, val: u8) {
    self.0.push(val);
  }

  #[inline]
  pub fn put_u16(&mut self, val: u16) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_u16_le(&mut self, val: u16) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_u32(&mut self, val: u32) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_u32_le(&mut self, val: u32) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_u64(&mut self, val: u64) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_u64_le(&mut self, val: u64) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_i8(&mut self, val: i8) {
    self.0.push(val as u8);
  }

  #[inline]
  pub fn put_i16(&mut self, val: i16) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_i16_le(&mut self, val: i16) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_i32(&mut self, val: i32) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_i32_le(&mut self, val: i32) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_i64(&mut self, val: i64) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_i64_le(&mut self, val: i64) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_f32(&mut self, val: f32) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_f32_le(&mut self, val: f32) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_f64(&mut self, val: f64) {
    self.0.extend_from_slice(&val.to_be_bytes());
  }

  #[inline]
  pub fn put_f64_le(&mut self, val: f64) {
    self.0.extend_from_slice(&val.to_le_bytes());
  }

  #[inline]
  pub fn put_bool(&mut self, val: bool) {
    self.0.push(val as u8);
  }

  //   #[inline]
  //   pub fn put_bytes(&mut self, val: u8, cnt: usize) {
  //     self.0.
  //   }

  #[inline]
  pub fn clear(&mut self) {
    let mt = self.0[0];
    self.0.clear();
    self.0.push(mt);
  }

  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub fn as_slice_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }

  //   #[inline]
  //   pub(crate) fn freeze(self) -> Bytes {
  //     self.0.freeze()
  //   }
}

impl std::io::Write for Message {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.put_slice(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

impl Deref for Message {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.0[Self::PREFIX_SIZE..]
  }
}

impl DerefMut for Message {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0[Self::PREFIX_SIZE..]
  }
}

impl AsRef<[u8]> for Message {
  fn as_ref(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }
}

impl AsMut<[u8]> for Message {
  fn as_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }
}

/// An ID of a type of message that can be received
/// on network channels from other members.
///
/// The list of available message types.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[repr(u8)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(u8), non_exhaustive)]
#[non_exhaustive]
pub(crate) enum MessageType {
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
      Self::Ping => write!(f, "ping"),
      Self::IndirectPing => write!(f, "indirect ping"),
      Self::AckResponse => write!(f, "ack"),
      Self::Suspect => write!(f, "suspect"),
      Self::Alive => write!(f, "alive"),
      Self::Dead => write!(f, "dead"),
      Self::PushPull => write!(f, "push pull"),
      Self::Compound => write!(f, "compound"),
      Self::User => write!(f, "user"),
      Self::Compress => write!(f, "compress"),
      Self::Encrypt => write!(f, "encrypt"),
      Self::NackResponse => write!(f, "nack"),
      Self::HasCrc => write!(f, "crc"),
      Self::ErrorResponse => write!(f, "error"),
      Self::HasLabel => write!(f, "label"),
    }
  }
}

impl MessageType {
  /// Returns the str of the [`MessageType`].
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      MessageType::Ping => "ping",
      MessageType::IndirectPing => "indirect ping",
      MessageType::AckResponse => "ack response",
      MessageType::Suspect => "suspect",
      MessageType::Alive => "alive",
      MessageType::Dead => "dead",
      MessageType::PushPull => "push pull",
      MessageType::Compound => "compound",
      MessageType::User => "user",
      MessageType::Compress => "compress",
      MessageType::Encrypt => "encrypt",
      MessageType::NackResponse => "nack response",
      MessageType::HasCrc => "crc",
      MessageType::ErrorResponse => "error",
      MessageType::HasLabel => "label",
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

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[repr(u8)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug), repr(u8), non_exhaustive)]
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
}

impl core::fmt::Display for NodeState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
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

pub(crate) trait Type {
  const PREALLOCATE: usize;

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message;
}

fn encode<C: Checksumer, T, const N: usize>(
  ty: MessageType,
  pv: ProtocolVersion,
  dv: DelegateVersion,
  msg: &T,
) -> Message
where
  T: Serialize<AllocSerializer<N>>,
{
  let mut ser = AllocSerializer::<N>::default();
  ser
    .write(&[ty as u8, pv as u8, dv as u8, 0, 0, 0, 0, 0])
    .unwrap();
  ser
    .serialize_value(msg)
    .map(|pos| {
      let mut data = ser.into_serializer().into_inner();
      let mut h = C::new();
      h.update(&data[8..]);
      let cks = h.finalize();
      data[4..8].copy_from_slice(&cks.to_be_bytes());
      Message(data)
    })
    .unwrap()
}

#[cfg(test)]
mod tests {
  use super::*;
  use rkyv::to_bytes;

  #[test]
  fn test_compress() {
    let c = Compress {
      algo: CompressionAlgo::Lzw,
      buf: Bytes::from_static(b"hello world"),
    };
    let bytes = to_bytes::<_, 128>(&CompressionAlgo::Lzw).unwrap();
    eprintln!("bytes: {:?}", bytes);
    let bytes = to_bytes::<_, 128>(&c).unwrap();
    eprintln!("bytes: {:?}", bytes);
    let archived = rkyv::check_archived_root::<Compress>(&bytes[..]).unwrap();
    assert_eq!(archived, &c);
  }
}
