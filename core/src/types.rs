//! Types used by the protocol.
//!
//! The encoding and decoding rules are similar to protobuf
//! e.g. For a struct, the root struct stores the length of each field.
//!
//! ```rust,no_compile
//! struct Name(Bytes);
//!
//! struct NodeId {
//!   name: Name,
//!   addr: NodeAddress,
//!   port: Option<u16>,
//! }
//!
//! struct Foo {
//!   seq_no: u32,
//!   from: NodeId,
//!   to: NodeId,
//! }
//! ```
//!
//! In the above example, `Name` is a 'atomic' type, which means it only contains one field.
//! For 'atomic' types, when encoding, they will only contains the data itself, no length information.
//! So, the encoded data for `Name` is just the bytes of the name, and the length is just `Name::len(self)`.
//!
//! Now, let us look the NodeId. It contains 3 fields, `name`, `addr` and `port`.
//! The encoded data format for `NodeId` is:
//!
//! Let us say the tag for name is 0, the tag for addr may be [1, 2, 3], represent for the 3 kinds of addr: ipv4, ipv6 and domain (e.g. www.example.com). the port tag is 4.
//!
//! If the all fields are not empty:
//!
//! ```text
//! name field tag (u8), name_length (u16), name, addr, port tag(u8), port, checksum (u32)
//! ```
//!
//! If the a field is empty, then the tag and the length of the field will not be included in the encoded data.
//!
//! ```text
//! addr tag (u8), addr length (u8), addr data, checksum (u32)
//! ```
//!
//! So now, the problem is that how we decode the `NodeId`?
//!
//! We requires the caller to provide the length information.
//!
//! Let say we have a decode funtion like `decode_node_id(buf: Bytes) -> Result<NodeId, Error>`.
//!
//! then we read the first byte as the tag, and then we match the tag to the field. If the num of readed bytes is equal to the length,
//! then we know that the we have all the data for the struct.
//!
//! The encoded data format for `Foo` is:
//!
//! ```text
//! seq_no, from_length (u32 varint), from, to_length(u32 varint), to
//! ```

use std::{
  io::{Error, ErrorKind},
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
  ops::{Deref, DerefMut},
  time::Instant,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

const VSN_SIZE: usize = 6;
const VSN_ENCODED_SIZE: usize = 8;
const VSN_EMPTY: [u8; VSN_SIZE] = [255; VSN_SIZE];

const MAX_DNS_DOMAIN_SIZE: usize = 253;

const LENGTH_SIZE: usize = core::mem::size_of::<u32>();
const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();

macro_rules! map_inlined {
  (match $ident: ident.$len: ident() {
    $($size: literal),* $(,)?
  } => $r: ident) => {
    match $len {
      $($size => {
        let mut arr = [0; $size];
        $r.read_exact(&mut arr).await?;
        $ident::from_array(arr).map_err(|e| Error::new(ErrorKind::InvalidData, e))
      },)*
      _ => unreachable!(),
    }
  };
}

/// Returns the encoded length of the value in LEB128 variable length format.
/// The returned value will be between 1 and 5, inclusive.
#[inline]
pub(crate) fn encoded_u32_len(value: u32) -> usize {
  ((((value | 1).leading_zeros() ^ 31) * 9 + 37) / 32) as usize
}

#[inline]
pub(crate) fn encode_u32(mut n: u32) -> (usize, [u8; 5]) {
  let mut buf = [0u8; 5];
  let mut i = 0;

  while n >= 0x80 {
    buf[i] = ((n as u8) & 0x7f) | 0x80;
    n >>= 7;
    i += 1;
  }

  buf[i] = n as u8;
  (i, buf)
}

#[inline]
pub(crate) fn encode_u32_to_buf(mut buf: impl BufMut, mut n: u32) {
  while n >= 0x80 {
    buf.put_u8(((n as u8) & 0x7F) | 0x80);
    n >>= 7;
  }
  buf.put_u8(n as u8);
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeU32Error;

impl core::fmt::Display for DecodeU32Error {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid u32")
  }
}

impl std::error::Error for DecodeU32Error {}

#[inline]
pub(crate) fn decode_u32_from_buf(mut buf: impl Buf) -> Result<(u32, usize), DecodeU32Error> {
  let mut n = 0;
  let mut shift = 0;
  let mut i = 0;
  while buf.has_remaining() {
    let b = buf.get_u8();
    i += 1;
    if b < 0x80 {
      return Ok((n | ((b as u32) << shift), i));
    }

    n |= ((b & 0x7f) as u32) << shift;
    if shift >= 25 {
      return Err(DecodeU32Error);
    }
    shift += 7;
  }

  Err(DecodeU32Error)
}

#[cfg(feature = "async")]
pub(crate) async fn decode_u32_from_reader<R: futures_util::io::AsyncRead + Unpin>(
  reader: &mut R,
) -> std::io::Result<(u32, usize)> {
  use futures_util::io::AsyncReadExt;

  let mut n = 0;
  let mut shift = 0;
  for i in 0..5 {
    let mut byte = [0; 1];
    reader.read_exact(&mut byte).await?;
    let b = byte[0];

    if b < 0x80 {
      return Ok((n | ((b as u32) << shift), i));
    }

    n |= ((b & 0x7f) as u32) << shift;
    shift += 7;
  }

  Err(Error::new(ErrorKind::InvalidData, "invalid u32"))
}

mod compress;
pub use compress::*;

mod name;
pub use name::*;

mod address;
pub use address::*;

mod id;
pub use id::*;

mod ack;
pub(crate) use ack::*;

mod alive;
pub(crate) use alive::*;

mod bad_state;
pub(crate) use bad_state::*;

mod err;
pub(crate) use err::*;

mod ping;
pub(crate) use ping::*;

mod push_pull_state;
pub(crate) use push_pull_state::*;

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
  #[error("invalid ip addr length {0}")]
  InvalidIpAddrLength(usize),
  #[error("invalid domain {0}")]
  InvalidDomain(#[from] InvalidDomain),
  #[error("invalid name {0}")]
  InvalidName(#[from] InvalidName),
  #[error("invalid string {0}")]
  InvalidErrorResponse(std::string::FromUtf8Error),
  #[error("invalid size {0}")]
  InvalidMessageSize(#[from] DecodeU32Error),
  #[error("{0}")]
  InvalidNodeState(#[from] InvalidNodeState),
  #[error("{0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  #[error("{0}")]
  InvalidCompressionAlgo(#[from] InvalidCompressionAlgo),
  #[error("failed to read full push node state ({0} / {1})")]
  FailReadRemoteState(usize, usize),
  #[error("failed to read full user state ({0} / {1})")]
  FailReadUserState(usize, usize),
  #[error("{0}")]
  Other(String),
}

impl DecodeError {
  pub(crate) fn other(s: impl core::fmt::Display) -> Self {
    Self::Other(format!("{s}"))
  }
}

#[viewit::viewit]
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct UserMsgHeader {
  len: u32, // Encodes the byte lengh of user state
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Message(pub(crate) BytesMut);

impl Default for Message {
  fn default() -> Self {
    Self::new()
  }
}

impl Message {
  const PREFIX_SIZE: usize = 5;

  #[inline]
  pub fn new() -> Self {
    Self::new_with_type(MessageType::User)
  }

  #[inline]
  pub(crate) fn new_with_type(ty: MessageType) -> Self {
    let mut this = BytesMut::with_capacity(Self::PREFIX_SIZE);
    this.put_u8(ty as u8);
    this.put_slice(&[0; core::mem::size_of::<u32>()]);
    Self(this)
  }

  pub(crate) fn compounds(mut msgs: Vec<Self>) -> Vec<BytesMut> {
    const MAX_MESSAGES: usize = 255;

    let mut bufs = Vec::with_capacity((msgs.len() + MAX_MESSAGES - 1) / MAX_MESSAGES);

    while msgs.len() > MAX_MESSAGES {
      bufs.push(Self::compound(msgs.drain(..MAX_MESSAGES).collect()));
    }

    if !msgs.is_empty() {
      bufs.push(Self::compound(msgs));
    }

    bufs
  }

  pub(crate) fn compound(msgs: Vec<Self>) -> BytesMut {
    let num_msgs = msgs.len();
    let total: usize = msgs.iter().map(|m| m.len()).sum();
    let mut buf = BytesMut::with_capacity(
      MessageType::SIZE
        + core::mem::size_of::<u8>()
        + num_msgs * core::mem::size_of::<u16>()
        + total,
    );
    // Write out the type
    buf.put_u8(MessageType::Compound as u8);
    // Write out the number of message
    buf.put_u8(num_msgs as u8);

    let mut compound = buf.split_off(num_msgs * 2);
    for msg in msgs {
      // Add the message length
      buf.put_u16(msg.len() as u16);
      // put msg into compound
      compound.put_slice(&msg.freeze());
    }

    buf.unsplit(compound);
    buf
  }

  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut this = BytesMut::with_capacity(cap + Self::PREFIX_SIZE);
    this.put_u8(MessageType::User as u8);
    this.put_slice(&[0; core::mem::size_of::<u32>()]);
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
  pub fn remaining(&self) -> usize {
    self.0.remaining()
  }

  #[inline]
  pub fn remaining_mut(&self) -> usize {
    self.0.remaining_mut()
  }

  #[inline]
  pub fn truncate(&mut self, len: usize) {
    self.0.truncate(len + Self::PREFIX_SIZE);
  }

  #[inline]
  pub fn put_slice(&mut self, buf: &[u8]) {
    self.0.put_slice(buf);
  }

  #[inline]
  pub fn put_u8(&mut self, val: u8) {
    self.0.put_u8(val);
  }

  #[inline]
  pub fn put_u16(&mut self, val: u16) {
    self.0.put_u16(val);
  }

  #[inline]
  pub fn put_u16_le(&mut self, val: u16) {
    self.0.put_u16_le(val);
  }

  #[inline]
  pub fn put_u32(&mut self, val: u32) {
    self.0.put_u32(val);
  }

  #[inline]
  pub fn put_u32_le(&mut self, val: u32) {
    self.0.put_u32_le(val);
  }

  #[inline]
  pub fn put_u64(&mut self, val: u64) {
    self.0.put_u64(val);
  }

  #[inline]
  pub fn put_u64_le(&mut self, val: u64) {
    self.0.put_u64_le(val);
  }

  #[inline]
  pub fn put_i8(&mut self, val: i8) {
    self.0.put_i8(val);
  }

  #[inline]
  pub fn put_i16(&mut self, val: i16) {
    self.0.put_i16(val);
  }

  #[inline]
  pub fn put_i16_le(&mut self, val: i16) {
    self.0.put_i16_le(val);
  }

  #[inline]
  pub fn put_i32(&mut self, val: i32) {
    self.0.put_i32(val);
  }

  #[inline]
  pub fn put_i32_le(&mut self, val: i32) {
    self.0.put_i32_le(val);
  }

  #[inline]
  pub fn put_i64(&mut self, val: i64) {
    self.0.put_i64(val);
  }

  #[inline]
  pub fn put_i64_le(&mut self, val: i64) {
    self.0.put_i64_le(val);
  }

  #[inline]
  pub fn put_f32(&mut self, val: f32) {
    self.0.put_f32(val);
  }

  #[inline]
  pub fn put_f32_le(&mut self, val: f32) {
    self.0.put_f32_le(val);
  }

  #[inline]
  pub fn put_f64(&mut self, val: f64) {
    self.0.put_f64(val);
  }

  #[inline]
  pub fn put_f64_le(&mut self, val: f64) {
    self.0.put_f64_le(val);
  }

  #[inline]
  pub fn put_bool(&mut self, val: bool) {
    self.0.put_u8(val as u8);
  }

  #[inline]
  pub fn put_bytes(&mut self, val: u8, cnt: usize) {
    self.0.put_bytes(val, cnt);
  }

  #[inline]
  pub fn clear(&mut self) {
    let mt = self.0[0];
    self.0.clear();
    self.0.put_u8(mt);
  }

  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub fn as_slice_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub(crate) fn freeze(mut self) -> Bytes {
    let size = self.0.len() - Self::PREFIX_SIZE;
    self.0[1..Self::PREFIX_SIZE].copy_from_slice(&(size as u32).to_be_bytes());
    self.0.freeze()
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

#[viewit::viewit(vis_all = "", getters(vis_all = "pub"), setters(vis_all = "pub"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Packet {
  /// The raw contents of the packet.
  #[viewit(getter(skip))]
  buf: BytesMut,

  /// Address of the peer. This is an actual [`SocketAddr`] so we
  /// can expose some concrete details about incoming packets.
  from: SocketAddr,

  /// The time when the packet was received. This should be
  /// taken as close as possible to the actual receipt time to help make an
  /// accurate RTT measurement during probes.
  timestamp: Instant,
}

impl Packet {
  #[inline]
  pub fn new(from: SocketAddr, timestamp: Instant) -> Self {
    Self {
      buf: BytesMut::new(),
      from,
      timestamp,
    }
  }

  #[inline]
  pub fn with_capacity(cap: usize, from: SocketAddr, timestamp: Instant) -> Self {
    Self {
      buf: BytesMut::with_capacity(cap),
      from,
      timestamp,
    }
  }

  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    self.buf.as_ref()
  }

  #[inline]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    self.buf.as_mut()
  }

  #[inline]
  pub fn into_inner(self) -> BytesMut {
    self.buf
  }

  #[inline]
  pub fn resize(&mut self, new_len: usize, val: u8) {
    self.buf.resize(new_len, val);
  }

  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.buf.reserve(additional);
  }

  #[inline]
  pub fn remaining(&self) -> usize {
    self.buf.remaining()
  }

  #[inline]
  pub fn remaining_mut(&self) -> usize {
    self.buf.remaining_mut()
  }

  #[inline]
  pub fn truncate(&mut self, len: usize) {
    self.buf.truncate(len);
  }

  #[inline]
  pub fn put_slice(&mut self, buf: &[u8]) {
    self.buf.put_slice(buf);
  }

  #[inline]
  pub fn put_u8(&mut self, val: u8) {
    self.buf.put_u8(val);
  }

  #[inline]
  pub fn put_u16(&mut self, val: u16) {
    self.buf.put_u16(val);
  }

  #[inline]
  pub fn put_u16_le(&mut self, val: u16) {
    self.buf.put_u16_le(val);
  }

  #[inline]
  pub fn put_u32(&mut self, val: u32) {
    self.buf.put_u32(val);
  }

  #[inline]
  pub fn put_u32_le(&mut self, val: u32) {
    self.buf.put_u32_le(val);
  }

  #[inline]
  pub fn put_u64(&mut self, val: u64) {
    self.buf.put_u64(val);
  }

  #[inline]
  pub fn put_u64_le(&mut self, val: u64) {
    self.buf.put_u64_le(val);
  }

  #[inline]
  pub fn put_i8(&mut self, val: i8) {
    self.buf.put_i8(val);
  }

  #[inline]
  pub fn put_i16(&mut self, val: i16) {
    self.buf.put_i16(val);
  }

  #[inline]
  pub fn put_i16_le(&mut self, val: i16) {
    self.buf.put_i16_le(val);
  }

  #[inline]
  pub fn put_i32(&mut self, val: i32) {
    self.buf.put_i32(val);
  }

  #[inline]
  pub fn put_i32_le(&mut self, val: i32) {
    self.buf.put_i32_le(val);
  }

  #[inline]
  pub fn put_i64(&mut self, val: i64) {
    self.buf.put_i64(val);
  }

  #[inline]
  pub fn put_i64_le(&mut self, val: i64) {
    self.buf.put_i64_le(val);
  }

  #[inline]
  pub fn put_f32(&mut self, val: f32) {
    self.buf.put_f32(val);
  }

  #[inline]
  pub fn put_f32_le(&mut self, val: f32) {
    self.buf.put_f32_le(val);
  }

  #[inline]
  pub fn put_f64(&mut self, val: f64) {
    self.buf.put_f64(val);
  }

  #[inline]
  pub fn put_f64_le(&mut self, val: f64) {
    self.buf.put_f64_le(val);
  }

  #[inline]
  pub fn put_bool(&mut self, val: bool) {
    self.buf.put_u8(val as u8);
  }

  #[inline]
  pub fn put_bytes(&mut self, val: u8, cnt: usize) {
    self.buf.put_bytes(val, cnt);
  }

  #[inline]
  pub fn clear(&mut self) {
    self.buf.clear();
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

impl MessageType {
  #[doc(hidden)]
  pub const SIZE: usize = core::mem::size_of::<Self>();
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
      MessageType::AckResponse => "ack",
      MessageType::Suspect => "suspect",
      MessageType::Alive => "alive",
      MessageType::Dead => "dead",
      MessageType::PushPull => "push pull",
      MessageType::Compound => "compound",
      MessageType::User => "user",
      MessageType::Compress => "compress",
      MessageType::Encrypt => "encrypt",
      MessageType::NackResponse => "nack",
      MessageType::HasCrc => "crc",
      MessageType::ErrorResponse => "error",
      MessageType::HasLabel => "label",
    }
  }

  #[inline]
  pub(crate) const fn as_err_str(&self) -> &'static str {
    match self {
      MessageType::Ping => "ping msg",
      MessageType::IndirectPing => "indirect ping msg",
      MessageType::AckResponse => "ack msg",
      MessageType::Suspect => "suspect msg",
      MessageType::Alive => "alive msg",
      MessageType::Dead => "dead msg",
      MessageType::PushPull => "push pull msg",
      MessageType::Compound => "compound msg",
      MessageType::User => "user msg",
      MessageType::Compress => "compress msg",
      MessageType::Encrypt => "encrypt msg",
      MessageType::NackResponse => "nack msg",
      MessageType::HasCrc => "crc msg",
      MessageType::ErrorResponse => "error msg",
      MessageType::HasLabel => "label msg",
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

impl NodeState {
  #[inline]
  pub(crate) const fn empty_metrics() -> [(&'static str, usize); 4] {
    [("alive", 0), ("suspect", 0), ("dead", 0), ("left", 0)]
  }
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
  id: NodeId,
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
  pub fn new(name: Name, addr: NodeAddress, state: NodeState) -> Self {
    Self {
      id: NodeId {
        name,
        port: None,
        addr,
      },
      meta: Bytes::new(),
      pmin: 0,
      pmax: 0,
      pcur: 0,
      dmin: 0,
      dmax: 0,
      dcur: 0,
      state,
    }
  }

  #[inline]
  pub fn with_port(mut self, port: u16) -> Self {
    self.id.port = Some(port);
    self
  }

  /// Return the node name
  #[inline]
  pub fn name(&self) -> &Name {
    &self.id.name
  }

  #[inline]
  pub const fn vsn(&self) -> [u8; 6] {
    [
      self.pcur, self.pmin, self.pmax, self.dcur, self.dmin, self.dmax,
    ]
  }

  #[inline]
  pub const fn address(&self) -> &NodeAddress {
    self.id.addr()
  }
}

impl core::fmt::Display for Node {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self.id.port {
      Some(port) => write!(f, "{}({}:{})", self.id.name.as_ref(), self.id.addr, port),
      None => write!(f, "{}({})", self.id.name.as_ref(), self.id.addr),
    }
  }
}