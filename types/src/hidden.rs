use std::{cell::RefCell, net::SocketAddr, ops::DerefMut, thread_local};

use prost::{
  bytes::{Buf, BufMut, Bytes, BytesMut},
  encoding::{bool, bytes, int32, skip_field, uint32, DecodeContext, WireType},
  DecodeError, EncodeError, Message,
};
use serde::{Deserialize, Serialize};

use super::{MessageType, NodeState};

const VSN_SIZE: usize = 6;
const VSN_ENCODED_SIZE: usize = 8;
const MAX_ENCODED_SOCKET_ADDR_SIZE: usize = 18;

thread_local! {
  static VSN_BUFFER: RefCell<Vec<u8>> = RefCell::new(vec![0; VSN_SIZE]);
  static SOCKET_ADDR_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(MAX_ENCODED_SOCKET_ADDR_SIZE));
}

#[inline]
fn encode_socket_addr<B>(addr: &SocketAddr, tag: u32, buf: &mut B)
where
  B: BufMut,
{
  SOCKET_ADDR_BUFFER.with(|b| {
    let mut b = b.borrow_mut();
    b.clear();
    match addr {
      SocketAddr::V4(v4) => {
        b.put_slice(v4.ip().octets().as_slice());
        b.put_u16(v4.port());
      }
      SocketAddr::V6(v6) => {
        b.put_slice(v6.ip().octets().as_slice());
        b.put_u16(v6.port());
      }
    }
    bytes::encode(tag, b.deref_mut(), buf);
  });
}

#[inline]
fn merge_socket_addr<B>(
  wire_type: WireType,
  addr: &SocketAddr,
  buf: &mut B,
  ctx: DecodeContext,
) -> Result<(), DecodeError>
where
  B: Buf,
{
  SOCKET_ADDR_BUFFER.with(|b| {
    let mut b = b.borrow_mut();
    b.clear();
    match addr {
      SocketAddr::V4(v4) => {
        b.put_slice(v4.ip().octets().as_slice());
        b.put_u16(v4.port());
      }
      SocketAddr::V6(v6) => {
        b.put_slice(v6.ip().octets().as_slice());
        b.put_u16(v6.port());
      }
    }
    bytes::merge(wire_type, b.deref_mut(), buf, ctx)
  })
}

#[inline]
fn encode_vsn<B>(vsn: &[u8], tag: u32, buf: &mut B)
where
  B: BufMut,
{
  VSN_BUFFER.with(|b| {
    let mut b = b.borrow_mut();
    b.clear();
    b.put_slice(vsn);
    bytes::encode(tag, b.deref_mut(), buf);
  });
}

#[inline]
fn merge_vsn<B>(
  wire_type: WireType,
  vsn: &[u8],
  buf: &mut B,
  ctx: DecodeContext,
) -> Result<(), DecodeError>
where
  B: Buf,
{
  VSN_BUFFER.with(|b| {
    let mut b = b.borrow_mut();
    b.clear();
    b.put_slice(vsn);
    bytes::merge(wire_type, b.deref_mut(), buf, ctx)
  })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[doc(hidden)]
pub enum EncodableSocketAddr {
  Local(SocketAddr),
  None,
}

impl Default for EncodableSocketAddr {
  fn default() -> Self {
    Self::None
  }
}

impl EncodableSocketAddr {
  #[inline]
  fn clear(&mut self) {
    match self {
      EncodableSocketAddr::Local(_) => *self = Self::None,
      EncodableSocketAddr::None => {}
    }
  }

  #[inline]
  const fn encoded_len(&self) -> usize {
    // The encoded len correct based on this assumption:
    // 1. the tag is less than 16
    // 2. the length is less than 128
    match self {
      EncodableSocketAddr::Local(p) => {
        if p.is_ipv4() {
          6 + 2
        } else {
          18 + 2
        }
      }
      EncodableSocketAddr::None => unreachable!(),
    }
  }
}

impl From<SocketAddr> for EncodableSocketAddr {
  fn from(addr: SocketAddr) -> Self {
    Self::Local(addr)
  }
}

impl Serialize for EncodableSocketAddr {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    match self {
      EncodableSocketAddr::Local(addr) => addr.serialize(serializer),
      EncodableSocketAddr::None => unreachable!(),
    }
  }
}

impl<'de> Deserialize<'de> for EncodableSocketAddr {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    SocketAddr::deserialize(deserializer).map(EncodableSocketAddr::Local)
  }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct Name(Bytes);

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
  fn clear(&mut self) {
    self.0.clear()
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

#[doc(hidden)]
pub trait MessageExt: Message {
  fn encode_with_prefix(&self) -> Result<Bytes, EncodeError>;
}

macro_rules! msg_ext {
  ($($ty: ident),+ $(,)?) => {
    $(
      impl MessageExt for $ty {
        fn encode_with_prefix(&self) -> Result<Bytes, EncodeError> {
          let encoded_len = self.encoded_len();
          let mut buf = BytesMut::with_capacity(1 + 4 + encoded_len);
          buf.put_u8(MessageType::$ty as u8);
          buf.put_u32(encoded_len as u32);
          self.encode(&mut buf).map(|_| buf.freeze())
        }
      }
    )*
  };
}

msg_ext!(Suspect, Dead, AckResponse, NackResponse, ErrorResponse,);

macro_rules! bad_bail {
  ($name: ident) => {
    #[viewit::viewit]
    #[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[doc(hidden)]
    pub struct $name {
      incarnation: u32,
      node: Name,
      from: Name,
    }

    impl Message for $name {
      #[allow(unused_variables)]
      fn encode_raw<B>(&self, buf: &mut B)
      where
        B: BufMut,
      {
        if self.incarnation != 0u32 {
          uint32::encode(1u32, &self.incarnation, buf);
        }
        if self.node != "" {
          bytes::encode(2u32, &self.node.0, buf);
        }
        if self.from != "" {
          bytes::encode(3u32, &self.from.0, buf);
        }
      }
      #[allow(unused_variables)]
      fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut B,
        ctx: DecodeContext,
      ) -> ::core::result::Result<(), DecodeError>
      where
        B: Buf,
      {
        const STRUCT_NAME: &str = stringify!($name);
        match tag {
          1u32 => {
            let value = &mut self.incarnation;
            uint32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
              error.push(STRUCT_NAME, "incarnation");
              error
            })
          }
          2u32 => {
            let value = &mut self.node.0;
            bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
              error.push(STRUCT_NAME, "node");
              error
            })
          }
          3u32 => {
            let value = &mut self.from.0;
            bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
              error.push(STRUCT_NAME, "from");
              error
            })
          }
          _ => skip_field(wire_type, tag, buf, ctx),
        }
      }
      #[inline]
      fn encoded_len(&self) -> usize {
        (if self.incarnation != 0u32 {
          uint32::encoded_len(1u32, &self.incarnation)
        } else {
          0
        }) + if self.node != "" {
          bytes::encoded_len(2u32, &self.node.0)
        } else {
          0
        } + if self.from != "" {
          bytes::encoded_len(3u32, &self.from.0)
        } else {
          0
        }
      }
      fn clear(&mut self) {
        self.incarnation = 0u32;
        self.node.0.clear();
        self.from.0.clear();
      }
    }
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[doc(hidden)]
pub struct AckResponse {
  seq_no: u32,
  payload: Bytes,
}

impl AckResponse {
  pub fn new(seq_no: u32, payload: Bytes) -> Self {
    Self { seq_no, payload }
  }
}

impl Message for AckResponse {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if self.seq_no != 0u32 {
      uint32::encode(1u32, &self.seq_no, buf);
    }
    if !self.payload.is_empty() {
      bytes::encode(2u32, &self.payload, buf);
    }
  }
  #[allow(unused_variables)]
  fn merge_field<B>(
    &mut self,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> ::core::result::Result<(), DecodeError>
  where
    B: Buf,
  {
    const STRUCT_NAME: &str = stringify!(AckResponse);
    match tag {
      1u32 => {
        let value = &mut self.seq_no;
        uint32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "seq_no");
          error
        })
      }
      2u32 => {
        let value = &mut self.payload;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "payload");
          error
        })
      }
      _ => skip_field(wire_type, tag, buf, ctx),
    }
  }
  #[inline]
  fn encoded_len(&self) -> usize {
    (if self.seq_no != 0u32 {
      uint32::encoded_len(1u32, &self.seq_no)
    } else {
      0
    }) + if !self.payload.is_empty() {
      bytes::encoded_len(2u32, &self.payload)
    } else {
      0
    }
  }
  fn clear(&mut self) {
    self.seq_no = 0u32;
    self.payload.clear();
  }
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Message)]
#[serde(transparent)]
#[repr(transparent)]
#[doc(hidden)]
pub struct NackResponse {
  #[prost(uint32, tag = "1")]
  seq_no: u32,
}

#[viewit::viewit]
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Message)]
#[serde(transparent)]
#[repr(transparent)]
#[doc(hidden)]
pub struct ErrorResponse {
  #[prost(string, tag = "1")]
  err: String,
}

impl<E: std::error::Error> From<E> for ErrorResponse {
  fn from(err: E) -> Self {
    Self {
      err: err.to_string(),
    }
  }
}

#[viewit::viewit]
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Message)]
#[doc(hidden)]
pub struct PushPullHeader {
  #[prost(uint32, tag = "1")]
  nodes: u32,
  #[prost(uint32, tag = "2")]
  user_state_len: u32, // Encodes the byte lengh of user state
  #[prost(bool, tag = "3")]
  join: bool, // Is this a join request or a anti-entropy run
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[doc(hidden)]
pub struct Alive {
  incarnation: u32,
  node: Name,
  addr: EncodableSocketAddr,
  meta: Bytes,
  // The versions of the protocol/delegate that are being spoken, order:
  // pmin, pmax, pcur, dmin, dmax, dcur
  vsn: [u8; VSN_SIZE],
}

impl Message for Alive {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if self.incarnation != 0u32 {
      uint32::encode(1u32, &self.incarnation, buf);
    }
    if !self.node.is_empty() {
      bytes::encode(2u32, &self.node.0, buf);
    }
    match self.addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 3u32, buf),
      EncodableSocketAddr::None => {}
    }
    if self.meta != b"" as &[u8] {
      bytes::encode(4u32, &self.meta, buf);
    }
    if self.vsn != b"" as &[u8] {
      encode_vsn(&self.vsn, 5u32, buf);
    }
  }
  #[allow(unused_variables)]
  fn merge_field<B>(
    &mut self,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> Result<(), DecodeError>
  where
    B: Buf,
  {
    const STRUCT_NAME: &str = "Alive";
    match tag {
      1u32 => {
        let value = &mut self.incarnation;
        uint32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "incarnation");
          error
        })
      }
      2u32 => {
        let value = &mut self.node.0;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "node");
          error
        })
      }
      3u32 => match &self.addr {
        EncodableSocketAddr::Local(addr) => {
          merge_socket_addr(wire_type, addr, buf, ctx).map_err(|mut error| {
            error.push(STRUCT_NAME, "addr");
            error
          })
        }
        EncodableSocketAddr::None => {
          let mut error = DecodeError::new("invalid addr");
          error.push(STRUCT_NAME, "addr");
          Err(error)
        }
      },
      4u32 => {
        let value = &mut self.meta;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "meta");
          error
        })
      }
      5u32 => merge_vsn(wire_type, &self.vsn, buf, ctx).map_err(|mut error| {
        error.push(STRUCT_NAME, "vsn");
        error
      }),
      _ => skip_field(wire_type, tag, buf, ctx),
    }
  }
  #[inline]
  fn encoded_len(&self) -> usize {
    (if self.incarnation != 0u32 {
      uint32::encoded_len(1u32, &self.incarnation)
    } else {
      0
    }) + if !self.node.is_empty() {
      bytes::encoded_len(2u32, &self.node.0)
    } else {
      0
    } + self.addr.encoded_len()
      + if self.meta != b"" as &[u8] {
        bytes::encoded_len(4u32, &self.meta)
      } else {
        0
      }
      + VSN_ENCODED_SIZE // vsn encoded size
  }
  fn clear(&mut self) {
    self.incarnation = 0u32;
    self.node.0.clear();
    self.addr.clear();
    self.meta.clear();
    self.vsn = [0; 6];
  }
}

#[test]
fn test_vsn_encode() {
  for i in 0..=255 {
    let x = bytes::encoded_len(1, &vec![i; 6]);
    let y = bytes::encoded_len(1, &vec![i; 6]);
    assert_eq!(x, 8, "idx: {}", i);
    assert_eq!(y, 8, "idx: {}", i);
    assert_eq!(x, y, "idx: {}", i);
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndirectPingRequest {
  seq_no: u32,
  target: EncodableSocketAddr,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect against an agent
  /// restart with a new name.
  node: Name,

  /// true if we'd like a nack back
  nack: bool,

  /// Source address, used for a direct reply
  source_addr: EncodableSocketAddr,
  /// Source name, used for a direct reply
  source_node: Name,
}

impl Message for IndirectPingRequest {
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if self.seq_no != 0u32 {
      uint32::encode(1u32, &self.seq_no, buf);
    }
    match self.target() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 2u32, buf),
      EncodableSocketAddr::None => {}
    }
    if !self.node.is_empty() {
      bytes::encode(3u32, &self.node.0, buf);
    }
    if self.nack {
      bool::encode(4u32, &self.nack, buf);
    }
    match self.source_addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 5u32, buf),
      EncodableSocketAddr::None => {}
    }
    if !self.source_node.is_empty() {
      bytes::encode(6u32, &self.source_node.0, buf);
    }
  }
  #[allow(unused_variables)]
  fn merge_field<B>(
    &mut self,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> Result<(), DecodeError>
  where
    B: Buf,
  {
    const STRUCT_NAME: &str = "IndirectPingRequest";
    match tag {
      1u32 => {
        let value = &mut self.seq_no;
        uint32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "seq_no");
          error
        })
      }
      2u32 => match self.target() {
        EncodableSocketAddr::Local(addr) => {
          merge_socket_addr(wire_type, addr, buf, ctx).map_err(|mut error| {
            error.push(STRUCT_NAME, "target");
            error
          })
        }
        EncodableSocketAddr::None => {
          let mut error = DecodeError::new("invalid target");
          error.push(STRUCT_NAME, "target");
          Err(error)
        }
      },
      3u32 => {
        let value = &mut self.node.0;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "node");
          error
        })
      }
      4u32 => {
        let value = &mut self.nack;
        bool::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "nack");
          error
        })
      }
      5u32 => match &self.source_addr {
        EncodableSocketAddr::Local(addr) => {
          merge_socket_addr(wire_type, addr, buf, ctx).map_err(|mut error| {
            error.push(STRUCT_NAME, "source_addr");
            error
          })
        }
        EncodableSocketAddr::None => {
          let mut error = DecodeError::new("invalid source_addr");
          error.push(STRUCT_NAME, "source_addr");
          Err(error)
        }
      },
      6u32 => {
        let value = &mut self.source_node.0;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "source_node");
          error
        })
      }
      _ => skip_field(wire_type, tag, buf, ctx),
    }
  }
  #[inline]
  fn encoded_len(&self) -> usize {
    (if self.seq_no != 0u32 {
      uint32::encoded_len(1u32, &self.seq_no)
    } else {
      0
    }) + self.target.encoded_len()
      + if !self.node.is_empty() {
        bytes::encoded_len(3u32, &self.node.0)
      } else {
        0
      }
      + if self.nack {
        bool::encoded_len(4u32, &self.nack)
      } else {
        0
      }
      + self.source_addr.encoded_len()
      + if !self.source_node.is_empty() {
        bytes::encoded_len(6u32, &self.source_node.0)
      } else {
        0
      }
  }
  fn clear(&mut self) {
    self.seq_no = 0u32;
    self.target.clear();
    self.node.clear();
    self.nack = false;
    self.source_addr.clear();
    self.source_node.clear();
  }
}

#[viewit::viewit]
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Message)]
#[doc(hidden)]
#[repr(transparent)]
#[serde(transparent)]
pub struct UserMsgHeader {
  #[prost(uint32, tag = "1")]
  len: u32, // Encodes the byte lengh of user state
}

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ping {
  seq_no: u32,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  node: Name,

  /// Source address, used for a direct reply
  source_addr: EncodableSocketAddr,
  /// Source name, used for a direct reply
  source_node: Name,
}

impl Ping {
  pub const fn new(seq_no: u32, node: Name, source_addr: SocketAddr, source_node: Name) -> Self {
    Self {
      seq_no,
      node,
      source_addr: EncodableSocketAddr::Local(source_addr),
      source_node,
    }
  }
}

impl Message for Ping {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if self.seq_no != 0u32 {
      uint32::encode(1u32, &self.seq_no, buf);
    }
    if !self.node.is_empty() {
      bytes::encode(2u32, &self.node.0, buf);
    }
    match self.source_addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 3u32, buf),
      EncodableSocketAddr::None => {}
    }
    if !self.source_node.is_empty() {
      bytes::encode(4u32, &self.source_node.0, buf);
    }
  }
  #[allow(unused_variables)]
  fn merge_field<B>(
    &mut self,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> Result<(), DecodeError>
  where
    B: Buf,
  {
    const STRUCT_NAME: &str = "Ping";
    match tag {
      1u32 => {
        let value = &mut self.seq_no;
        uint32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "seq_no");
          error
        })
      }
      2u32 => {
        let value = &mut self.node.0;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "node");
          error
        })
      }
      3u32 => match &self.source_addr {
        EncodableSocketAddr::Local(addr) => {
          merge_socket_addr(wire_type, addr, buf, ctx).map_err(|mut error| {
            error.push(STRUCT_NAME, "source_addr");
            error
          })
        }
        EncodableSocketAddr::None => {
          let mut error = DecodeError::new("invalid source_addr");
          error.push(STRUCT_NAME, "source_addr");
          Err(error)
        }
      },
      4u32 => {
        let value = &mut self.source_node.0;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "source_node");
          error
        })
      }
      _ => skip_field(wire_type, tag, buf, ctx),
    }
  }
  #[inline]
  fn encoded_len(&self) -> usize {
    (if self.seq_no != 0u32 {
      uint32::encoded_len(1u32, &self.seq_no)
    } else {
      0
    }) + if !self.node.is_empty() {
      bytes::encoded_len(2u32, &self.node.0)
    } else {
      0
    } + self.source_addr.encoded_len()
      + if !self.source_node.is_empty() {
        bytes::encoded_len(4u32, &self.source_node.0)
      } else {
        0
      }
  }
  fn clear(&mut self) {
    self.seq_no = 0u32;
    self.node.clear();
    self.source_addr.clear();
    self.source_node.clear();
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[doc(hidden)]
pub struct PushNodeState {
  name: Name,
  addr: EncodableSocketAddr,
  meta: Bytes,
  incarnation: u32,
  state: NodeState,
  vsn: [u8; VSN_SIZE],
}

impl Message for PushNodeState {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if !self.name.is_empty() {
      bytes::encode(1u32, &self.name.0, buf);
    }
    match self.addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 2u32, buf),
      EncodableSocketAddr::None => {}
    }
    if self.meta != b"" as &[u8] {
      bytes::encode(3u32, &self.meta, buf);
    }
    if self.incarnation != 0u32 {
      uint32::encode(4u32, &self.incarnation, buf);
    }
    if self.state as i32 != 0i32 {
      int32::encode(5u32, &(self.state as i32), buf);
    }
    if self.vsn != b"" as &[u8] {
      encode_vsn(&self.vsn, 6u32, buf);
    }
  }
  #[allow(unused_variables)]
  fn merge_field<B>(
    &mut self,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> Result<(), DecodeError>
  where
    B: Buf,
  {
    const STRUCT_NAME: &str = "PushNodeState";
    match tag {
      1u32 => {
        let value = &mut self.name.0;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "name");
          error
        })
      }
      2u32 => match &self.addr {
        EncodableSocketAddr::Local(addr) => {
          merge_socket_addr(wire_type, addr, buf, ctx).map_err(|mut error| {
            error.push(STRUCT_NAME, "source_addr");
            error
          })
        }
        EncodableSocketAddr::None => {
          let mut error = DecodeError::new("invalid source_addr");
          error.push(STRUCT_NAME, "source_addr");
          Err(error)
        }
      },
      3u32 => {
        let value = &mut self.meta;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "meta");
          error
        })
      }
      4u32 => {
        let value = &mut self.incarnation;
        uint32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "incarnation");
          error
        })
      }
      5u32 => {
        let value = &mut (self.state as i32);
        int32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "state");
          error
        })
      }
      6u32 => merge_vsn(wire_type, &self.vsn, buf, ctx).map_err(|mut error| {
        error.push(STRUCT_NAME, "vsn");
        error
      }),
      _ => skip_field(wire_type, tag, buf, ctx),
    }
  }
  #[inline]
  fn encoded_len(&self) -> usize {
    (if !self.name.is_empty() {
      bytes::encoded_len(1u32, &self.name.0)
    } else {
      0
    }) + self.addr.encoded_len()
      + if self.meta != b"" as &[u8] {
        bytes::encoded_len(3u32, &self.meta)
      } else {
        0
      }
      + if self.incarnation != 0u32 {
        uint32::encoded_len(4u32, &self.incarnation)
      } else {
        0
      }
      + if self.state as i32 != 0i32 {
        int32::encoded_len(5u32, &(self.state as i32))
      } else {
        0
      }
      + VSN_ENCODED_SIZE
  }
  fn clear(&mut self) {
    self.name.clear();
    self.addr.clear();
    self.meta.clear();
    self.incarnation = 0u32;
    self.state = NodeState::Alive;
    self.vsn = [0; VSN_SIZE];
  }
}
