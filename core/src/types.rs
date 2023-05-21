use std::{
  cell::RefCell,
  net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
  ops::{Deref, DerefMut},
  thread_local,
};

use prost::{
  bytes::{Buf, BufMut, Bytes, BytesMut},
  encoding::{bool, bytes, int32, skip_field, uint32, DecodeContext, WireType},
  DecodeError, EncodeError, Message as ProstMessage,
};
use serde::{Deserialize, Serialize};
use showbiz_types::{MessageType, Name, NodeState};

const VSN_SIZE: usize = 6;
const VSN_ENCODED_SIZE: usize = 8;
const VSN_EMPTY: [u8; VSN_SIZE] = [255; VSN_SIZE];
const V4_SOCKET_ADDR_SIZE: usize = 6;
const V6_SOCKET_ADDR_SIZE: usize = 18;
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
fn encode_vsn<B>(vsn: &[u8], tag: u32, buf: &mut B)
where
  B: BufMut,
{
  VSN_BUFFER.with(|b| {
    if vsn == VSN_EMPTY {
      return;
    }
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidCompressionAlgo(u8);

impl core::fmt::Display for InvalidCompressionAlgo {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "invalid compression algo {}", self)
  }
}

impl std::error::Error for InvalidCompressionAlgo {}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionAlgo {
  #[default]
  LZW = 0,
  None = 1,
}

impl CompressionAlgo {
  pub fn is_none(&self) -> bool {
    matches!(self, Self::None)
  }
}

impl TryFrom<u8> for CompressionAlgo {
  type Error = InvalidCompressionAlgo;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::LZW),
      1 => Ok(Self::None),
      _ => Err(InvalidCompressionAlgo(value)),
    }
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Compress {
  algo: CompressionAlgo,
  buf: Bytes,
}

impl ProstMessage for Compress {
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
    Self: Sized,
  {
    if self.algo as i32 != 0i32 {
      int32::encode(1u32, &(self.algo as i32), buf);
    }
    if !self.buf.is_empty() {
      bytes::encode(2u32, &self.buf, buf);
    }
  }

  fn merge_field<B>(
    &mut self,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> Result<(), DecodeError>
  where
    B: Buf,
    Self: Sized,
  {
    const STRUCT_NAME: &str = "Compress";
    match tag {
      1u32 => {
        let value = &mut (self.algo as i32);
        int32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "algo");
          error
        })
      }
      2u32 => {
        let value = &mut self.buf;
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "buf");
          error
        })
      }
      _ => skip_field(wire_type, tag, buf, ctx),
    }
  }

  fn encoded_len(&self) -> usize {
    (if self.algo as i32 != 0i32 {
      int32::encoded_len(1u32, &(self.algo as i32))
    } else {
      0
    }) + if !self.buf.is_empty() {
      bytes::encoded_len(2u32, &self.buf)
    } else {
      0
    }
  }

  fn clear(&mut self) {
    self.algo = CompressionAlgo::LZW;
    self.buf.clear();
  }
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
  fn merge<B>(
    &mut self,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
  ) -> Result<(), DecodeError>
  where
    B: Buf,
  {
    SOCKET_ADDR_BUFFER.with(|b| {
      let mut b = b.borrow_mut();
      b.clear();
      match self {
        Self::Local(addr) => {
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
        }
        Self::None => {
          bytes::merge(wire_type, b.deref_mut(), buf, ctx).and_then(|_| match b.len() {
            V4_SOCKET_ADDR_SIZE => {
              *self = EncodableSocketAddr::Local(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(b[0], b[1], b[2], b[3]),
                u16::from_be_bytes([b[4], b[5]]),
              )));
              Ok(())
            }
            V6_SOCKET_ADDR_SIZE => {
              *self = EncodableSocketAddr::Local(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::new(
                  u16::from_be_bytes([b[0], b[1]]),
                  u16::from_be_bytes([b[2], b[3]]),
                  u16::from_be_bytes([b[4], b[5]]),
                  u16::from_be_bytes([b[6], b[7]]),
                  u16::from_be_bytes([b[8], b[9]]),
                  u16::from_be_bytes([b[10], b[11]]),
                  u16::from_be_bytes([b[12], b[13]]),
                  u16::from_be_bytes([b[14], b[15]]),
                ),
                u16::from_be_bytes([b[16], b[17]]),
                0,
                0,
              )));
              Ok(())
            }
            _ => return Err(DecodeError::new("invalid socket addr")),
          })
        }
      }
    })
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

  #[inline]
  pub(crate) const fn addr(&self) -> SocketAddr {
    match self {
      EncodableSocketAddr::Local(addr) => *addr,
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

#[doc(hidden)]
pub trait ProstMessageExt: ProstMessage {
  fn encode_with_prefix(&self) -> Result<Bytes, EncodeError>;
}

macro_rules! msg_ext {
  ($($ty: ident),+ $(,)?) => {
    $(
      impl ProstMessageExt for $ty {
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
    pub(crate) struct $name {
      incarnation: u32,
      node: Name,
      from: Name,
    }

    impl ProstMessage for $name {
      #[allow(unused_variables)]
      fn encode_raw<B>(&self, buf: &mut B)
      where
        B: BufMut,
      {
        if self.incarnation != 0u32 {
          uint32::encode(1u32, &self.incarnation, buf);
        }
        if self.node != "" {
          bytes::encode(2u32, self.node.bytes(), buf);
        }
        if self.from != "" {
          bytes::encode(3u32, self.from.bytes(), buf);
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
            let value = self.node.bytes_mut();
            bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
              error.push(STRUCT_NAME, "node");
              error
            })
          }
          3u32 => {
            let value = self.from.bytes_mut();
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
          bytes::encoded_len(2u32, self.node.bytes())
        } else {
          0
        } + if self.from != "" {
          bytes::encoded_len(3u32, self.from.bytes())
        } else {
          0
        }
      }
      fn clear(&mut self) {
        self.incarnation = 0u32;
        self.node.clear();
        self.from.clear();
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
pub(crate) struct AckResponse {
  seq_no: u32,
  payload: Bytes,
}

impl AckResponse {
  pub fn new(seq_no: u32, payload: Bytes) -> Self {
    Self { seq_no, payload }
  }
}

impl ProstMessage for AckResponse {
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
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, prost::Message)]
#[serde(transparent)]
#[repr(transparent)]
#[doc(hidden)]
pub(crate) struct NackResponse {
  #[prost(uint32, tag = "1")]
  seq_no: u32,
}

#[viewit::viewit]
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, prost::Message)]
#[serde(transparent)]
#[repr(transparent)]
#[doc(hidden)]
pub(crate) struct ErrorResponse {
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
#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, prost::Message)]
#[doc(hidden)]
pub(crate) struct PushPullHeader {
  #[prost(uint32, tag = "1")]
  nodes: u32,
  #[prost(uint32, tag = "2")]
  user_state_len: u32, // Encodes the byte lengh of user state
  #[prost(bool, tag = "3")]
  join: bool, // Is this a join request or a anti-entropy run
}

#[viewit::viewit]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[doc(hidden)]
pub(crate) struct Alive {
  incarnation: u32,
  node: Name,
  addr: EncodableSocketAddr,
  meta: Bytes,
  // The versions of the protocol/delegate that are being spoken, order:
  // pmin, pmax, pcur, dmin, dmax, dcur
  vsn: [u8; VSN_SIZE],
}

impl Default for Alive {
  fn default() -> Self {
    Self {
      incarnation: 0,
      node: Name::default(),
      addr: EncodableSocketAddr::default(),
      meta: Bytes::new(),
      vsn: VSN_EMPTY,
    }
  }
}

impl ProstMessage for Alive {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if self.incarnation != 0u32 {
      uint32::encode(1u32, &self.incarnation, buf);
    }
    if !self.node.is_empty() {
      bytes::encode(2u32, self.node.bytes(), buf);
    }
    match self.addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 3u32, buf),
      EncodableSocketAddr::None => {}
    }
    if self.meta != b"" as &[u8] {
      bytes::encode(4u32, &self.meta, buf);
    }
    if self.vsn != VSN_EMPTY {
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
        let value = self.node.bytes_mut();
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "node");
          error
        })
      }
      3u32 => self.addr.merge(wire_type, buf, ctx).map_err(|mut error| {
        error.push(STRUCT_NAME, "addr");
        error
      }),
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
      bytes::encoded_len(2u32, self.node.bytes())
    } else {
      0
    } + self.addr.encoded_len()
      + if self.meta != b"" as &[u8] {
        bytes::encoded_len(4u32, &self.meta)
      } else {
        0
      }
      + if self.vsn != VSN_EMPTY {
        VSN_ENCODED_SIZE
      } else {
        0
      }
  }
  fn clear(&mut self) {
    self.incarnation = 0u32;
    self.node.clear();
    self.addr.clear();
    self.meta.clear();
    self.vsn = VSN_EMPTY;
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
pub(crate) struct IndirectPingRequest {
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

impl ProstMessage for IndirectPingRequest {
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
      bytes::encode(3u32, self.node.bytes(), buf);
    }
    if self.nack {
      bool::encode(4u32, &self.nack, buf);
    }
    match self.source_addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 5u32, buf),
      EncodableSocketAddr::None => {}
    }
    if !self.source_node.is_empty() {
      bytes::encode(6u32, self.source_node.bytes(), buf);
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
      2u32 => self.target.merge(wire_type, buf, ctx).map_err(|mut error| {
        error.push(STRUCT_NAME, "target");
        error
      }),
      3u32 => {
        let value = self.node.bytes_mut();
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
      5u32 => self
        .source_addr
        .merge(wire_type, buf, ctx)
        .map_err(|mut error| {
          error.push(STRUCT_NAME, "source_addr");
          error
        }),
      6u32 => {
        let value = self.source_node.bytes_mut();
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
        bytes::encoded_len(3u32, self.node.bytes())
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
        bytes::encoded_len(6u32, self.source_node.bytes())
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
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, prost::Message)]
#[doc(hidden)]
#[repr(transparent)]
#[serde(transparent)]
pub(crate) struct UserMsgHeader {
  #[prost(uint32, tag = "1")]
  len: u32, // Encodes the byte lengh of user state
}

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Ping {
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

impl ProstMessage for Ping {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if self.seq_no != 0u32 {
      uint32::encode(1u32, &self.seq_no, buf);
    }
    if !self.node.is_empty() {
      bytes::encode(2u32, self.node.bytes(), buf);
    }
    match self.source_addr() {
      EncodableSocketAddr::Local(addr) => encode_socket_addr(addr, 3u32, buf),
      EncodableSocketAddr::None => {}
    }
    if !self.source_node.is_empty() {
      bytes::encode(4u32, self.source_node.bytes(), buf);
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
        let value = self.node.bytes_mut();
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "node");
          error
        })
      }
      3u32 => self
        .source_addr
        .merge(wire_type, buf, ctx)
        .map_err(|mut error| {
          error.push(STRUCT_NAME, "source_addr");
          error
        }),
      4u32 => {
        let value = self.source_node.bytes_mut();
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
      bytes::encoded_len(2u32, self.node.bytes())
    } else {
      0
    } + self.source_addr.encoded_len()
      + if !self.source_node.is_empty() {
        bytes::encoded_len(4u32, self.source_node.bytes())
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[doc(hidden)]
pub(crate) struct PushNodeState {
  name: Name,
  addr: EncodableSocketAddr,
  meta: Bytes,
  incarnation: u32,
  state: NodeState,
  vsn: [u8; VSN_SIZE],
}

impl PushNodeState {
  #[inline]
  pub const fn pmin(&self) -> u8 {
    self.vsn[0]
  }
  #[inline]
  pub const fn pmax(&self) -> u8 {
    self.vsn[1]
  }
  #[inline]
  pub const fn pcur(&self) -> u8 {
    self.vsn[2]
  }
  #[inline]
  pub const fn dmin(&self) -> u8 {
    self.vsn[3]
  }
  #[inline]
  pub const fn dmax(&self) -> u8 {
    self.vsn[4]
  }
  #[inline]
  pub const fn dcur(&self) -> u8 {
    self.vsn[5]
  }
}

impl Default for PushNodeState {
  fn default() -> Self {
    Self {
      name: Name::default(),
      addr: EncodableSocketAddr::None,
      meta: Bytes::default(),
      incarnation: 0,
      state: NodeState::default(),
      vsn: VSN_EMPTY,
    }
  }
}

impl ProstMessage for PushNodeState {
  #[allow(unused_variables)]
  fn encode_raw<B>(&self, buf: &mut B)
  where
    B: BufMut,
  {
    if !self.name.is_empty() {
      bytes::encode(1u32, self.name.bytes(), buf);
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
    if self.vsn != VSN_EMPTY {
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
        let value = self.name.bytes_mut();
        bytes::merge(wire_type, value, buf, ctx).map_err(|mut error| {
          error.push(STRUCT_NAME, "name");
          error
        })
      }
      2u32 => self.addr.merge(wire_type, buf, ctx).map_err(|mut error| {
        error.push(STRUCT_NAME, "addr");
        error
      }),
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
      bytes::encoded_len(1u32, self.name.bytes())
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
      + if self.vsn != VSN_EMPTY {
        VSN_ENCODED_SIZE
      } else {
        0
      }
  }
  fn clear(&mut self) {
    self.name.clear();
    self.addr.clear();
    self.meta.clear();
    self.incarnation = 0u32;
    self.state = NodeState::Alive;
    self.vsn = VSN_EMPTY;
  }
}

#[test]
fn test_push_state_enc_dec() {
  use std::net::*;
  let mut buf = BytesMut::new();
  let mut push_state = PushNodeState::default();
  push_state.name = "foo".to_string().into();
  push_state.addr = EncodableSocketAddr::Local(SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
    8080,
  ));

  push_state.encode(&mut buf).unwrap();
  let val = PushNodeState::decode(&mut buf).unwrap();
  assert_eq!(val, push_state);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    let mut this = BytesMut::with_capacity(Self::PREFIX_SIZE);
    this.put_u8(MessageType::User as u8);
    this.put_slice(&[0; core::mem::size_of::<u32>()]);
    Self(this)
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
