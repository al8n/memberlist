use std::{
  cell::RefCell,
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
  ops::{Deref, DerefMut},
  thread_local,
  time::Instant,
};

use prost::{
  bytes::{Buf, BufMut, Bytes, BytesMut},
  encoding::{bool, bytes, int32, skip_field, uint32, DecodeContext, WireType},
  DecodeError, EncodeError, Message as ProstMessage,
};
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
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
    #[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
    pub(crate) struct $name {
      incarnation: u32,
      node: NodeId,
      from: NodeId,
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

impl Dead {
  #[inline]
  pub(crate) fn dead_self(&self) -> bool {
    self.node == self.from
  }
}

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
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
#[derive(Copy, Clone, PartialEq, Eq, Hash, prost::Message)]
#[repr(transparent)]
#[doc(hidden)]
pub(crate) struct NackResponse {
  #[prost(uint32, tag = "1")]
  seq_no: u32,
}

#[viewit::viewit]
#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
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
#[derive(PartialEq, Eq, Hash, prost::Message)]
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
#[derive(Debug, Clone)]
#[doc(hidden)]
pub(crate) struct Alive {
  incarnation: u32,
  node: NodeId,
  meta: Bytes,
  // The versions of the protocol/delegate that are being spoken, order:
  // pmin, pmax, pcur, dmin, dmax, dcur
  vsn: [u8; VSN_SIZE],
}

impl Default for Alive {
  fn default() -> Self {
    Self {
      incarnation: 0,
      node: NodeId::default(),
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
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IndirectPingRequest {
  seq_no: u32,
  target: NodeAddress,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect against an agent
  /// restart with a new name.
  node: Name,

  /// true if we'd like a nack back
  nack: bool,

  /// Source address, used for a direct reply
  source: NodeId,
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
#[derive(Copy, Clone, PartialEq, Eq, Hash, prost::Message)]
#[doc(hidden)]
#[repr(transparent)]
pub(crate) struct UserMsgHeader {
  #[prost(uint32, tag = "1")]
  len: u32, // Encodes the byte lengh of user state
}

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Ping {
  seq_no: u32,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  node: Name,

  /// Source node, used for a direct reply
  source: NodeId,
}

impl Ping {
  pub const fn new(seq_no: u32, node: Name, source: NodeId) -> Self {
    Self {
      seq_no,
      node,
      source,
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[doc(hidden)]
pub(crate) struct PushNodeState {
  node: NodeId,
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
      node: NodeId::default(),
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

  #[inline]
  pub(crate) fn encode<M: ProstMessage>(
    msg: &M,
    ty: MessageType,
  ) -> Result<Self, prost::EncodeError> {
    let encoded_len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(Self::PREFIX_SIZE + encoded_len);
    buf.put_u8(ty as u8);
    buf.put_u32(encoded_len as u32);
    msg.encode(&mut buf)?;
    Ok(Self(buf))
  }

  pub(crate) fn compound(msgs: Vec<Self>) -> Bytes {
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
    buf.freeze()
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

/// The Address for a node, can be an ip or a domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NodeAddress {
  /// e.g. `128.0.0.1`
  Ip(IpAddr),
  /// e.g. `www.example.com`
  Domain(Name),
}

impl Default for NodeAddress {
  #[inline]
  fn default() -> Self {
    Self::Domain(Name::new())
  }
}

impl NodeAddress {
  #[inline]
  pub const fn is_ip(&self) -> bool {
    matches!(self, Self::Ip(_))
  }

  #[inline]
  pub const fn is_domain(&self) -> bool {
    matches!(self, Self::Domain(_))
  }

  #[inline]
  pub const fn unwrap_domain(&self) -> &str {
    match self {
      Self::Ip(_) => unreachable!(),
      Self::Domain(addr) => addr.as_str(),
    }
  }

  #[inline]
  pub(crate) const fn unwrap_ip(&self) -> IpAddr {
    match self {
      NodeAddress::Ip(addr) => *addr,
      _ => unreachable!(),
    }
  }
}

impl From<IpAddr> for NodeAddress {
  fn from(addr: IpAddr) -> Self {
    Self::Ip(addr)
  }
}

impl From<String> for NodeAddress {
  fn from(addr: String) -> Self {
    Self::Domain(addr.into())
  }
}

impl core::fmt::Display for NodeAddress {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Ip(addr) => write!(f, "{}", addr),
      Self::Domain(addr) => write!(f, "{}", addr),
    }
  }
}

#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone)]
pub struct NodeId {
  #[viewit(getter(const, style = "ref"))]
  name: Name,
  port: Option<u16>,
  #[viewit(getter(const, style = "ref"))]
  addr: NodeAddress,
}

impl Eq for NodeId {}

impl PartialEq for NodeId {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.port == other.port && self.addr == other.addr
  }
}

impl core::hash::Hash for NodeId {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.port.hash(state);
    self.addr.hash(state);
  }
}

impl Default for NodeId {
  #[inline]
  fn default() -> Self {
    Self {
      name: Name(Bytes::new()),
      port: None,
      addr: NodeAddress::Domain(Name::new()),
    }
  }
}

impl NodeId {
  #[inline]
  pub fn from_domain(domain: String) -> Self {
    Self {
      name: Name(Bytes::new()),
      port: None,
      addr: NodeAddress::Domain(domain.into()),
    }
  }

  #[inline]
  pub const fn from_ip(ip: IpAddr) -> Self {
    Self {
      name: Name(Bytes::new()),
      port: None,
      addr: NodeAddress::Ip(ip),
    }
  }

  #[inline]
  pub const fn from_addr(addr: NodeAddress) -> Self {
    Self {
      name: Name(Bytes::new()),
      port: None,
      addr,
    }
  }
}

impl From<SocketAddr> for NodeId {
  fn from(addr: SocketAddr) -> Self {
    Self {
      name: Name(Bytes::new()),
      port: Some(addr.port()),
      addr: NodeAddress::Ip(addr.ip()),
    }
  }
}

impl core::fmt::Display for NodeId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match &self.addr {
      NodeAddress::Ip(addr) => {
        if let Some(port) = self.port {
          write!(f, "{}({}:{})", self.name.as_str(), addr, port)
        } else {
          write!(f, "{}({})", self.name.as_str(), addr)
        }
      }
      NodeAddress::Domain(addr) => {
        if let Some(port) = self.port {
          write!(f, "{}({}:{})", self.name.as_str(), addr, port)
        } else {
          write!(f, "{}({})", self.name.as_str(), addr)
        }
      }
    }
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

#[derive(Clone)]
pub struct Name(Bytes);

impl Buf for Name {
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
    Self::new()
  }
}

impl Name {
  #[inline]
  pub const fn new() -> Self {
    Self(Bytes::new())
  }

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

  pub fn as_str(&self) -> &str {
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

impl From<String> for Name {
  fn from(s: String) -> Self {
    Self(Bytes::from(s))
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
