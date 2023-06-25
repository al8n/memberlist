use std::net::SocketAddr;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{decode_u32_from_buf, encode_u32_to_buf, DecodeError, Name};

const V4_ADDR_SIZE: usize = 4;
const V6_ADDR_SIZE: usize = 16;
const PORT_SIZE: usize = core::mem::size_of::<u16>();

#[viewit::viewit]
pub(crate) struct NodeIdRef<'a> {
  name: &'a Name,
  addr: SocketAddr,
}

impl<'a> core::fmt::Display for NodeIdRef<'a> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}({})", self.name, self.addr)
  }
}

#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

  #[inline]
  pub fn from_addr(addr: SocketAddr) -> Self {
    Self {
      name: Name::new(),
      addr,
    }
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    // addr + addr tag
    (if self.name.is_empty() {
      0
    } else {
      self.name.encoded_len() + 1 // name + name tag
    }) + if self.addr.is_ipv4() { 6 } else { 18 }
      + 1
  }

  #[inline]
  pub(crate) fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    if !self.name.is_empty() {
      // put tag
      buf.put_u8(1);
      self.name.encode_to(buf);
    }
    match self.addr {
      SocketAddr::V4(addr) => {
        buf.put_u8(2);
        buf.put_slice(&addr.ip().octets());
        buf.put_u16(addr.port());
      }
      SocketAddr::V6(addr) => {
        buf.put_u8(3);
        buf.put_slice(&addr.ip().octets());
        buf.put_u16(addr.port());
      }
    }
  }

  #[inline]
  pub(crate) fn decode_len(buf: impl Buf) -> Result<usize, DecodeError> {
    decode_u32_from_buf(buf)
      .map(|(len, _)| len as usize)
      .map_err(From::from)
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: Bytes) -> Result<Self, DecodeError> {
    let mut addr = None;
    let mut name = Name::new();
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          let len = Name::decode_len(&mut buf)?;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated("node id"));
          }
          name = Name::decode_from(buf.split_to(len))?;
        }
        2 => {
          if 6 > buf.remaining() {
            return Err(DecodeError::Truncated("node id"));
          }

          let mut octets = [0u8; V4_ADDR_SIZE];
          buf.copy_to_slice(&mut octets);
          let ip = std::net::Ipv4Addr::from(octets);
          let port = buf.get_u16();
          addr = Some(SocketAddr::new(std::net::IpAddr::V4(ip), port));
        }
        3 => {
          if buf.remaining() < V6_ADDR_SIZE + PORT_SIZE {
            return Err(DecodeError::Truncated("node id"));
          }
          let mut octets = [0; V6_ADDR_SIZE];
          buf.copy_to_slice(&mut octets);
          let ip = std::net::Ipv6Addr::from(octets);
          let port = buf.get_u16();
          addr = Some(SocketAddr::new(std::net::IpAddr::V6(ip), port));
        }
        _ => {}
      }
    }

    let addr = addr.ok_or(DecodeError::Truncated("node id"))?;
    Ok(Self { name, addr })
  }
}

impl From<SocketAddr> for NodeId {
  fn from(addr: SocketAddr) -> Self {
    Self {
      name: Name::new(),
      addr,
    }
  }
}

impl core::fmt::Display for NodeId {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}({})", self.name.as_str(), self.addr)
  }
}
