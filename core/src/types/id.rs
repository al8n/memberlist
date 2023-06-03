use std::net::{IpAddr, SocketAddr};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
  decode_u32_from_buf, encode_u32_to_buf, encoded_u32_len, DecodeError, Domain, InvalidDomain,
  Name, NodeAddress,
};

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
      name: Name::new(),
      port: None,
      addr: NodeAddress::Domain(Domain::new()),
    }
  }
}

impl NodeId {
  #[inline]
  pub fn new(name: Name, addr: SocketAddr) -> Self {
    Self {
      name,
      port: Some(addr.port()),
      addr: NodeAddress::Ip(addr.ip()),
    }
  }

  #[inline]
  pub fn from_domain(domain: String) -> Result<Self, InvalidDomain> {
    Domain::try_from(domain).map(|domain| Self {
      name: Name::new(),
      port: None,
      addr: NodeAddress::Domain(domain),
    })
  }

  #[inline]
  pub fn from_ip(ip: IpAddr) -> Self {
    Self {
      name: Name::new(),
      port: None,
      addr: NodeAddress::Ip(ip),
    }
  }

  #[inline]
  pub fn from_addr(addr: NodeAddress) -> Self {
    Self {
      name: Name::new(),
      port: None,
      addr,
    }
  }

  #[inline]
  pub fn encoded_len(&self) -> usize {
    let basic_len = if self.name.is_empty() {
      0
    } else {
      self.name.encoded_len() + 1 // name + name tag
    }
    + self.addr.encoded_len() + 1 // addr + addr tag
    + if self.port.is_some() {
      2 + 1 // port + port tag
    } else {
      0
    };

    encoded_u32_len(basic_len as u32) + basic_len
  }

  #[inline]
  pub(crate) fn encode_to(&self, mut buf: &mut BytesMut) {
    encode_u32_to_buf(&mut buf, self.encoded_len() as u32);
    if !self.name.is_empty() {
      // put tag
      buf.put_u8(1);
      self.name.encode_to(buf);
    }

    buf.put_u8(2);
    self.addr.encode_to(buf);
    if let Some(port) = self.port {
      // put tag
      buf.put_u8(3);
      buf.put_u16(port);
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
    let mut this = Self::default();
    while buf.has_remaining() {
      match buf.get_u8() {
        1 => {
          let len = Name::decode_len(&mut buf)?;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated("node id"));
          }
          this.name = Name::decode_from(buf.split_to(len))?;
        }
        2 => {
          let len = NodeAddress::decode_len(&mut buf)?;
          if len > buf.remaining() {
            return Err(DecodeError::Truncated("node id"));
          }

          this.addr = NodeAddress::decode_from(buf.split_to(len))?;
        }
        3 => {
          if buf.remaining() < 2 {
            return Err(DecodeError::Truncated("node id"));
          }
          this.port = Some(buf.get_u16());
        }
        _ => {}
      }
    }
    Ok(this)
  }
}

impl From<SocketAddr> for NodeId {
  fn from(addr: SocketAddr) -> Self {
    Self {
      name: Name::new(),
      port: Some(addr.port()),
      addr: NodeAddress::Ip(addr.ip()),
    }
  }
}

#[cfg(test)]
impl From<&str> for NodeId {
  fn from(addr: &str) -> Self {
    Self {
      name: Name::try_from(addr).unwrap(),
      port: None,
      addr: NodeAddress::Domain(Default::default()),
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
