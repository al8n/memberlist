use std::{
  io::{self, Error, ErrorKind},
  net::{IpAddr, SocketAddr},
};

use bytes::{Buf, BufMut, BytesMut};

use super::{DecodeError, Domain, InvalidDomain, Name, NodeAddress, LENGTH_SIZE};

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
  pub const fn encoded_len(&self) -> usize {
    core::mem::size_of::<u32>()
      + self.name.encoded_len()
      + self.addr.encoded_len()
      + if self.port.is_some() { 3 } else { 1 } // port
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u32(self.encoded_len() as u32);
    self.name.encode_to(buf);
    self.addr.encode_to(buf);
    if let Some(port) = self.port {
      buf.put_u8(1);
      buf.put_u16(port);
    } else {
      buf.put_u8(0);
    }
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    if buf.remaining() < LENGTH_SIZE {
      return Err(DecodeError::Truncated("node id"));
    }
    let size = buf.get_u32() as usize;
    let name = Name::decode_from(&mut buf)?;
    let addr = NodeAddress::decode_from(&mut buf)?;
    let port = match buf.get_u8() {
      0 => None,
      1 => Some(buf.get_u16()),
      b => return Err(DecodeError::UnknownMarkBit(b)),
    };
    Ok(Self { name, port, addr })
  }

  #[cfg(feature = "async")]
  #[inline]
  pub(crate) async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> io::Result<Self> {
    use futures_util::io::AsyncReadExt;

    let mut buf = [0u8; LENGTH_SIZE];
    r.read_exact(&mut buf).await?;
    let size = u32::from_be_bytes(buf) as usize;
    let name = Name::decode_from_reader(r).await?;
    let addr = NodeAddress::decode_from_reader(r).await?;
    let mut mark = [0; 1];
    r.read_exact(&mut mark).await?;
    let port = match mark[0] {
      0 => None,
      1 => {
        let mut buf = [0; 2];
        r.read_exact(&mut buf).await?;
        Some(u16::from_be_bytes(buf))
      }
      b => {
        return Err(Error::new(
          ErrorKind::InvalidData,
          format!("unknown mark bit: {}", b),
        ))
      }
    };

    Ok(Self { name, addr, port })
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
