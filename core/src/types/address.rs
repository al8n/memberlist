use std::{
  io::{self, Error, ErrorKind},
  net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use super::{DecodeError, LENGTH_SIZE};

const V4_ADDR_SIZE: usize = 4;
const V6_ADDR_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct Domain(SmolStr);

impl core::fmt::Display for Domain {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl Domain {
  /// The maximum length of a domain name, in bytes.
  pub const MAX_SIZE: usize = 253;
}

impl Default for Domain {
  fn default() -> Self {
    Self::new()
  }
}

impl Domain {
  #[inline]
  pub fn new() -> Self {
    Self(SmolStr::new(""))
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  pub(crate) fn from_bytes(s: &[u8]) -> Result<Self, InvalidDomain> {
    match core::str::from_utf8(s) {
      Ok(s) => Self::try_from(s),
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  pub(crate) fn from_array<const N: usize>(s: [u8; N]) -> Result<Self, InvalidDomain> {
    match core::str::from_utf8(&s) {
      Ok(domain) => Self::try_from(domain),
      Err(e) => Err(e.into()),
    }
  }

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    core::mem::size_of::<u8>() + self.0.len()
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u8(self.0.len() as u8);
    buf.put(self.0.as_bytes());
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    let len = buf.get_u8() as usize;
    if len > Self::MAX_SIZE {
      return Err(DecodeError::InvalidDomain(InvalidDomain::InvalidLength(
        len,
      )));
    }
    if len > buf.remaining() {
      return Err(DecodeError::Truncated("name"));
    }

    let s = &buf.chunk()[..len];
    Self::from_bytes(s).map_err(From::from)
  }

  #[cfg(feature = "async")]
  #[inline]
  pub(crate) async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> io::Result<Self> {
    use futures_util::io::AsyncReadExt;

    let mut len_buf = [0; 1];
    r.read_exact(&mut len_buf).await?;
    let len = len_buf[0] as usize;

    if len <= 23 {
      map_inlined!(match Self.len() {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18,
        19, 20, 21, 22, 23
      } => r)
    } else {
      let mut buf = vec![0; len];
      r.read_exact(&mut buf).await?;
      Self::from_bytes(&buf).map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }
  }
}

impl Serialize for Domain {
  fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(self.as_str())
  }
}

impl<'de> Deserialize<'de> for Domain {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    SmolStr::deserialize(deserializer)
      .and_then(|n| Domain::try_from(n).map_err(|e| serde::de::Error::custom(e)))
  }
}

impl AsRef<str> for Domain {
  fn as_ref(&self) -> &str {
    self.as_str()
  }
}

impl core::cmp::PartialOrd for Domain {
  fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
    self.as_str().partial_cmp(other.as_str())
  }
}

impl core::cmp::Ord for Domain {
  fn cmp(&self, other: &Self) -> core::cmp::Ordering {
    self.as_str().cmp(other.as_str())
  }
}

impl core::cmp::PartialEq for Domain {
  fn eq(&self, other: &Self) -> bool {
    self.as_str() == other.as_str()
  }
}

impl core::cmp::PartialEq<str> for Domain {
  fn eq(&self, other: &str) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&str> for Domain {
  fn eq(&self, other: &&str) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::PartialEq<String> for Domain {
  fn eq(&self, other: &String) -> bool {
    self.as_str() == other
  }
}

impl core::cmp::PartialEq<&String> for Domain {
  fn eq(&self, other: &&String) -> bool {
    self.as_str() == *other
  }
}

impl core::cmp::Eq for Domain {}

impl core::hash::Hash for Domain {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.as_str().hash(state)
  }
}

impl Domain {
  pub fn as_bytes(&self) -> &[u8] {
    self.0.as_bytes()
  }

  pub fn as_str(&self) -> &str {
    self.0.as_str()
  }
}

impl TryFrom<&str> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    is_valid_domain_name(s).map(|_| Self(s.into()))
  }
}

impl TryFrom<String> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    is_valid_domain_name(s.as_str()).map(|_| Self(s.into()))
  }
}

impl TryFrom<SmolStr> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: SmolStr) -> Result<Self, Self::Error> {
    is_valid_domain_name(s.as_str()).map(|_| Self(s))
  }
}

impl TryFrom<&SmolStr> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: &SmolStr) -> Result<Self, Self::Error> {
    is_valid_domain_name(s.as_str()).map(|_| Self(s.clone()))
  }
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidDomain {
  #[error("each label must be between 1 and 63 characters long, got {0}")]
  InvalidLabelSize(usize),
  #[error("{0}")]
  Other(&'static str),
  #[error("invalid domain length {0}, must be between 1 and 253")]
  InvalidLength(usize),
  #[error("invalid domain {0}")]
  Utf8(#[from] core::str::Utf8Error),
}

#[inline]
fn is_valid_domain_name(domain: &str) -> Result<(), InvalidDomain> {
  if domain.is_empty() || domain.len() > Domain::MAX_SIZE {
    return Err(InvalidDomain::InvalidLength(domain.len()));
  }

  for label in domain.split('.') {
    let len = label.len();
    // Each label must be between 1 and 63 characters long
    if len < 1 || len > 63 {
      return Err(InvalidDomain::InvalidLabelSize(len));
    }
    // Labels must start and end with an alphanumeric character
    if !label.chars().next().unwrap().is_alphanumeric()
      || !label.chars().last().unwrap().is_alphanumeric()
    {
      return Err(InvalidDomain::Other(
        "label must start and end with an alphanumeric character",
      ));
    }
    // Labels can contain alphanumeric characters and hyphens, but hyphen cannot be at the start or end
    if label.chars().any(|c| !c.is_alphanumeric() && c != '-') {
      return Err(InvalidDomain::Other("label can contain alphanumeric characters and hyphens, but hyphen cannot be at the start or end"));
    }
  }
  Ok(())
}

/// The Address for a node, can be an ip or a domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NodeAddress {
  /// e.g. `128.0.0.1`
  Ip(IpAddr),
  /// e.g. `www.example.com`
  Domain(Domain),
}

impl Default for NodeAddress {
  #[inline]
  fn default() -> Self {
    Self::Domain(Domain::new())
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

  #[inline]
  pub(crate) fn encoded_len(&self) -> usize {
    1 // type mark
    + match self {
      Self::Ip(addr) => match addr {
        IpAddr::V4(_) => 4,
        IpAddr::V6(_) => 16,
      },
      Self::Domain(addr) => addr.encoded_len(),
    }
  }

  #[inline]
  pub(crate) fn encode_to(&self, buf: &mut BytesMut) {
    buf.put_u32(self.encoded_len() as u32);
    match self {
      Self::Ip(addr) => match addr {
        IpAddr::V4(addr) => {
          buf.put_u8(0);
          buf.put_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
          buf.put_u8(1);
          buf.put_slice(&addr.octets())
        }
      },
      Self::Domain(addr) => {
        buf.put_u8(2);
        addr.encode_to(buf);
      }
    }
  }

  #[inline]
  pub(crate) fn decode_from(mut buf: impl Buf) -> Result<Self, DecodeError> {
    match buf.get_u8() {
      0 => match buf.remaining() {
        4 => {
          let mut addr = [0; V4_ADDR_SIZE];
          addr.copy_from_slice(&buf.chunk()[..V4_ADDR_SIZE]);
          Ok(Self::Ip(IpAddr::V4(Ipv4Addr::from(addr))))
        }
        r => Err(DecodeError::InvalidIpAddrLength(r)),
      },
      1 => match buf.remaining() {
        16 => {
          let mut addr = [0; V6_ADDR_SIZE];
          addr.copy_from_slice(&buf.chunk()[..V6_ADDR_SIZE]);
          Ok(Self::Ip(IpAddr::V6(Ipv6Addr::from(addr))))
        }
        r => Err(DecodeError::InvalidIpAddrLength(r)),
      },
      2 => Domain::decode_from(buf).map(Self::Domain),
      b => Err(DecodeError::UnknownMarkBit(b)),
    }
  }

  #[cfg(feature = "async")]
  #[inline]
  pub(crate) async fn decode_from_reader<R: futures_util::io::AsyncRead + Unpin>(
    r: &mut R,
  ) -> std::io::Result<Self> {
    use futures_util::io::AsyncReadExt;

    let mut msg_len = [0; LENGTH_SIZE];
    r.read_exact(&mut msg_len).await?;
    let len = u32::from_be_bytes(msg_len) as usize;

    let mut marker = [0u8; 1];
    r.read_exact(&mut marker).await?;
    match marker[0] {
      0 => {
        if len != V4_ADDR_SIZE {
          return Err(Error::new(
            ErrorKind::InvalidData,
            DecodeError::InvalidIpAddrLength(len - 1),
          ));
        }
        let mut addr = [0; V4_ADDR_SIZE];
        r.read_exact(&mut addr).await?;
        Ok(Self::Ip(IpAddr::V4(Ipv4Addr::from(addr))))
      }
      1 => {
        if len != V6_ADDR_SIZE {
          return Err(Error::new(
            ErrorKind::InvalidData,
            DecodeError::InvalidIpAddrLength(len - 1),
          ));
        }
        let mut addr = [0; V6_ADDR_SIZE];
        r.read_exact(&mut addr).await?;
        Ok(Self::Ip(IpAddr::V6(Ipv6Addr::from(addr))))
      }
      2 => Domain::decode_from_reader(r).await.map(Self::Domain),
      b => Err(Error::new(
        ErrorKind::InvalidData,
        DecodeError::UnknownMarkBit(b),
      )),
    }
  }
}

impl From<IpAddr> for NodeAddress {
  fn from(addr: IpAddr) -> Self {
    Self::Ip(addr)
  }
}

impl From<Domain> for NodeAddress {
  fn from(addr: Domain) -> Self {
    Self::Domain(addr)
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
