use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone)]
pub struct Domain(Bytes);

impl core::fmt::Display for Domain {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
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
    Self(Bytes::new())
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
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
    Bytes::deserialize(deserializer)
      .and_then(|n| Domain::try_from(n).map_err(serde::de::Error::custom))
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
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  #[inline]
  pub fn as_str(&self) -> &str {
    core::str::from_utf8(&self.0).unwrap()
  }
}

impl TryFrom<&str> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    is_valid_domain_name(s).map(|_| Self(Bytes::copy_from_slice(s.as_bytes())))
  }
}

impl TryFrom<String> for Domain {
  type Error = InvalidDomain;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    is_valid_domain_name(s.as_str()).map(|_| Self(s.into()))
  }
}

impl TryFrom<Bytes> for Domain {
  type Error = InvalidDomain;

  fn try_from(buf: Bytes) -> Result<Self, Self::Error> {
    match core::str::from_utf8(&buf) {
      Ok(s) => is_valid_domain_name(s).map(|_| Self(buf)),
      Err(e) => Err(InvalidDomain::Utf8(e)),
    }
  }
}

impl TryFrom<&Bytes> for Domain {
  type Error = InvalidDomain;

  fn try_from(buf: &Bytes) -> Result<Self, Self::Error> {
    match core::str::from_utf8(buf) {
      Ok(s) => is_valid_domain_name(s).map(|_| Self(buf.clone())),
      Err(e) => Err(InvalidDomain::Utf8(e)),
    }
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
    if !(1..=63).contains(&len) {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
  Ip(IpAddr),
  Socket(SocketAddr),
  Domain { domain: Domain, port: Option<u16> },
}

impl core::fmt::Display for Address {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Socket(addr) => write!(f, "{addr}"),
      Self::Domain { domain, port } => match port {
        Some(port) => write!(f, "{domain}:{port}"),
        None => write!(f, "{domain}"),
      },
      Self::Ip(addr) => write!(f, "{addr}"),
    }
  }
}

impl From<Ipv4Addr> for Address {
  fn from(addr: Ipv4Addr) -> Self {
    Self::Ip(addr.into())
  }
}

impl From<Ipv6Addr> for Address {
  fn from(addr: Ipv6Addr) -> Self {
    Self::Ip(addr.into())
  }
}

impl From<IpAddr> for Address {
  fn from(addr: IpAddr) -> Self {
    Self::Ip(addr)
  }
}

impl From<(Ipv4Addr, u16)> for Address {
  fn from(addr: (Ipv4Addr, u16)) -> Self {
    Self::Socket(addr.into())
  }
}

impl From<(Ipv6Addr, u16)> for Address {
  fn from(addr: (Ipv6Addr, u16)) -> Self {
    Self::Socket(addr.into())
  }
}

impl From<(IpAddr, u16)> for Address {
  fn from(addr: (IpAddr, u16)) -> Self {
    Self::Socket(addr.into())
  }
}

impl From<SocketAddrV4> for Address {
  fn from(addr: SocketAddrV4) -> Self {
    Self::Socket(addr.into())
  }
}

impl From<SocketAddrV6> for Address {
  fn from(addr: SocketAddrV6) -> Self {
    Self::Socket(addr.into())
  }
}

impl From<SocketAddr> for Address {
  fn from(addr: SocketAddr) -> Self {
    Self::Socket(addr)
  }
}

impl From<(Domain, u16)> for Address {
  fn from((domain, port): (Domain, u16)) -> Self {
    Self::Domain {
      domain,
      port: Some(port),
    }
  }
}

impl From<Domain> for Address {
  fn from(domain: Domain) -> Self {
    Self::Domain { domain, port: None }
  }
}
