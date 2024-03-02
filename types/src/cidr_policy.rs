pub use ipnet;
use ipnet::*;
use std::{collections::HashSet, net::IpAddr, str::FromStr};

/// Classless Inter-Domain Routing (CIDR) policy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct CIDRsPolicy {
  allowed_cidrs: Option<HashSet<IpNet>>,
}

impl Default for CIDRsPolicy {
  fn default() -> Self {
    Self::allow_all()
  }
}

impl CIDRsPolicy {
  /// Allow connection from any [`IpNet`].
  #[inline(always)]
  pub const fn allow_all() -> Self {
    Self {
      allowed_cidrs: None,
    }
  }

  /// Block connection from any [`IpNet`].
  #[inline]
  pub fn block_all() -> Self {
    Self {
      allowed_cidrs: Some(HashSet::new()),
    }
  }

  /// Create a default [`CIDRsPolicy`].
  #[inline(always)]
  pub const fn new() -> Self {
    Self::allow_all()
  }

  /// Add a new [`IpNet`] to the list of allowed [`IpNet`].
  pub fn add(&mut self, ip: IpNet) {
    self
      .allowed_cidrs
      .get_or_insert_with(HashSet::new)
      .insert(ip);
  }

  /// Remove an [`IpNet`] from the list of allowed [`IpNet`].
  pub fn remove(&mut self, ip: &IpNet) {
    if let Some(allowed_cidrs) = self.allowed_cidrs.as_mut() {
      allowed_cidrs.remove(ip);
    }
  }

  /// Remove an [`IpNet`]s from the list of allowed [`IpNet`] by [`IpAddr`].
  pub fn remove_by_ip(&mut self, ip: &IpAddr) {
    if let Some(allowed_cidrs) = self.allowed_cidrs.as_mut() {
      allowed_cidrs.retain(|cidr| !cidr.contains(ip));
    }
  }

  /// Returns an iterator over the allowed [`IpNet`]s.
  pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
    self
      .allowed_cidrs
      .as_ref()
      .into_iter()
      .flat_map(|x| x.iter())
  }

  /// Reports whether the network includes ip.
  pub fn contains(&self, ip: &IpAddr) -> bool {
    self
      .allowed_cidrs
      .as_ref()
      .map_or(true, |x| x.contains(&IpNet::from(*ip)))
  }

  /// Returns `true` if the [`IpAddr`] is allowed.
  pub fn is_allowed(&self, ip: &IpAddr) -> bool {
    self
      .allowed_cidrs
      .as_ref()
      .map_or(true, |x| x.iter().any(|cidr| cidr.contains(ip)))
  }

  /// Returns `true` if the [`IpAddr`] is blocked.
  pub fn is_blocked(&self, ip: &IpAddr) -> bool {
    !self.is_allowed(ip)
  }

  /// Returns `true` connection from any IP is blocked.
  pub fn is_block_all(&self) -> bool {
    self.allowed_cidrs.as_ref().map_or(false, |x| x.is_empty())
  }

  /// Returns `true` connection from any IP is allowed.
  pub fn is_allow_all(&self) -> bool {
    self.allowed_cidrs.is_none()
  }

  /// Try to parse a [`CIDRsPolicy`] from an iter.
  pub fn try_from_iter<A: TryInto<IpNet, Error = AddrParseError>, I: Iterator<Item = A>>(
    iter: I,
  ) -> Result<Self, AddrParseError> {
    let mut allowed_cidrs = HashSet::new();
    for ip in iter {
      allowed_cidrs.insert(ip.try_into()?);
    }
    Ok(Self {
      allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
    })
  }
}

impl From<HashSet<IpNet>> for CIDRsPolicy {
  fn from(allowed_cidrs: HashSet<IpNet>) -> Self {
    Self {
      allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
    }
  }
}

impl<A: Into<IpNet>> FromIterator<A> for CIDRsPolicy {
  fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
    let mut allowed_cidrs = HashSet::new();
    for ip in iter {
      allowed_cidrs.insert(ip.into());
    }
    Self {
      allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
    }
  }
}

impl<'a, A: AsRef<str>> TryFrom<&'a [A]> for CIDRsPolicy {
  type Error = AddrParseError;

  fn try_from(iter: &'a [A]) -> Result<Self, Self::Error> {
    let mut allowed_cidrs = HashSet::new();
    for ip in iter {
      allowed_cidrs.insert(IpNet::from_str(ip.as_ref().trim())?);
    }
    Ok(Self {
      allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
    })
  }
}

impl<A: TryInto<IpNet, Error = AddrParseError>> TryFrom<Vec<A>> for CIDRsPolicy {
  type Error = AddrParseError;

  fn try_from(iter: Vec<A>) -> Result<Self, Self::Error> {
    let mut allowed_cidrs = HashSet::new();
    for ip in iter {
      allowed_cidrs.insert(ip.try_into()?);
    }
    Ok(Self {
      allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
    })
  }
}
