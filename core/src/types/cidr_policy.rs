pub use ipnet;
use ipnet::*;
use std::{collections::HashSet, net::IpAddr, str::FromStr};

/// Classless Inter-Domain Routing (CIDR) policy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
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
  #[inline]
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
  #[inline]
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
      if allowed_cidrs.is_empty() {
        self.allowed_cidrs = None;
      }
    }
  }

  /// Remove [`IpNet`]s from the list of allowed [`IpNet`] by [`IpAddr`].
  pub fn remove_by_ip(&mut self, ip: &IpAddr) {
    if let Some(allowed_cidrs) = self.allowed_cidrs.as_mut() {
      allowed_cidrs.retain(|cidr| !cidr.contains(ip));
      if allowed_cidrs.is_empty() {
        self.allowed_cidrs = None;
      }
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

  /// Reports whether the network is allowed.
  pub fn is_allowed_net(&self, ip: &IpNet) -> bool {
    self.allowed_cidrs.as_ref().map_or(true, |x| x.contains(ip))
  }

  /// Returns `true` if the [`IpNet`] is blocked.
  pub fn is_blocked_net(&self, ip: &IpNet) -> bool {
    !self.is_allowed_net(ip)
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
    self.allowed_cidrs.as_ref().is_some_and(|x| x.is_empty())
  }

  /// Returns `true` connection from any IP is allowed.
  pub fn is_allow_all(&self) -> bool {
    self.allowed_cidrs.is_none()
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

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn test_cidr_policy() {
    let mut policy = CIDRsPolicy::default();
    assert!(policy.is_allow_all());
    assert!(!policy.is_block_all());

    let net0: IpNet = "127.0.1.1/16".parse().unwrap();
    let net1: IpNet = "127.0.1.1/24".parse().unwrap();
    let net2: IpNet = "128.0.0.2/16".parse().unwrap();
    policy.add(net0);
    policy.add(net1);
    policy.add(net2);

    assert!(policy.is_allowed(&net0.addr()));
    // should not remove
    policy.remove(&net0);
    assert!(!policy.is_allowed_net(&net0));
    assert!(policy.is_allowed(&"127.0.1.1".parse().unwrap()));
    assert!(policy.is_allowed_net(&net1));
    policy.remove(&net0);
    assert!(policy.is_allowed(&"127.0.1.1".parse().unwrap()));
    policy.remove_by_ip(&net1.addr());
    assert!(!policy.is_allowed(&"127.0.1.1".parse().unwrap()));
    policy.remove_by_ip(&"128.0.0.2".parse().unwrap());
    // we have removed all the allowed cidrs, so we should allow all now
    assert!(policy.is_allowed(&"128.0.0.2".parse().unwrap()));
    assert!(policy.is_allowed_net(&"128.0.0.2/8".parse().unwrap()));
    assert!(policy.is_allow_all());
  }

  #[test]
  fn test_block_all() {
    let policy = CIDRsPolicy::block_all();
    assert!(policy.is_block_all());
    assert!(policy.is_blocked(&"127.0.0.1".parse().unwrap()));
    assert!(policy.is_blocked_net(&"127.0.0.1/8".parse().unwrap()));
  }

  #[test]
  fn test_try_from_iter() {
    let ip1 = IpNet::from_str("127.0.0.1/24").unwrap();
    let ip2 = IpNet::from_str("127.0.0.2/16").unwrap();
    let ip3 = IpNet::from_str("127.0.0.3/8").unwrap();
    let cidrs = vec![ip1, ip2, ip3];
    let policy = cidrs.into_iter().collect::<CIDRsPolicy>();

    let policy2 = CIDRsPolicy::from([ip1, ip2, ip3].into_iter().collect::<HashSet<_>>());
    assert_eq!(policy, policy2);
  }
}
