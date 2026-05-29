//! `AdvertiseAddrResolver` — picks one `SocketAddr` from a candidate set
//! during local-node advertise resolution. Called once at `Transport::new`
//! when the configured advertise address is `MaybeResolved::Unresolved(addr)`
//! and `Resolver::resolve(&addr)` returns multiple candidates.

use std::net::SocketAddr;

/// Picks one `SocketAddr` from a candidate set.
pub trait AdvertiseAddrResolver: 'static {
  /// Error type returned by [`Self::pick`].
  type Error: core::error::Error + 'static;

  /// Pick one candidate. Empty input is an error.
  fn pick(&self, candidates: Vec<SocketAddr>) -> Result<SocketAddr, Self::Error>;
}

/// Error variants returned by the default [`AdvertiseAddrResolver`] impls
/// shipped with this crate.
#[derive(Debug, thiserror::Error)]
pub enum AdvertiseResolutionError {
  /// The candidate set was empty.
  #[error("advertise resolution: no candidate addresses returned")]
  Empty,
}

/// Default — returns the first candidate.
pub struct FirstAddrResolver;

impl AdvertiseAddrResolver for FirstAddrResolver {
  type Error = AdvertiseResolutionError;

  fn pick(&self, candidates: Vec<SocketAddr>) -> Result<SocketAddr, Self::Error> {
    candidates
      .into_iter()
      .next()
      .ok_or(AdvertiseResolutionError::Empty)
  }
}

/// Returns the first IPv4 candidate; falls through to the first of any
/// family if none are IPv4.
pub struct Ipv4PreferringResolver;

impl AdvertiseAddrResolver for Ipv4PreferringResolver {
  type Error = AdvertiseResolutionError;

  fn pick(&self, candidates: Vec<SocketAddr>) -> Result<SocketAddr, Self::Error> {
    let v4 = candidates.iter().find(|s| s.is_ipv4()).copied();
    if let Some(s) = v4 {
      return Ok(s);
    }
    candidates
      .into_iter()
      .next()
      .ok_or(AdvertiseResolutionError::Empty)
  }
}

/// Returns the first IPv6 candidate; falls through to the first of any
/// family if none are IPv6.
pub struct Ipv6PreferringResolver;

impl AdvertiseAddrResolver for Ipv6PreferringResolver {
  type Error = AdvertiseResolutionError;

  fn pick(&self, candidates: Vec<SocketAddr>) -> Result<SocketAddr, Self::Error> {
    let v6 = candidates.iter().find(|s| s.is_ipv6()).copied();
    if let Some(s) = v6 {
      return Ok(s);
    }
    candidates
      .into_iter()
      .next()
      .ok_or(AdvertiseResolutionError::Empty)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

  fn v4(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
  }
  fn v6(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
  }

  #[test]
  fn first_addr_returns_first() {
    let r = FirstAddrResolver;
    let picked = r.pick(vec![v4(1), v4(2), v4(3)]).unwrap();
    assert_eq!(picked, v4(1));
  }

  #[test]
  fn first_addr_errors_on_empty() {
    let r = FirstAddrResolver;
    let err = r.pick(vec![]).unwrap_err();
    assert!(matches!(err, AdvertiseResolutionError::Empty));
  }

  #[test]
  fn ipv4_preferring_picks_ipv4_when_present() {
    let r = Ipv4PreferringResolver;
    let picked = r.pick(vec![v6(1), v4(2), v4(3)]).unwrap();
    assert_eq!(picked, v4(2));
  }

  #[test]
  fn ipv4_preferring_falls_through_to_first_when_no_ipv4() {
    let r = Ipv4PreferringResolver;
    let picked = r.pick(vec![v6(1), v6(2)]).unwrap();
    assert_eq!(picked, v6(1));
  }

  #[test]
  fn ipv4_preferring_errors_on_empty() {
    let r = Ipv4PreferringResolver;
    assert!(r.pick(vec![]).is_err());
  }

  #[test]
  fn ipv6_preferring_picks_ipv6_when_present() {
    let r = Ipv6PreferringResolver;
    let picked = r.pick(vec![v4(1), v6(2), v4(3)]).unwrap();
    assert_eq!(picked, v6(2));
  }

  #[test]
  fn ipv6_preferring_falls_through_to_first_when_no_ipv6() {
    let r = Ipv6PreferringResolver;
    let picked = r.pick(vec![v4(1), v4(2)]).unwrap();
    assert_eq!(picked, v4(1));
  }
}
