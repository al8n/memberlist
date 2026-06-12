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
#[non_exhaustive]
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
mod tests;
