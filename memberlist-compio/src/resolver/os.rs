//! OS-based resolver — uses compio's getaddrinfo equivalent.

use crate::{Address, resolver::Resolver};
use compio::net::ToSocketAddrsAsync;
use hostaddr::Host;
use std::{io, net::SocketAddr};

/// OS-based resolver — uses compio's `ToSocketAddrsAsync` (`getaddrinfo`).
/// UDP-only DNS; large hostname records may be truncated. Use
/// `DnsResolver` (feature `dns`) for TCP-first DNS guaranteed to fetch
/// the full list.
pub struct OsResolver;

impl Resolver for OsResolver {
  type Address = Address;
  type Error = io::Error;

  async fn resolve(&self, addr: &Self::Address) -> Result<Vec<SocketAddr>, Self::Error> {
    let port = addr.port().unwrap_or(0);
    let host_str = match addr.host() {
      Host::Ip(ip) => ip.to_string(),
      Host::Domain(name) => name.to_string(),
    };
    let resolved: Vec<SocketAddr> = (host_str.as_str(), port)
      .to_socket_addrs_async()
      .await?
      .collect();
    Ok(resolved)
  }
}
