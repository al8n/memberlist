//! Address resolution for memberlist-compio.

mod advertise;
mod os;
mod socket_addr;

#[cfg(feature = "dns")]
mod dns;

#[cfg(feature = "getifs")]
mod getifs;

pub use advertise::{
  AdvertiseAddrResolver, AdvertiseResolutionError, FirstAddrResolver, Ipv4PreferringResolver,
  Ipv6PreferringResolver,
};
#[cfg(feature = "dns")]
pub use dns::{DEFAULT_DNS_TIMEOUT, DnsResolver};
#[cfg(feature = "getifs")]
pub use getifs::{LocalAddrResolver, LocalAddrScope, local_advertise};
pub use os::OsResolver;
pub use socket_addr::SocketAddrResolver;

use std::net::SocketAddr;

/// Resolve a user-facing address into one or more concrete [`SocketAddr`]s.
///
/// The input address type is the implementor's choice — [`SocketAddrResolver`]
/// takes [`SocketAddr`] (identity pass-through), the OS and DNS resolvers
/// take [`HostAddr<SmolStr>`](hostaddr::HostAddr). Custom resolvers may
/// take any type (e.g. a service-discovery handle, a `String`-keyed
/// catalogue lookup).
///
/// AFIT (no `async-trait`) — compio is `!Send`-first so the trait has no
/// `Send`/`Sync` bound. Pass an instance per-`join_with` call; the
/// caller owns its lifetime and can reuse across multiple joins.
#[allow(async_fn_in_trait)]
pub trait Resolver: 'static {
  /// The user-facing address type this resolver consumes.
  type Address;

  /// The error type returned by resolution.
  type Error: core::error::Error + 'static;

  /// Resolve `addr` to its concrete socket addresses.
  async fn resolve(&self, addr: &Self::Address) -> Result<Vec<SocketAddr>, Self::Error>;
}
