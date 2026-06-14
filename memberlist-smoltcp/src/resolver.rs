//! Synchronous address resolution: the [`Resolver`] trait and the trivial
//! [`SocketAddrResolver`].
//!
//! Resolution happens only at the driver boundary — construction (the local
//! advertise address) and [`join`](crate::Memberlist::join) (the seeds) — so a
//! resolver is passed to those methods rather than stored on the
//! [`Memberlist`](crate::Memberlist). Everything past the boundary is a wire
//! [`SocketAddr`]; the embedded engine only ever sees resolved addresses.

use core::net::SocketAddr;

/// Synchronously resolves an unresolved address (e.g. a `host:port` name) into
/// candidate wire [`SocketAddr`]s.
///
/// Synchronous — and free of `Send`/`Sync`/`'static` bounds — because smoltcp is
/// a caller-poll driver with no async runtime and a single-threaded stack.
/// `resolve` returns zero or more candidates, since one name may map to several
/// A/AAAA records.
pub trait Resolver {
  /// The unresolved address this resolver accepts.
  type Address;

  /// The error returned when resolution fails. `'static` so the driver can box
  /// it into a typed [`InitError`](crate::InitError) / [`JoinError`](crate::JoinError)
  /// while preserving the `source()` chain.
  type Error: core::error::Error + 'static;

  /// Resolves `address` into zero or more candidate wire addresses.
  fn resolve(
    &self,
    address: &Self::Address,
  ) -> Result<impl Iterator<Item = SocketAddr>, Self::Error>;
}

/// A [`Resolver`] for callers that already hold wire [`SocketAddr`]s: it passes
/// each address through unchanged and never fails.
#[derive(Debug, Clone, Copy, Default)]
pub struct SocketAddrResolver;

impl Resolver for SocketAddrResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;

  fn resolve(&self, address: &SocketAddr) -> Result<impl Iterator<Item = SocketAddr>, Self::Error> {
    Ok(core::iter::once(*address))
  }
}
