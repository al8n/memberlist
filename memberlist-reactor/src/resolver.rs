//! Address resolution: the [`AddressResolver`] trait, the [`MaybeResolved`]
//! seed/advertise form, and the trivial [`SocketAddrResolver`].
//!
//! Resolution happens only at the boundary — the bootstrap constructor (the
//! local advertise address) and `join` (the seeds) — so a resolver is passed to
//! those methods rather than stored on the `Memberlist`. Everything past the
//! boundary is a wire [`SocketAddr`]; the cluster only ever gossips resolved
//! addresses.

use std::{future::Future, net::SocketAddr};

/// Resolves an unresolved address (e.g. a `host:port` domain name) into wire
/// [`SocketAddr`]s.
///
/// Invoked only at the boundary (bootstrap + `join`). `resolve` returns zero or
/// more candidates — a name may map to several A/AAAA records. The returned
/// future is `Send` so resolution can run on a multi-threaded runtime; it is
/// written `-> impl Future + Send` rather than `async fn` so the `Send` bound is
/// part of the trait contract.
pub trait AddressResolver: Send + Sync + 'static {
  /// The unresolved address this resolver accepts.
  type Address: Send + Sync + 'static;

  /// The error returned when resolution fails.
  type Error: core::error::Error + Send + Sync + 'static;

  /// Resolves `address` into zero or more candidate wire addresses.
  fn resolve(
    &self,
    address: &Self::Address,
  ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_;
}

/// A seed or advertise address that is either an already-resolved wire
/// [`SocketAddr`] or an unresolved address to pass through an
/// [`AddressResolver`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MaybeResolved<A> {
  /// An already-resolved wire address — used verbatim, no resolver needed.
  Resolved(SocketAddr),
  /// An unresolved address, resolved at the boundary.
  Unresolved(A),
}

/// An [`AddressResolver`] for callers that already hold wire [`SocketAddr`]s: it
/// passes each address through unchanged and never fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct SocketAddrResolver;

impl AddressResolver for SocketAddrResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;

  fn resolve(
    &self,
    address: &SocketAddr,
  ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
    let addr = *address;
    async move { Ok(vec![addr]) }
  }
}
