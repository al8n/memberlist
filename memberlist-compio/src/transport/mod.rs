//! `Transport` trait — abstracts a per-backend driver (TCP/TLS/QUIC).
//!
//! Concrete impls live in `src/{tcp,tls,quic}.rs`. The Sans-I/O machine
//! endpoint (`StreamEndpoint<I, SocketAddr, R>` for TCP/TLS;
//! `QuicEndpoint<I>` for QUIC) is built inside `T::run` from the
//! transport's stored config — `Memberlist::new` is generic over `T` and
//! cannot build the backend's private record-layer config + dial closures,
//! so the endpoint never flows through the `TransportRuntime<T, D>` bundle.

use core::future::Future;
use std::net::SocketAddr;

use crate::{
  delegate::Delegate,
  maybe_resolved::MaybeResolved,
  resolver::{AdvertiseAddrResolver, Resolver},
};

pub mod runtime;
pub use runtime::TransportRuntime;

/// Abstracts a per-backend memberlist driver. The trait owns construction,
/// resource ownership (bound sockets / TCP listener / quinn endpoint),
/// the Sans-I/O machine endpoint, and the I/O event loop.
///
/// `Self::Error` is bounded by `From<std::io::Error>` — convertible from
/// the OS layer. No `TransportError` super-trait; the
/// remote-vs-local-failure distinction lives in the state machine's
/// `dial_failed`/suspect/dead transitions.
///
/// `Resolver` and `AdvertiseAddrResolver` are call-site arguments to
/// `Self::new`, NOT associated types — users can swap resolvers without
/// changing the Transport type. The compatibility bound
/// `RES: Resolver<Address = Self::Address>` enforces type alignment.
///
/// `Self::run`'s future is `!Send` — compio is thread-per-core,
/// `!Send`-first. A future driver crate built on a `Send`-required
/// runtime defines its own Transport trait with a `Send` bound.
pub trait Transport: Sized + 'static {
  /// Per-backend error type.
  type Error: core::error::Error + From<std::io::Error> + Send + Sync + 'static;

  /// Node identifier type.
  type Id;

  /// User-facing unresolved address type.
  type Address;

  /// Per-backend transport-knobs block, embedded into `Options<Self>`.
  type Options;

  /// Construct the transport. Resolves `options.advertise_addr` via the
  /// caller-supplied resolvers if it is `MaybeResolved::Unresolved(...)`;
  /// binds the UDP socket / TCP listener / quinn endpoint and stores them.
  fn new<RES, AR>(
    options: Self::Options,
    resolver: &RES,
    advertise_resolver: &AR,
  ) -> impl Future<Output = Result<Self, Self::Error>>
  where
    RES: Resolver<Address = Self::Address>,
    AR: AdvertiseAddrResolver;

  /// Local node identifier.
  fn local_id(&self) -> &Self::Id;

  /// Original advertise input form (`Resolved` if user supplied an
  /// already-resolved SocketAddr; `Unresolved` if user supplied a
  /// hostname that was resolved at construction).
  fn local_address(&self) -> &MaybeResolved<Self::Address, SocketAddr>;

  /// Bound advertise SocketAddr — what the local node gossips to peers
  /// and what the UDP socket is bound to.
  fn advertise_address(&self) -> &SocketAddr;

  /// Run the I/O event loop. Consumes `self` (sockets and listener move
  /// into the loop), the `TransportRuntime<Self, D>` bundle (channels,
  /// snapshot, delegate, tuning knobs), and the gossip RNG `gossip_rng` the node
  /// constructor drew or was handed; the body builds the machine endpoint from
  /// `self`'s stored config, seeding it with `gossip_rng`. Returns when shutdown
  /// is requested.
  fn run<D, G>(self, runtime: TransportRuntime<Self, D>, gossip_rng: G) -> impl Future<Output = ()>
  where
    D: Delegate<Id = Self::Id, Address = SocketAddr>,
    G: rand::Rng + Send + Unpin + 'static;
}

#[cfg(test)]
mod tests;
