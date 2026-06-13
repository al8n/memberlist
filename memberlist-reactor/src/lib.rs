//! `memberlist-reactor` — reactor-I/O async driver for the Sans-I/O
//! [`memberlist-proto`](memberlist_proto).
//!
//! Drives memberlist-proto's Sans-I/O super-machines — `StreamEndpoint`
//! (TCP / TLS) and `QuicEndpoint` (QUIC, which embeds `quinn-proto`) — on
//! **reactor-pattern** async runtimes (tokio, smol, ...) through the
//! [`agnostic`] runtime abstraction, so one implementation serves every reactor
//! runtime. Each backend runs a quinn-style `Future::poll` pump that owns its
//! machine and the underlying sockets, while a uniform `Memberlist` handle hands
//! commands to it through shared state + a stored waker. It is the reactor
//! sibling of `memberlist-compio` (completion I/O); both share only
//! `memberlist-proto`.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]

mod cidr;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
mod command;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
mod delegate;
mod error;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
mod events;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
mod memberlist;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
mod observation;
mod options;
#[cfg(feature = "quic")]
mod quic_driver;
mod resolver;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
mod shared;
mod snapshot;
#[cfg(any(feature = "tcp", feature = "tls"))]
mod stream_driver;
mod transform;

#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
use rand::{
  SeedableRng,
  rngs::{StdRng, SysRng},
};

/// The driver's machine endpoints stay generic over the gossip RNG `G`, exactly
/// like proto, so a caller can inject their own via the `*_with_rng`
/// constructors. `G` defaults to [`StdRng`] (a ChaCha CSPRNG) — the RNG the
/// plain `tcp`/`tls`/`quic` constructors seed from the OS — so the common
/// 3-parameter spellings stay terse and secure by default.
#[cfg(any(feature = "tcp", feature = "tls"))]
pub(crate) type StreamEndpoint<I, A, R, G = StdRng> =
  memberlist_proto::streams::StreamEndpoint<I, A, R, G>;
#[cfg(feature = "quic")]
pub(crate) type QuicEndpoint<I, G = StdRng> = memberlist_proto::QuicEndpoint<I, G>;

/// A fresh `StdRng` seeded directly from the operating system entropy source
/// ([`SysRng`], i.e. `getrandom`) — never from a thread-local generator, so a
/// process that forks after building a node cannot inherit a parent's RNG state
/// and derive the same gossip schedule. This is the default RNG the plain
/// `tcp`/`tls`/`quic` constructors seed; a caller wanting a different RNG uses
/// the `*_with_rng` constructors instead. The backend constructors return a
/// `Result`, so an OS entropy failure is surfaced as [`Error::Entropy`] rather
/// than panicking.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub(crate) fn gossip_rng() -> Result<StdRng, crate::Error> {
  StdRng::try_from_rng(&mut SysRng).map_err(|e| crate::Error::Entropy(std::io::Error::other(e)))
}

pub use error::Error;
pub use memberlist_proto::LabelError;
pub use options::{Channel, DriverOptions, MemberlistOptions, Options};
pub use resolver::{AddressResolver, MaybeResolved, SocketAddrResolver};
#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
pub use resolver::{LocalAddrResolver, LocalAddrScope, local_advertise};
pub use snapshot::MemberlistSnapshot;

#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use delegate::{Delegate, VoidDelegate};
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use events::EventStream;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use memberlist::Memberlist;
#[cfg(feature = "quic")]
pub use memberlist_proto::QuicOptions;
#[cfg(feature = "tls")]
pub use memberlist_proto::TlsOptions;
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use memberlist_proto::event::Event;
#[cfg(feature = "tcp")]
pub use memberlist_proto::streams::LabelOptions;
#[cfg(feature = "cidr")]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use memberlist_proto::{
  ChecksumAlgorithm, ChecksumOptions, CompressAlgorithm, CompressionOptions, EncryptionOptions,
  Keyring, SecretKey,
};
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use memberlist_proto::{Node, typed::NodeState};

/// The node-identity bound shared across the driver and handle — everything the
/// machine's `Endpoint<I, _>` requires of the identity type `I`. A blanket impl
/// covers every type that satisfies the bounds, so users never implement it.
pub trait NodeId:
  memberlist_proto::Id
  + memberlist_proto::Data
  + memberlist_proto::CheapClone
  + core::fmt::Debug
  + core::fmt::Display
  + Send
  + Sync
  + Unpin
  + 'static
{
}

impl<T> NodeId for T where
  T: memberlist_proto::Id
    + memberlist_proto::Data
    + memberlist_proto::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + Unpin
    + 'static
{
}
