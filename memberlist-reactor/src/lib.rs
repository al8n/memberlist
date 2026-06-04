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

pub use error::Error;
pub use memberlist_proto::LabelError;
pub use options::{Channel, DriverOptions, MemberlistOptions, Options};
pub use resolver::{AddressResolver, MaybeResolved, SocketAddrResolver};
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
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub use memberlist_proto::{CompressionOptions, EncryptionOptions, Keyring, SecretKey};
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
