//! `tokio`-runtime type aliases for the legacy `memberlist-core` resolvers.
//!
//! The legacy `TokioTcpMemberlist` / `TokioTlsMemberlist` /
//! `TokioQuicMemberlist` aliases (built on the frozen `memberlist-net` /
//! `memberlist-quic` transports) were removed in the decoupling.
//! The high-level driver was subsequently removed too (rebuilt next as a
//! super-state-machine); drive `crate::machine::Endpoint` with
//! `crate::codec` directly.

pub use agnostic::tokio::TokioRuntime;

/// [`SocketAddrResolver`](memberlist_core::transport::resolver::socket_addr::SocketAddrResolver) type alias for using `tokio` runtime.
pub type TokioSocketAddrResolver =
  memberlist_core::transport::resolver::socket_addr::SocketAddrResolver<TokioRuntime>;

/// [`HostAddrResolver`](memberlist_core::transport::resolver::address::HostAddrResolver) type alias for using `tokio` runtime.
pub type TokioHostAddrResolver =
  memberlist_core::transport::resolver::address::HostAddrResolver<TokioRuntime>;

/// [`DnsResolver`](memberlist_core::transport::resolver::dns::DnsResolver) type alias for using `tokio` runtime.
#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub type TokioDnsResolver = memberlist_core::transport::resolver::dns::DnsResolver<TokioRuntime>;
