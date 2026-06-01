//! `smol`-runtime type aliases for the legacy `memberlist-core` resolvers.
//!
//! The legacy `SmolTcpMemberlist` / `SmolTlsMemberlist` /
//! `SmolQuicMemberlist` aliases (built on the frozen `memberlist-net` /
//! `memberlist-quic` transports) were removed in the decoupling.
//! The high-level driver was subsequently removed too (rebuilt next as a
//! super-state-machine); drive `crate::machine::Endpoint` with
//! `crate::codec` directly.

pub use agnostic::smol::SmolRuntime;

/// [`SocketAddrResolver`](memberlist_core::transport::resolver::socket_addr::SocketAddrResolver) type alias for using `smol` runtime.
pub type SmolSocketAddrResolver =
  memberlist_core::transport::resolver::socket_addr::SocketAddrResolver<SmolRuntime>;

/// [`HostAddrResolver`](memberlist_core::transport::resolver::address::HostAddrResolver) type alias for using `smol` runtime.
pub type SmolHostAddrResolver =
  memberlist_core::transport::resolver::address::HostAddrResolver<SmolRuntime>;

/// [`DnsResolver`](memberlist_core::transport::resolver::dns::DnsResolver) type alias for using `smol` runtime.
#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub type SmolDnsResolver = memberlist_core::transport::resolver::dns::DnsResolver<SmolRuntime>;
