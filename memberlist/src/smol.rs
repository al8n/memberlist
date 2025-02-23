pub use agnostic::smol::SmolRuntime;

#[cfg(feature = "net")]
#[cfg_attr(docsrs, doc(cfg(feature = "net")))]
pub use memberlist_net::SmolNetTransport;

#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use memberlist_quic::SmolQuicTransport;

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

/// [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) type alias for using `smol` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type SmolTcp = memberlist_net::stream_layer::tcp::Tcp<SmolRuntime>;

/// [`Tls`](memberlist_net::stream_layer::tls::Tls) type alias for using `smol` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type SmolTls = memberlist_net::stream_layer::tls::Tls<SmolRuntime>;

/// [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) type alias for using `smol` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type SmolQuinn = memberlist_quic::stream_layer::quinn::Quinn<SmolRuntime>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) stream layer with `smol` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type SmolTcpMemberlist<I, A, D> =
  memberlist_core::Memberlist<SmolNetTransport<I, A, SmolTcp>, D>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tls`](memberlist_net::stream_layer::tls::Tls) stream layer with `smol` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type SmolTlsMemberlist<I, A, D> =
  memberlist_core::Memberlist<SmolNetTransport<I, A, SmolTls>, D>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) stream layer with `smol` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type SmolQuicMemberlist<I, A, D> =
  memberlist_core::Memberlist<SmolQuicTransport<I, A, SmolQuinn>, D>;
