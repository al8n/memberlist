pub use agnostic::async_std::AsyncStdRuntime;

#[cfg(feature = "net")]
#[cfg_attr(docsrs, doc(cfg(feature = "net")))]
pub use memberlist_net::AsyncStdNetTransport;

#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use memberlist_quic::AsyncStdQuicTransport;

/// [`SocketAddrResolver`](memberlist_core::transport::resolver::socket_addr::SocketAddrResolver) type alias for using `async-std` runtime.
pub type AsyncStdSocketAddrResolver =
  memberlist_core::transport::resolver::socket_addr::SocketAddrResolver<AsyncStdRuntime>;

/// [`HostAddrResolver`](memberlist_core::transport::resolver::address::HostAddrResolver) type alias for using `async-std` runtime.
pub type AsyncStdHostAddrResolver =
  memberlist_core::transport::resolver::address::HostAddrResolver<AsyncStdRuntime>;

/// [`DnsResolver`](memberlist_core::transport::resolver::dns::DnsResolver) type alias for using `async-std` runtime.
#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub type AsyncStdDnsResolver =
  memberlist_core::transport::resolver::dns::DnsResolver<AsyncStdRuntime>;

/// [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) type alias for using `async-std` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type AsyncStdTcp = memberlist_net::stream_layer::tcp::Tcp<AsyncStdRuntime>;

/// [`Tls`](memberlist_net::stream_layer::tls::Tls) type alias for using `async-std` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type AsyncStdTls = memberlist_net::stream_layer::tls::Tls<AsyncStdRuntime>;

/// [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) type alias for using `async-std` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type AsyncStdQuinn = memberlist_quic::stream_layer::quinn::Quinn<AsyncStdRuntime>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) stream layer with `async_std` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type AsyncStdTcpMemberlist<I, A, D> =
  memberlist_core::Memberlist<AsyncStdNetTransport<I, A, AsyncStdTcp>, D>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tls`](memberlist_net::stream_layer::tls::Tls) stream layer with `async_std` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type AsyncStdTlsMemberlist<I, A, D> =
  memberlist_core::Memberlist<AsyncStdNetTransport<I, A, AsyncStdTls>, D>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) stream layer with `async_std` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type AsyncStdQuicMemberlist<I, A, D> =
  memberlist_core::Memberlist<AsyncStdQuicTransport<I, A, AsyncStdQuinn>, D>;
