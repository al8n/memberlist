pub use agnostic::tokio::TokioRuntime;

#[cfg(feature = "net")]
#[cfg_attr(docsrs, doc(cfg(feature = "net")))]
pub use memberlist_net::TokioNetTransport;

#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use memberlist_quic::TokioQuicTransport;

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

/// [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) type alias for using `tokio` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type TokioTcp = memberlist_net::stream_layer::tcp::Tcp<TokioRuntime>;

/// [`Tls`](memberlist_net::stream_layer::tls::Tls) type alias for using `tokio` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type TokioTls = memberlist_net::stream_layer::tls::Tls<TokioRuntime>;

/// [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) type alias for using `tokio` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type TokioQuinn = memberlist_quic::stream_layer::quinn::Quinn<TokioRuntime>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) stream layer with `tokio` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type TokioTcpMemberlist<I, A, D = memberlist_core::delegate::CompositeDelegate<I, A>> =
  memberlist_core::Memberlist<TokioNetTransport<I, A, TokioTcp>, D>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tls`](memberlist_net::stream_layer::tls::Tls) stream layer with `tokio` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type TokioTlsMemberlist<I, A, D = memberlist_core::delegate::CompositeDelegate<I, A>> =
  memberlist_core::Memberlist<TokioNetTransport<I, A, TokioTls>, D>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) stream layer with `tokio` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type TokioQuicMemberlist<I, A, D = memberlist_core::delegate::CompositeDelegate<I, A>> =
  memberlist_core::Memberlist<TokioQuicTransport<I, A, TokioQuinn>, D>;
