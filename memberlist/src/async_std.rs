/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) stream layer with `async_std` runtime.
#[cfg(all(
  any(feature = "tcp", feature = "tls", feature = "native-tls"),
  not(target_family = "wasm")
))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(
    any(feature = "tcp", feature = "tls", feature = "native-tls"),
    not(target_family = "wasm")
  )))
)]
pub type AsyncStdTcpMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::tcp::Tcp<agnostic::async_std::AsyncStdRuntime>,
    W,
    agnostic::async_std::AsyncStdRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tls`](memberlist_net::stream_layer::tls::Tls) stream layer with `async_std` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type AsyncStdTlsMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::tls::Tls<agnostic::async_std::AsyncStdRuntime>,
    W,
    agnostic::async_std::AsyncStdRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`NativeTls`](memberlist_net::stream_layer::native_tls::NativeTls) stream layer with `async_std` runtime.
#[cfg(all(feature = "native-tls", not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(feature = "native-tls", not(target_family = "wasm"))))
)]
pub type AsyncStdNativeTlsMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::native_tls::NativeTls<agnostic::async_std::AsyncStdRuntime>,
    W,
    agnostic::async_std::AsyncStdRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) stream layer with `async_std` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type AsyncStdQuicMemberlist<I, A, W, D> = memberlist_core::Memberlist<
  memberlist_quic::QuicTransport<
    I,
    A,
    memberlist_quic::stream_layer::quinn::Quinn<agnostic::async_std::AsyncStdRuntime>,
    W,
    agnostic::async_std::AsyncStdRuntime,
  >,
  D,
>;
