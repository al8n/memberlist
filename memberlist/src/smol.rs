/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tcp`](memberlist_net::stream_layer::tcp::Tcp) stream layer with `smol` runtime.
#[cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm")))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(any(feature = "tcp", feature = "tls"), not(target_family = "wasm"))))
)]
pub type SmolTcpMemberlist<I, A, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::tcp::Tcp<agnostic::smol::SmolRuntime>,
    agnostic::smol::SmolRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`NetTransport`](memberlist_net::NetTransport) and [`Tls`](memberlist_net::stream_layer::tls::Tls) stream layer with `smol` runtime.
#[cfg(all(feature = "tls", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "tls", not(target_family = "wasm")))))]
pub type SmolTlsMemberlist<I, A, D> = memberlist_core::Memberlist<
  memberlist_net::NetTransport<
    I,
    A,
    memberlist_net::stream_layer::tls::Tls<agnostic::smol::SmolRuntime>,
    agnostic::smol::SmolRuntime,
  >,
  D,
>;

/// Memberlist type alias for using [`QuicTransport`](memberlist_quic::QuicTransport) and [`Quinn`](memberlist_quic::stream_layer::quinn::Quinn) stream layer with `smol` runtime.
#[cfg(all(feature = "quinn", not(target_family = "wasm")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quinn", not(target_family = "wasm")))))]
pub type SmolQuicMemberlist<I, A, D> = memberlist_core::Memberlist<
  memberlist_quic::QuicTransport<
    I,
    A,
    memberlist_quic::stream_layer::quinn::Quinn<agnostic::smol::SmolRuntime>,
    agnostic::smol::SmolRuntime,
  >,
  D,
>;
