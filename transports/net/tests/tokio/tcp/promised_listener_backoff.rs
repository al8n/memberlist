use super::*;
use memberlist_core::transport::{tests::AddressKind, Lpe};
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::{tests::listener_backoff, NetTransport};
use nodecraft::resolver::socket_addr::SocketAddrResolver;
use smol_str::SmolStr;

unit_tests_with_expr!(run(
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v4_listener_backoff({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    if let Err(e) = listener_backoff::<
      SocketAddrResolver<TokioRuntime>,
      NetTransport<SmolStr, SocketAddrResolver<TokioRuntime>, Tcp<TokioRuntime>, Lpe<_, _>>,
      Tcp<TokioRuntime>,
    >(s, kind)
    .await
    {
      panic!("{}", e);
    }
  }),
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v6_listener_backoff({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    if let Err(e) = listener_backoff::<
      SocketAddrResolver<TokioRuntime>,
      NetTransport<SmolStr, SocketAddrResolver<TokioRuntime>, Tcp<TokioRuntime>, Lpe<_, _>>,
      Tcp<TokioRuntime>,
    >(s, kind)
    .await
    {
      panic!("{}", e);
    }
  })
));
