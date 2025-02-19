#[macro_export]
macro_rules! promised_listener_backoff_test_suites {
  ($($prefix:literal: )? $layer:ident <$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v4_listener_backoff >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          if let Err(e) = memberlist_net::tests::listener_backoff::<
            memberlist_net::resolver::socket_addr::SocketAddrResolver<$rt>,
            memberlist_net::NetTransport<smol_str::SmolStr, memberlist_net::resolver::socket_addr::SocketAddrResolver<$rt>, $layer<$rt>, memberlist_core::transport::$rt>,
            $layer<$rt>,
          >(<$layer<$rt> as memberlist_net::stream_layer::StreamLayer>::new(s).await.unwrap(), kind)
          .await
          {
            panic!("{}", e);
          }
        }),
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v6_listener_backoff >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          if let Err(e) = memberlist_net::tests::listener_backoff::<
            memberlist_net::resolver::socket_addr::SocketAddrResolver<$rt>,
            memberlist_net::NetTransport<smol_str::SmolStr, memberlist_net::resolver::socket_addr::SocketAddrResolver<$rt>, $layer<$rt>, memberlist_core::transport::$rt>,
            $layer<$rt>,
          >(<$layer<$rt> as memberlist_net::stream_layer::StreamLayer>::new(s).await.unwrap(), kind)
          .await
          {
            panic!("{}", e);
          }
        })
      ));
    }
  };
}
