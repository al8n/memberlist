#[macro_export]
macro_rules! join_dead_node_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident ({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_join_dead_node >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next(0);
          let cl = <$layer<$rt> as memberlist_net::stream_layer::StreamLayer>::new(c).await.unwrap();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&cl, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransportTestPromisedClient::new(cl, ln);
          if let Err(e) =
          memberlist_net::tests::join_dead_node::join_dead_node::<$layer<$rt>, $rt>(s, client, kind)
              .await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_join_dead_node >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next(0);
          let cl = <$layer<$rt> as memberlist_net::stream_layer::StreamLayer>::new(c).await.unwrap();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&cl, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransportTestPromisedClient::new(cl, ln);
          if let Err(e) =
          memberlist_net::tests::join_dead_node::join_dead_node::<$layer<$rt>, $rt>(s, client, kind)
              .await
          {
            panic!("{}", e);
          }
        })
      ));
    }
  }
}
