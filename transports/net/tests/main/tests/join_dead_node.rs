#[macro_export]
macro_rules! join_dead_node_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident ({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_join_dead_node >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next(0);
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransportTestPromisedClient::new(c, ln);
          if let Err(e) =
          memberlist_net::tests::join_dead_node::join_dead_node::<_, $rt>(s, client, kind)
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
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransportTestPromisedClient::new(c, ln);
          if let Err(e) =
          memberlist_net::tests::join_dead_node::join_dead_node::<_, $rt>(s, client, kind)
              .await
          {
            panic!("{}", e);
          }
        })
      ));
    }
  }
}
