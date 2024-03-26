#[macro_export]
macro_rules! __handle_join_dead_node {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_join_dead_node_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          let client_addr = memberlist_core::transport::tests::AddressKind::V4.next(0);
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          memberlist_quic::tests::join_dead_node::join_dead_node::<_, $rt>(
            s,
            client,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await.unwrap()
        }),
        [< $($prefix)? _handle_v6_join_dead_node_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          let client_addr = memberlist_core::transport::tests::AddressKind::V6.next(0);
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          memberlist_quic::tests::join_dead_node::join_dead_node::<_, $rt>(
            s,
            client,
            memberlist_core::transport::tests::AddressKind::V6,
          ).await.unwrap()
        }),
      ));
    }
  };
}

#[macro_export]
macro_rules! handle_join_dead_node_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    $crate::__handle_join_dead_node!($($prefix: )? $rt::$run({ $s }));
  };
}
