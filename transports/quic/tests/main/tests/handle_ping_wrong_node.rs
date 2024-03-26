#[macro_export]
macro_rules! __handle_ping_wrong_node {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_wrong_node_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping_wrong_node::ping_wrong_node::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_wrong_node_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping_wrong_node::ping_wrong_node::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V6,
          ).await {
            panic!("{}", e);
          }
        }),
      ));
    }
  };
}

#[macro_export]
macro_rules! handle_ping_wrong_node_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    $crate::__handle_ping_wrong_node!($($prefix: )? $rt::$run({ $s }));
  };
}
