#[macro_export]
macro_rules! handle_ping_wrong_node_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[should_panic]
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v4_ping_wrong_node >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping_wrong_node::ping_wrong_node::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        #[should_panic]
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? v6_ping_wrong_node >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping_wrong_node::ping_wrong_node::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        })
      ));
    }
  }
}
