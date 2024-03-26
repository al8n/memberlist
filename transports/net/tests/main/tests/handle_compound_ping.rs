#[macro_export]
macro_rules! handle_compound_ping_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix: snake)? _v4_compound_ping >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_compound_ping::compound_ping::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix: snake)? _v6_compound_ping >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_compound_ping::compound_ping::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        })
      ));
    }
  };
}
