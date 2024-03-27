#[macro_export]
macro_rules! join_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v4_join >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          if let Err(e) = memberlist_net::tests::join::join::<$layer<$rt>, $layer<$rt>, $rt>(s, c, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v6_join >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          if let Err(e) = memberlist_net::tests::join::join::<$layer<$rt>, $layer<$rt>, $rt>(s, c, kind).await {
            panic!("{}", e);
          }
        })
      ));
    }
  };
}
