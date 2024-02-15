#[macro_export]
macro_rules! __handle_join {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_join_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::join::join::<_, _, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_join_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::join::join::<_, _, $rt>(
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
macro_rules! handle_join_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    $crate::__handle_join!($($prefix: )? $rt::$run({ $s }));
  };
}
