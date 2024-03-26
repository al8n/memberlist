#[macro_export]
macro_rules! __handle_indirect_ping {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_indirect_ping_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_indirect_ping::indirect_ping::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_indirect_ping_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_indirect_ping::indirect_ping::<_, $rt>(
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
macro_rules! handle_indirect_ping_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    $crate::__handle_indirect_ping!($($prefix: )? $rt::$run({ $s }));
  };
}
