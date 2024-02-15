#[macro_export]
macro_rules! __handle_piggyback {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_piggyback_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::packet_piggyback::packet_piggyback::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_piggyback_with_label_and_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::packet_piggyback::packet_piggyback::<_, $rt>(
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
macro_rules! handle_piggyback_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    $crate::__handle_piggyback!($($prefix: )? $rt::$run({ $s }));
  };
}
