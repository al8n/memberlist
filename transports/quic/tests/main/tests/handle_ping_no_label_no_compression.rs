#[macro_export]
macro_rules! handle_ping_no_label_no_compression_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    $crate::__handle_ping_no_label_no_compression!($($prefix: )? $layer<$rt>::$run({ $s }));
  };
}

#[macro_export]
macro_rules! __handle_ping_no_label_no_compression {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_no_label_no_compression_client_no_label_no_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_no_compression::server_no_label_no_compression_client_no_label_no_compression::<$layer<$rt>, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_no_compression_client_no_label_no_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_no_compression::server_no_label_no_compression_client_no_label_no_compression::<$layer<$rt>, $rt>(
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
