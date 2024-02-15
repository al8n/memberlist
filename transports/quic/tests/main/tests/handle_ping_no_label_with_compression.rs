#[macro_export]
macro_rules! handle_ping_no_label_with_compression_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    #[cfg(feature = "compression")]
    $crate::__handle_ping_no_label_with_compression!($($prefix: )? $rt::$run({ $s }));
  };
}

#[macro_export]
macro_rules! __handle_ping_no_label_with_compression {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_no_label_with_compression_client_no_label_with_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_with_compression::server_no_label_with_compression_client_no_label_with_compression::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_with_compression_client_no_label_with_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_with_compression::server_no_label_with_compression_client_no_label_with_compression::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V6,
          ).await {
            panic!("{}", e);
          }
        }),
      ));


      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_no_label_no_compression_client_no_label_with_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_with_compression::server_no_label_no_compression_client_no_label_with_compression::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_no_compression_client_no_label_with_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_with_compression::server_no_label_no_compression_client_no_label_with_compression::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V6,
          ).await {
            panic!("{}", e);
          }
        }),
      ));

      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_no_label_with_compression_client_no_label_no_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_with_compression::server_no_label_with_compression_client_no_label_no_compression::<_, $rt>(
            s,
            c,
            memberlist_core::transport::tests::AddressKind::V4,
          ).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_with_compression_client_no_label_no_compression >] ({
          let s = $s;
          let c = $s;
          if let Err(e) = memberlist_quic::tests::handle_ping::no_label_with_compression::server_no_label_with_compression_client_no_label_no_compression::<_, $rt>(
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
