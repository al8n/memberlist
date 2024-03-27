#[macro_export]
macro_rules! __handle_ping_label_only {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, true).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, true).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption_panic >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, false).await {
            assert!(e.to_string().contains("timeout"));
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption_panic >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, false).await {
            assert!(e.to_string().contains("timeout"));
          }
        }),
      ));

      // All of below are fail tests, because if the server is not has label, and the client has label
      // then
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, true).await {
            panic!("{e}");
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, true).await {
            panic!("{e}");
          }
        }),
        [< $($prefix)? _handle_v4_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption_panic >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, false).await {
            assert!(e.to_string().contains("timeout"));
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption_panic >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, false).await {
            assert!(e.to_string().contains("timeout"));
          }
        }),
      ));
    }
  }
}
