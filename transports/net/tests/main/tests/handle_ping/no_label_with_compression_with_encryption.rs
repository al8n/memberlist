#[macro_export]
macro_rules! __handle_ping_no_label_compression_and_encryption {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix)? _handle_v4_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v4_ping_server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix)? _handle_v6_ping_server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption::<$layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        }),
      ));
    }
  }
}
