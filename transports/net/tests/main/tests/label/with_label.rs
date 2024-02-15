#[macro_export]
macro_rules! __label_only {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_server_with_label_client_with_label>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label::server_with_label_client_with_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_server_with_label_client_with_label>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label::server_with_label_client_with_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await
          {
            panic!("{}", e);
          }
        }),
      ));

      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_server_with_label_client_no_label>] ({
          let s = $s;
          if let Err(e) =
          memberlist_net::tests::label::with_label::server_with_label_client_no_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, true).await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_server_with_label_client_no_label>] ({
          let s = $s;
          if let Err(e) =
          memberlist_net::tests::label::with_label::server_with_label_client_no_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, true).await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v4_server_with_label_client_no_label_panic>] ({
          let s = $s;
          let res =
          memberlist_net::tests::label::with_label::server_with_label_client_no_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, false).await;
          assert!(res.unwrap_err().to_string().contains("timeout"));
        }),
        [< $($prefix:snake)? _v6_server_with_label_client_no_label_panic>] ({
          let s = $s;
          let res =
          memberlist_net::tests::label::with_label::server_with_label_client_no_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, false).await;
          assert!(res.unwrap_err().to_string().contains("timeout"));
        }),
      ));

      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_server_no_label_client_with_label>] ({
          let s = $s;
          if let Err(e) =
          memberlist_net::tests::label::with_label::server_no_label_client_with_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, true).await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_server_no_label_client_with_label>] ({
          let s = $s;
          if let Err(e) =
          memberlist_net::tests::label::with_label::server_no_label_client_with_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, true).await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v4_server_no_label_client_with_label_panic>] ({
          let s = $s;
          let res =
          memberlist_net::tests::label::with_label::server_no_label_client_with_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V4, false).await;
          assert!(res.unwrap_err().to_string().contains("timeout"));
        }),
        [< $($prefix:snake)? _v6_server_no_label_client_with_label_panic>] ({
          let s = $s;
          let res =
          memberlist_net::tests::label::with_label::server_no_label_client_with_label::<_, $rt>(s, memberlist_core::transport::tests::AddressKind::V6, false).await;
          assert!(res.unwrap_err().to_string().contains("timeout"));
        }),
      ));
    }
  }
}
