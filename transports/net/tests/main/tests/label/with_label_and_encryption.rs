#[macro_export]
macro_rules! __label_and_encryption {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v4_with_label_with_encryption_client_with_label_with_encryption>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label_and_encryption::server_with_label_with_encryption_client_with_label_with_encryption::<
            _,
            $rt,
          >(s, memberlist_core::transport::tests::AddressKind::V4)
          .await
          {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v6_with_label_with_encryption_client_with_label_with_encryption>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label_and_encryption::server_with_label_with_encryption_client_with_label_with_encryption::<
            _,
            $rt,
          >(s, memberlist_core::transport::tests::AddressKind::V6)
          .await
          {
            panic!("{}", e);
          }
        }),
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v4_with_label_with_encryption_client_with_label_no_encryption>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label_and_encryption::server_with_label_with_encryption_client_with_label_no_encryption::<
            _,
            $rt,
          >(s, memberlist_core::transport::tests::AddressKind::V4)
          .await
          {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v6_with_label_client_no_label_and_encryption>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label_and_encryption::server_with_label_with_encryption_client_with_label_no_encryption::<
            _,
            $rt,
          >(s, memberlist_core::transport::tests::AddressKind::V6)
          .await
          {
            panic!("{}", e);
          }
        }),
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v4_with_label_no_encryption_client_with_label_with_encryption>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label_and_encryption::server_with_label_no_encryption_client_with_label_with_encryption::<
            _,
            $rt,
          >(s, memberlist_core::transport::tests::AddressKind::V4)
          .await
          {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v6_with_label_no_encryption_client_with_label_with_encryption>] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::label::with_label_and_encryption::server_with_label_no_encryption_client_with_label_with_encryption::<
            _,
            $rt,
          >(s, memberlist_core::transport::tests::AddressKind::V6)
          .await
          {
            panic!("{}", e);
          }
        }),
      ));
    }
  }
}
