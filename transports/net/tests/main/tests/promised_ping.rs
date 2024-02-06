#[macro_export]
macro_rules! promised_ping_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v4_promised_ping >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v6_promised_ping >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v4_promised_ping_no_label >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_no_label::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v6_promised_ping_no_label >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_no_label::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_promised_ping_label_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_label_only::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_promised_ping_label_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_label_only::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v4_promised_ping_compression_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_compression_only::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v6_promised_ping_compression_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_compression_only::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v4_promised_ping_encryption_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_encryption_only::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v6_promised_ping_encryption_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_encryption_only::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v4_promised_ping_label_and_encryption >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_label_and_encryption::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "encryption")]
        [< $($prefix:snake)? _v6_promised_ping_label_and_encryption >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_label_and_encryption::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v4_promised_ping_label_and_compression >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_label_and_compression::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v6_promised_ping_label_and_compression >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) = memberlist_net::tests::promised_ping::promised_ping_label_and_compression::<_, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_promised_ping_no_label_no_compression_no_encryption >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) =
          memberlist_net::tests::promised_ping::promised_ping_no_label_no_compression_no_encryption::<_, $rt>(s, client, kind).await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_promised_ping_no_label_no_compression_no_encryption >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next();
          let ln = memberlist_net::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_net::tests::NetTransporTestPromisedClient::new(client_addr, c, ln);
          if let Err(e) =
          memberlist_net::tests::promised_ping::promised_ping_no_label_no_compression_no_encryption::<_, $rt>(s, client, kind).await
          {
            panic!("{}", e);
          }
        })
      ));
    }
  }
}