#[macro_export]
macro_rules! promised_push_pull_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident ({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_promised_push_pull_label_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) = memberlist_quic::tests::promised_push_pull::promised_push_pull_label_only::<$layer<$rt>, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_promised_push_pull_label_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) = memberlist_quic::tests::promised_push_pull::promised_push_pull_label_only::<$layer<$rt>, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v4_promised_push_pull_compression_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) = memberlist_quic::tests::promised_push_pull::promised_push_pull_compression_only::<$layer<$rt>, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v6_promised_push_pull_compression_only >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) = memberlist_quic::tests::promised_push_pull::promised_push_pull_compression_only::<$layer<$rt>, $rt>(s, client, kind).await {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v4_promised_push_pull_label_and_compression >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) =
          memberlist_quic::tests::promised_push_pull::promised_push_pull_label_and_compression::<$layer<$rt>, $rt>(s, client, kind).await
          {
            panic!("{}", e);
          }
        }),
        #[cfg(feature = "compression")]
        [< $($prefix:snake)? _v6_promised_push_pull_label_and_compression >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) =
          memberlist_quic::tests::promised_push_pull::promised_push_pull_label_and_compression::<$layer<$rt>, $rt>(s, client, kind).await
          {
            panic!("{}", e);
          }
        })
      ));

      memberlist_core::unit_tests_with_expr!($run(
        [< $($prefix:snake)? _v4_promised_push_pull_no_label_no_compression >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V4;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) =
          memberlist_quic::tests::promised_push_pull::promised_push_pull_no_label_no_compression::<$layer<$rt>, $rt>(s, client, kind)
              .await
          {
            panic!("{}", e);
          }
        }),
        [< $($prefix:snake)? _v6_promised_push_pull_no_label_no_compression >] ({
          let s = $s;
          let kind = memberlist_core::transport::tests::AddressKind::V6;
          let c = $s;
          let client_addr = kind.next(0);
          let c = <$layer<$rt> as memberlist_quic::stream_layer::StreamLayer>::new(c).await.unwrap();
          let (_, ln, connector) = memberlist_quic::stream_layer::StreamLayer::bind(&c, client_addr).await.unwrap();
          let client = memberlist_quic::tests::QuicTransportTestPromisedClient::new(c, ln, connector);
          if let Err(e) =
          memberlist_quic::tests::promised_push_pull::promised_push_pull_no_label_no_compression::<$layer<$rt>, $rt>(s, client, kind)
              .await
          {
            panic!("{}", e);
          }
        })
      ));
    }
  }
}
