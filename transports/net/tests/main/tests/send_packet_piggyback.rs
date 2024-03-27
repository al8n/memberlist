#[macro_export]
macro_rules! send_packet_piggyback_test_suites {
  ($($prefix:literal: )? $stream_layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    paste::paste! {
      memberlist_core::unit_tests_with_expr!($run(
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v4_packet_piggyback >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::packet_piggyback::packet_piggyback::<$stream_layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V4).await {
            panic!("{}", e);
          }
        }),
        #[cfg(all(feature = "encryption", feature = "compression"))]
        [< $($prefix:snake)? _v6_packet_piggyback >] ({
          let s = $s;
          if let Err(e) = memberlist_net::tests::packet_piggyback::packet_piggyback::<$stream_layer<$rt>, $rt>(s, memberlist_core::transport::tests::AddressKind::V6).await {
            panic!("{}", e);
          }
        })
      ));
    }
  };
}
