use super::*;

macro_rules! conflict_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into()).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_encryption >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr).with_primary_key(Some(TEST_KEYS[0]));
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr).with_compressor(Some(Default::default()));
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into()).await;
        });
      }

      #[cfg(all(feature = "encryption", feature = "compression"))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression_and_encryption >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr).with_primary_key(Some(TEST_KEYS[0])).with_compressor(Some(Default::default()));
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into()).await;
        });
      }
    }
  };
}

test_mods!(conflict_delegate);
