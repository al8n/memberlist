use super::*;

macro_rules! conflict_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));
            t1_opts
          }, "conflict".into(), Options::lan()).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into(), Options::lan().with_compress_algo(Default::default())).await;
        });
      }
    }
  };
}

test_mods!(conflict_delegate);
