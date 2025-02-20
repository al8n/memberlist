use super::*;

macro_rules! join_cancel_passive {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_cancel_passive >]() {
        [< $rt:snake _run >](async move {
          let id1: SmolStr = "join_cancel_passive_node_1".into();
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id1.clone(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let id2: SmolStr = "join_cancel_passive_node_2".into();
          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id2.clone(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_join_cancel_passive::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(id1, t1_opts, Options::lan(), id2, t2_opts, Options::lan()).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_cancel_passive_with_compression >]() {
        [< $rt:snake _run >](async move {
          let id1: SmolStr = "join_cancel_passive_node_1".into();
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id1.clone(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let id2: SmolStr = "join_cancel_passive_node_2".into();
          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id2.clone(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_join_cancel_passive::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(id1, t1_opts, Options::lan().with_compress_algo(Some(Default::default())), id2, t2_opts, Options::lan().with_compress_algo(Some(Default::default()))).await;
        });
      }
    }
  };
}

test_mods!(join_cancel_passive);
