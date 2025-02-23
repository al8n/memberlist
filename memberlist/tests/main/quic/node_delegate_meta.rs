use super::*;

macro_rules! node_delegate_meta {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _node_delegate_meta >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_node_delegate_meta::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, opts, t2_opts, Options::lan()).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _node_delegate_meta_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("node_delegate_meta_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_node_delegate_meta::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan().with_compress_algo(Some(Default::default())), t2_opts, Options::lan().with_compress_algo(Some(Default::default()))).await;
        });
      }
    }
  };
}

test_mods!(node_delegate_meta);
