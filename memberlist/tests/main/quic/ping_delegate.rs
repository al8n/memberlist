use super::*;

macro_rules! ping_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, opts, t2_opts, Options::lan()).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan().with_compress_algo(Some(Default::default())), t2_opts, Options::lan().with_compress_algo(Some(Default::default()))).await;
        });
      }
    }
  };
}

test_mods!(ping_delegate);
