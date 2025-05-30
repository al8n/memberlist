use super::*;

macro_rules! ping_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan(), t2_opts, Options::lan()).await;
        });
      }

      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate_with_checksum >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan().with_checksum_algo(Default::default()).with_offload_size(10), t2_opts, Options::lan().with_checksum_algo(Default::default()).with_offload_size(10)).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "lz4",
        feature = "zstd",
        feature = "brotli",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan().with_compress_algo(Default::default()).with_offload_size(10), t2_opts, Options::lan().with_compress_algo(Default::default()).with_offload_size(10)).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan().with_primary_key(TEST_KEYS[0]).with_offload_size(10), t2_opts, Options::lan().with_primary_key(TEST_KEYS[1])).await;
        });
      }

      #[cfg(all(
        feature = "encryption",
        any(
          feature = "snappy",
          feature = "lz4",
          feature = "zstd",
          feature = "brotli",
        ),
        any(
          feature = "crc32",
          feature = "xxhash32",
          feature = "xxhash64",
          feature = "xxhash3",
          feature = "murmur3",
        )
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate_with_all >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          let opts1 = Options::lan().with_compress_algo(Default::default()).with_offload_size(10).with_primary_key(TEST_KEYS[0]).with_checksum_algo(Default::default());

          let opts2 = Options::lan().with_compress_algo(Default::default()).with_offload_size(10).with_primary_key(TEST_KEYS[0]).with_checksum_algo(Default::default());


          memberlist_ping_delegate::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, opts1, t2_opts, opts2).await;
        });
      }
    }
  };
}

test_mods!(ping_delegate);
