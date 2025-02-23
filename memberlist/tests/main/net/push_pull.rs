use super::*;

macro_rules! push_pull {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _push_pull >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t1 = NetTransport::new(t1_opts).await.unwrap();
          let t2 = NetTransport::new(t2_opts).await.unwrap();
          push_pull::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(
            t1,
            Options::lan(),
            t2,
            Options::lan(),
          ).await;
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
      fn [< test_ $rt:snake _ $kind:snake _push_pull_with_checksum >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t1 = NetTransport::new(t1_opts).await.unwrap();
          let t2 = NetTransport::new(t2_opts).await.unwrap();
          push_pull::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(
            t1,
            Options::lan().with_checksum_algo(Default::default()),
            t2,
            Options::lan().with_checksum_algo(Default::default()),
          ).await;
        });
      }


      #[cfg(any(
        feature = "snappy",
        feature = "lz4",
        feature = "zstd",
        feature = "brotli",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _push_pull_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t1 = NetTransport::new(t1_opts).await.unwrap();
          let t2 = NetTransport::new(t2_opts).await.unwrap();
          push_pull::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(
            t1,
            Options::lan().with_compress_algo(Default::default()),
            t2,
            Options::lan().with_compress_algo(Default::default()),
          ).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _push_pull_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));
          let t1 = NetTransport::new(t1_opts).await.unwrap();
          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let opts1 = Options::lan().with_primary_key(TEST_KEYS[0]).with_secret_keys(TEST_KEYS.into());
          let opts2 = Options::lan().with_primary_key(TEST_KEYS[1]).with_secret_keys(TEST_KEYS.into());
          push_pull::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(
            t1,
            opts1,
            t2,
            opts2,
          ).await;
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
      fn [< test_ $rt:snake _ $kind:snake _push_pull_with_all >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));
          let t1 = NetTransport::new(t1_opts).await.unwrap();

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("push_pull_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let opts1 = Options::lan().with_compress_algo(Default::default()).with_primary_key(TEST_KEYS[0]).with_checksum_algo(Default::default()).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());
          let opts2 = Options::lan().with_compress_algo(Default::default()).with_offload_size(10).with_primary_key(TEST_KEYS[1]).with_checksum_algo(Default::default()).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());

          push_pull::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(
            t1,
            opts1,
            t2,
            opts2,
          ).await;
        });
      }
    }
  };
}

test_mods!(push_pull);
