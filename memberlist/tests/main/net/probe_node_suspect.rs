use super::*;

macro_rules! probe_node_suspect {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = NetTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          probe_node_suspect(
            t1,
            Options::lan(),
            t2,
            Options::lan(),
            t3,
            Options::lan(),
            suspect_node,
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
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect_with_checksum >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = NetTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          let opts1 = Options::lan().with_checksum_algo(Some(Default::default())).with_label("test".try_into().unwrap());
          let opts2 = Options::lan().with_checksum_algo(Some(Default::default())).with_offload_size(10).with_label("test".try_into().unwrap());
          let opts3 = Options::lan().with_checksum_algo(Some(Default::default())).with_offload_size(10).with_label("test".try_into().unwrap());

          probe_node_suspect(
            t1,
            opts1,
            t2,
            opts2,
            t3,
            opts3,
            suspect_node,
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
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = NetTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          let opts1 = Options::lan().with_compress_algo(Some(Default::default())).with_label("test".try_into().unwrap());
          let opts2 = Options::lan().with_compress_algo(Some(Default::default())).with_offload_size(10).with_label("test".try_into().unwrap());
          let opts3 = Options::lan().with_compress_algo(Some(Default::default())).with_offload_size(10).with_label("test".try_into().unwrap());

          probe_node_suspect(
            t1,
            opts1,
            t2,
            opts2,
            t3,
            opts3,
            suspect_node,
          ).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = NetTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          let opts1 = Options::lan().with_primary_key(Some(TEST_KEYS[0])).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());
          let opts2 = Options::lan().with_offload_size(10).with_primary_key(Some(TEST_KEYS[1])).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());
          let opts3 = Options::lan().with_offload_size(10).with_primary_key(Some(TEST_KEYS[2])).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());

          probe_node_suspect(
            t1,
            opts1,
            t2,
            opts2,
            t3,
            opts3,
            suspect_node,
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
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect_with_all >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();

          let mut t2_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = NetTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          let opts1 = Options::lan().with_compress_algo(Some(Default::default())).with_primary_key(Some(TEST_KEYS[0])).with_checksum_algo(Some(Default::default())).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());
          let opts2 = Options::lan().with_compress_algo(Some(Default::default())).with_offload_size(10).with_primary_key(Some(TEST_KEYS[1])).with_checksum_algo(Some(Default::default())).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());
          let opts3 = Options::lan().with_compress_algo(Some(Default::default())).with_offload_size(10).with_primary_key(Some(TEST_KEYS[2])).with_checksum_algo(Some(Default::default())).with_secret_keys(TEST_KEYS.into()).with_label("test".try_into().unwrap());

          probe_node_suspect(
            t1,
            opts1,
            t2,
            opts2,
            t3,
            opts3,
            suspect_node,
          ).await;
        });
      }
    }
  };
}

test_mods!(probe_node_suspect);
