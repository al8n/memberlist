use super::*;

macro_rules! dead_node_alive_replay {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
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
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay_with_checksum >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan().with_checksum_algo(Default::default()).with_offload_size(10);

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan().with_compress_algo(Default::default()).with_offload_size(10);

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan().with_primary_key(TEST_KEYS[0]).with_offload_size(10);

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
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
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay_with_compression_and_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan().with_primary_key(TEST_KEYS[0]).with_offload_size(10).with_compress_algo(Default::default()).with_checksum_algo(Default::default());

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
        });
      }
    }
  };
}

test_mods!(dead_node_alive_replay);
