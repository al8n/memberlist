use super::*;

macro_rules! alive_node_conflict {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_conflict >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("alive_node_conflict_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          alive_node_conflict(t1, t1_opts, "test".into()).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_conflict_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("alive_node_conflict_1".into(), $expr).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          alive_node_conflict(t1, t1_opts, "test".into()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_conflict_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("alive_node_conflict_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          alive_node_conflict(t1, t1_opts, "test".into()).await;
        });
      }

      #[cfg(all(feature = "encryption", feature = "compression"))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_conflict_with_compression_and_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("alive_node_conflict_1".into(), $expr).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10).with_compressor(Some(Default::default()));
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          alive_node_conflict(t1, t1_opts, "test".into()).await;
        });
      }
    }
  };
}

test_mods!(alive_node_conflict);
