use super::*;

macro_rules! probe_node_suspect {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          probe_node_suspect(
            t1,
            t1_opts,
            t2,
            Options::lan(),
            t3,
            Options::lan(),
            suspect_node,
          ).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_suspect_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("probe_node_suspect_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(t3_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let suspect_node = Node::new(
            "probe_node_suspect_node_4".into(),
            addr,
          );

          probe_node_suspect(
            t1,
            Options::lan().with_compress_algo(Some(Default::default())),
            t2,
            Options::lan().with_compress_algo(Some(Default::default())),
            t3,
            Options::lan().with_compress_algo(Some(Default::default())),
            suspect_node,
          ).await;
        });
      }
    }
  };
}

test_mods!(probe_node_suspect);
