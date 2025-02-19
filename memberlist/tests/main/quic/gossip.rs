use super::*;

macro_rules! gossip {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _gossip >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_3".into(), $expr);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(t3_opts).await.unwrap();

          gossip(
            t1,
            t1_opts,
            t2,
            Options::lan(),
            t3,
            Options::lan(),
          ).await;
        });
      }

      #[test]
      fn [< test_ $rt:snake _ $kind:snake _gossip_with_label >]() {
        [< $rt:snake _run >](async move {
          let label: memberlist::quic::Label = "gossip".try_into().unwrap();
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_1".into(), $expr).with_label(label.clone());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_2".into(), $expr).with_label(label.clone());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_3".into(), $expr).with_label(label);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(t3_opts).await.unwrap();

          gossip(
            t1,
            t1_opts,
            t2,
            Options::lan(),
            t3,
            Options::lan(),
          ).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _gossip_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_2".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_3".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(t3_opts).await.unwrap();

          gossip(
            t1,
            t1_opts,
            t2,
            Options::lan(),
            t3,
            Options::lan(),
          ).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _gossip_with_label_and_compression >]() {
        [< $rt:snake _run >](async move {
          let label: memberlist::quic::Label = "gossip".try_into().unwrap();
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_1".into(), $expr)
            .with_compressor(Some(Default::default()))
            .with_label(label.clone());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_2".into(), $expr)
            .with_compressor(Some(Default::default()))
            .with_label(label.clone());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("gossip_node_3".into(), $expr)
            .with_compressor(Some(Default::default()))
            .with_label(label);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(t3_opts).await.unwrap();

          gossip(
            t1,
            t1_opts,
            t2,
            Options::lan(),
            t3,
            Options::lan(),
          ).await;
        });
      }
    }
  };
}

test_mods!(gossip);
