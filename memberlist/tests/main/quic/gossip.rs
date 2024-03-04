use super::*;

macro_rules! gossip {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _gossip >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_2".into());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_3".into());
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t3_opts).await.unwrap();

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
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_1".into()).with_label(label.clone());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_2".into()).with_label(label.clone());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_3".into()).with_label(label);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t3_opts).await.unwrap();

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
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_1".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_2".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_3".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t3_opts).await.unwrap();

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
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_1".into())
            .with_compressor(Some(Default::default()))
            .with_label(label.clone());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_2".into())
            .with_compressor(Some(Default::default()))
            .with_label(label.clone());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut t3_opts = QuicTransportOptions::<SmolStr, _>::new("gossip_node_3".into())
            .with_compressor(Some(Default::default()))
            .with_label(label);
          t3_opts.add_bind_address(next_socket_addr_v4(0));
          let t3 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t3_opts).await.unwrap();

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
