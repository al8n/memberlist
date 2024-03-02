use super::*;

macro_rules! probe_node_awareness_missed_nack {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_awareness_missed_nack >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("probe_node_awareness_missed_nack_node_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _>::new("probe_node_awareness_missed_nack_node_2".into());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();
          let t2_opts = Options::lan();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let node3 = Node::new(
            "probe_node_awareness_missed_nack_node_3".into(),
            addr,
          );


          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let node4 = Node::new(
            "probe_node_awareness_missed_nack_node_4".into(),
            addr,
          );

          probe_node_awareness_missed_nack(
            t1,
            t1_opts,
            t2,
            t2_opts,
            node3,
            node4,
          ).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_awareness_missed_nack_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("probe_node_awareness_missed_nack_node_1".into()).with_compressor(Some(Default::default()));
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, _>::new("probe_node_awareness_missed_nack_node_2".into()).with_compressor(Some(Default::default()));
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = QuicTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();
          let t2_opts = Options::lan();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let node3 = Node::new(
            "probe_node_awareness_missed_nack_node_3".into(),
            addr,
          );


          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let node4 = Node::new(
            "probe_node_awareness_missed_nack_node_4".into(),
            addr,
          );

          probe_node_awareness_missed_nack(
            t1,
            t1_opts,
            t2,
            t2_opts,
            node3,
            node4,
          ).await;
        });
      }
    }
  };
}

test_mods!(probe_node_awareness_missed_nack(
  std::time::Duration::from_millis(50)
));
