use super::*;

macro_rules! reset_nodes {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _reset_nodes >]() {
        use std::net::SocketAddr;

        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("reset_nodes_node_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut addr: SocketAddr = "127.0.0.1:7969".parse().unwrap();
          addr.set_port(t1.advertise_address().port());
          let n1 = Node::new(
            "node1".into(),
            addr,
          );

          let mut addr: SocketAddr = "127.0.0.2:7969".parse().unwrap();
          addr.set_port(t1.advertise_address().port());
          let n2 = Node::new(
            "node2".into(),
            addr,
          );

          let mut addr: SocketAddr = "127.0.0.3:7969".parse().unwrap();
          addr.set_port(t1.advertise_address().port());
          let n3 = Node::new(
            "node3".into(),
            addr,
          );

          reset_nodes(t1, t1_opts, n1, n2, n3).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _reset_nodes_with_compression >]() {
        use std::net::SocketAddr;

        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("reset_nodes_node_1".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut addr: SocketAddr = "127.0.0.1:7969".parse().unwrap();
          addr.set_port(t1.advertise_address().port());
          let n1 = Node::new(
            "node1".into(),
            addr,
          );

          let mut addr: SocketAddr = "127.0.0.2:7969".parse().unwrap();
          addr.set_port(t1.advertise_address().port());
          let n2 = Node::new(
            "node2".into(),
            addr,
          );

          let mut addr: SocketAddr = "127.0.0.3:7969".parse().unwrap();
          addr.set_port(t1.advertise_address().port());
          let n3 = Node::new(
            "node3".into(),
            addr,
          );

          reset_nodes(t1, t1_opts, n1, n2, n3).await;
        });
      }
    }
  };
}

test_mods!(reset_nodes);
