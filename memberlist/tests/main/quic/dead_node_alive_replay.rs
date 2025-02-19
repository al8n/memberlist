use super::*;

macro_rules! dead_node_alive_replay {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _dead_node_alive_replay_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("dead_node_alive_replay_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
        });
      }
    }
  };
}

test_mods!(dead_node_alive_replay);
