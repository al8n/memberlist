use super::*;

macro_rules! dead_node_alive_replay {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _net_dead_node_alive_replay >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("dead_node_alive_replay_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          dead_node_alive_replay(t1, t1_opts, test_node).await;
        });
      }
    }
  };
}

test_mods!(dead_node_alive_replay);
