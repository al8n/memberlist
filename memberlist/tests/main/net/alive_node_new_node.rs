use super::*;

macro_rules! alive_node_new_node {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _net_alive_node_new_node >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("alive_node_new_node_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();
          let test_node = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());

          alive_node_new_node(t1, t1_opts, test_node).await;
        });
      }
    }
  };
}

test_mods!(alive_node_new_node);
