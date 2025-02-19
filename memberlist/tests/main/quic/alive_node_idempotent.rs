use super::*;

macro_rules! alive_node_idempotent {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_idempotent >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("alive_node_idempotent_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          alive_node_idempotent(t1, t1_opts, test_node).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_idempotent_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("alive_node_idempotent_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let test_node: Node<SmolStr, std::net::SocketAddr> = Node::new("test".into(), "127.0.0.1:8000".parse().unwrap());
          alive_node_idempotent(t1, t1_opts, test_node).await;
        });
      }
    }
  };
}

test_mods!(alive_node_idempotent);
