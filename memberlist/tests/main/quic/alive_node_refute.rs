use super::*;

macro_rules! alive_node_refute {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_refute >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("alive_node_refute_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          alive_node_refute(t1, t1_opts).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _alive_node_refute_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new("alive_node_refute_1".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          alive_node_refute(t1, t1_opts).await;
        });
      }
    }
  };
}

test_mods!(alive_node_refute);
