use super::*;

macro_rules! suspect_node_refute {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _suspect_node_refute >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("suspect_node_refute_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          suspect_node_refute(t1, t1_opts).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _suspect_node_refute_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("suspect_node_refute_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          suspect_node_refute(t1, t1_opts).await;
        });
      }
    }
  };
}

test_mods!(suspect_node_refute);
