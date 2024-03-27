use super::*;

macro_rules! join_cancel_passive {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_cancel_passive >]() {
        [< $rt:snake _run >](async move {
          let id1: SmolStr = "join_cancel_passive_node_1".into();
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id1.clone(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let id2: SmolStr = "join_cancel_passive_node_2".into();
          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id2.clone(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_join_cancel_passive::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>, _>(id1, t1_opts, Options::lan(), id2, t2_opts, Options::lan()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_cancel_passive_with_compression >]() {
        [< $rt:snake _run >](async move {
          let id1: SmolStr = "join_cancel_passive_node_1".into();
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id1.clone(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let id2: SmolStr = "join_cancel_passive_node_2".into();
          let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id2.clone(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_join_cancel_passive::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>, _>(id1, t1_opts, Options::lan(), id2, t2_opts, Options::lan()).await;
        });
      }
    }
  };
}

test_mods!(join_cancel_passive);
