use super::*;

macro_rules! create {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _create >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("create_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_create::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _create_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("create_node_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_create::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan()).await;
        });
      }
    }
  };
}

test_mods!(create);
