use super::*;

macro_rules! ping_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, opts, t2_opts, Options::lan()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_1".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let opts = Options::lan();

          let mut t2_opts = QuicTransportOptions::<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("ping_delegate_node_2".into(), $expr).with_compressor(Some(Default::default())).with_offload_size(10);
          t2_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_ping_delegate::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(t1_opts, opts, t2_opts, Options::lan()).await;
        });
      }
    }
  };
}

test_mods!(ping_delegate);
