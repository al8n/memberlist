use super::*;

macro_rules! shutdown_cleanup {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _shutdown_cleanup >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("shutdown_cleanup_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          memberlist_shutdown_cleanup::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _, _>(t1_opts, |addr| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("shutdown_cleanup_node_1".into(), $expr);
            t1_opts.add_bind_address(addr);
            t1_opts
          }, Options::lan()).await;
        });
      }

      // FIX(al8n)
      // #[test]
      // fn [< test_ $rt:snake _ $kind:snake _shutdown_cleanup2 >]() {
      //   [< $rt:snake _run >](async move {
      //     let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("shutdown_cleanup_node_1".into(), $expr).with_connection_ttl(Some(std::time::Duration::from_millis(20)));
      //     t1_opts.add_bind_address(next_socket_addr_v4(0));

      //     let mut t2_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("shutdown_cleanup_node_2".into(), $expr);
      //     t2_opts.add_bind_address(next_socket_addr_v4(0));

      //     memberlist_shutdown_cleanup2::<QuicTransport<SmolStr, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _, _>(t1_opts, Options::lan(), t2_opts, Options::lan(), |addr| async move {
      //       let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("shutdown_cleanup_node_1".into(), $expr);
      //       t1_opts.add_bind_address(addr);
      //       t1_opts
      //     }).await;
      //   });
      // }
    }
  };
}

test_mods!(shutdown_cleanup);
