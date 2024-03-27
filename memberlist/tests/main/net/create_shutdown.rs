use super::*;

macro_rules! create_shutdown {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _create_shutdown >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options("create_shutdown_node_1".into(), $expr);
          t1_opts.add_bind_address(next_socket_addr_v4(0));


          memberlist_create_shutdown::<NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>, _>(t1_opts, Options::lan()).await;
        });
      }
    }
  };
}

test_mods!(create_shutdown);
