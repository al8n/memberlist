use super::*;

macro_rules! conflict_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate(|id| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap()
          }, "conflict".into(), $expr).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate(|id| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id).with_compressor(Some(Default::default()));
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            QuicTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap()
          }, "conflict".into(), $expr).await;
        });
      }
    }
  };
}

test_mods!(conflict_delegate);
