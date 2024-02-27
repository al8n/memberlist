use super::*;

macro_rules! conflict_delegate {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate(|id| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new(id);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap()
          }, "conflict".into()).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate(|id| async move {
            let mut t1_opts = QuicTransportOptions::<SmolStr, _>::new(id).with_compressor(Some(Default::default()));
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            QuicTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap()
          }, "conflict".into()).await;
        });
      }
    }
  };
}

test_mods!(conflict_delegate);