use super::*;

macro_rules! join_with_labels {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _join_with_labels >]() {
        [< $rt:snake _run >](async move {
          memberlist_join_with_labels(|idx, label| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(format!("join_with_labels_node_{idx}").into()).
              with_label(label);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            NetTransport::<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(t1_opts).await.unwrap()
          }).await;
        });
      }
    }
  };
}

test_mods!(join_with_labels);
