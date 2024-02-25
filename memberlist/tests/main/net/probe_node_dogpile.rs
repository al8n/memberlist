use super::*;

macro_rules! probe_node_dogpile {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _probe_node_dogpile >]() {
        [< $rt:snake _run >](async move {

          let bad = Node::new("bad".into(), "127.0.0.1:8000".parse().unwrap());
          probe_node_dogpile(|idx| {
            async move {
              let mut t1_opts = NetTransportOptions::<SmolStr, _>::new(format!("probe_node_dogpile_{idx}").into());
              t1_opts.add_bind_address(next_socket_addr_v4(0));

              NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap()
            }
          }, bad).await;
        });
      }
    }
  };
}

test_mods!(probe_node_dogpile);
