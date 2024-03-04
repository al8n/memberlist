use super::*;

macro_rules! ping {
  ($rt: ident ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_1".into());
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_2".into());
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let bad = Node::new(
            "bad".into(),
            addr,
          );
          ping(t1, t1_opts, t2, bad).await;
        });
      }

      #[cfg(feature = "compression")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_with_compression >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_1".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_2".into()).with_compressor(Some(Default::default())).with_offload_size(10);
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let bad = Node::new(
            "bad".into(),
            addr,
          );
          ping(t1, t1_opts, t2, bad).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_with_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_1".into()).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10);
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_2".into()).with_primary_key(Some(TEST_KEYS[1]));
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let bad = Node::new(
            "bad".into(),
            addr,
          );
          ping(t1, t1_opts, t2, bad).await;
        });
      }

      #[cfg(all(feature = "encryption", feature = "compression"))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _ping_with_compression_and_encryption >]() {
        [< $rt:snake _run >](async move {
          let mut t1_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_1".into()).with_primary_key(Some(TEST_KEYS[0])).with_offload_size(10).with_compressor(Some(Default::default()));
          t1_opts.add_bind_address(next_socket_addr_v4(0));

          let t1 = NetTransport::<_, _, _, Lpe<_, _>, [< $rt:camel Runtime >]>::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t1_opts).await.unwrap();
          let t1_opts = Options::lan();

          let mut t2_opts = NetTransportOptions::<SmolStr, _>::new("ping_node_2".into()).with_primary_key(Some(TEST_KEYS[1])).with_compressor(Some(Default::default()));
          t2_opts.add_bind_address(next_socket_addr_v4(0));
          let t2 = NetTransport::new(SocketAddrResolver::<[< $rt:camel Runtime >]>::new(), $expr, t2_opts).await.unwrap();

          let mut addr = next_socket_addr_v4(0);
          addr.set_port(t1.advertise_address().port());
          let bad = Node::new(
            "bad".into(),
            addr,
          );
          ping(t1, t1_opts, t2, bad).await;
        });
      }
    }
  };
}

test_mods!(ping);
