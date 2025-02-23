use super::*;

macro_rules! conflict_delegate {
  ($layer:ident<$rt: ident> ($kind:literal, $expr: expr)) => {
    paste::paste! {
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into(), Options::lan()).await;
        });
      }

      #[cfg(feature = "encryption")]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_encryption >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into(), Options::lan().with_primary_key(Some(TEST_KEYS[0]))).await;
        });
      }

      #[cfg(any(
        feature = "snappy",
        feature = "brotli",
        feature = "zstd",
        feature = "lz4",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into(), Options::lan().with_compress_algo(Some(Default::default()))).await;
        });
      }

      #[cfg(any(
        feature = "crc32",
        feature = "xxhash32",
        feature = "xxhash64",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_checksum >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into(), Options::lan().with_checksum_algo(Some(Default::default()))).await;
        });
      }

      #[cfg(all(
        feature = "encryption",
        any(
          feature = "snappy",
          feature = "lz4",
          feature = "zstd",
          feature = "brotli",
        ),
        any(
          feature = "crc32",
          feature = "xxhash32",
          feature = "xxhash64",
          feature = "xxhash3",
          feature = "murmur3",
        )
      ))]
      #[test]
      fn [< test_ $rt:snake _ $kind:snake _conflict_delegate_with_compression_and_encryption >]() {
        [< $rt:snake _run >](async move {
          memberlist_conflict_delegate::<_, NetTransport<_, SocketAddrResolver<[< $rt:camel Runtime >]>, _, [< $rt:camel Runtime >]>, _>(|id| async move {
            let mut t1_opts = NetTransportOptions::<SmolStr, _, $layer<[< $rt:camel Runtime >]>>::with_stream_layer_options(id, $expr);
            t1_opts.add_bind_address(next_socket_addr_v4(0));

            t1_opts
          }, "conflict".into(), Options::lan()
            .with_primary_key(Some(TEST_KEYS[0]))
            .with_checksum_algo(Some(Default::default()))
            .with_compress_algo(Some(Default::default()))).await;
        });
      }
    }
  };
}

test_mods!(conflict_delegate);
