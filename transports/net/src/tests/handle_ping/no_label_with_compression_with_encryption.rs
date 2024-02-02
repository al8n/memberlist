use super::*;

unit_tests_with_expr!(run(
  handle_v4_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v4());
    let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts).await.unwrap();
    let res = handle_ping(trans, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      let unencrypted = read_encrypted_data(pk, &[], src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw));
    opts.add_bind_address(next_socket_addr_v6());
    let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts).await.unwrap();
    let res = handle_ping(trans, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |src| {
      let unencrypted = read_encrypted_data(pk, &[], src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;
    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
));
