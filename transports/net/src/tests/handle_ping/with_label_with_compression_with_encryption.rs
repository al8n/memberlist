use super::*;

unit_tests_with_expr!(run(
  handle_v4_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v4_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw))
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);
    opts.add_bind_address(next_socket_addr_v4());
    let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts).await.unwrap();
    let res = handle_ping(trans, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      let unencrypted = read_encrypted_data(pk, received_label.as_bytes(), src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v4()).await;

    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
  handle_v6_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let label = Label::from_static("test_handle_v6_ping_server_with_label_with_compression_no_encryption_client_no_label_no_compression_no_encryption");
    let pk = SecretKey::from([1; 32]);
    let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_with_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
      .with_primary_key(Some(pk))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_compressor(Some(Compressor::Lzw))
      .with_label(label.cheap_clone())
      .with_skip_inbound_label_check(true);

    opts.add_bind_address(next_socket_addr_v6());
    let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<TokioRuntime>::new(), Tcp::<TokioRuntime>::new(), opts).await.unwrap();
    let res = handle_ping(trans, |p| {
      Message::ping(p).encode_to_vec().unwrap()
    }, |mut src| {
      let received_label = read_label(src)?;
      assert_eq!(received_label, label);
      src = &src[label.encoded_overhead()..];
      let unencrypted = read_encrypted_data(pk, received_label.as_bytes(), src)?;
      verify_checksum(&unencrypted)?;
      let uncompressed = read_compressed_data(&unencrypted[5..])?;
      Message::<SmolStr, SocketAddr>::decode(&uncompressed).map(|(_, msg)| msg.unwrap_ack()).map_err(Into::into)
    }, next_socket_addr_v6()).await;
    if let Err(e) = res {
      panic!("{:?}", e);
    }
  }),
));
