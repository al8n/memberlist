use super::*;

pub async fn v4_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4())
    .await?
    .with_receive_encrypted(Some(pk));

  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6())
    .await?
    .with_receive_encrypted(Some(pk));
  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v4_server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption<
  S,
  R,
>(
  s: S,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)));

  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(false);
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption<
  S,
  R,
>(
  s: S,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)));

  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(false);
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v4_server_no_label_no_compression_with_encryption_client_no_label_no_compression_with_encryption<
  S,
  R,
>(
  s: S,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)))
    .with_receive_encrypted(Some(pk));

  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_with_encryption".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_no_label_no_compression_with_encryption_client_no_label_no_compression_with_encryption<
  S,
  R,
>(
  s: S,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)))
    .with_receive_encrypted(Some(pk));

  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_with_encryption".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
