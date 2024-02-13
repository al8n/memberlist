use super::*;

pub async fn server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next())
    .await?
    .with_receive_encrypted(Some(pk));

  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_no_encryption").into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption<
  S,
  R,
>(
  s: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)));

  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption").into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(false);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_no_label_no_compression_with_encryption_client_no_label_no_compression_with_encryption<
  S,
  R,
>(
  s: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)))
    .with_receive_encrypted(Some(pk));

  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_no_label_no_compression_with_encryption_client_no_label_no_compression_with_encryption").into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
