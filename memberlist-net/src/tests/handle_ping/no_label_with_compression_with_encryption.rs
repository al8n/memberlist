use super::*;

pub async fn server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_receive_encrypted(Some(pk))
    .with_receive_compressed(true);

  let name = format!("{kind}_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption");
  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption<
  S,
  R,
>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)))
    .with_send_compressed(Some(Compressor::default()));

  let name = format!("{kind}_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_with_encryption");
  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(false);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption<
  S,
  R,
>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)))
    .with_receive_encrypted(Some(pk))
    .with_send_compressed(Some(Compressor::default()))
    .with_receive_compressed(true);

  let name = format!("{kind}_ping_server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption");
  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
