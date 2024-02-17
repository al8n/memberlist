use super::*;

#[cfg(feature = "encryption")]
pub async fn server_with_label_with_encryption_client_with_label_with_encryption<S, R>(
  s: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  <<R as Runtime>::JoinHandle<()> as Future>::Output: Send,
{
  let name = format!("{kind}_server_with_label_with_encryption_client_with_label_with_encryption");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next())
    .await?
    .with_receive_encrypted(Some(pk))
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_verify_label(true);

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

#[cfg(feature = "encryption")]
pub async fn server_with_label_with_encryption_client_with_label_no_encryption<S, R>(
  s: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  <<R as Runtime>::JoinHandle<()> as Future>::Output: Send,
{
  let name = format!("{kind}_server_with_label_with_encryption_client_with_label_no_encryption");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next())
    .await?
    .with_send_label(true)
    .with_label(label.cheap_clone())
    .with_receive_verify_label(true)
    .with_receive_encrypted(Some(pk));

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_with_label_no_encryption_client_with_label_with_encryption<S, R>(
  s: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  <<R as Runtime>::JoinHandle<()> as Future>::Output: Send,
{
  let name = format!("{kind}_server_with_label_no_encryption_client_with_label_with_encryption");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next())
    .await?
    .with_send_encrypted(Some((EncryptionAlgo::PKCS7, pk)))
    .with_receive_verify_label(true)
    .with_label(label.cheap_clone())
    .with_send_label(true);

  let mut opts = NetTransportOptions::new(name.into())
    .with_label(label)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(false);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
