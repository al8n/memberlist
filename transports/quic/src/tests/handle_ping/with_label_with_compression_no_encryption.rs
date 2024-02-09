use super::*;

pub async fn server_with_label_with_compression_no_encryption_client_with_label_no_compression_no_encryption<
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
  let client = NetTransporTestClient::<R>::new(kind.next())
    .await?
    .with_receive_compressed(true);
  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_with_label_with_compression_no_encryption_client_with_label_no_compression_no_encryption").into())
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_with_label_no_compression_no_encryption_client_with_label_with_compression_no_encryption<
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
  let client = NetTransporTestClient::<R>::new(kind.next())
    .await?
    .with_send_compressed(Some(Compressor::default()));
  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_with_label_no_compression_no_encryption_client_with_label_with_compression_no_encryption").into());
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_with_label_with_compression_no_encryption_client_with_label_with_compression_no_encryption<
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
  let client = NetTransporTestClient::<R>::new(kind.next())
    .await?
    .with_send_compressed(Some(Compressor::default()))
    .with_receive_compressed(true);
  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_with_label_with_compression_no_encryption_client_with_label_with_compression_no_encryption").into())
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}