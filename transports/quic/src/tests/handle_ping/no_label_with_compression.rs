use super::*;

pub async fn server_no_label_with_compression_client_no_label_with_compression<S, R>(
  s: S,
  c: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let mut opts = QuicTransportOptions::new(
    format!("{kind}_ping_server_no_label_with_compression_client_no_label_with_compression").into(),
  )
  .with_compressor(Some(Default::default()));
  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  let remote_addr = trans.advertise_address();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_send_compressed(Some(Default::default()))
    .with_receive_compressed(true);
  handle_ping(trans, tc).await?;
  Ok(())
}

pub async fn server_no_label_with_compression_client_no_label_no_compression<S, R>(
  s: S,
  c: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let mut opts = QuicTransportOptions::new(
    format!("{kind}_ping_server_no_label_with_compression_client_no_label_no_compression").into(),
  )
  .with_compressor(Some(Default::default()));
  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  let remote_addr = trans.advertise_address();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_receive_compressed(true);
  handle_ping(trans, tc).await?;
  Ok(())
}

pub async fn server_no_label_no_compression_client_no_label_with_compression<S, R>(
  s: S,
  c: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let mut opts = QuicTransportOptions::new(
    format!("{kind}_ping_server_no_label_with_compression_client_no_label_no_compression").into(),
  );
  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  let remote_addr = trans.advertise_address();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_send_compressed(Some(Default::default()));
  handle_ping(trans, tc).await?;
  Ok(())
}
