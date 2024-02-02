use super::*;

pub async fn v4_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption<
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
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4()).await?;
  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into());
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption<
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
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6()).await?;
  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into());
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
