use super::*;

pub async fn v4_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption<
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
  let label = Label::try_from("test_handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption")?;
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4())
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_verify_label(true);
  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption".into()).with_label(label);
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption<
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
  let label = Label::try_from("test_handle_v6_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption")?;
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6())
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_verify_label(true);
  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption".into()).with_label(label);
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v4_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let label = Label::try_from("test_handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption")?;
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4())
    .await?
    .with_label(label.cheap_clone())
    .with_receive_verify_label(true);
  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into())
    .with_label(label)
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let label = Label::try_from("test_handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption")?;
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6())
    .await?
    .with_label(label.cheap_clone())
    .with_receive_verify_label(true);
  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption".into())
    .with_label(label)
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v4_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let label = Label::try_from("test_handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption")?;
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v4())
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true);
  let mut opts = NetTransportOptions::new("test_handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption".into())
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(next_socket_addr_v4());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn v6_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption<
  S,
  R,
>(
  s: S,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let label = Label::try_from("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption")?;
  let client = NetTransporTestClient::<R>::new(next_socket_addr_v6())
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true);
  let mut opts = NetTransportOptions::new("test_handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption".into())
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(next_socket_addr_v6());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
