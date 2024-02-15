use super::*;

pub async fn server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption<
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
  let client = NetTransportTestClient::<R>::new(kind.next()).await?;
  let mut opts = NetTransportOptions::new(format!("{kind}_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption").into());
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
