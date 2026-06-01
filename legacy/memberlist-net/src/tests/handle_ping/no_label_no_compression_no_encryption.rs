use super::*;

pub async fn server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption<
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
  let client = NetTransportTestClient::<R>::new(kind.next(0)).await?;
  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(format!("{kind}_ping_server_no_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption").into(), s);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
