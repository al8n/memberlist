use super::*;

pub async fn server_with_label_with_compression_client_with_label_no_compression<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name =
    format!("{kind}_ping_server_with_label_with_compression_client_with_label_no_compression");
  let label = Label::try_from(&name)?;
  let mut opts = QuicTransportOptions::<_, _, S>::new(name.into(), s)
    .with_label(label.cheap_clone())
    .with_compressor(Some(Default::default()));

  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_receive_verify_label(true)
    .with_receive_compressed(true);
  handle_ping(trans, tc).await?;
  Ok(())
}

pub async fn server_with_label_no_compression_client_with_label_with_compression<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name =
    format!("{kind}_ping_server_with_label_no_compression_client_with_label_with_compression");
  let label = Label::try_from(&name)?;
  let mut opts =
    QuicTransportOptions::<_, _, S>::new(name.into(), s).with_label(label.cheap_clone());

  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_receive_verify_label(true)
    .with_send_compressed(Some(Default::default()));
  handle_ping(trans, tc).await?;
  Ok(())
}

pub async fn server_with_label_with_compression_client_with_label_with_compression<S, R>(
  s: S::Options,
  c: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name =
    format!("{kind}_ping_server_with_label_with_compression_client_with_label_with_compression");
  let label = Label::try_from(&name)?;
  let mut opts = QuicTransportOptions::<_, _, S>::new(name.into(), s)
    .with_label(label.cheap_clone())
    .with_compressor(Some(Default::default()))
    .with_offload_size(10);

  let local_addr = kind.next(0);
  opts.add_bind_address(local_addr);
  let trans = QuicTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;
  let remote_addr = trans.advertise_address();
  let c = S::new(c).await.unwrap();
  let tc = QuicTransportTestClient::<S, R>::new(local_addr, *remote_addr, c)
    .await?
    .with_label(label)
    .with_send_label(true)
    .with_receive_verify_label(true)
    .with_receive_compressed(true)
    .with_send_compressed(Some(Default::default()));
  handle_ping(trans, tc).await?;
  Ok(())
}
