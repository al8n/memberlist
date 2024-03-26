use super::*;

pub async fn server_with_label_client_with_label<S, R>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_server_with_label_client_with_label");
  let label = Label::try_from(&name)?;
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_verify_label(true);
  let mut opts = NetTransportOptions::<_, _, S>::new(name.into(), s).with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_with_label_client_no_label<S, R>(
  s: S::Options,
  kind: AddressKind,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_server_with_label_client_no_label");
  let label = Label::try_from(&name)?;
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_receive_verify_label(true);
  let mut opts = NetTransportOptions::<_, _, S>::new(name.into(), s)
    .with_label(label)
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}

pub async fn server_no_label_client_with_label<S, R>(
  s: S::Options,
  kind: AddressKind,
  server_check_label: bool,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_server_no_label_client_with_label");
  let label = Label::try_from(&name)?;
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true);
  let mut opts = NetTransportOptions::<_, _, S>::new(name.into(), s)
    .with_skip_inbound_label_check(server_check_label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts)
    .await
    .unwrap();
  handle_ping(trans, client).await?;
  Ok(())
}
