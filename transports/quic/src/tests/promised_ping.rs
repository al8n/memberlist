use memberlist_core::transport::{tests::promised_ping as promised_ping_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

#[cfg(feature = "compression")]
pub async fn promised_ping<S, R>(
  s: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new(name.into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_no_label<S, R>(
  s: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_no_label");

  let mut opts =
    QuicTransportOptions::new(name.into()).with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_compression_only<S, R>(
  s: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_compression_only");

  let mut opts =
    QuicTransportOptions::new(name.into()).with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_label_and_compression<S, R>(
  s: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_label_and_compression");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new(name.into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

pub async fn promised_ping_no_label_no_compression<S, R>(
  s: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_no_compression");

  let mut opts = QuicTransportOptions::new(name.into());
  opts.add_bind_address(kind.next(0));
  let trans =
    QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts).await?;
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

pub async fn promised_ping_label_only<S, R>(
  s: S,
  client: QuicTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: RuntimeLite,
{
  let name = format!("{kind}_promised_ping_label_only");
  let label = Label::try_from(&name)?;

  let mut opts = QuicTransportOptions::new(name.into()).with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = QuicTransport::<_, _, _, Lpe<_, _>, _>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}
