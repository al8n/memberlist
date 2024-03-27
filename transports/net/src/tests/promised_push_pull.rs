use memberlist_core::transport::{tests::promised_push_pull as promised_push_pull_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{NetTransport, NetTransportOptions};

use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn promised_push_pull<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label)
    .with_offload_size(100);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn promised_push_pull_no_label<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_no_label");
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_push_pull_compression_only<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_compression_only");

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_push_pull_label_and_compression<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_label_and_compression");
  let label = Label::try_from(&name)?;

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

#[cfg(feature = "encryption")]
pub async fn promised_push_pull_encryption_only<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_encryption_only");
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

#[cfg(feature = "encryption")]
pub async fn promised_push_pull_label_and_encryption<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_lable_and_encryption");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

pub async fn promised_push_pull_no_label_no_compression_no_encryption<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_no_compression_no_encryption");

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}

pub async fn promised_push_pull_label_only<S, R>(
  s: S::Options,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  let name = format!("{kind}_promised_push_pull_label_only");
  let label = Label::try_from(&name)?;

  let mut opts =
    NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s).with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new(opts)
    .await
    .unwrap();
  promised_push_pull_in(trans, client).await?;
  Ok(())
}
