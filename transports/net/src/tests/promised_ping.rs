use memberlist_core::transport::{tests::promised_ping as promised_ping_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn promised_ping<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn promised_ping_no_label<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_no_label");
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_compression_only<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_compression_only");

  let mut opts = NetTransportOptions::new(name.into()).with_compressor(Some(Compressor::default()));
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn promised_ping_label_and_compression<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_label_and_compression");
  let label = Label::try_from(&name)?;

  let mut opts = NetTransportOptions::new(name.into())
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "encryption")]
pub async fn promised_ping_encryption_only<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_encryption_only");
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

#[cfg(feature = "encryption")]
pub async fn promised_ping_label_and_encryption<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_lable_and_encryption");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::new(name.into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

pub async fn promised_ping_no_label_no_compression_no_encryption<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_no_compression_no_encryption");

  let mut opts = NetTransportOptions::new(name.into());
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}

pub async fn promised_ping_label_only<S, R>(
  s: S,
  client: NetTransportTestPromisedClient<S>,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let name = format!("{kind}_promised_ping_label_only");
  let label = Label::try_from(&name)?;

  let mut opts = NetTransportOptions::new(name.into()).with_label(label);
  opts.add_bind_address(kind.next());
  let trans = NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s, opts)
    .await
    .unwrap();
  promised_ping_in(trans, client, kind).await?;
  Ok(())
}
