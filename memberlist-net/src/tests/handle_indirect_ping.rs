use memberlist_core::transport::{tests::handle_indirect_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{NetTransport, NetTransportOptions};

use super::*;

#[cfg(all(feature = "compression", feature = "encryption"))]
pub async fn indirect_ping<S, R>(s: S::Options, kind: AddressKind) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let name = format!("{kind}_indirect_ping");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_encrypted(Some(pk))
    .with_receive_compressed(true)
    .with_receive_verify_label(true);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_offload_size(10)
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_indirect_ping(trans, client).await?;
  Ok(())
}

#[cfg(feature = "compression")]
pub async fn indirect_ping_compression_only<S, R>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let name = format!("{kind}_indirect_ping");
  let label = Label::try_from(&name)?;
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_compressed(true)
    .with_receive_verify_label(true);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_compressor(Some(Compressor::default()))
    .with_offload_size(10)
    .with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_indirect_ping(trans, client).await?;
  Ok(())
}

#[cfg(feature = "encryption")]
pub async fn indirect_ping_encryption_only<S, R>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let name = format!("{kind}_indirect_ping");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_encrypted(Some(pk))
    .with_receive_verify_label(true);

  let mut opts = NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(true)
    .with_label(label)
    .with_offload_size(10);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_indirect_ping(trans, client).await?;
  Ok(())
}

#[cfg(not(any(feature = "compression", feature = "encryption")))]
pub async fn indirect_ping_no_encryption_no_compression<S, R>(
  s: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  let name = format!("{kind}_indirect_ping");
  let label = Label::try_from(&name)?;
  let client = NetTransportTestClient::<R>::new(kind.next(0))
    .await?
    .with_label(label.cheap_clone())
    .with_send_label(true)
    .with_receive_verify_label(true);

  let mut opts =
    NetTransportOptions::<_, _, S>::with_stream_layer_options(name.into(), s).with_label(label);
  opts.add_bind_address(kind.next(0));
  let trans = NetTransport::<_, SocketAddrResolver<R>, _, _>::new(opts)
    .await
    .unwrap();
  handle_indirect_ping(trans, client).await?;
  Ok(())
}
