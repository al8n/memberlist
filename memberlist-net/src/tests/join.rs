#[cfg(all(feature = "encryption", feature = "compression"))]
use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn join<S1, S2, R>(
  s1: S1::Options,
  s2: S2::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S1: StreamLayer<Runtime = R>,
  S2: StreamLayer<Runtime = R>,
  R: Runtime,
{
  use memberlist_core::transport::{tests::join as join_in, Lpe};
  use nodecraft::resolver::socket_addr::SocketAddrResolver;

  use crate::{NetTransport, NetTransportOptions};
  use nodecraft::CheapClone;

  let name = format!("{kind}_join");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts1 = NetTransportOptions::<_, _, S1>::with_stream_layer_options("node 1".into(), s1)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts1.add_bind_address(kind.next(0));

  let mut opts2 = NetTransportOptions::<_, _, S2>::with_stream_layer_options("node 2".into(), s2)
    .with_primary_key(Some(pk))
    .with_encryption_algo(EncryptionAlgo::PKCS7)
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts2.add_bind_address(kind.next(0));

  join_in::<
    _,
    NetTransport<_, SocketAddrResolver<R>, _, _>,
    NetTransport<_, SocketAddrResolver<R>, _, _>,
    _,
  >(opts1, opts2)
  .await?;
  Ok(())
}
