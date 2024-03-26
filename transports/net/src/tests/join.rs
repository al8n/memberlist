use memberlist_core::transport::{tests::join as join_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{NetTransport, NetTransportOptions};

use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn join<S1, S2, R>(
  s1: S1::Options,
  s2: S2::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S1: StreamLayer,
  S2: StreamLayer,
  R: Runtime,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_join");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::<_, _, S1>::new("node 1".into(), s1)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let trans1 = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;

  let mut opts = NetTransportOptions::<_, _, S2>::new("node 2".into(), s2)
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next(0));

  let trans2 = NetTransport::<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>::new((), opts).await?;

  join_in(trans1, trans2).await?;
  Ok(())
}
