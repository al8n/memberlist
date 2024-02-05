use memberlist_core::transport::{tests::join as join_in, Lpe};
use nodecraft::resolver::socket_addr::SocketAddrResolver;

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

#[cfg(all(feature = "encryption", feature = "compression"))]
pub async fn join<S1, S2, R>(s1: S1, s2: S2, kind: AddressKind) -> Result<(), AnyError>
where
  S1: StreamLayer,
  S2: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  use nodecraft::CheapClone;

  let name = format!("{kind}_join");
  let label = Label::try_from(&name)?;
  let pk = SecretKey::from([1; 32]);

  let mut opts = NetTransportOptions::new("node 1".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next());
  let trans1 =
    NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s1, opts).await?;

  let mut opts = NetTransportOptions::new("node 2".into())
    .with_primary_key(Some(pk))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_compressor(Some(Compressor::default()))
    .with_label(label);
  opts.add_bind_address(kind.next());

  let trans2 =
    NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s2, opts).await?;

  join_in(trans1, trans2).await?;
  Ok(())
}