use crate::{NetTransport, NetTransportOptions};

use super::*;
use agnostic::Runtime;
use futures::{Future, Stream};
use memberlist_core::{transport::Lpe, Memberlist, Options};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone, Node};

pub async fn gossip_mismatched_keys<S, R>(s1: S, s2: S, kind: AddressKind) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  // Create two agents with different gossip keys
  let pk1 = SecretKey::Aes192(*b"4W6DGn2VQVqDEceOdmuRTQ==");

  let name1 = "gossip_mismatched_keys1";
  let mut opts = NetTransportOptions::new(SmolStr::from(name1))
    .with_primary_key(Some(pk1))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next());
  let trans1 =
    NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s1, opts).await?;
  let m1 = Memberlist::new(trans1, Options::default()).await?;
  let m1_addr = m1.advertise_addr();

  let name2 = "gossip_mismatched_keys2";
  let pk2 = SecretKey::Aes192(*b"XhX/w702/JKKK7/7OtM9Ww==");
  let mut opts = NetTransportOptions::new(SmolStr::from(name2))
    .with_primary_key(Some(pk2))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next());
  let trans2 =
    NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s2, opts).await?;
  let m2 = Memberlist::new(trans2, Options::default()).await?;

  // Make sure we get this error on the joining side
  m2.join_many([Node::new(name1.into(), *m1_addr)].into_iter())
    .await
    .map(|_| {})
    .map_err(Into::into)
}

pub async fn gossip_mismatched_keys_with_label<S, R>(
  s1: S,
  s2: S,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  // Create two agents with different gossip keys
  let pk1 = SecretKey::Aes192(*b"4W6DGn2VQVqDEceOdmuRTQ==");

  let name1 = "gossip_mismatched_keys1";
  let label = Label::try_from("gossip_mismatched_keys")?;
  let mut opts = NetTransportOptions::new(SmolStr::from(name1))
    .with_primary_key(Some(pk1))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next());
  let trans1 =
    NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s1, opts).await?;
  let m1 = Memberlist::new(trans1, Options::default()).await?;
  let m1_addr = m1.advertise_addr();

  let name2 = "gossip_mismatched_keys2";
  let pk2 = SecretKey::Aes192(*b"XhX/w702/JKKK7/7OtM9Ww==");
  let mut opts = NetTransportOptions::new(SmolStr::from(name2))
    .with_primary_key(Some(pk2))
    .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
    .with_gossip_verify_outgoing(true)
    .with_label(label);
  opts.add_bind_address(kind.next());
  let trans2 =
    NetTransport::<_, _, _, Lpe<_, _>>::new(SocketAddrResolver::<R>::new(), s2, opts).await?;
  let m2 = Memberlist::new(trans2, Options::default()).await?;

  // Make sure we get this error on the joining side
  m2.join_many([Node::new(name1.into(), *m1_addr)].into_iter())
    .await
    .map(|_| {})
    .map_err(Into::into)
}
