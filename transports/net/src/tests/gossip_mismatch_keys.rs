use crate::{NetTransport, NetTransportOptions};

use super::*;
use memberlist_core::{
  transport::{Lpe, MaybeResolvedAddress},
  Memberlist, Options,
};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone, Node};

pub async fn gossip_mismatched_keys<S, R>(
  s1: S::Options,
  s2: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  // Create two agents with different gossip keys
  let pk1 = SecretKey::Aes192(*b"4W6DGn2VQVqDEceOdmuRTQ==");

  let name1 = "gossip_mismatched_keys1";
  let mut opts =
    NetTransportOptions::<_, _, S>::with_stream_layer_options(SmolStr::from(name1), s1)
      .with_primary_key(Some(pk1))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next(0));
  let m1 = Memberlist::<NetTransport<_, SocketAddrResolver<R>, S, Lpe<_, _>, _>, _>::new(
    opts,
    Options::default(),
  )
  .await?;
  let m1_addr = m1.advertise_address();

  let name2 = "gossip_mismatched_keys2";
  let pk2 = SecretKey::Aes192(*b"XhX/w702/JKKK7/7OtM9Ww==");
  let mut opts =
    NetTransportOptions::<_, _, S>::with_stream_layer_options(SmolStr::from(name2), s2)
      .with_primary_key(Some(pk2))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true);
  opts.add_bind_address(kind.next(0));
  let m2 = Memberlist::<NetTransport<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>, _>::new(
    opts,
    Options::default(),
  )
  .await?;

  // Make sure we get this error on the joining side
  m2.join_many(
    [Node::new(
      name1.into(),
      MaybeResolvedAddress::resolved(*m1_addr),
    )]
    .into_iter(),
  )
  .await
  .map(|_| {})
  .map_err(Into::into)
}

pub async fn gossip_mismatched_keys_with_label<S, R>(
  s1: S::Options,
  s2: S::Options,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  S: StreamLayer,
  R: Runtime,
{
  // Create two agents with different gossip keys
  let pk1 = SecretKey::Aes192(*b"4W6DGn2VQVqDEceOdmuRTQ==");

  let name1 = "gossip_mismatched_keys1";
  let label = Label::try_from("gossip_mismatched_keys")?;
  let mut opts =
    NetTransportOptions::<_, _, S>::with_stream_layer_options(SmolStr::from(name1), s1)
      .with_primary_key(Some(pk1))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_label(label.cheap_clone());
  opts.add_bind_address(kind.next(0));
  let m1 = Memberlist::<NetTransport<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>, _>::new(
    opts,
    Options::default(),
  )
  .await?;
  let m1_addr = m1.advertise_address();

  let name2 = "gossip_mismatched_keys2";
  let pk2 = SecretKey::Aes192(*b"XhX/w702/JKKK7/7OtM9Ww==");
  let mut opts =
    NetTransportOptions::<_, _, S>::with_stream_layer_options(SmolStr::from(name2), s2)
      .with_primary_key(Some(pk2))
      .with_encryption_algo(Some(EncryptionAlgo::PKCS7))
      .with_gossip_verify_outgoing(true)
      .with_label(label);
  opts.add_bind_address(kind.next(0));
  let m2 = Memberlist::<NetTransport<_, SocketAddrResolver<R>, _, Lpe<_, _>, _>, _>::new(
    opts,
    Options::default(),
  )
  .await?;

  // Make sure we get this error on the joining side
  m2.join_many(
    [Node::new(
      name1.into(),
      MaybeResolvedAddress::resolved(*m1_addr),
    )]
    .into_iter(),
  )
  .await
  .map(|_| {})
  .map_err(Into::into)
}
