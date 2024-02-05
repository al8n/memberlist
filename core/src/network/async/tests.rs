use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use bytes::Bytes;
use futures::{Future, FutureExt, Stream};

use crate::{
  memberlist::tests::{get_memberlist, test_config},
  security::SecretKey,
  transport::net::NetTransport,
  types::{Ack, Alive, EncodeHeader, IndirectPing, MessageType, Ping, Type, ENCODE_HEADER_SIZE},
  CompressionAlgo, Memberlist, Message, Options,
};

pub async fn test_gossip_mismatched_keys<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  // Create two agents with different gossip keys
  let c1 =
    test_config::<R>().with_secret_key(Some(SecretKey::Aes192(*b"4W6DGn2VQVqDEceOdmuRTQ==")));

  let m1 = Memberlist::new(c1).await.unwrap();
  let bind_port = m1.inner.id.addr().port();

  let c2 = test_config::<R>()
    .with_secret_key(Some(SecretKey::Aes192(*b"XhX/w702/JKKK7/7OtM9Ww==")))
    .with_bind_port(Some(bind_port));
  let m2 = Memberlist::new(c2).await.unwrap();

  // Make sure we get this error on the joining side
  m2.join_many(
    [(
      (m1.inner.opts.bind_addr, m1.inner.opts.bind_port.unwrap()).into(),
      m1.inner.opts.name.clone(),
    )]
    .into_iter(),
  )
  .await
  .unwrap_err();
}
