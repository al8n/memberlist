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
  security::SecretKey,
  showbiz::tests::{get_showbiz, test_config},
  transport::net::NetTransport,
  types::{Ack, Alive, EncodeHeader, IndirectPing, MessageType, Ping, Type, ENCODE_HEADER_SIZE},
  CompressionAlgo, Memberlist, Message, Options,
};

async fn listen_udp<R>() -> Result<<R::Net as Net>::UdpSocket, std::io::Error>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  for port in 60000..61000 {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    match <<R::Net as Net>::UdpSocket as UdpSocket>::bind(addr).await {
      Ok(socket) => return Ok(socket),
      Err(_) => continue,
    }
  }
  Err(std::io::Error::new(
    std::io::ErrorKind::Other,
    "no available port",
  ))
}

pub async fn test_handle_ping<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_showbiz::<_, R>(Some(|c: Options<NetTransport<R>>| {
    c.with_compression_algo(CompressionAlgo::None)
  }))
  .await
  .unwrap();

  let udp = listen_udp::<R>().await.unwrap();

  let udp_addr = udp.local_addr().unwrap();

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: ServerId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m.inner.id.clone(),
  };

  let buf = ping.encode();
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();

  // Wait for response
  let (tx, rx) = futures::channel::oneshot::channel();
  R::spawn_detach(async {
    futures::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let mut in_ = vec![0; 1500];
  let (n, _) = udp.recv_from(&mut in_).await.unwrap();

  in_.truncate(n);

  assert_eq!(in_[0], MessageType::Ack as u8);
  let (_, ack) = Ack::decode(&in_).unwrap();
  assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);
  tx.send(()).unwrap();
}

pub async fn test_handle_compound_ping<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_showbiz::<_, R>(Some(|c: Options<NetTransport<R>>| {
    c.with_compression_algo(CompressionAlgo::None)
  }))
  .await
  .unwrap();

  let udp = listen_udp::<R>().await.unwrap();

  let udp_addr = udp.local_addr().unwrap();

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: ServerId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m.inner.id.clone(),
  };

  let buf = ping.encode();
  // Make a compound message
  let buf = Message::compound(vec![buf.clone(), buf.clone(), buf]);
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();

  // Wait for responses
  let (tx, rx) = futures::channel::oneshot::channel();
  R::spawn_detach(async {
    futures::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(200)).fuse() => {
        panic!("timeout")
      }
    }
  });

  for _ in 0..3 {
    let mut in_ = vec![0; 1500];
    let (n, _) = udp.recv_from(&mut in_).await.unwrap();
    in_.truncate(n);
    assert_eq!(in_[0], MessageType::Ack as u8);

    let (_, ack) = Ack::decode(&in_).unwrap();
    assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);
  }

  tx.send(()).unwrap();
}

pub async fn test_handle_ping_wrong_node<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_showbiz::<_, R>(Some(|c: Options<NetTransport<R>>| {
    c.with_compression_algo(CompressionAlgo::None)
  }))
  .await
  .unwrap();

  let udp = listen_udp::<R>().await.unwrap();

  let udp_addr = udp.local_addr().unwrap();

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: ServerId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m
      .inner
      .id
      .clone()
      .set_addr({
        let mut a = udp_addr;
        a.set_port(12345);
        a
      })
      .set_name(Name::from_str_unchecked("bad")),
  };

  let buf = ping.encode();
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();

  // Wait for response
  udp.set_read_timeout(Some(Duration::from_millis(50)));
  let mut in_ = vec![0; 1500];
  udp.recv_from(&mut in_).await.unwrap_err();
}

pub async fn test_handle_indirect_ping<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_showbiz::<_, R>(Some(|c: Options<NetTransport<R>>| {
    c.with_compression_algo(CompressionAlgo::None)
  }))
  .await
  .unwrap();

  let udp = listen_udp::<R>().await.unwrap();

  let udp_addr = udp.local_addr().unwrap();

  // Encode a ping
  let ping = IndirectPing {
    seq_no: 100,
    source: ServerId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m.inner.id.clone(),
  };

  let buf = ping.encode();
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();

  // Wait for response
  let (tx, rx) = futures::channel::oneshot::channel();
  R::spawn_detach(async {
    futures::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let mut in_ = vec![0; 1500];
  let (n, _) = udp.recv_from(&mut in_).await.unwrap();
  in_.truncate(n);
  assert_eq!(in_[0], MessageType::Ack as u8);

  let (_, ack) = Ack::decode(&in_).unwrap();
  assert_eq!(ack.seq_no, 100, "bad sequence no: {}", ack.seq_no);
  tx.send(()).unwrap();
}

pub async fn test_tcp_ping<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
}

pub async fn test_tcp_push_pull<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
}

pub async fn test_send_msg_piggyback<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_showbiz::<_, R>(Some(|opts: Options<_>| {
    opts.with_compression_algo(CompressionAlgo::None)
  }))
  .await
  .unwrap();

  // Add a message to be broadcast
  let id = ServerId::new(
    Name::from_str_unchecked("rand"),
    "127.0.0.255:0".parse().unwrap(),
  );
  let a = Alive {
    incarnation: 10,
    meta: Bytes::new(),
    node: id.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m.broadcast(id.clone(), a.encode()).await;

  let udp = listen_udp::<R>().await.unwrap();
  let udp_addr = udp.local_addr().unwrap();

  // Encode a ping
  let ping = IndirectPing {
    seq_no: 42,
    source: ServerId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m.inner.id.clone(),
  };

  let buf = ping.encode();
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();

  // Wait for response
  let (tx, rx) = futures::channel::oneshot::channel();
  R::spawn_detach(async {
    futures::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let mut in_ = vec![0; 1500];
  let (n, _) = udp.recv_from(&mut in_).await.unwrap();
  in_.truncate(n);

  // get the parts
  let mut in_: Bytes = in_.into();
  let h = EncodeHeader::from_bytes(&in_.split_to(ENCODE_HEADER_SIZE)).unwrap();
  assert_eq!(h.meta.ty, MessageType::Compound);
  assert_eq!(h.meta.msgs, 2);

  let (trunc, parts) = Message::decode_compound_message(2, in_).unwrap();
  assert_eq!(trunc, 0);

  let (_, ack) = Ack::decode(&parts[0]).unwrap();
  assert_eq!(ack.seq_no, 42, "bad sequence no");

  let (_, alive) = Alive::decode(&parts[1]).unwrap();
  assert_eq!(alive.incarnation, 10);
  assert_eq!(alive.node, id);
  tx.send(()).unwrap();
}

pub async fn test_raw_send_udp_crc<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
}

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
