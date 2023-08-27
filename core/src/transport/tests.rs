use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use futures_util::{Future, FutureExt, Stream};

use crate::{
  showbiz::tests::get_showbiz,
  types::{AckResponse, IndirectPing, MessageType, Ping, Type},
  CompressionAlgo, Message, Name, NodeId, Options,
};

use super::net::NetTransport;

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
    source: NodeId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m.inner.id.clone(),
  };

  let buf = ping.encode();
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();
  tracing::error!("id {}", m.inner.id.addr());

  // Wait for response
  let (tx, rx) = futures_channel::oneshot::channel();
  R::spawn_detach(async {
    futures_util::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let mut in_ = vec![0; 1500];
  let (n, _) = udp.recv_from(&mut in_).await.unwrap();

  in_.truncate(n);

  assert_eq!(in_[0], MessageType::AckResponse as u8);
  let cks = u32::from_be_bytes(in_[in_.len() - 4..in_.len()].try_into().unwrap());
  tracing::error!("receive len {} cks {cks} data {:?}", n, in_);
  let (_, ack) = AckResponse::decode(&in_).unwrap();
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
    source: NodeId::new(Name::from_str_unchecked("test"), udp_addr),
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
  let (tx, rx) = futures_channel::oneshot::channel();
  R::spawn_detach(async {
    futures_util::select! {
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
    assert_eq!(in_[0], MessageType::AckResponse as u8);

    let (_, ack) = AckResponse::decode(&in_).unwrap();
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
    source: NodeId::new(Name::from_str_unchecked("test"), udp_addr),
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
    source: NodeId::new(Name::from_str_unchecked("test"), udp_addr),
    target: m.inner.id.clone(),
  };

  let buf = ping.encode();
  // Send
  udp
    .send_to(buf.underlying_bytes(), m.inner.id.addr())
    .await
    .unwrap();

  // Wait for response
  let (tx, rx) = futures_channel::oneshot::channel();
  R::spawn_detach(async {
    futures_util::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let mut in_ = vec![0; 1500];
  let (n, _) = udp.recv_from(&mut in_).await.unwrap();
  in_.truncate(n);
  assert_eq!(in_[0], MessageType::AckResponse as u8);

  let (_, ack) = AckResponse::decode(&in_).unwrap();
  assert_eq!(ack.seq_no, 100, "bad sequence no: {}", ack.seq_no);
  tx.send(()).unwrap();
}

pub async fn test_transport_join_runner() {
  todo!()
}

pub async fn test_transport_send_runner() {
  todo!()
}

pub async fn test_transport_tcp_listen_back_off_runner() {
  todo!()
}
