use std::{
  future::Future,
  net::SocketAddr,
  sync::Arc,
  time::{Duration, Instant},
};

use agnostic::Runtime;
use bytes::Bytes;
use either::Either;
use futures::{FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone, Node};
use smol_str::SmolStr;
use transformable::Transformable;

use crate::{
  delegate::VoidDelegate,
  tests::{get_memberlist, next_socket_addr_v4, next_socket_addr_v6, AnyError},
  transport::{Ack, Alive, IndirectPing, Message},
  Options,
};

use super::{Ping, Transport};

/// The kind of address
pub enum AddressKind {
  /// V4
  V4,
  /// V6
  V6,
}

impl core::fmt::Display for AddressKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::V4 => write!(f, "v4"),
      Self::V6 => write!(f, "v6"),
    }
  }
}

impl AddressKind {
  /// Get the next address
  pub fn next(&self) -> SocketAddr {
    match self {
      Self::V4 => next_socket_addr_v4(),
      Self::V6 => next_socket_addr_v6(),
    }
  }
}

/// The client used to send/receive data to a transport
pub trait TestPacketClient: Sized + Send + Sync + 'static {
  /// Send all data to the transport
  fn send_to(
    &mut self,
    addr: &SocketAddr,
    data: &[u8],
  ) -> impl Future<Output = Result<(), AnyError>> + Send;

  /// Receive data from the transport
  fn recv_from(&mut self) -> impl Future<Output = Result<(Bytes, SocketAddr), AnyError>> + Send;

  /// Local address of the client
  fn local_addr(&self) -> SocketAddr;
}

/// The client used to send/receive data to a transport
pub trait TestPromisedClient: Sized + Send + Sync + 'static {
  type Stream: Send + Sync + 'static;

  /// Accept a connection from the remote
  fn accept(&self) -> impl Future<Output = Result<(Self::Stream, SocketAddr), AnyError>> + Send;

  /// Local address of the client
  fn local_addr(&self) -> SocketAddr;
}

/// Unit test for handling [`Ping`] message
pub async fn handle_ping<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
  C: TestPacketClient,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;

  let source_addr = client.local_addr();

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: Node::new("test".into(), source_addr),
    target: m.advertise_node(),
  };

  let buf = Message::from(ping).encode_to_vec()?;
  // Send
  client.send_to(m.advertise_addr(), &buf).await?;

  // Wait for response
  let (tx, rx) = async_channel::bounded(1);
  let tx1 = tx.clone();
  R::spawn_detach(async move {
    futures::select! {
      _ = rx.recv().fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        tx1.close();
      }
    }
  });

  let (in_, _) = R::timeout(Duration::from_secs_f64(2.5), client.recv_from())
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;
  let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
  assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);

  futures::select! {
    res = tx.send(()).fuse() => {
      if res.is_err() {
        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
      } else {
        Ok(())
      }
    }
  }
}

pub async fn handle_compound_ping<A, T, C, E, R>(
  trans: T,
  mut client: C,
  encoder: E,
) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
  C: TestPacketClient,
  E: FnOnce(&[Message<SmolStr, SocketAddr>]) -> Result<Bytes, AnyError>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;

  let source_addr = client.local_addr();

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: Node::new("test".into(), source_addr),
    target: m.advertise_node(),
  };

  let msgs = [
    Message::from(ping.cheap_clone()),
    Message::from(ping.cheap_clone()),
    Message::from(ping.cheap_clone()),
  ];
  let buf = encoder(&msgs)?;

  // Send
  client.send_to(m.advertise_addr(), &buf).await.unwrap();

  // Wait for response
  let (tx, rx) = async_channel::bounded(1);
  let tx1 = tx.clone();
  R::spawn_detach(async move {
    futures::select! {
      _ = rx.recv().fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        tx1.close();
      }
    }
  });

  for _ in 0..3 {
    let (in_, _) = R::timeout(Duration::from_secs_f64(2.5), client.recv_from())
      .await
      .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;
    let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
    assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);
  }

  futures::select! {
    res = tx.send(()).fuse() => {
      if res.is_err() {
        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
      } else {
        Ok(())
      }
    }
  }
}

pub async fn handle_indirect_ping<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
  C: TestPacketClient,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;

  let source_addr = client.local_addr();

  // Encode a ping
  let ping = IndirectPing {
    seq_no: 100,
    source: Node::new("test".into(), source_addr),
    target: m.advertise_node(),
  };

  let buf = Message::from(ping).encode_to_vec()?;
  // Send
  client.send_to(m.advertise_addr(), &buf).await.unwrap();

  // Wait for response
  let (tx, rx) = async_channel::bounded(1);
  let tx1 = tx.clone();
  R::spawn_detach(async move {
    futures::select! {
      _ = rx.recv().fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        tx1.close();
      }
    }
  });

  let (in_, _) = R::timeout(Duration::from_secs_f64(2.5), client.recv_from())
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;
  let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
  assert_eq!(ack.seq_no, 100, "bad sequence no: {}", ack.seq_no);
  futures::select! {
    res = tx.send(()).fuse() => {
      if res.is_err() {
        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
      } else {
        Ok(())
      }
    }
  }
}

pub async fn handle_ping_wrong_node<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
  C: TestPacketClient,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
  let source_addr = client.local_addr();

  // Encode a ping
  let ping: Ping<SmolStr, _> = Ping {
    seq_no: 42,
    source: Node::new("test".into(), source_addr),
    target: Node::new("bad".into(), {
      let mut a = source_addr;
      a.set_port(12345);
      a
    }),
  };

  let buf = Message::from(ping).encode_to_vec()?;
  // Send
  client.send_to(m.advertise_addr(), &buf).await?;

  // Wait for response
  R::timeout(Duration::from_secs_f64(2.5), client.recv_from()).await??;
  Ok(())
}

pub async fn send_packet_piggyback<A, T, C, D, R>(
  trans: T,
  mut client: C,
  decoder: D,
) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
  C: TestPacketClient,
  D: FnOnce(Bytes) -> Result<[Message<SmolStr, SocketAddr>; 2], AnyError>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
  let source_addr = client.local_addr();

  // Add a message to be broadcast
  let n: Node<SmolStr, SocketAddr> = Node::new("rand".into(), *m.advertise_addr());
  let a = Alive {
    incarnation: 10,
    meta: Bytes::new(),
    node: n.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m.broadcast(Either::Left(n.id().clone()), Message::from(a))
    .await;

  // Encode a ping
  let ping = IndirectPing {
    seq_no: 42,
    source: Node::new("test".into(), source_addr),
    target: m.advertise_node(),
  };

  let buf = Message::from(ping).encode_to_vec()?;
  // Send
  client.send_to(m.advertise_addr(), &buf).await?;

  // Wait for response
  let (tx, rx) = async_channel::bounded(1);
  let tx1 = tx.clone();
  R::spawn_detach(async move {
    futures::select! {
      _ = rx.recv().fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        tx1.close();
      }
    }
  });

  let (in_, _) = R::timeout(Duration::from_secs_f64(2.5), client.recv_from())
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;

  // get the parts
  let parts = decoder(in_)?;

  let [m1, m2] = parts;
  let ack = m1.unwrap_ack();
  assert_eq!(ack.seq_no, 42, "bad sequence no");

  let alive = m2.unwrap_alive();
  assert_eq!(alive.incarnation, 10);
  assert_eq!(alive.node, n);

  futures::select! {
    res = tx.send(()).fuse() => {
      if res.is_err() {
        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
      } else {
        Ok(())
      }
    }
  }
}

macro_rules! unwrap_ok {
  ($tx:ident.send($expr:expr)) => {
    match $expr {
      ::std::result::Result::Ok(val) => val,
      ::std::result::Result::Err(e) => {
        $tx.send(e).await.unwrap();
        return;
      }
    }
  };
}

pub async fn promised_ping<A, T, P, R>(
  trans: T,
  promised: P,
  kind: AddressKind,
) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Stream = P::Stream, Runtime = R>,
  P: TestPromisedClient,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let promised_addr = promised.local_addr();
  let promised = Arc::new(promised);

  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
  let ping_timeout = m.inner.opts.probe_interval;
  let ping_time_max = m.inner.opts.probe_interval + Duration::from_secs(10);

  // Do a normal rount trip
  let node = Node::new("mongo".into(), kind.next());
  let ping_out = Ping {
    seq_no: 23,
    source: m.advertise_node(),
    target: node.cheap_clone(),
  };

  let (ping_err_tx, ping_err_rx) = async_channel::bounded::<AnyError>(1);
  let m1 = m.clone();
  let node1 = node.cheap_clone();
  let p1 = promised.clone();
  let ping_err_tx1 = ping_err_tx.clone();
  R::spawn_detach(async move {
    let (mut stream, addr) = unwrap_ok!(ping_err_tx1.send(unwrap_ok!(ping_err_tx1.send(
      R::timeout(ping_time_max, p1.accept())
        .await
        .map_err(Into::into)
    ))));

    let (_, p) = unwrap_ok!(ping_err_tx1.send(
      m1.inner
        .transport
        .read_message(&addr, &mut stream)
        .await
        .map_err(Into::into)
    ));
    let ping_in = p.unwrap_ping();
    assert_eq!(ping_in.seq_no, 23);
    assert_eq!(ping_in.target, node1);

    let ack = Ack {
      seq_no: 23,
      payload: Bytes::new(),
    };

    unwrap_ok!(ping_err_tx1.send(
      m1.inner
        .transport
        .send_message(&mut stream, ack.into())
        .await
        .map_err(Into::into)
    ));
  });

  let did_contact = m
    .send_ping_and_wait_for_ack(&promised_addr, ping_out.clone(), ping_timeout)
    .await?;

  if !did_contact {
    return Err(std::io::Error::new(std::io::ErrorKind::Other, "expected successful ping").into());
  }

  if !ping_err_rx.is_empty() {
    return Err(ping_err_rx.recv().await.unwrap());
  }

  // Make sure a mis-matched sequence number is caught.
  let p2 = promised.clone();
  let m2 = m.clone();
  let ping_err_tx2 = ping_err_tx.clone();
  R::spawn_detach(async move {
    let (mut stream, addr) = unwrap_ok!(ping_err_tx2.send(unwrap_ok!(ping_err_tx2.send(
      R::timeout(ping_time_max, p2.accept())
        .await
        .map_err(Into::into)
    ))));

    let (_, p) = unwrap_ok!(ping_err_tx2.send(
      m2.inner
        .transport
        .read_message(&addr, &mut stream)
        .await
        .map_err(Into::into)
    ));

    let ping_in = p.unwrap_ping();
    let ack = Ack {
      seq_no: ping_in.seq_no + 1,
      payload: Bytes::new(),
    };

    unwrap_ok!(ping_err_tx2.send(
      m2.inner
        .transport
        .send_message(&mut stream, ack.into())
        .await
        .map_err(Into::into)
    ));
  });

  let did_contact = m
    .send_ping_and_wait_for_ack(&promised_addr, ping_out.clone(), ping_timeout)
    .await?;
  if did_contact {
    return Err(std::io::Error::new(std::io::ErrorKind::Other, "expected failed ping").into());
  }

  if !ping_err_rx.is_empty() {
    panic!("{}", ping_err_rx.recv().await.unwrap());
  }

  // Make sure an unexpected message type is handled gracefully.
  let p3 = promised.clone();
  let m3 = m.clone();
  let ping_err_tx3 = ping_err_tx.clone();
  R::spawn_detach(async move {
    let (mut stream, addr) = unwrap_ok!(ping_err_tx3.send(unwrap_ok!(ping_err_tx3.send(
      R::timeout(ping_time_max, p3.accept())
        .await
        .map_err(Into::into)
    ))));

    let _ = unwrap_ok!(ping_err_tx3.send(
      m3.inner
        .transport
        .read_message(&addr, &mut stream)
        .await
        .map_err(Into::into)
    ));

    unwrap_ok!(ping_err_tx3.send(
      m3.inner
        .transport
        .send_message(
          &mut stream,
          IndirectPing {
            seq_no: 0,
            source: Node::new("unknown source".into(), kind.next()),
            target: Node::new("unknown target".into(), kind.next()),
          }
          .into()
        )
        .await
        .map_err(Into::into)
    ));
  });

  let err = m
    .send_ping_and_wait_for_ack(&promised_addr, ping_out.clone(), ping_timeout)
    .await
    .expect_err("expected failed ping");

  tracing::error!("{}", err);

  if !ping_err_rx.is_empty() {
    panic!("{}", ping_err_rx.recv().await.unwrap());
  }

  // Make sure failed I/O respects the deadline. In this case we try the
  // common case of the receiving node being totally down.
  drop(promised);
  let start_ping = Instant::now();
  m.send_ping_and_wait_for_ack(&promised_addr, ping_out, ping_timeout)
    .await
    .expect_err("expected failed ping");
  let elapsed = start_ping.elapsed();
  assert!(
    elapsed <= ping_time_max,
    "elapsed: {:?}, take too long to fail ping",
    elapsed
  );
  Ok(())
}

// pub async fn test_transport_join_runner() {
//   todo!()
// }

// pub async fn test_transport_send_runner() {
//   todo!()
// }

// pub async fn test_transport_tcp_listen_back_off_runner() {
//   todo!()
// }
