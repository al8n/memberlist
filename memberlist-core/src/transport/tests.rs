// #![allow(clippy::blocks_in_conditions, unreachable_code)]
// use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

// use agnostic_lite::{time::Instant, RuntimeLite};
// use bytes::Bytes;

// use futures::FutureExt;
// use memberlist_proto::Data;
// use nodecraft::{resolver::AddressResolver, CheapClone, Node};
// use smol_str::SmolStr;

// use crate::{
//   delegate::{mock::MockDelegate, CompositeDelegate, VoidDelegate},
//   state::LocalNodeState,
//   tests::{get_memberlist, next_socket_addr_v4, next_socket_addr_v6, AnyError},
//   transport::{Ack, Alive, IndirectPing, MaybeResolvedAddress, Message},
//   proto::Epoch,
//   Member, Memberlist, Options,
// };

// use super::{Meta, NodeState, Ping, PushNodeState, PushPull, State, Transport};

// mod unimplemented;
// pub use unimplemented::*;

// const TIMEOUT_DURATION: Duration = Duration::from_secs(5);
// const WAIT_DURATION: Duration = Duration::from_secs(6);

// /// The kind of address
// pub enum AddressKind {
//   /// V4
//   V4,
//   /// V6
//   V6,
// }

// impl core::fmt::Display for AddressKind {
//   fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//     match self {
//       Self::V4 => write!(f, "v4"),
//       Self::V6 => write!(f, "v6"),
//     }
//   }
// }

// impl AddressKind {
//   /// Get the next address
//   pub fn next(&self, network: u8) -> SocketAddr {
//     match self {
//       Self::V4 => next_socket_addr_v4(network),
//       Self::V6 => next_socket_addr_v6(),
//     }
//   }
// }

// /// A trait for testing packet
// pub trait TestPacketStream: Send + Sync + 'static {
//   /// Send all data to the transport
//   fn send_to(&mut self, data: &[u8]) -> impl Future<Output = Result<(), AnyError>> + Send;

//   /// Receive data from the transport
//   fn recv_from(&mut self) -> impl Future<Output = Result<(Bytes, SocketAddr), AnyError>> + Send;

//   /// Finish the stream
//   fn finish(&mut self) -> impl Future<Output = Result<(), AnyError>> + Send;
// }

// /// A trait for testing packet connection
// pub trait TestPacketConnection: Send + Sync + 'static {
//   /// The stream type
//   type Stream: TestPacketStream;

//   /// Accept from remote
//   fn accept(&self) -> impl Future<Output = Result<Self::Stream, AnyError>> + Send;

//   /// Connect to the remote address
//   fn connect(&self) -> impl Future<Output = Result<Self::Stream, AnyError>> + Send;
// }

// /// The client used to send/receive data to a transport
// pub trait TestPacketClient: Sized + Send + Sync + 'static {
//   /// The connection type
//   type Connection: TestPacketConnection;
//   /// Accept from remote
//   fn accept(&mut self) -> impl Future<Output = Result<Self::Connection, AnyError>> + Send;

//   /// Connect to the remote address
//   fn connect(
//     &self,
//     addr: SocketAddr,
//   ) -> impl Future<Output = Result<Self::Connection, AnyError>> + Send;

//   /// Local address of the client
//   fn local_addr(&self) -> SocketAddr;

//   /// Close the client
//   fn close(&mut self) -> impl Future<Output = ()> + Send;
// }

// /// A trait for testing promised stream
// pub trait TestPromisedStream: Send + Sync + 'static {
//   /// finish the stream
//   fn finish(&mut self) -> impl Future<Output = Result<(), AnyError>> + Send;
// }

// /// A trait for testing promised connection
// pub trait TestPromisedConnection: Send + Sync + 'static {
//   /// The stream type
//   type Stream: TestPromisedStream;

//   /// Accept from remote
//   fn accept(&self) -> impl Future<Output = Result<(Self::Stream, SocketAddr), AnyError>> + Send;

//   /// Connect to the remote address
//   fn connect(&self) -> impl Future<Output = Result<Self::Stream, AnyError>> + Send;
// }

// /// The client used to send/receive data to a transport
// pub trait TestPromisedClient: Sized + Send + Sync + 'static {
//   /// The stream type
//   type Stream: TestPromisedStream;
//   /// The connection type
//   type Connection: TestPromisedConnection<Stream = Self::Stream>;

//   /// Connect to the remote address
//   fn connect(
//     &self,
//     addr: SocketAddr,
//   ) -> impl Future<Output = Result<Self::Connection, AnyError>> + Send;

//   /// Accept a connection from the remote
//   fn accept(&self) -> impl Future<Output = Result<Self::Connection, AnyError>> + Send;

//   /// Local address of the client
//   fn local_addr(&self) -> std::io::Result<SocketAddr>;

//   /// Close the client
//   fn close(&self) -> impl Future<Output = Result<(), AnyError>> + Send;
// }

// /// Unit test for handling [`Ping`] message
// pub async fn handle_ping<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   C: TestPacketClient,
//   R: RuntimeLite,
// {
//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
//   let source_addr = client.local_addr();

//   // Encode a ping
//   let ping = Ping::new(
//     42,
//     Node::new("test".into(), source_addr),
//     m.advertise_node(),
//   );

//   let buf = m
//     .encoder([Message::from(ping)])
//     .encode()
//     .next()
//     .unwrap()
//     .unwrap();
//   // Send
//   let connection = client.connect(*m.advertise_address()).await?;
//   let mut send_stream = connection.connect().await?;
//   if let Err(e) = send_stream.send_to(buf.as_slice()).await {
//     panic!("failed to send: {}", e);
//   }

//   // Wait for response
//   let (tx, rx) = async_channel::bounded(1);
//   let tx1 = tx.clone();
//   R::spawn_detach(async move {
//     futures::select! {
//       _ = rx.recv().fuse() => {},
//       _ = R::sleep(TIMEOUT_DURATION).fuse() => {
//         tx1.close();
//       }
//     }
//   });

//   let (in_, _) = R::timeout(WAIT_DURATION, async {
//     let connection = client.accept().await.unwrap();
//     let mut recv_stream = connection.accept().await.unwrap();
//     recv_stream.recv_from().await.unwrap()
//   })
//   .await
//   .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))?;
//   let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
//   assert_eq!(
//     ack.sequence_number(),
//     42,
//     "bad sequence no: {}",
//     ack.sequence_number()
//   );

//   let res = futures::select! {
//     res = tx.send(()).fuse() => {
//       if res.is_err() {
//         Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
//       } else {
//         Ok(())
//       }
//     }
//   };
//   let _ = m.shutdown().await;
//   client.close().await;
//   res
// }

// /// Unit test for handle compound ping functionality.
// pub async fn handle_compound_ping<A, T, C, E, R>(
//   trans: T,
//   mut client: C,
//   encoder: E,
// ) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
//   C: TestPacketClient,
//   E: FnOnce(&[Message<SmolStr, SocketAddr>]) -> Result<Bytes, AnyError>,
//   R: RuntimeLite,
// {
//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;

//   let source_addr = client.local_addr();

//   // Encode a ping
//   let ping = Ping::new(
//     42,
//     Node::new("test".into(), source_addr),
//     m.advertise_node(),
//   );
//   let msgs = [
//     Message::from(ping.cheap_clone()),
//     Message::from(ping.cheap_clone()),
//     Message::from(ping.cheap_clone()),
//   ];
//   let buf = encoder(&msgs)?;

//   // Send
//   let connection = client.connect(*m.advertise_address()).await?;
//   let mut send_stream = connection.connect().await?;
//   send_stream.send_to(&buf).await?;

//   // Wait for response
//   let (tx, rx) = async_channel::bounded(1);
//   let tx1 = tx.clone();
//   R::spawn_detach(async move {
//     futures::select! {
//       _ = rx.recv().fuse() => {},
//       _ = R::sleep(TIMEOUT_DURATION).fuse() => {
//         tx1.close();
//       }
//     }
//   });

//   let connection = R::timeout(WAIT_DURATION, client.accept()).await??;
//   for _ in 0..3 {
//     let (in_, _) = R::timeout(WAIT_DURATION, async {
//       let mut recv_stream = connection.accept().await?;
//       recv_stream.recv_from().await
//     })
//     .await
//     .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;
//     let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
//     assert_eq!(
//       ack.sequence_number(),
//       42,
//       "bad sequence no: {}",
//       ack.sequence_number()
//     );
//   }

//   let res = futures::select! {
//     res = tx.send(()).fuse() => {
//       if res.is_err() {
//         Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
//       } else {
//         Ok(())
//       }
//     }
//   };

//   let _ = m.shutdown().await;
//   client.close().await;
//   res
// }

// /// Unit test for handle indirect ping functionality.
// pub async fn handle_indirect_ping<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   C: TestPacketClient,
//   R: RuntimeLite,
// {
//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;

//   let source_addr = client.local_addr();

//   // Encode a ping
//   let ping = IndirectPing::new(
//     100,
//     Node::new("test".into(), source_addr),
//     m.advertise_node(),
//   );

//   let buf = m
//     .encoder([Message::from(ping)])
//     .encode()
//     .next()
//     .unwrap()
//     .unwrap();
//   // Send
//   let connection = client.connect(*m.advertise_address()).await?;
//   let mut send_stream = connection.connect().await?;
//   send_stream.send_to(buf.as_slice()).await?;

//   // Wait for response
//   let (tx, rx) = async_channel::bounded(1);
//   let tx1 = tx.clone();
//   R::spawn_detach(async move {
//     futures::select! {
//       _ = rx.recv().fuse() => {},
//       _ = R::sleep(TIMEOUT_DURATION).fuse() => {
//         tx1.close();
//       }
//     }
//   });

//   let (in_, _) = R::timeout(WAIT_DURATION, async {
//     let connection = client.accept().await?;
//     let mut recv_stream = connection.accept().await?;
//     recv_stream.recv_from().await
//   })
//   .await
//   .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;
//   let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
//   assert_eq!(
//     ack.sequence_number(),
//     100,
//     "bad sequence no: {}",
//     ack.sequence_number()
//   );
//   let res = futures::select! {
//     res = tx.send(()).fuse() => {
//       if res.is_err() {
//         Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
//       } else {
//         Ok(())
//       }
//     }
//   };

//   let _ = m.shutdown().await;
//   client.close().await;
//   res
// }

// /// Unit test for handle ping wrong node functionality.
// pub async fn handle_ping_wrong_node<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   C: TestPacketClient,
//   R: RuntimeLite,
// {
//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
//   let source_addr = client.local_addr();

//   // Encode a ping
//   let ping: Ping<SmolStr, _> = Ping::new(
//     42,
//     Node::new("test".into(), source_addr),
//     Node::new("bad".into(), {
//       let mut a = source_addr;
//       a.set_port(12345);
//       a
//     }),
//   );

//   let buf = m.encoder([ping.into()]).encode().next().unwrap().unwrap();
//   // Send
//   let connection = client.connect(*m.advertise_address()).await?;
//   let mut send_stream = connection.connect().await?;
//   send_stream.send_to(buf.as_slice()).await?;

//   // Wait for response
//   let res = R::timeout(WAIT_DURATION, async {
//     let connection = client.accept().await?;
//     let mut recv_stream = connection.accept().await?;
//     recv_stream.recv_from().await
//   })
//   .await;

//   #[allow(clippy::single_match)]
//   match res {
//     Ok(Ok(_)) => {
//       panic!("should got timeout error");
//     }
//     _ => {}
//   }
//   let _ = m.shutdown().await;
//   client.close().await;
//   Ok(())
// }

// /// Unit test for send packet piggyback functionality.
// pub async fn send_packet_piggyback<A, T, C, D, R>(
//   trans: T,
//   mut client: C,
//   decoder: D,
// ) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   C: TestPacketClient,
//   D: FnOnce(Bytes) -> Result<[Message<SmolStr, SocketAddr>; 2], AnyError>,
//   R: RuntimeLite,
// {
//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
//   let source_addr = client.local_addr();

//   // Add a message to be broadcast
//   let n: Node<SmolStr, SocketAddr> = Node::new("rand".into(), *m.advertise_address());
//   let a = Alive::new(10, n.clone()).with_meta(Meta::empty());
//   m.broadcast(n.id().clone(), Message::from(a)).await;

//   // Encode a ping
//   let ping = Ping::new(
//     42,
//     Node::new("test".into(), source_addr),
//     m.advertise_node(),
//   );

//   let buf = m.encoder([ping.into()]).encode().next().unwrap().unwrap();
//   // Send
//   let connection = client.connect(*m.advertise_address()).await?;
//   let mut send_stream = connection.connect().await?;
//   send_stream.send_to(buf.as_slice()).await?;

//   // Wait for response
//   let (tx, rx) = async_channel::bounded(1);
//   let tx1 = tx.clone();
//   R::spawn_detach(async move {
//     futures::select! {
//       _ = rx.recv().fuse() => {},
//       _ = R::sleep(TIMEOUT_DURATION).fuse() => {
//         tx1.close();
//       }
//     }
//   });

//   let (in_, _) = R::timeout(WAIT_DURATION, async {
//     let connection = client.accept().await?;
//     let mut recv_stream = connection.accept().await?;
//     recv_stream.recv_from().await
//   })
//   .await
//   .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))??;

//   // get the parts
//   let parts = decoder(in_)?;

//   let [m1, m2] = parts;
//   let ack = m1.unwrap_ack();
//   assert_eq!(ack.sequence_number(), 42, "bad sequence no");

//   let alive = m2.unwrap_alive();
//   assert_eq!(alive.incarnation(), 10);
//   assert_eq!(alive.node(), &n);

//   let res = futures::select! {
//     res = tx.send(()).fuse() => {
//       if res.is_err() {
//         Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout").into())
//       } else {
//         Ok(())
//       }
//     }
//   };
//   let _ = m.shutdown().await;
//   client.close().await;
//   res
// }

// macro_rules! unwrap_ok {
//   ($tx:ident.send($expr:expr)) => {
//     match $expr {
//       ::std::result::Result::Ok(val) => val,
//       ::std::result::Result::Err(e) => {
//         $tx.send(e).await.unwrap();
//         return;
//       }
//     }
//   };
// }

// macro_rules! panic_on_err {
//   ($expr:expr) => {
//     match $expr {
//       ::std::result::Result::Ok(val) => val,
//       ::std::result::Result::Err(e) => {
//         panic!("{}", e);
//       }
//     }
//   };
// }

// /// Unit test for send ping through promised connection.
// pub async fn promised_ping<A, T, P, R>(
//   trans: T,
//   promised: P,
//   kind: AddressKind,
// ) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   P: TestPromisedClient,
//   P::Stream: TestPromisedStream + AsMut<T::Connection>,
//   R: RuntimeLite,
// {
//   let promised_addr = promised.local_addr()?;
//   let promised = Arc::new(promised);

//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
//   let ping_timeout = m.inner.opts.probe_interval + Duration::from_secs(2);
//   let ping_time_max = m.inner.opts.probe_interval + Duration::from_secs(10);

//   // Do a normal rount trip
//   let node = Node::new("mongo".into(), kind.next(0));
//   let ping_out = Ping::new(23, m.advertise_node(), node.cheap_clone());

//   let (ping_err_tx, ping_err_rx) = async_channel::bounded::<AnyError>(1);
//   let m1 = m.clone();
//   let node1 = node.cheap_clone();
//   let p1 = promised.clone();
//   let ping_err_tx1 = ping_err_tx.clone();
//   let (tx, rx) = async_channel::bounded(1);
//   let (tx1, rx1) = async_channel::bounded(1);
//   let (tx2, rx2) = async_channel::bounded(1);
//   let (tx3, rx3) = async_channel::bounded(1);
//   R::spawn_detach(async move {
//     let mut num_accepted = 0;
//     let acceptor = unwrap_ok!(ping_err_tx1.send(unwrap_ok!(ping_err_tx1.send(
//       R::timeout(ping_time_max, p1.accept())
//         .await
//         .map_err(Into::into)
//     ))));
//     loop {
//       futures::select! {
//         stream = acceptor.accept().fuse() => {
//           num_accepted += 1;
//           match stream {
//             Ok((mut stream, addr)) if num_accepted == 1 => {
//               let p = unwrap_ok!(ping_err_tx1.send(
//                 m1.read_message(&addr, stream.as_mut())
//                   .await
//                   .map_err(Into::into)
//               ));
//               let ping_in = Message::decode(&p).unwrap().1.unwrap_ping();
//               assert_eq!(ping_in.sequence_number(), 23);
//               assert_eq!(ping_in.target(), &node1);

//               let ack: Message<SmolStr, SocketAddr> = Ack::new(23).into();

//               unwrap_ok!(ping_err_tx1.send(
//                 m1.send_message(stream.as_mut(), [ack])
//                   .await
//                   .map_err(Into::into)
//               ));

//               let _ = TestPromisedStream::finish(&mut stream).await;
//               // wait for the next ping
//               let _ = rx1.recv().await;
//             },
//             Ok((mut stream, addr)) if num_accepted == 2 => {
//               let p = unwrap_ok!(ping_err_tx1.send(
//                 m1
//                   .read_message(&addr, stream.as_mut())
//                   .await
//                   .map_err(Into::into)
//               ));

//               let ping_in = Message::<SmolStr, SocketAddr>::decode(&p).unwrap().1.unwrap_ping();
//               let ack: Message<SmolStr, SocketAddr> = Ack::new(ping_in.sequence_number() + 1).into();

//               unwrap_ok!(ping_err_tx1.send(
//                 m1.send_message(stream.as_mut(), [ack])
//                   .await
//                   .map_err(Into::into)
//               ));

//               // wait for the next ping
//               let _ = stream.finish().await;
//               let _ = rx2.recv().await;
//             },
//             Ok((mut stream, addr)) if num_accepted == 3 => {
//               let _ = unwrap_ok!(ping_err_tx1.send(
//                 m1.inner
//                   .transport
//                   .read(&addr, stream.as_mut())
//                   .await
//                   .map_err(Into::into)
//               ));

//               unwrap_ok!(ping_err_tx1.send(
//                 m1.send_message(
//                     stream.as_mut(),
//                     [Message::from(IndirectPing::new(0, Node::new(SmolStr::from("unknown source"), kind.next(0)), Node::new(SmolStr::from("unknown target"), kind.next(0))))]
//                   )
//                   .await
//                   .map_err(Into::into)
//               ));

//               let _ = stream.finish().await;
//               // wait for the next ping
//               let _ = rx3.recv().await;
//             }
//             Ok(_) => {
//               let _ = rx.recv().await;
//             },
//             Err(e) => {
//               let _ = ping_err_tx1.send(e).await;
//             }
//           }
//         }
//         _ = rx.recv().fuse() => {
//           return;
//         }
//       }
//     }
//   });

//   let did_contact = panic_on_err!(
//     m.send_ping_and_wait_for_ack(&promised_addr, ping_out.clone(), R::now() + ping_timeout)
//       .await
//   );

//   if !did_contact {
//     return Err(std::io::Error::new(std::io::ErrorKind::Other, "expected successful ping").into());
//   }

//   if !ping_err_rx.is_empty() {
//     return Err(ping_err_rx.recv().await.unwrap());
//   }
//   let _ = tx1.send(()).await;

//   // Make sure a mis-matched sequence number is caught.
//   let err = m
//     .send_ping_and_wait_for_ack(&promised_addr, ping_out.clone(), R::now() + ping_timeout)
//     .await
//     .expect_err("expected failed ping");
//   if !err
//     .to_string()
//     .contains("sequence number mismatch: ping(23), ack(24)")
//   {
//     panic!("should catch sequence number mismatch err, but got err: {err}");
//   }

//   if !ping_err_rx.is_empty() {
//     panic!("{}", ping_err_rx.recv().await.unwrap());
//   }
//   let _ = tx2.send(()).await;

//   // Make sure an unexpected message type is handled gracefully.
//   let err = m
//     .send_ping_and_wait_for_ack(&promised_addr, ping_out.clone(), R::now() + ping_timeout)
//     .await
//     .expect_err("expected failed ping");

//   if !err
//     .to_string()
//     .contains("unexpected message: expected Ack, got IndirectPing")
//   {
//     panic!("should catch unexpected message type err, but got err: {err}");
//   }

//   if !ping_err_rx.is_empty() {
//     panic!("{}", ping_err_rx.recv().await.unwrap());
//   }
//   let _ = tx3.send(()).await;
//   let _ = tx.send(()).await;

//   // Make sure failed I/O respects the deadline. In this case we try the
//   // common case of the receiving node being totally down.
//   let _ = promised.close().await;
//   drop(promised);

//   // Wait the listener task fully exit.
//   R::sleep(Duration::from_secs(1)).await;

//   let start_ping = R::now();
//   let did_contact = m
//     .send_ping_and_wait_for_ack(&promised_addr, ping_out, R::now() + ping_timeout)
//     .await?;
//   let elapsed = start_ping.elapsed();
//   assert!(!did_contact, "expected failed ping");
//   assert!(
//     elapsed <= ping_time_max,
//     "elapsed: {:?}, take too long to fail ping",
//     elapsed
//   );
//   let _ = m.shutdown().await;
//   Ok(())
// }

// /// Unit test for send push/pull through promised connection.
// pub async fn promised_push_pull<A, T, P, R>(trans: T, promised: P) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   P: TestPromisedClient,
//   P::Stream: TestPromisedStream + AsMut<T::Connection>,
//   R: RuntimeLite,
// {
//   let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;
//   let bind_addr = *m.advertise_address();
//   let id0: SmolStr = "Test 0".into();
//   {
//     let mut members = m.inner.nodes.write().await;
//     members.nodes.push(Member {
//       state: LocalNodeState {
//         server: Arc::new(NodeState::new(id0.cheap_clone(), bind_addr, State::Alive)),
//         incarnation: Arc::new(0.into()),
//         state_change: Epoch::now() - Duration::from_secs(1),
//         state: State::Suspect,
//       },
//       suspicion: None,
//     });

//     members.node_map.insert(id0.cheap_clone(), 0);
//   }

//   let connector = promised.connect(bind_addr).await?;

//   let push_pull = PushPull::new(
//     false,
//     [
//       PushNodeState::new(1, id0.cheap_clone(), bind_addr, State::Alive),
//       PushNodeState::new(1, "Test 1".into(), bind_addr, State::Alive),
//       PushNodeState::new(1, "Test 2".into(), bind_addr, State::Alive),
//     ]
//     .into_iter(),
//   );

//   // Send the push/pull indicator
//   let mut conn = connector.connect().await?;
//   let push_pull: Message<_, _> = push_pull.into();
//   m.send_message(conn.as_mut(), [push_pull]).await?;
//   // Read the message type
//   let msg = m.read_message(&bind_addr, conn.as_mut()).await?;
//   let readed_push_pull = Message::<SmolStr, SocketAddr>::decode(&msg)
//     .unwrap()
//     .1
//     .unwrap_push_pull();
//   assert!(!readed_push_pull.join());
//   assert_eq!(readed_push_pull.states().len(), 1);
//   assert_eq!(readed_push_pull.states()[0].id(), &id0);
//   assert_eq!(
//     readed_push_pull.states()[0].incarnation(),
//     0,
//     "bad incarnation"
//   );
//   assert_eq!(
//     readed_push_pull.states()[0].state(),
//     State::Suspect,
//     "bad state"
//   );
//   m.shutdown().await?;
//   Ok(())
// }

// /// Unit test for join functionality for the transport.
// pub async fn join<A, T1, T2, R>(trans1: T1::Options, trans2: T2::Options) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T1: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   T2: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   R: RuntimeLite,
// {
//   let m1 = Memberlist::<T1, _>::new(trans1, Options::default())
//     .await
//     .map_err(|e| {
//       tracing::error!("fail to start memberlist node 1: {}", e);
//       e
//     })?;
//   let m2 = Memberlist::<T2, _>::new(trans2, Options::default())
//     .await
//     .map_err(|e| {
//       tracing::error!("fail to start memberlist node 2: {}", e);
//       e
//     })?;
//   m2.join(Node::new(
//     m1.local_id().cheap_clone(),
//     MaybeResolvedAddress::resolved(*m1.advertise_address()),
//   ))
//   .await
//   .map_err(|e| {
//     tracing::error!("fail to join: {}", e);
//     e
//   })?;
//   assert_eq!(m2.num_members().await, 2);
//   assert_eq!(m2.estimate_num_nodes(), 2);
//   m1.shutdown().await?;
//   m2.shutdown().await?;
//   Ok(())
// }

// /// Unit test for join a dead node
// pub async fn join_dead_node<A, T, P, R>(trans1: T::Options, promised: P, fake_id: T::Id)
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
//   P: TestPromisedClient,
//   R: RuntimeLite,
// {
//   let local_addr = promised.local_addr().unwrap();

//   let m = Memberlist::<T, _>::new(
//     trans1,
//     Options::default().with_timeout(Duration::from_millis(50)),
//   )
//   .await
//   .unwrap();

//   // Ensure we don't hang forever
//   R::spawn_detach(async move {
//     R::sleep(TIMEOUT_DURATION).await;
//     panic!("should have timed out by now");
//   });

//   let target = Node::new(fake_id, MaybeResolvedAddress::resolved(local_addr));
//   m.join(target).await.unwrap_err();
//   m.shutdown().await.unwrap();
//   promised.close().await.unwrap();
// }

// /// Unit test for send functionality for the transport.
// pub async fn send<A, T1, T2, R>(trans1: T1::Options, trans2: T2::Options) -> Result<(), AnyError>
// where
//   A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
//   T1: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   T2: Transport<Id = SmolStr, Resolver = A, ResolvedAddress = SocketAddr, Runtime = R>,
//   R: RuntimeLite,
// {
//   let m1 = Memberlist::<T1, _>::with_delegate(
//     CompositeDelegate::new().with_node_delegate(MockDelegate::<SmolStr, SocketAddr>::new()),
//     trans1,
//     Options::default(),
//   )
//   .await?;
//   let m2 = Memberlist::<T2, _>::new(trans2, Options::default()).await?;

//   m2.join(Node::new(
//     m1.local_id().cheap_clone(),
//     MaybeResolvedAddress::resolved(*m1.advertise_address()),
//   ))
//   .await?;
//   assert_eq!(m2.num_members().await, 2);
//   assert_eq!(m2.estimate_num_nodes(), 2);

//   m2.send(m1.advertise_address(), Bytes::from_static(b"send"))
//     .await
//     .map_err(|e| {
//       tracing::error!("fail to send packet {e}");
//       e
//     })
//     .unwrap();

//   m2.send_reliable(m1.advertise_address(), Bytes::from_static(b"send_reliable"))
//     .await
//     .map_err(|e| {
//       tracing::error!("fail to send message {e}");
//       e
//     })
//     .unwrap();

//   R::sleep(WAIT_DURATION).await;

//   let mut msgs1 = m1.delegate().unwrap().node_delegate().get_messages().await;
//   msgs1.sort();
//   assert_eq!(msgs1, ["send".as_bytes(), "send_reliable".as_bytes()]);
//   m1.shutdown().await?;
//   m2.shutdown().await?;
//   Ok(())
// }
