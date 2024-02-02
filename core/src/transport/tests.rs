use std::{future::Future, net::SocketAddr, time::Duration};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, Node};
use smol_str::SmolStr;
use transformable::Transformable;

use crate::{
  delegate::VoidDelegate,
  tests::{get_memberlist, AnyError},
  transport::Message,
  Options,
};

use super::{Ping, Transport};

/// The client used to send/receive data to a transport
pub trait Client: Sized + Send + Sync + 'static {
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

/// Unit test for handling [`Ping`] message
pub async fn handle_ping<A, T, C, R>(trans: T, mut client: C) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  T: Transport<Id = SmolStr, Resolver = A, Runtime = R>,
  C: Client,
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
  let (tx, rx) = futures::channel::oneshot::channel();
  R::spawn_detach(async {
    futures::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let (in_, _) = client.recv_from().await?;
  let ack = Message::<SmolStr, SocketAddr>::decode(&in_).map(|(_, msg)| msg.unwrap_ack())?;
  assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);
  tx.send(()).unwrap();
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
