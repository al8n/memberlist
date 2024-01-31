use std::{future::Future, net::SocketAddr, time::Duration};

use agnostic::{net::{Net, UdpSocket}, Runtime};
use futures::{FutureExt, Stream};
use memberlist_core::{types::{Ack, Ping}, transport::{Node, Wire, AddressResolver}, Memberlist, Options};
use smol_str::SmolStr;

use crate::{NetTransport, NetTransportOptions, StreamLayer};


async fn listen_udp<R>(addr: SocketAddr) -> Result<<R::Net as Net>::UdpSocket, std::io::Error>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  <<R::Net as Net>::UdpSocket as UdpSocket>::bind(addr).await
}

/// Unit test for handling [`Ping`] message
pub async fn handle_ping<A, S, R, PE, AD, W>(
  resolver: A,
  stream_layer: S,
  opts: NetTransportOptions<SmolStr, A>,
  ping_encoder: PE,
  ack_decoder: AD,
  client_addr: SocketAddr,
)
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  PE: FnOnce(Ping<SmolStr, SocketAddr>) -> Vec<u8>,
  AD: FnOnce(&[u8]) -> Ack,
  W: Wire<Id = SmolStr, Address = SocketAddr>,
{
  let trans = NetTransport::<_, _, S, W>::new(resolver, stream_layer, opts).await.expect("failed to create transport");
  let m = Memberlist::new(trans, Options::default()).await.expect("failed to create memberlist");

  let udp = listen_udp::<R>(client_addr).await.unwrap();

  let udp_addr = udp.local_addr().unwrap();

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: Node::new("test".into(), udp_addr),
    target: m.advertise_node(),
  };

  let buf = ping_encoder(ping);
  // Send
  udp
    .send_to(&buf, m.advertise_addr())
    .await
    .expect("failed to send ping");

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

  let ack = ack_decoder(&in_);
  assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);
  tx.send(()).unwrap();
}
