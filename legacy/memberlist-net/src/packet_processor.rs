use std::{
  net::SocketAddr,
  ops::ControlFlow,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use agnostic::{
  Runtime, RuntimeLite,
  net::{Net, UdpSocket as _},
};
use bytes::Bytes;
use memberlist_core::transport::{Packet, PacketProducer, Transport};
use nodecraft::resolver::AddressResolver;

pub(super) struct PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
  T::Runtime: Runtime,
{
  pub(super) packet_tx: PacketProducer<T::ResolvedAddress, <T::Runtime as RuntimeLite>::Instant>,
  pub(super) socket: Arc<<<T::Runtime as Runtime>::Net as Net>::UdpSocket>,
  pub(super) local_addr: SocketAddr,
  pub(super) shutdown: Arc<AtomicBool>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  #[cfg(feature = "metrics")]
  pub(super) metric_labels: std::sync::Arc<memberlist_core::proto::MetricLabels>,
}

impl<A, T> PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  A::Address: Send + Sync + 'static,
  T: Transport<Resolver = A, ResolvedAddress = SocketAddr>,
  T::Runtime: Runtime,
{
  pub(super) async fn run(self) {
    let Self {
      packet_tx,
      socket,
      shutdown,
      local_addr,
      ..
    } = self;

    tracing::info!("memberlist_net: udp listening on {local_addr}");

    let mut buf = vec![0; 65536];
    loop {
      let fut1 = self.shutdown_rx.recv();
      let fut2 = async {
        match socket.recv_from(&mut buf).await {
          Ok((n, addr)) => {
            // Check the length - it needs to have at least one byte to be a
            // proper message.
            if n < 1 {
              tracing::error!(local=%local_addr, from=%addr, err = "memberlist_net.packet.processor: UDP packet too short (0 bytes)");
              return ControlFlow::Continue(());
            }

            tracing::trace!(local=%local_addr, from=%addr, packet=?&buf[..n], "memberlist_net.packet.processor");

            let start = <T::Runtime as RuntimeLite>::now();

            // #[cfg(feature = "metrics")]
            // {
            //   use agnostic::time::Instant;

            //   metrics::counter!("memberlist.packet.bytes.processing", self.metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);
            // }

            if let Err(e) = packet_tx
              .send(Packet::new(addr, start, Bytes::copy_from_slice(&buf[..n])))
              .await
            {
              tracing::error!(local=%local_addr, from=%addr, err = %e, "memberlist_net.packet: failed to send packet");
            }

            #[cfg(feature = "metrics")]
            metrics::counter!("memberlist.packet.received", self.metric_labels.iter())
              .increment(n as u64);
            ControlFlow::Continue(())
          }
          Err(e) => {
            if shutdown.load(Ordering::SeqCst) {
              return ControlFlow::Break(());
            }

            tracing::error!(local=%local_addr, err = %e, "memberlist_net.packet: error reading UDP packet");
            ControlFlow::Continue(())
          }
        }
      };

      futures::pin_mut!(fut1, fut2);

      match futures::future::select(fut1, fut2).await {
        futures::future::Either::Left((_, _)) => break,
        futures::future::Either::Right((flow, _)) => match flow {
          ControlFlow::Continue(_) => continue,
          ControlFlow::Break(_) => break,
        },
      }
    }
    drop(socket);
    tracing::info!(
      "memberlist.transport.net: packet processor on {} exit",
      local_addr
    );
  }
}
