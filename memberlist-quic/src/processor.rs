use std::ops::ControlFlow;

use super::*;
use memberlist_core::transport::Packet;

pub(super) struct Processor<
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A>,
  S: StreamLayer<Runtime = A::Runtime>,
> {
  pub(super) local_addr: SocketAddr,
  pub(super) acceptor: S::Acceptor,
  pub(super) packet_tx: PacketProducer<T::ResolvedAddress, <T::Runtime as RuntimeLite>::Instant>,
  pub(super) stream_tx: StreamProducer<T::ResolvedAddress, T::Connection>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  #[cfg(feature = "metrics")]
  pub(super) metric_labels: Arc<memberlist_core::proto::MetricLabels>,
}

impl<A, T, S> Processor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  A::Address: Send + Sync + 'static,
  T: Transport<
      Resolver = A,
      ResolvedAddress = SocketAddr,
      Connection = S::Stream,
      Runtime = A::Runtime,
    >,
  S: StreamLayer<Runtime = A::Runtime>,
{
  pub(super) async fn run(self) {
    let Self {
      acceptor,
      packet_tx,
      stream_tx,
      shutdown_rx,
      local_addr,
      #[cfg(feature = "metrics")]
      metric_labels,
    } = self;

    Self::listen(
      local_addr,
      acceptor,
      stream_tx,
      packet_tx,
      shutdown_rx,
      #[cfg(feature = "metrics")]
      metric_labels,
    )
    .await;
  }

  async fn listen(
    local_addr: SocketAddr,
    mut acceptor: S::Acceptor,
    stream_tx: StreamProducer<T::ResolvedAddress, T::Connection>,
    packet_tx: PacketProducer<T::ResolvedAddress, <T::Runtime as RuntimeLite>::Instant>,
    shutdown_rx: async_channel::Receiver<()>,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_core::proto::MetricLabels>,
  ) {
    tracing::info!("memberlist.transport.quic: listening stream on {local_addr}");

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let mut loop_delay = Duration::ZERO;
    loop {
      let fut1 = shutdown_rx.recv();
      let fut2 = async {
        match acceptor.accept().await {
          Ok((connection, remote_addr)) => {
            // Reset backoff delay on successful accept
            loop_delay = Duration::ZERO;

            let shutdown_rx = shutdown_rx.clone();
            let packet_tx = packet_tx.clone();
            let stream_tx = stream_tx.clone();
            #[cfg(feature = "metrics")]
            let metric_labels = metric_labels.clone();

            <T::Runtime as RuntimeLite>::spawn_detach(Self::handle_connection(
              connection,
              local_addr,
              remote_addr,
              stream_tx,
              packet_tx,
              shutdown_rx,
              #[cfg(feature = "metrics")]
              metric_labels,
            ));
            ControlFlow::Continue(())
          }
          Err(e) => {
            if shutdown_rx.is_closed() {
              return ControlFlow::Break(());
            }

            if loop_delay == Duration::ZERO {
              loop_delay = BASE_DELAY;
            } else {
              loop_delay *= 2;
            }

            if loop_delay > MAX_DELAY {
              loop_delay = MAX_DELAY;
            }

            tracing::error!(local_addr=%local_addr, err = %e, "memberlist.transport.quic: error accepting stream connection");
            <T::Runtime as RuntimeLite>::sleep(loop_delay).await;
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

    tracing::debug!(local=%local_addr, "memberlist.transport.quic: processor exits");
    let _ = acceptor.close().await;
  }

  #[allow(clippy::too_many_arguments)]
  async fn handle_connection(
    conn: S::Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    stream_tx: StreamProducer<T::ResolvedAddress, T::Connection>,
    packet_tx: PacketProducer<T::ResolvedAddress, <T::Runtime as RuntimeLite>::Instant>,
    shutdown_rx: async_channel::Receiver<()>,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_core::proto::MetricLabels>,
  ) {
    // Spawn a detached task to listen for datagrams on this connection.
    // Datagrams are a separate channel from streams in QUIC, so they
    // arrive independently of bidirectional streams.
    {
      let datagram_packet_tx = packet_tx.clone();
      let datagram_conn = conn.clone();
      #[cfg(feature = "metrics")]
      let datagram_metric_labels = metric_labels.clone();
      <T::Runtime as RuntimeLite>::spawn_detach(async move {
        loop {
          match datagram_conn.recv_datagram().await {
            Ok(bytes) => {
              let start = <T::Runtime as RuntimeLite>::now();
              if let Err(e) = datagram_packet_tx
                .send(Packet::new(remote_addr, start, bytes.clone()))
                .await
              {
                tracing::error!(local=%local_addr, from=%remote_addr, err = %e, "memberlist_quic.packet: failed to send packet from datagram");
              }
              #[cfg(feature = "metrics")]
              metrics::counter!("memberlist.packet.received", datagram_metric_labels.iter())
                .increment(bytes.len() as u64);
            }
            Err(e) => {
              tracing::debug!(local=%local_addr, from=%remote_addr, err = %e, "memberlist_quic.packet: datagram stream closed");
              return;
            }
          }
        }
      });
    }

    // Listen for shutdown signal in a select with accept_bi() so that
    // accept_bi() does not block shutdown indefinitely.
    let shutdown_for_select = shutdown_rx.clone();
    loop {
      let fut1 = shutdown_for_select.recv();
      let fut2 = conn.accept_bi();

      futures::pin_mut!(fut1, fut2);

      match futures::future::select(fut1, fut2).await {
        futures::future::Either::Left((_, _)) => break,
        futures::future::Either::Right((accept_result, _)) => {
          let (stream, stream_remote_addr) = match accept_result {
            Ok(pair) => pair,
            Err(e) => {
              tracing::debug!(local=%local_addr, from=%remote_addr, err = %e, "memberlist.transport.quic: failed to accept stream, shutting down the connection handler");
              break;
            }
          };

          // Spawn a detached task to handle this single stream so that
          // one slow/blocked stream does not prevent accepting others.
          let stream_tx = stream_tx.clone();

          <T::Runtime as RuntimeLite>::spawn_detach(async move {
            if let Err(e) = stream_tx.send(stream_remote_addr, stream).await {
              tracing::error!(local_addr=%local_addr, err = %e,
                "memberlist.transport.quic: failed to send stream connection");
            }
          });
        }
      }
    }

    tracing::debug!(local=%local_addr, remote=%remote_addr, "memberlist.transport.quic: connection handler exits");
    let _ = conn.close().await;
  }

}
