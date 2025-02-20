use super::*;
use futures::AsyncReadExt;
use memberlist_core::transport::Packet;

pub(super) struct Processor<
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A>,
  S: StreamLayer<Runtime = A::Runtime>,
> {
  pub(super) local_addr: SocketAddr,
  pub(super) acceptor: S::Acceptor,
  pub(super) packet_tx: PacketProducer<
    <T::Resolver as AddressResolver>::ResolvedAddress,
    <T::Runtime as RuntimeLite>::Instant,
  >,
  pub(super) stream_tx:
    StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  pub(super) timeout: Option<Duration>,
  #[cfg(feature = "metrics")]
  pub(super) metric_labels: Arc<memberlist_core::types::MetricLabels>,
}

impl<A, T, S> Processor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  A::Address: Send + Sync + 'static,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer<Runtime = A::Runtime>,
{
  pub(super) async fn run(self) {
    let Self {
      acceptor,
      packet_tx,
      stream_tx,
      shutdown_rx,
      local_addr,
      timeout,
      #[cfg(feature = "metrics")]
      metric_labels,
    } = self;

    Self::listen(
      local_addr,
      acceptor,
      stream_tx,
      packet_tx,
      shutdown_rx,
      timeout,
      #[cfg(feature = "metrics")]
      metric_labels,
    )
    .await;
  }

  async fn listen(
    local_addr: SocketAddr,
    mut acceptor: S::Acceptor,
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    packet_tx: PacketProducer<
      <T::Resolver as AddressResolver>::ResolvedAddress,
      <T::Runtime as RuntimeLite>::Instant,
    >,
    shutdown_rx: async_channel::Receiver<()>,
    timeout: Option<Duration>,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_core::types::MetricLabels>,
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
      futures::select! {
        _ = shutdown_rx.recv().fuse() => {
          tracing::info!(local=%local_addr, "memberlist.transport.quic: shutdown stream listener");
          break;
        }
        connection = acceptor.accept().fuse() => {
          match connection {
            Ok((connection, remote_addr)) => {
              let shutdown_rx = shutdown_rx.clone();
              let packet_tx = packet_tx.clone();
              let stream_tx = stream_tx.clone();
              #[cfg(feature = "metrics")]
              let metric_labels = metric_labels.clone();
              <T::Runtime as RuntimeLite>::spawn_detach(async move {
                Self::handle_connection(
                  connection,
                  local_addr,
                  remote_addr,
                  stream_tx,
                  packet_tx,
                  timeout,
                  shutdown_rx,
                  #[cfg(feature = "metrics")] metric_labels,
                ).await;
              });
            }
            Err(e) => {
              if shutdown_rx.is_closed() {
                break;
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
              continue;
            }
          }
        }
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
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    packet_tx: PacketProducer<
      <T::Resolver as AddressResolver>::ResolvedAddress,
      <T::Runtime as RuntimeLite>::Instant,
    >,
    timeout: Option<Duration>,
    shutdown_rx: async_channel::Receiver<()>,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_core::types::MetricLabels>,
  ) {
    loop {
      futures::select! {
        incoming = conn.accept_bi().fuse() => {
          match incoming {
            Ok((mut stream, remote_addr)) => {
              let mut stream_kind_buf = [0; 1];
              if let Err(e) = stream.read_exact(&mut stream_kind_buf).await {
                tracing::error!(local=%local_addr, from=%remote_addr, err = %e, "memberlist.transport.quic: failed to read stream kind");
                continue;
              }
              let stream_kind = stream_kind_buf[0];
              match StreamType::try_from(stream_kind) {
                Ok(StreamType::Stream) => {
                  if let Err(e) = stream_tx
                    .send(remote_addr, stream)
                    .await
                  {
                    tracing::error!(local_addr=%local_addr, err = %e, "memberlist.transport.quic: failed to send stream connection");
                  }
                }
                Ok(StreamType::Packet) => {
                  Self::handle_packet(
                    &mut stream,
                    local_addr,
                    remote_addr,
                    &packet_tx,
                    timeout,
                    #[cfg(feature = "metrics")] &metric_labels,
                  ).await;
                }
                Err(val) => {
                  tracing::error!(local=%local_addr, from=%remote_addr, tag=%val, "memberlist.transport.quic: unknown stream");
                  break;
                }
              }
            }
            Err(e) => {
              tracing::debug!(local=%local_addr, from=%remote_addr, err = %e, "memberlist.transport.quic: failed to accept stream, shutting down the connection handler");
              break;
            }
          }
        },
        _ = shutdown_rx.recv().fuse() => {
          break;
        }
      }
    }

    tracing::debug!(local=%local_addr, remote=%remote_addr, "memberlist.transport.quic: connection handler exits");
    let _ = conn.close().await;
  }

  #[allow(clippy::too_many_arguments)]
  async fn handle_packet(
    stream: &mut S::Stream,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    packet_tx: &PacketProducer<
      <T::Resolver as AddressResolver>::ResolvedAddress,
      <T::Runtime as RuntimeLite>::Instant,
    >,
    timeout: Option<Duration>,
    #[cfg(feature = "metrics")] metric_labels: &memberlist_core::types::MetricLabels,
  ) {
    let start = <T::Runtime as RuntimeLite>::now();

    let res = if let Some(timeout) = timeout {
      match <T::Runtime as RuntimeLite>::timeout(timeout, stream.read_packet()).await {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(e.into()),
      }
    } else {
      stream.read_packet().await
    };

    let msg = match res {
      Ok(msg) => msg,
      Err(e) => {
        tracing::error!(local=%local_addr, from=%remote_addr, err = %e, "memberlist_quic.packet: fail to handle UDP packet");
        return;
      }
    };
    tracing::trace!(local=%local_addr, from=%remote_addr, len = %msg.len(), data=?msg.as_ref(), "memberlist_quic.packet: received packet");
    let _read = msg.len();

    if let Err(e) = packet_tx.send(Packet::new(remote_addr, start, msg)).await {
      tracing::error!(local=%local_addr, from=%remote_addr, err = %e, "memberlist_quic.packet: failed to send packet");
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("memberlist.packet.received", metric_labels.iter()).increment(_read as u64);
  }
}
