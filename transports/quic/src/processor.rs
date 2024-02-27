use memberlist_core::{
  transport::{
    stream::{PacketProducer, StreamProducer},
    TimeoutableReadStream,
  },
  types::Packet,
};
use memberlist_utils::OneOrMore;

use super::*;

pub(super) struct Processor<
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A>,
  S: StreamLayer,
> {
  pub(super) label: Label,
  pub(super) local_addr: SocketAddr,
  pub(super) acceptor: S::Acceptor,
  pub(super) packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  pub(super) stream_tx:
    StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,

  pub(super) shutdown: Arc<AtomicBool>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,

  pub(super) skip_inbound_label_check: bool,
  pub(super) timeout: Option<Duration>,

  #[cfg(feature = "compression")]
  pub(super) offload_size: usize,

  #[cfg(feature = "metrics")]
  pub(super) metric_labels: Arc<memberlist_utils::MetricLabels>,
}

impl<A, T, S> Processor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream>,
  S: StreamLayer,
{
  pub(super) async fn run(self) {
    let Self {
      acceptor,
      packet_tx,
      stream_tx,
      shutdown_rx,
      shutdown,
      local_addr,
      label,
      skip_inbound_label_check,
      timeout,
      #[cfg(feature = "compression")]
      offload_size,
      #[cfg(feature = "metrics")]
      metric_labels,
    } = self;

    Self::listen(
      local_addr,
      label,
      acceptor,
      stream_tx,
      packet_tx,
      shutdown,
      shutdown_rx,
      skip_inbound_label_check,
      timeout,
      #[cfg(feature = "compression")]
      offload_size,
      #[cfg(feature = "metrics")]
      metric_labels,
    )
    .await;
  }

  #[allow(clippy::too_many_arguments)]
  async fn listen(
    local_addr: SocketAddr,
    label: Label,
    mut acceptor: S::Acceptor,
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    shutdown: Arc<AtomicBool>,
    shutdown_rx: async_channel::Receiver<()>,
    skip_inbound_label_check: bool,
    timeout: Option<Duration>,
    #[cfg(feature = "compression")] offload_size: usize,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_utils::MetricLabels>,
  ) {
    tracing::info!(
      target: "memberlist.transport.quic",
      "listening stream on {local_addr}"
    );

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
          tracing::info!(target = "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
          return;
        }
        connection = acceptor.accept().fuse() => {
          match connection {
            Ok((connection, remote_addr)) => {
              tracing::error!("DEBUG: local {local_addr} accept a connection for {remote_addr}");
              let shutdown_rx = shutdown_rx.clone();
              let packet_tx = packet_tx.clone();
              let stream_tx = stream_tx.clone();
              let label = label.cheap_clone();
              #[cfg(feature = "metrics")]
              let metric_labels = metric_labels.clone();
              let (finish_tx, _finish_rx) = channel();
              <T::Runtime as Runtime>::spawn_detach(async move {
                Self::handle_connection(
                  connection,
                  local_addr,
                  remote_addr,
                  label,
                  stream_tx,
                  packet_tx,
                  timeout,
                  skip_inbound_label_check,
                  shutdown_rx,
                  #[cfg(feature = "compression")] offload_size,
                  #[cfg(feature = "metrics")] metric_labels,
                ).await;
                let _ = finish_tx.send(());
              });
            }
            Err(e) => {
              if shutdown.load(Ordering::SeqCst) {
                tracing::info!(target = "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
                return;
              }

              if loop_delay == Duration::ZERO {
                loop_delay = BASE_DELAY;
              } else {
                loop_delay *= 2;
              }

              if loop_delay > MAX_DELAY {
                loop_delay = MAX_DELAY;
              }

              tracing::error!(target =  "memberlist.transport.quic", local_addr=%local_addr, err = %e, "error accepting stream connection");
              <T::Runtime as Runtime>::sleep(loop_delay).await;
              continue;
            }
          }
        }
      }
    }
  }

  #[allow(clippy::too_many_arguments)]
  async fn handle_connection(
    conn: S::Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    label: Label,
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    timeout: Option<Duration>,
    skip_inbound_label_check: bool,
    shutdown_rx: async_channel::Receiver<()>,
    #[cfg(feature = "compression")] offload_size: usize,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_utils::MetricLabels>,
  ) {
    tracing::error!("DEBUG: local {local_addr} handle_connection for {remote_addr}");
    loop {
      futures::select! {
        incoming = conn.accept_bi().fuse() => {
          match incoming {
            Ok((mut stream, remote_addr)) => {
              let mut stream_kind_buf = [0; 1];
              if let Err(e) = stream.peek_exact(&mut stream_kind_buf).await {
                tracing::error!(target = "memberlist.transport.quic", local=%local_addr, from=%remote_addr, err = %e, "failed to read stream kind");
                continue;
              }
              let stream_kind = stream_kind_buf[0];
              if stream_kind == StreamType::Stream as u8 {
                if let Err(e) = stream_tx
                  .send(remote_addr, stream)
                  .await
                {
                  tracing::error!(target =  "memberlist.transport.quic", local_addr=%local_addr, err = %e, "failed to send stream connection");
                }
              } else {
                // consume peeked byte
                stream.read_exact(&mut stream_kind_buf).await.unwrap();
                let packet_tx = packet_tx.clone();
                let label = label.cheap_clone();
                #[cfg(feature = "metrics")]
                let metric_labels = metric_labels.clone();
                let (finish_tx, _finish_rx) = channel();
                <T::Runtime as Runtime>::spawn_detach(async move {
                  Self::handle_packet(
                    stream,
                    local_addr,
                    remote_addr,
                    label,
                    packet_tx.clone(),
                    timeout,
                    skip_inbound_label_check,
                    #[cfg(feature = "compression")] offload_size,
                    #[cfg(feature = "metrics")] metric_labels,
                  ).await;
                  let _ = finish_tx.send(());
                });
              }
            }
            Err(e) => {
              tracing::debug!(target = "memberlist.transport.quic", local=%local_addr, from=%remote_addr, err = %e, "failed to accept stream, shutting down the connection handler");
              return;
            }
          }
        },
        _ = shutdown_rx.recv().fuse() => {
          tracing::info!(target = "memberlist.transport.quic", local=%local_addr, remote=%remote_addr, "shutdown connection handler");
          return;
        }
      }
    }
  }

  #[allow(clippy::too_many_arguments)]
  async fn handle_packet(
    mut stream: S::Stream,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    label: Label,
    packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    timeout: Option<Duration>,
    skip_inbound_label_check: bool,
    #[cfg(feature = "compression")] offload_size: usize,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_utils::MetricLabels>,
  ) {
    let start = Instant::now();
    if let Some(timeout) = timeout {
      stream.set_read_deadline(Some(start + timeout));
    }

    let (_read, msg) = match Self::handle_packet_in(
      stream,
      &label,
      skip_inbound_label_check,
      #[cfg(feature = "compression")]
      offload_size,
    )
    .await
    {
      Ok(msg) => msg,
      Err(e) => {
        tracing::error!(target = "memberlist.packet", local=%local_addr, from=%remote_addr, err = %e, "fail to handle UDP packet");
        return;
      }
    };

    #[cfg(feature = "metrics")]
    {
      metrics::counter!("memberlist.packet.bytes.processing", metric_labels.iter())
        .increment(start.elapsed().as_secs_f64().round() as u64);
    }

    tracing::error!("DEBUG: loacl {local_addr} receive msg {:?}", msg);
    if let Err(e) = packet_tx.send(Packet::new(msg, remote_addr, start)).await {
      tracing::error!(target = "memberlist.packet", local=%local_addr, from=%remote_addr, err = %e, "failed to send packet");
    }

    #[cfg(feature = "metrics")]
    metrics::counter!("memberlist.packet.received", metric_labels.iter()).increment(_read as u64);
  }

  async fn handle_packet_in(
    mut recv_stream: S::Stream,
    label: &Label,
    skip_inbound_label_check: bool,
    #[cfg(feature = "compression")] offload_size: usize,
  ) -> Result<
    (
      usize,
      OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut tag = [0u8; 2];
    let mut readed = 0;
    recv_stream
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    let packet_label = if tag[0] == Label::TAG {
      let label_size = tag[1] as usize;
      // consume peeked
      recv_stream.read_exact(&mut tag).await.unwrap();

      let mut label = vec![0u8; label_size];
      recv_stream
        .read_exact(&mut label)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += 2 + label_size;
      Label::try_from(Bytes::from(label)).map_err(|e| QuicTransportError::Label(e.into()))?
    } else {
      Label::empty()
    };

    if !skip_inbound_label_check && packet_label.ne(label) {
      tracing::error!(target = "memberlist.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    #[cfg(not(feature = "compression"))]
    return {
      let (read, msgs) = Self::decode_without_compression(&mut recv_stream).await?;
      readed += read;
      Ok((readed, msgs))
    };

    #[cfg(feature = "compression")]
    {
      let (read, msgs) = Self::decode_with_compression(&mut recv_stream, offload_size).await?;
      readed += read;
      Ok((readed, msgs))
    }
  }

  fn decode_batch(
    mut src: &[u8],
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let num_msgs = src[0] as usize;
    src = &src[1..];
    tracing::error!("DEBUG: how many num msgs {num_msgs}");
    let mut msgs = OneOrMore::with_capacity(num_msgs);

    for _ in 0..num_msgs {
      let expected_msg_len = NetworkEndian::read_u32(&src[..MAX_MESSAGE_LEN_SIZE]) as usize;

      src = &src[MAX_MESSAGE_LEN_SIZE..];
      let (readed, msg) =
        <T::Wire as Wire>::decode_message(src).map_err(QuicTransportError::Wire)?;
      debug_assert_eq!(
        expected_msg_len, readed,
        "expected message length {expected_msg_len} but got {readed}",
      );
      src = &src[readed..];
      msgs.push(msg);
    }

    Ok(msgs)
  }

  async fn decode_without_compression(
    conn: &mut S::Stream,
  ) -> Result<
    (
      usize,
      OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut read = 0;
    let mut tag = [0u8; HEADER_SIZE];
    conn
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    read += HEADER_SIZE;

    if Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG == tag[0] {
      let msg_len = NetworkEndian::read_u32(&tag[1..]) as usize;
      // consume peeked header
      conn.read_exact(&mut tag).await.unwrap();

      if msg_len < MAX_INLINED_BYTES {
        tracing::error!("DEBUG: recv inlined {tag:?} len {msg_len}");
        let mut buf = [0u8; MAX_INLINED_BYTES];
        conn
          .read_exact(&mut buf[..msg_len])
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        read += msg_len + 1;
        Self::decode_batch(&buf[..msg_len]).map(|msgs| (read, msgs))
      } else {
        tracing::error!("DEBUG: recv {tag:?} {msg_len}");
        let mut buf = vec![0; msg_len];
        conn
          .read_exact(&mut buf)
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        tracing::error!("DEBUG: recv {buf:?}");
        read += msg_len + 1;
        Self::decode_batch(&buf).map(|msgs| (read, msgs))
      }
    } else {
      <T::Wire as Wire>::decode_message_from_reader(conn)
        .await
        .map(|(_, msg)| (read, msg.into()))
        .map_err(QuicTransportError::IO)
    }
  }

  #[cfg(feature = "compression")]
  async fn decode_with_compression(
    conn: &mut S::Stream,
    offload_size: usize,
  ) -> Result<
    (
      usize,
      OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut tag = [0u8; HEADER_SIZE];
    conn
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;

    if !COMPRESS_TAG.contains(&tag[0]) {
      return Self::decode_without_compression(conn).await;
    }

    // consume peeked bytes
    conn.read_exact(&mut tag).await.unwrap();
    let readed = HEADER_SIZE;
    let compressor = Compressor::try_from(tag[0])?;
    let msg_len = NetworkEndian::read_u32(&tag[1..]) as usize;
    if msg_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      conn
        .read_exact(&mut buf[..msg_len])
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      let compressed = &buf[..msg_len];
      Self::decompress_and_decode(compressor, compressed).map(|msgs| (readed + msg_len, msgs))
    } else if msg_len <= offload_size {
      let mut buf = vec![0; msg_len];
      conn
        .read_exact(&mut buf)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      let compressed = &buf[..msg_len];
      Self::decompress_and_decode(compressor, compressed).map(|msgs| (readed + msg_len, msgs))
    } else {
      let mut buf = vec![0; msg_len];
      conn
        .read_exact(&mut buf)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      let (tx, rx) = futures::channel::oneshot::channel();
      rayon::spawn(move || {
        if tx
          .send(Self::decompress_and_decode(compressor, &buf))
          .is_err()
        {
          tracing::error!(
            target = "memberlist.transport.quic",
            "failed to send decompressed message"
          );
        }
      });

      match rx.await {
        Ok(Ok(msgs)) => Ok((readed + msg_len, msgs)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(QuicTransportError::ComputationTaskFailed),
      }
    }
  }

  #[cfg(feature = "compression")]
  fn decompress_and_decode(
    compressor: Compressor,
    src: &[u8],
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    use bytes::Buf;

    let mut uncompressed: Bytes = compressor.decompress(src)?.into();

    if Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG
      == uncompressed[0]
    {
      uncompressed.advance(1);
      let _total_len = NetworkEndian::read_u32(&uncompressed[..MAX_MESSAGE_LEN_SIZE]) as usize;
      uncompressed.advance(MAX_MESSAGE_LEN_SIZE);
      let num_msgs = uncompressed[0] as usize;
      uncompressed.advance(1);

      let mut msgs = OneOrMore::with_capacity(num_msgs);
      for _ in 0..num_msgs {
        let expected_msg_len =
          NetworkEndian::read_u32(&uncompressed[..MAX_MESSAGE_LEN_SIZE]) as usize;
        uncompressed.advance(MAX_MESSAGE_LEN_SIZE);
        let (readed, msg) =
          <T::Wire as Wire>::decode_message(&uncompressed).map_err(QuicTransportError::Wire)?;
        debug_assert_eq!(
          expected_msg_len, readed,
          "expected bytes read {expected_msg_len} but got {readed}",
        );
        uncompressed.advance(readed);
        msgs.push(msg);
      }

      Ok(msgs)
    } else {
      <T::Wire as Wire>::decode_message(&uncompressed)
        .map(|(_, msg)| msg.into())
        .map_err(QuicTransportError::Wire)
    }
  }
}
