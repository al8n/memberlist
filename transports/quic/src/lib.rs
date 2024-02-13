//! [`Transport`](memberlist_core::Transport)'s network transport layer based on QUIC.
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  marker::PhantomData,
  net::{IpAddr, SocketAddr},
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::{Runtime, WaitableSpawner};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use futures::FutureExt;
use memberlist_core::{
  transport::{
    stream::{
      packet_stream, promised_stream, PacketProducer, PacketSubscriber, StreamProducer,
      StreamSubscriber,
    },
    TimeoutableReadStream, Transport, TransportError, Wire,
  },
  types::{Message, Packet},
};
use memberlist_utils::{net::CIDRsPolicy, Label, LabelError, OneOrMore, SmallVec, TinyVec};
use nodecraft::{resolver::AddressResolver, CheapClone, Id};
use pollster::FutureExt as _;

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;
#[cfg(feature = "compression")]
use compressor::*;

/// Exports unit tests.
#[cfg(any(test, feature = "test"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests;

mod error;
pub use error::*;
mod io;
mod options;
pub use options::*;
/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`QuicTransport`](crate::QuicTransport).
pub mod stream_layer;
use stream_layer::*;

const MAX_MESSAGE_LEN_SIZE: usize = core::mem::size_of::<u32>();
const MAX_MESSAGE_SIZE: usize = u32::MAX as usize;
// compound tag + MAX_MESSAGE_LEN_SIZE
const PACKET_HEADER_OVERHEAD: usize = 1 + 1 + MAX_MESSAGE_LEN_SIZE;
const PACKET_OVERHEAD: usize = MAX_MESSAGE_LEN_SIZE;
const NUM_PACKETS_PER_BATCH: usize = 255;
const HEADER_SIZE: usize = 1 + MAX_MESSAGE_LEN_SIZE;

#[cfg(feature = "compression")]
const COMPRESS_HEADER: usize = 1 + MAX_MESSAGE_LEN_SIZE;

const MAX_INLINED_BYTES: usize = 64;

#[derive(Copy, Clone)]
#[repr(u8)]
enum StreamType {
  Stream = 0,
  Packet = 1,
}

/// A [`Transport`] implementation based on QUIC
pub struct QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  opts: QuicTransportOptions<I, A>,
  advertise_addr: A::ResolvedAddress,
  local_addr: A::Address,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  #[allow(dead_code)]
  stream_layer: S,
  v4_round_robin: AtomicUsize,
  v4_connectors: SmallVec<S::Connector>,
  v6_round_robin: AtomicUsize,
  v6_connectors: SmallVec<S::Connector>,

  wg: WaitableSpawner<A::Runtime>,
  resolver: A,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,

  max_payload_size: usize,
  _marker: PhantomData<W>,
}

impl<I, A, S, W> QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  /// Creates a new quic transport.
  pub async fn new(
    resolver: A,
    stream_layer: S,
    opts: QuicTransportOptions<I, A>,
  ) -> Result<Self, QuicTransportError<A, S, W>> {
    Self::new_in(resolver, stream_layer, opts).await
  }

  async fn new_in(
    resolver: A,
    stream_layer: S,
    opts: QuicTransportOptions<I, A>,
  ) -> Result<Self, QuicTransportError<A, S, W>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addresses.is_empty() {
      return Err(QuicTransportError::EmptyBindAddresses);
    }

    let (stream_tx, stream_rx) = promised_stream::<Self>();
    let (packet_tx, packet_rx) = packet_stream::<Self>();
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut v4_connectors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut v6_connectors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut v4_acceptors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut v6_acceptors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut resolved_bind_address = SmallVec::new();

    for addr in opts.bind_addresses.iter() {
      let addr = resolver
        .resolve(addr)
        .await
        .map_err(|e| QuicTransportError::Resolve {
          addr: addr.cheap_clone(),
          err: e,
        })?;

      let bind_port = addr.port();

      let (local_addr, acceptor, connector) = if bind_port == 0 {
        let mut retries = 0;
        loop {
          match stream_layer.bind(addr).await {
            Ok(res) => break res,
            Err(e) => {
              if retries < 9 {
                retries += 1;
                continue;
              }
              return Err(QuicTransportError::ListenPromised(addr, e));
            }
          }
        }
      } else {
        match stream_layer.bind(addr).await {
          Ok(res) => res,
          Err(e) => return Err(QuicTransportError::ListenPromised(addr, e)),
        }
      };

      if local_addr.is_ipv4() {
        v4_acceptors.push((local_addr, acceptor));
        v4_connectors.push(connector);
      } else {
        v6_acceptors.push((local_addr, acceptor));
        v6_connectors.push(connector);
      }
      // If the config port given was zero, use the first TCP listener
      // to pick an available port and then apply that to everything
      // else.
      let addr = if bind_port == 0 { local_addr } else { addr };
      resolved_bind_address.push(addr);
    }

    let wg = WaitableSpawner::<A::Runtime>::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    let expose_addr_index = Self::find_advertise_addr_index(&resolved_bind_address);
    let advertise_addr = resolved_bind_address[expose_addr_index];
    let self_addr = opts.bind_addresses[expose_addr_index].cheap_clone();

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for (local_addr, acceptor) in v4_acceptors.into_iter().chain(v6_acceptors.into_iter()) {
      Processor::<A, Self, S> {
        wg: wg.clone(),
        acceptor,
        packet_tx: packet_tx.clone(),
        stream_tx: stream_tx.clone(),
        label: opts.label.clone(),
        local_addr,
        shutdown: shutdown.clone(),
        timeout: opts.timeout,
        shutdown_rx: shutdown_rx.clone(),
        skip_inbound_label_check: opts.skip_inbound_label_check,
        #[cfg(feature = "compression")]
        offload_size: opts.offload_size,
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
      }
      .run();
    }

    // find final advertise address
    let final_advertise_addr = if advertise_addr.ip().is_unspecified() {
      let ip = local_ip_address::local_ip().map_err(|e| match e {
        local_ip_address::Error::LocalIpAddressNotFound => QuicTransportError::NoPrivateIP,
        e => QuicTransportError::NoInterfaceAddresses(e),
      })?;
      SocketAddr::new(ip, advertise_addr.port())
    } else {
      advertise_addr
    };

    Ok(Self {
      advertise_addr: final_advertise_addr,
      local_addr: self_addr,
      max_payload_size: MAX_MESSAGE_SIZE.min(stream_layer.max_stream_data()),
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      v4_connectors,
      v6_connectors,
      v4_round_robin: AtomicUsize::new(0),
      v6_round_robin: AtomicUsize::new(0),
      stream_layer,
      resolver,
      shutdown_tx,
      _marker: PhantomData,
    })
  }

  fn find_advertise_addr_index(addrs: &[SocketAddr]) -> usize {
    for (i, addr) in addrs.iter().enumerate() {
      if !addr.ip().is_unspecified() {
        return i;
      }
    }

    0
  }
}

impl<I, A, S, W> QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  fn fix_packet_overhead(&self) -> usize {
    let mut overhead = self.opts.label.encoded_overhead();

    #[cfg(feature = "compression")]
    if self.opts.compressor.is_some() {
      overhead += 1 + core::mem::size_of::<u32>();
    }

    overhead
  }

  fn next_connector(&self, addr: &A::ResolvedAddress) -> &S::Connector {
    if addr.is_ipv4() {
      // if there's no v4 sockets, we assume remote addr can accept both v4 and v6
      // give a try on v6
      if self.v4_connectors.is_empty() {
        let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.v6_connectors.len();
        &self.v6_connectors[idx]
      } else {
        let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.v4_connectors.len();
        &self.v4_connectors[idx]
      }
    } else if self.v6_connectors.is_empty() {
      let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.v4_connectors.len();
      &self.v4_connectors[idx]
    } else {
      let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.v6_connectors.len();
      &self.v6_connectors[idx]
    }
  }
}

#[derive(Debug)]
struct Batch<I, A> {
  num_packets: usize,
  packets: TinyVec<Message<I, A>>,
  estimate_encoded_len: usize,
}

impl<I, A> Batch<I, A> {
  fn estimate_encoded_len(&self) -> usize {
    if self.packets.len() == 1 {
      return self.estimate_encoded_len - PACKET_OVERHEAD;
    }
    self.estimate_encoded_len
  }
}

impl<I, A, S, W> Transport for QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  type Error = QuicTransportError<A, S, W>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = A::Runtime;

  async fn resolve(
    &self,
    addr: &<Self::Resolver as AddressResolver>::Address,
  ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
    self
      .resolver
      .resolve(addr)
      .await
      .map_err(|e| Self::Error::Resolve {
        addr: addr.cheap_clone(),
        err: e,
      })
  }

  #[inline(always)]
  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  #[inline(always)]
  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.local_addr
  }

  #[inline(always)]
  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  #[inline(always)]
  fn max_payload_size(&self) -> usize {
    self.max_payload_size
  }

  #[inline(always)]
  fn packet_overhead(&self) -> usize {
    PACKET_OVERHEAD
  }

  #[inline(always)]
  fn packets_header_overhead(&self) -> usize {
    // 1 for StreamType
    1 + self.fix_packet_overhead() + PACKET_HEADER_OVERHEAD
  }

  fn blocked_address(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error> {
    let ip = addr.ip();
    if self.opts.cidrs_policy.is_blocked(&ip) {
      Err(Self::Error::BlockedIp(ip))
    } else {
      Ok(())
    }
  }

  async fn read_message(
    &self,
    _from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    let mut tag = [0u8; 3];
    conn
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    let stream_label = if tag[1] == Label::TAG {
      let label_size = tag[2] as usize;
      // consume peeked
      conn.read_exact(&mut tag).await.unwrap();
      let mut label = vec![0u8; label_size];
      conn
        .read_exact(&mut label)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      Label::try_from(label).map_err(|e| QuicTransportError::Label(e.into()))?
    } else {
      // consume stream type tag
      conn.read_exact(&mut [0; 1]).await.unwrap();
      Label::empty()
    };

    let label = &self.opts.label;

    if !self.opts.skip_inbound_label_check && stream_label.ne(label) {
      tracing::error!(target = "memberlist.transport.quic.read_message", local_label=%label, remote_label=%stream_label, "discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), stream_label).into());
    }

    let readed = stream_label.encoded_overhead();

    #[cfg(not(feature = "compression"))]
    return self
      .read_message_without_compression(conn)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(feature = "compression")]
    self
      .read_message_with_compression(conn)
      .await
      .map(|(read, msg)| (readed + read, msg))
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    #[cfg(not(feature = "compression"))]
    let buf = self.send_message_without_comression(msg).await?;

    #[cfg(feature = "compression")]
    let buf = self.send_message_with_compression(msg).await?;

    conn
      .write_all(buf)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, std::time::Instant), Self::Error> {
    let start = Instant::now();
    let encoded_size = W::encoded_len(&packet);
    self
      .send_batch(
        *addr,
        Batch {
          packets: TinyVec::from(packet),
          num_packets: 1,
          estimate_encoded_len: self.packets_header_overhead() + PACKET_OVERHEAD + encoded_size,
        },
      )
      .await
      .map(|sent| (sent, start))
  }

  async fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<(usize, std::time::Instant), Self::Error> {
    let start = Instant::now();

    let mut batches =
      SmallVec::<Batch<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>::new();
    let packets_overhead = self.packets_header_overhead();
    let mut estimate_batch_encoded_size = packets_overhead;
    let mut current_packets_in_batch = 0;

    // get how many packets a batch
    for packet in packets.iter() {
      let ep_len = W::encoded_len(packet);
      // check if we reach the maximum packet size
      let current_encoded_size = ep_len + estimate_batch_encoded_size;
      if current_encoded_size >= self.max_payload_size()
        || current_packets_in_batch >= NUM_PACKETS_PER_BATCH
      {
        batches.push(Batch {
          packets: TinyVec::with_capacity(current_packets_in_batch),
          num_packets: current_packets_in_batch,
          estimate_encoded_len: estimate_batch_encoded_size,
        });
        estimate_batch_encoded_size =
          packets_overhead + PACKET_HEADER_OVERHEAD + PACKET_OVERHEAD + ep_len;
        current_packets_in_batch = 1;
      } else {
        estimate_batch_encoded_size += PACKET_OVERHEAD + ep_len;
        current_packets_in_batch += 1;
      }
    }

    // consume the packets to small batches according to batch_offsets.

    // if batch_offsets is empty, means that packets can be sent by one I/O call
    if batches.is_empty() {
      self
        .send_batch(
          *addr,
          Batch {
            num_packets: packets.len(),
            packets,
            estimate_encoded_len: estimate_batch_encoded_size,
          },
        )
        .await
        .map(|sent| (sent, start))
    } else {
      let mut batch_idx = 0;
      for (idx, packet) in packets.into_iter().enumerate() {
        let batch = &mut batches[batch_idx];
        batch.packets.push(packet);
        if batch.num_packets == idx - 1 {
          batch_idx += 1;
        }
      }

      let mut total_bytes_sent = 0;
      let resps =
        futures::future::join_all(batches.into_iter().map(|b| self.send_batch(*addr, b))).await;

      for res in resps {
        match res {
          Ok(sent) => {
            total_bytes_sent += sent;
          }
          Err(e) => return Err(e),
        }
      }
      Ok((total_bytes_sent, start))
    }
  }

  async fn dial_timeout(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    timeout: std::time::Duration,
  ) -> Result<Self::Stream, Self::Error> {
    self
      .next_connector(addr)
      .open_bi_with_timeout(*addr, timeout)
      .await
      .map_err(|e| Self::Error::Stream(e.into()))
  }

  async fn cache_stream(
    &self,
    _addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    mut stream: Self::Stream,
  ) -> Result<(), Self::Error> {
    // Cache QUIC stream make no sense, so just wait all data have been sent to the client and return
    stream
      .finish()
      .await
      .map_err(|e| Self::Error::Stream(e.into()))?;
    Ok(())
  }

  fn packet(
    &self,
  ) -> PacketSubscriber<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress> {
    self.packet_rx.clone()
  }

  fn stream(
    &self,
  ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Stream> {
    self.stream_rx.clone()
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    if self.shutdown_tx.is_closed() {
      return Ok(());
    }

    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();

    for connector in self.v4_connectors.iter().chain(self.v6_connectors.iter()) {
      if let Err(e) = connector
        .close()
        .await
        .map_err(|e| Self::Error::Stream(e.into()))
      {
        tracing::error!(target = "memberlist.transport.quic", err = %e, "failed to close connector");
      }
    }

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }
}

impl<I, A, S, W> Drop for QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  fn drop(&mut self) {
    if self.shutdown_tx.is_closed() {
      return;
    }
    let _ = self.shutdown().block_on();
  }
}

struct Processor<
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A>,
  S: StreamLayer,
> {
  label: Label,
  local_addr: SocketAddr,
  acceptor: S::Acceptor,
  packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,

  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,

  skip_inbound_label_check: bool,
  timeout: Option<Duration>,
  wg: WaitableSpawner<T::Runtime>,

  #[cfg(feature = "compression")]
  offload_size: usize,

  #[cfg(feature = "metrics")]
  metric_labels: Arc<memberlist_utils::MetricLabels>,
}

impl<A, T, S> Processor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream>,
  S: StreamLayer,
{
  fn run(self) {
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
      wg,
      #[cfg(feature = "compression")]
      offload_size,
      #[cfg(feature = "metrics")]
      metric_labels,
    } = self;

    let swg = wg.clone();
    wg.spawn_detach(async move {
      Self::listen(
        local_addr,
        label,
        acceptor,
        packet_tx,
        stream_tx,
        swg,
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
    });
  }

  #[allow(clippy::too_many_arguments)]
  async fn listen(
    local_addr: SocketAddr,
    label: Label,
    mut acceptor: S::Acceptor,
    packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    wg: WaitableSpawner<T::Runtime>,
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
        incoming = acceptor.accept_bi().fuse() => {
          match incoming {
            Ok((mut stream, remote_addr)) => {
              // No error, reset loop delay
              loop_delay = Duration::ZERO;
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
                wg.spawn_detach(async move {
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
                });
              }
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
    stream.set_read_timeout(timeout);

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
        let mut buf = [0u8; MAX_INLINED_BYTES];
        conn
          .read_exact(&mut buf[..msg_len + 1])
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        read += msg_len + 1;
        Self::decode_batch(&buf[..msg_len]).map(|msgs| (read, msgs))
      } else {
        let mut buf = vec![0; msg_len];
        conn
          .read_exact(&mut buf)
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
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
