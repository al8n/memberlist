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

use agnostic::Runtime;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use futures::{
  channel::oneshot::{self, channel},
  FutureExt,
};
use memberlist_core::{
  transport::{
    stream::{packet_stream, promised_stream, PacketSubscriber, StreamSubscriber},
    Transport, TransportError, Wire,
  },
  types::Message,
};

use memberlist_utils::{net::CIDRsPolicy, Label, LabelError, SmallVec, TinyVec};
use nodecraft::{resolver::AddressResolver, CheapClone, Id};

mod processor;
use processor::*;

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

#[cfg(feature = "tokio")]
/// [`QuicTransport`](crate::QuicTransport) based on [`tokio`](https://crates.io/crates/tokio).
pub type TokioQuicTransport<I, A, S, W> = QuicTransport<I, A, S, W, agnostic::tokio::TokioRuntime>;

#[cfg(feature = "async-std")]
/// [`QuicTransport`](crate::QuicTransport) based on [`async-std`](https://crates.io/crates/async-std).
pub type AsyncStdQuicTransport<I, A, S, W> =
  QuicTransport<I, A, S, W, agnostic::async_std::AsyncStdRuntime>;

#[cfg(feature = "smol")]
/// [`QuicTransport`](crate::QuicTransport) based on [`smol`](https://crates.io/crates/smol).
pub type SmolQuicTransport<I, A, S, W> = QuicTransport<I, A, S, W, agnostic::smol::SmolRuntime>;

/// A [`Transport`] implementation based on QUIC
pub struct QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  opts: QuicTransportOptions<I, A>,
  advertise_addr: A::ResolvedAddress,
  local_addr: A::Address,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  #[allow(dead_code)]
  stream_layer: S,
  connection_pool: Arc<SkipMap<SocketAddr, S::Connection>>,
  v4_round_robin: AtomicUsize,
  v4_connectors: SmallVec<S::Connector>,
  v6_round_robin: AtomicUsize,
  v6_connectors: SmallVec<S::Connector>,
  handles: futures::lock::Mutex<SmallVec<oneshot::Receiver<()>>>,
  resolver: A,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,

  max_payload_size: usize,
  _marker: PhantomData<W>,
}

impl<I, A, S, W, R> QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
  <R::Interval as futures::Stream>::Item: Send + 'static,
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

    let shutdown = Arc::new(AtomicBool::new(false));
    let expose_addr_index = Self::find_advertise_addr_index(&resolved_bind_address);
    let advertise_addr = resolved_bind_address[expose_addr_index];
    let self_addr = opts.bind_addresses[expose_addr_index].cheap_clone();
    let mut handles = SmallVec::new();

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for (local_addr, acceptor) in v4_acceptors.into_iter().chain(v6_acceptors.into_iter()) {
      let (finish_tx, finish_rx) = channel();
      let processor = Processor::<A, Self, S> {
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
      };

      R::spawn_detach(async {
        processor.run().await;
        let _ = finish_tx.send(());
      });
      handles.push(finish_rx);
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

    let connection_pool = Arc::new(SkipMap::new());
    let interval = <A::Runtime as Runtime>::interval(opts.connection_pool_cleanup_period);
    let pool = connection_pool.clone();
    let (cleaner_finish_tx, cleaner_finish_rx) = channel();
    let shutdown_rx = shutdown_rx.clone();
    R::spawn_detach(async move {
      Self::connection_pool_cleaner(pool, interval, shutdown_rx).await;
      let _ = cleaner_finish_tx.send(());
    });
    handles.push(cleaner_finish_rx);

    Ok(Self {
      advertise_addr: final_advertise_addr,
      connection_pool,
      local_addr: self_addr,
      max_payload_size: MAX_MESSAGE_SIZE.min(stream_layer.max_stream_data()),
      opts,
      packet_rx,
      stream_rx,
      handles: futures::lock::Mutex::new(handles),
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

  async fn connection_pool_cleaner(
    pool: Arc<SkipMap<SocketAddr, S::Connection>>,
    interval: impl futures::Stream,
    shutdown_rx: async_channel::Receiver<()>,
  ) {
    use futures::StreamExt;

    futures::pin_mut!(interval);

    loop {
      futures::select! {
        _ = interval.next().fuse() => {
          for ent in pool.iter() {
            if ent.value().is_closed().await {
              ent.remove();
            }
          }
        }
        _ = shutdown_rx.recv().fuse() => {
          return;
        }
      }
    }
  }
}

impl<I, A, S, W, R> QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  fn fix_packet_overhead(&self) -> usize {
    #[cfg(feature = "compression")]
    return {
      let mut overhead = self.opts.label.encoded_overhead();

      if self.opts.compressor.is_some() {
        overhead += 1 + core::mem::size_of::<u32>();
      }

      overhead
    };

    #[cfg(not(feature = "compression"))]
    self.opts.label.encoded_overhead()
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

  async fn fetch_stream(
    &self,
    addr: SocketAddr,
    timeout: Option<Duration>,
  ) -> Result<S::Stream, QuicTransportError<A, S, W>> {
    if let Some(ent) = self.connection_pool.get(&addr) {
      let connection = ent.value();
      if !connection.is_closed().await {
        if let Some(timeout) = timeout {
          return connection
            .open_bi_with_timeout(timeout)
            .await
            .map(|(s, _)| s)
            .map_err(|e| QuicTransportError::Stream(e.into()));
        } else {
          return connection
            .open_bi()
            .await
            .map(|(s, _)| s)
            .map_err(|e| QuicTransportError::Stream(e.into()));
        }
      }
    }

    let connector = self.next_connector(&addr);
    let connection = connector
      .connect(addr)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    connection
      .open_bi()
      .await
      .map(|(s, _)| {
        self.connection_pool.insert(addr, connection);
        s
      })
      .map_err(|e| QuicTransportError::Stream(e.into()))
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

impl<I, A, S, W, R> Transport for QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
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
    let buf = self.send_message_without_compression(msg).await?;

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
    self.fetch_stream(*addr, Some(timeout)).await
  }

  async fn cache_stream(
    &self,
    _addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    mut stream: Self::Stream,
  ) -> Result<(), Self::Error> {
    // Cache QUIC stream make no sense, so just wait all data have been sent to the client and return
    stream
      .close()
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
    let mut handles = self.handles.lock().await;
    let _ = futures::future::join_all(handles.drain(..)).await;
    Ok(())
  }
}

impl<I, A, S, W, R> Drop for QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  fn drop(&mut self) {
    self.shutdown_tx.close();
  }
}
