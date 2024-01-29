#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]

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
use byteorder::{NetworkEndian, ByteOrder};
use bytes::Bytes;
use futures::{AsyncReadExt, FutureExt};
use memberlist_core::{
  transport::{
    stream::{PacketProducer, PacketSubscriber, StreamProducer, StreamSubscriber},
    Transport, TransportError, Wire,
  },
  types::{Message, Packet},
};
use memberlist_utils::{net::CIDRsPolicy, Label, LabelError, OneOrMore, SmallVec, TinyVec};
use nodecraft::{resolver::AddressResolver, CheapClone, Id};
use peekable::future::AsyncPeekExt;

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;

#[cfg(feature = "compression")]
use compressor::*;

mod io;

mod stream_layer;
pub use stream_layer::*;


const DEFAULT_PORT: u16 = 7946;

const MAX_MESSAGE_LEN_SIZE: usize = core::mem::size_of::<u32>();
// compound tag + MAX_MESSAGE_LEN_SIZE
const PACKET_HEADER_OVERHEAD: usize = 1 + MAX_MESSAGE_LEN_SIZE;
const PACKET_OVERHEAD: usize = MAX_MESSAGE_LEN_SIZE;

#[cfg(feature = "compression")]
const COMPRESS_HEADER: usize = 1 + MAX_MESSAGE_LEN_SIZE;

const MAX_INLINED_BYTES: usize = 64;

/// Errors that can occur when using [`NetTransport`].
#[derive(thiserror::Error)]
pub enum QuicTransportError<A: AddressResolver, S: StreamLayer, W: Wire> {
  /// Returns when there is no explicit advertise address and no private IP address found.
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  /// Returns when there is no interface addresses found.
  #[error("failed to get interface addresses {0}")]
  NoInterfaceAddresses(#[from] local_ip_address::Error),
  /// Returns when there is no bind address provided.
  #[error("at least one bind address is required")]
  EmptyBindAddresses,
  /// Returns when the ip is blocked.
  #[error("the ip {0} is blocked")]
  BlockedIp(IpAddr),
  /// Returns when the packet buffer size is too small.
  #[error("failed to resize packet buffer {0}")]
  ResizePacketBuffer(std::io::Error),
  /// Returns when the packet socket fails to bind.
  #[error("failed to start packet listener on {0}: {1}")]
  ListenPacket(SocketAddr, std::io::Error),
  /// Returns when the promised listener fails to bind.
  #[error("failed to start promised listener on {0}: {1}")]
  ListenPromised(SocketAddr, std::io::Error),
  /// Returns when we fail to resolve an address.
  #[error("failed to resolve address {addr}: {err}")]
  Resolve {
    /// The address we failed to resolve.
    addr: A::Address,
    /// The error that occurred.
    err: A::Error,
  },
  /// Returns when the label error.
  #[error(transparent)]
  Label(#[from] LabelError),
  #[error(transparent)]
  Stream(S::Error),

  /// Returns when encode/decode error.
  #[error("wire error: {0}")]
  Wire(W::Error),
  /// Returns when the packet is too large.
  #[error("packet too large, the maximum packet can be sent is 65535, got {0}")]
  PacketTooLarge(usize),
  /// Returns when there is a custom error.
  #[error("custom error: {0}")]
  Custom(std::borrow::Cow<'static, str>),

  /// Returns when fail to compress/decompress message.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error("compressor: {0}")]
  Compressor(#[from] compressor::CompressorError),

  /// Returns when the computation task panic
  #[error("computation task panic")]
  #[cfg(feature = "compression")]
  ComputationTaskFailed,
}

impl<A: AddressResolver, S: StreamLayer, W: Wire> core::fmt::Debug for QuicTransportError<A, S, W> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A: AddressResolver, S: StreamLayer, W: Wire> TransportError for QuicTransportError<A, S, W> {
  fn is_remote_failure(&self) -> bool {
    if let Self::Stream(e) = self {
      e.is_remote_failure()
    } else {
      false
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}

/// Used to configure a net transport.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize",
    deserialize = "I: for<'a> serde::Deserialize<'a>, A: AddressResolver, A::Address: for<'a> serde::Deserialize<'a>, A::ResolvedAddress: for<'a> serde::Deserialize<'a>"
  ))
)]
pub struct QuicTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  /// The local node's ID.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the id of the node."),),
    setter(attrs(doc = "Set the id of the node. (Builder pattern)"),)
  )]
  id: I,

  /// The local node's address.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the address of the node."),),
    setter(attrs(doc = "Set the address of the node. (Builder pattern)"),)
  )]
  address: A::Address,

  /// The address to advertise to other nodes. If not set,
  /// the transport will attempt to discover the local IP address
  /// to use.
  #[viewit(
    getter(const, attrs(doc = "Get the advertise address of the node."),),
    setter(attrs(doc = "Set the advertise address of the node. (Builder pattern)"),)
  )]
  advertise_address: Option<A::ResolvedAddress>,

  /// A list of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(doc = "Get a list of addresses to bind to for both TCP and UDP communications."),
    ),
    setter(attrs(
      doc = "Set the list of addresses to bind to for both TCP and UDP communications. (Builder pattern)"
    ),)
  )]
  bind_addresses: SmallVec<IpAddr>,

  /// The port for bind addresses of the node.
  ///
  /// Default is `7946`.
  #[viewit(
    getter(const, attrs(doc = "Get the port for bind address of the node."),),
    setter(attrs(doc = "Set the port for bind address of the node. (Builder pattern)"),)
  )]
  bind_port: Option<u16>,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the label of the node."),),
    setter(attrs(doc = "Set the label of the node. (Builder pattern)"),)
  )]
  label: Label,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get if the check that inbound packets and gossip streams need to be label prefixed."
      ),
    ),
    setter(attrs(
      doc = "Set if the check that inbound packets and gossip streams need to be label prefixed. (Builder pattern)"
    ),)
  )]
  skip_inbound_label_check: bool,

  /// Policy for Classless Inter-Domain Routing (CIDR).
  ///
  /// By default, allow any connection
  #[cfg_attr(feature = "serde", serde(default))]
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the policy for Classless Inter-Domain Routing (CIDR)."),
    ),
    setter(attrs(
      doc = "Set the policy for Classless Inter-Domain Routing (CIDR). (Builder pattern)"
    ),)
  )]
  cidrs_policy: CIDRsPolicy,

  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the compression algorithm used for outgoing.",
        cfg(feature = "compression"),
        cfg_attr(docsrs, doc(cfg(feature = "compression")))
      ),
    ),
    setter(attrs(
      doc = "Set the compression algorithm used for outgoing. (Builder pattern)",
      cfg(feature = "compression"),
      cfg_attr(docsrs, doc(cfg(feature = "compression")))
    ),)
  )]
  compressor: Option<Compressor>,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the size of a message that should be offload to [`rayon`] thread pool for encryption or compression.",
        cfg(feature = "compression"),
        cfg_attr(docsrs, doc(cfg(feature = "compression")))
      ),
    ),
    setter(attrs(
      doc = "Set the size of a message that should be offload to [`rayon`] thread pool for encryption or compression. (Builder pattern)",
      cfg(feature = "compression"),
      cfg_attr(docsrs, doc(cfg(feature = "compression")))
    ),)
  )]
  offload_size: usize,

  /// The metrics labels.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  #[viewit(
    getter(
      style = "ref",
      result(
        converter(fn = "Option::as_deref"),
        type = "Option<&memberlist_utils::MetricLabels>"
      ),
      attrs(
        doc = "Get the metrics labels.",
        cfg(feature = "metrics"),
        cfg_attr(docsrs, doc(cfg(feature = "metrics")))
      ),
    ),
    setter(attrs(
      doc = "Set the metrics labels. (Builder pattern)",
      cfg(feature = "metrics"),
      cfg_attr(docsrs, doc(cfg(feature = "metrics")))
    ))
  )]
  metric_labels: Option<Arc<memberlist_utils::MetricLabels>>,
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>> QuicTransportOptions<I, A> {
  /// Creates a new net transport options by id and address, other configurations are left default.
  pub fn new(id: I, address: A::Address) -> Self {
    Self {
      id,
      address,
      advertise_address: None,
      bind_addresses: SmallVec::new(),
      bind_port: Some(DEFAULT_PORT),
      label: Label::empty(),
      skip_inbound_label_check: false,
      cidrs_policy: CIDRsPolicy::allow_all(),
      #[cfg(feature = "compression")]
      compressor: None,
      #[cfg(feature = "compression")]
      offload_size: 1024,
      #[cfg(feature = "metrics")]
      metric_labels: None,
    }
  }
}

/// A [`Transport`] implementation based on QUIC
pub struct QuicTransport<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer, W> {
  opts: Arc<QuicTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  stream_layer: Arc<S>,
  connectors: SmallVec<S::Connector>,
  round_robin: AtomicUsize,

  // wg: AsyncWaitGroup,
  resolver: Arc<A>,
  // shutdown: Arc<AtomicBool>,
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
  fn fix_packet_overhead(&self) -> usize {
    let mut overhead = self.opts.label.encoded_overhead();

    #[cfg(feature = "compression")]
    if self.opts.compressor.is_some() {
      overhead += 1 + core::mem::size_of::<u32>();
    }

    overhead
  }

  #[inline]
  fn next_connector(&self) -> &S::Connector {
    let idx = self
      .round_robin
      .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
      % self.connectors.len();
    &self.connectors[idx]
  }
}

struct Batch<I, A> {
  num_packets: usize,
  packets: TinyVec<Message<I, A>>,
  estimate_encoded_len: usize,
}

impl<I, A> Batch<I, A> {
  fn estimate_encoded_len(&self) -> usize {
    if self.packets.len() == 1 {
      return self.estimate_encoded_len - PACKET_HEADER_OVERHEAD - PACKET_OVERHEAD;
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
    &self.opts.address
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
    self.fix_packet_overhead() + PACKET_HEADER_OVERHEAD
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
    from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    let mut tag = [0u8; 2];
    let mut readed = 0;
    conn.peek_exact(&mut tag).await.map_err(|e| QuicTransportError::Stream(e.into()))?;
    let mut stream_label = if tag[0] == Label::TAG {
      let label_size = tag[1] as usize;
      // consume peeked
      conn.read_exact(&mut tag).await.unwrap();

      let mut label = vec![0u8; label_size];
      conn
        .read_exact(&mut label)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += 2 + label_size;
      Label::try_from(Bytes::from(label)).map_err(|e| QuicTransportError::Label(e.into()))?
    } else {
      Label::empty()
    };

    let label = &self.opts.label;

    if self.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(target: "memberlist.transport.quic.read_message", "unexpected double stream label header");
        return Err(LabelError::duplicate(label.cheap_clone(), stream_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      stream_label = label.cheap_clone();
    }

    if stream_label.ne(&self.opts.label) {
      tracing::error!(target: "memberlist.transport.quic.read_message", local_label=%label, remote_label=%stream_label, "discarding stream with unacceptable label");
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
    todo!()
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
    let mut estimate_batch_encoded_size = 0;
    let mut current_packets_in_batch = 0;

    // get how many packets a batch
    for packet in packets.iter() {
      let ep_len = W::encoded_len(packet);
      // check if we reach the maximum packet size
      let current_encoded_size = ep_len + estimate_batch_encoded_size;
      if current_encoded_size >= self.max_payload_size() {
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
      .next_connector()
      .open_bi_with_timeout(*addr, timeout)
      .await
      .map_err(|e| Self::Error::Stream(e.into()))
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
    todo!()
  }

  fn block_shutdown(&self) -> Result<(), Self::Error> {
    todo!()
  }
}

struct Processor<
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A>,
  S: StreamLayer,
> {
  label: Label,
  local_addr: SocketAddr,
  uni: S::UniAcceptor,
  bi: S::BiAcceptor,
  packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
  
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,

  skip_inbound_label_check: bool,

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
  async fn run(self) {
    let Self {
      bi,
      uni,
      packet_tx,
      stream_tx,
      shutdown_rx,
      shutdown,
      local_addr,
      label,
      skip_inbound_label_check,
      #[cfg(feature = "compression")]
      offload_size,
      #[cfg(feature = "metrics")]
      metric_labels,
    } = self;

    <T::Runtime as Runtime>::spawn_detach(Self::listen_packet(
      local_addr,
      label,
      uni,
      packet_tx,
      shutdown.clone(),
      shutdown_rx.clone(),
      skip_inbound_label_check,
      #[cfg(feature = "compression")]
      offload_size,
      #[cfg(feature = "metrics")]
      metric_labels,
    ));

    <T::Runtime as Runtime>::spawn_detach(Self::listen_stream(
      local_addr,
      bi,
      stream_tx,
      shutdown,
      shutdown_rx,
    ));
  }

  async fn listen_stream(
    local_addr: SocketAddr,
    acceptor: S::BiAcceptor,
    stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
    shutdown: Arc<AtomicBool>,
    shutdown_rx: async_channel::Receiver<()>,
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
          tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
          return;
        }
        incoming = acceptor.accept_bi().fuse() => {
          match incoming {
            Ok((stream, remote_addr)) => {
              // No error, reset loop delay
              loop_delay = Duration::ZERO;
              if let Err(e) = stream_tx
                .send(remote_addr, stream)
                .await
              {
                tracing::error!(target:  "memberlist.transport.quic", local_addr=%local_addr, err = %e, "failed to send stream connection");
              }
            }
            Err(e) => {
              if shutdown.load(Ordering::SeqCst) {
                tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
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

              tracing::error!(target:  "memberlist.transport.quic", local_addr=%local_addr, err = %e, "error accepting stream connection");
              <T::Runtime as Runtime>::sleep(loop_delay).await;
              continue;
            }
          }
        }
      }
    }
  }

  #[allow(clippy::too_many_arguments)]
  async fn listen_packet(
    local_addr: SocketAddr,
    label: Label,
    acceptor: S::UniAcceptor,
    packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    shutdown: Arc<AtomicBool>,
    shutdown_rx: async_channel::Receiver<()>,
    skip_inbound_label_check: bool,
    #[cfg(feature = "compression")] offload_size: usize,
    #[cfg(feature = "metrics")] metric_labels: Arc<memberlist_utils::MetricLabels>,
  ) {
    tracing::info!(
      target: "memberlist.transport.quic",
      "listening packet on {local_addr}"
    );
    loop {
      futures::select! {
        _ = shutdown_rx.recv().fuse() => {
          tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown packet listener");
          return;
        }
        incoming = acceptor.accept_uni().fuse() => {
          match incoming {
            Ok((recv_stream, remote_addr)) => {
              let start = Instant::now();
              // TODO: set timeout for recv_stream
              let (read, msg) = match Self::handle_packet(
                recv_stream,
                &label,
                skip_inbound_label_check,
                #[cfg(feature = "compression")] offload_size,
              ).await {
                Ok(msg) => msg,
                Err(e) => {
                  tracing::error!(target: "memberlist.packet", local=%local_addr, from=%remote_addr, err = %e, "fail to handle UDP packet");
                  continue;
                }
              };

              #[cfg(feature = "metrics")]
              {
                metrics::counter!("memberlist.packet.bytes.processing", metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);
              }

              if let Err(e) = packet_tx.send(Packet::new(msg, remote_addr, start)).await {
                tracing::error!(target: "memberlist.packet", local=%local_addr, from=%remote_addr, err = %e, "failed to send packet");
              }

              #[cfg(feature = "metrics")]
              metrics::counter!("memberlist.packet.received", metric_labels.iter()).increment(read as u64);
            }
            Err(e) => {
              if shutdown.load(Ordering::SeqCst) {
                tracing::info!(target: "memberlist.transport.quic", local=%local_addr, "shutdown stream listener");
                return;
              }

              tracing::error!(target: "memberlist.transport.quic", err=%e, "failed to accept packet connection");
            }
          }
        }
      }
    }
  }

  async fn handle_packet(
    mut recv_stream: S::ReadStream,
    label: &Label,
    skip_inbound_label_check: bool,
    #[cfg(feature = "compression")] offload_size: usize,
  ) -> Result<
    (usize, OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>),
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let mut tag = [0u8; 2];
    let mut readed = 0;
    recv_stream.peek_exact(&mut tag).await.map_err(|e| QuicTransportError::Stream(e.into()))?;
    let mut packet_label = if tag[0] == Label::TAG {
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

    if skip_inbound_label_check {
      if !packet_label.is_empty() {
        return Err(LabelError::duplicate(label.cheap_clone(), packet_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      packet_label = label.cheap_clone();
    }

    if packet_label.ne(label) {
      tracing::error!(target: "memberlist.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    #[cfg(not(feature = "compression"))]
    return Self::decode(&buf[..total_len]).map(|msgs| (readed, msgs));

    if total_len <= MAX_INLINED_BYTES {
      let mut buf = [0u8; MAX_INLINED_BYTES];
      recv_stream
        .read_exact(&mut buf[..total_len])
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += total_len;

      

      #[cfg(feature = "compression")]
      {
        if !COMPRESS_TAG.contains(&tag) {
          return Self::decode(&buf[..total_len]).map(|msgs| (readed, msgs));
        }

        Self::decompress_and_decode(tag, &buf[..total_len]).map(|msgs| (readed, msgs))
      }
    } else {
      let mut buf = vec![0; total_len];
      recv_stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += total_len;

      #[cfg(not(feature = "compression"))]
      return Self::decode(&buf).map(|msgs| (readed, msgs));

      #[cfg(feature = "compression")]
      {
        if !COMPRESS_TAG.contains(&tag) {
          return Self::decode(&buf[..total_len]).map(|msgs| (readed, msgs));
        }

        Self::handle_compressed(tag, buf, offload_size).await.map(|msgs| (readed, msgs))
      }
    }
  }

  fn decode(mut src: &[u8]) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    if Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG == src[0] {
      src = &src[1..];
      let num_msgs = NetworkEndian::read_u32(&src[..MAX_MESSAGE_LEN_SIZE]) as usize;
      src = &src[MAX_MESSAGE_LEN_SIZE..];
      let mut msgs = OneOrMore::with_capacity(num_msgs);

      for _ in 0..num_msgs {
        let expected_msg_len = NetworkEndian::read_u32(&src[..MAX_MESSAGE_LEN_SIZE]) as usize;
        src = &src[MAX_MESSAGE_LEN_SIZE..];
        let (readed, msg) = <T::Wire as Wire>::decode_message(src).map_err(QuicTransportError::Wire)?;

        debug_assert_eq!(
          expected_msg_len,
          readed,
          "expected message length {expected_msg_len} but got {readed}",
        );
        src = &src[readed..];
        msgs.push(msg);
      }

      Ok(msgs)
    } else {
      <T::Wire as Wire>::decode_message(src).map(|(_, msg)| msg.into()).map_err(QuicTransportError::Wire)
    }
  }

  #[cfg(feature = "compression")]
  fn decompress_and_decode(
    tag: u8,
    src: &[u8],
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    let compressor = Compressor::try_from(tag)?;
    let uncompressed = compressor.decompress(src)?;
    Self::decode(&uncompressed)
  }

  #[cfg(feature = "compression")]
  async fn handle_compressed(
    tag: u8,
    src: Vec<u8>,
    offload_size: usize,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    QuicTransportError<T::Resolver, S, T::Wire>,
  > {
    if src.len() <= offload_size {
      return Self::decompress_and_decode(tag, &src);
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    rayon::spawn(move || {
      if tx.send(Self::decompress_and_decode(tag, &src)).is_err() {
        tracing::error!(target: "memberlist.transport.quic", "failed to send decompressed message");
      }
    });

    match rx.await {
      Ok(Ok(msgs)) => Ok(msgs),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(QuicTransportError::ComputationTaskFailed),
    }
  }
}
