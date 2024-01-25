use std::{
  io::{Error, ErrorKind},
  marker::PhantomData,
  net::{IpAddr, SocketAddr},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use checksum::CHECKSUM_SIZE;
use compressor::UnknownCompressor;
use futures::{io::BufReader, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, FutureExt};
use label::{LabelAsyncIOExt, LabelBufExt};
use peekable::future::AsyncPeekExt;
use security::{Encryptor, SecurityError};
use showbiz_core::{
  transport::{
    resolver::AddressResolver,
    stream::{
      packet_stream, promised_stream, PacketProducer, PacketSubscriber, StreamProducer,
      StreamSubscriber,
    },
    Id, Transport, TransportError, Wire,
  },
  types::{Message, Packet, SmallVec, TinyVec},
  CheapClone,
};
use showbiz_utils::{net::CIDRsPolicy, OneOrMore};
use wg::AsyncWaitGroup;

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;
#[cfg(feature = "compression")]
use compressor::Compressor;

/// Encrypt/decrypt related.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub mod security;
#[cfg(feature = "encryption")]
use security::{EncryptionAlgo, SecretKey, SecretKeyring, SecretKeys};

/// Errors for the net transport.
pub mod error;
use error::*;

/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`NetTransport`](crate::NetTransport).
pub mod stream_layer;
use stream_layer::*;

mod label;
pub use label::Label;

mod checksum;
pub use checksum::Checksumer;

mod read_from_promised;
mod send_by_packet;
mod send_by_promised;

use crate::label::{LabelBufMutExt, LabelError};

const CHECKSUM_TAG: core::ops::RangeInclusive<u8> = 44..=64;
const ENCRYPT_TAG: core::ops::RangeInclusive<u8> = 65..=85;
const COMPRESS_TAG: core::ops::RangeInclusive<u8> = 86..=126;
const LABEL_TAG: u8 = 127;

const MAX_PACKET_SIZE: usize = u16::MAX as usize;
/// max message bytes is `u16::MAX`
const PACKET_OVERHEAD: usize = core::mem::size_of::<u16>();
/// tag + num msgs (max number of messages is `255`)
const PACKET_HEADER_OVERHEAD: usize = 1 + 1;
const NUM_PACKETS_PER_BATCH: usize = 255;
const MAX_INLINED_BYTES: usize = 64;
const COMPRESS_HEADER: usize = 1 + core::mem::size_of::<u32>();
const CHECKSUM_HEADER: usize = 1 + CHECKSUM_SIZE;

#[cfg(feature = "compression")]
const PROMISED_COMPRESS_OVERHEAD: usize = 1 + core::mem::size_of::<u32>();

const DEFAULT_PORT: u16 = 7946;
const LABEL_MAX_SIZE: usize = 255;
const DEFAULT_BUFFER_SIZE: usize = 4096;
const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;

/// Used to buffer incoming packets during read
/// operations.
const PACKET_BUF_SIZE: usize = 65536;

/// A large buffer size that we attempt to set UDP
/// sockets to in order to handle a large volume of messages.
const UDP_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

/// Errors that can occur when using [`NetTransport`].
#[derive(thiserror::Error)]
pub enum NetTransportError<A: AddressResolver, W: Wire> {
  /// Connection error.
  #[error("connection error: {0}")]
  Connection(#[from] ConnectionError),
  /// IO error.
  #[error("io error: {0}")]
  IO(#[from] std::io::Error),
  /// Returns when there is no explicit advertise address and no private IP address found.
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  /// Returns when there is no interface addresses found.
  #[error("failed to get interface addresses {0}")]
  NoInterfaceAddresses(#[from] local_ip_address::Error),
  /// Returns when there is no bind address provided.
  #[error("at least one bind address is required")]
  EmptyBindAddrs,
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
  Resolve { addr: A::Address, err: A::Error },
  /// Returns when fail to compress
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error("failed to compress message: {0}")]
  Compress(#[from] compressor::CompressError),
  /// Returns when fail to decompress
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error("failed to decompress message: {0}")]
  Decompress(#[from] compressor::DecompressError),
  /// Returns when there is a unkstartn compressor.
  #[cfg(feature = "compression")]
  #[error("{0}")]
  UnknownCompressor(#[from] UnknownCompressor),
  /// Returns when there is a security error. e.g. encryption/decryption error.
  #[error("{0}")]
  Security(#[from] SecurityError),
  /// Returns when receiving a compressed message, but compression is disabled.
  #[error("receive a compressed message, but compression is disabled on this node")]
  CompressionDisabled,
  /// Returns when the label error.
  #[error("{0}")]
  Label(#[from] label::LabelError),
  /// Returns when getting unkstartn checksumer
  #[error("{0}")]
  UnknownChecksumer(#[from] checksum::UnknownChecksumer),
  /// Returns when the computation task panic
  #[error("computation task panic")]
  ComputationTaskFailed,
  /// Returns when encode/decode error.
  #[error("wire error: {0}")]
  Wire(W::Error),
  /// Returns when the packet is too large.
  #[error("packet too large, the maximum packet can be sent is 65535, got {0}")]
  PacketTooLarge(usize),
  /// Returns when there is a custom error.
  #[error("custom error: {0}")]
  Custom(std::borrow::Cow<'static, str>),
}

impl<A: AddressResolver, W: Wire> core::fmt::Debug for NetTransportError<A, W> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A: AddressResolver, W: Wire> TransportError for NetTransportError<A, W> {
  fn io(err: std::io::Error) -> Self {
    Self::IO(err)
  }

  fn is_remote_failure(&self) -> bool {
    if let Self::Connection(e) = self {
      e.is_remote_failure()
    } else {
      false
    }
  }

  fn is_unexpected_eof(&self) -> bool {
    match self {
      Self::Connection(e) => e.is_eof(),
      Self::IO(e) => e.kind() == std::io::ErrorKind::UnexpectedEof,
      _ => false,
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}

fn default_gossip_verify_outgoing() -> bool {
  true
}

/// Used to configure a net transport.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize",
    deserialize = "I: for<'a> serde::Deserialize<'a>, A: AddressResolver, A::Address: for<'a> serde::Deserialize<'a>, A::ResolvedAddress: for<'a> serde::Deserialize<'a>"
  ))
)]
pub struct NetTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  /// The local node's ID.
  id: I,

  /// The local node's address.
  address: A::Address,

  /// The address to advertise to other nodes. If not set,
  /// the transport will attempt to discover the local IP address
  /// to use.
  advertise_address: Option<A::ResolvedAddress>,

  /// A list of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(getter(style = "ref", const,))]
  bind_addrs: SmallVec<IpAddr>,
  bind_port: Option<u16>,

  label: Label,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  skip_inbound_label_check: bool,

  /// Controls whether to enforce encryption for outgoing
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  #[cfg_attr(feature = "serde", serde(default = "default_gossip_verify_outgoing"))]
  gossip_verify_outgoing: bool,

  max_payload_size: usize,

  /// Policy for Classless Inter-Domain Routing (CIDR).
  ///
  /// By default, allow any connection
  #[viewit(getter(style = "ref", const,))]
  #[cfg_attr(feature = "serde", serde(default))]
  cidrs_policy: CIDRsPolicy,

  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization. This is only available starting at protocol version 1.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  compressor: Option<Compressor>,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  #[cfg(any(feature = "compression", feature = "encryption"))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "compression", feature = "encryption"))))]
  offload_size: usize,

  /// Used to initialize the primary encryption key in a keyring.
  ///
  /// **Note: This field will not be used when network layer is secure**
  ///
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  primary_key: Option<SecretKey>,

  /// Holds all of the encryption keys used internally. It is
  /// automatically initialized using the SecretKey and SecretKeys values.
  ///
  /// **Note: This field will not be used if the network layer is secure.**
  #[viewit(getter(
    style = "ref",
    result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeys>")
  ))]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  secret_keys: Option<SecretKeys>,

  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  encryption_algo: Option<EncryptionAlgo>,

  /// The checksumer to use for checksumming packets.
  #[cfg_attr(feature = "serde", serde(default))]
  checksumer: Checksumer,

  #[cfg(feature = "metrics")]
  metric_labels: Option<Arc<showbiz_utils::MetricLabels>>,
}

pub struct NetTransport<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer, W> {
  opts: Arc<NetTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  promised_listeners: SmallVec<Arc<S::Listener>>,
  sockets: SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>,
  stream_layer: Arc<S>,
  #[cfg(feature = "encryption")]
  encryptor: Option<Encryptor>,

  wg: AsyncWaitGroup,
  resolver: Arc<A>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  _marker: PhantomData<W>,
}

impl<I, A, S, W> NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire,
{
  /// Creates a new net transport.
  pub async fn new(
    resolver: A,
    stream_layer: S,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A, W>> {
    match opts.bind_port {
      Some(0) | None => Self::retry(resolver, stream_layer, 10, opts).await,
      _ => Self::retry(resolver, stream_layer, 1, opts).await,
    }
  }

  async fn new_in(
    resolver: Arc<A>,
    stream_layer: Arc<S>,
    opts: Arc<NetTransportOptions<I, A>>,
    #[cfg(feature = "encryption")] encryptor: Option<Encryptor>,
  ) -> Result<Self, NetTransportError<A, W>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addrs.is_empty() {
      return Err(NetTransportError::EmptyBindAddrs);
    }

    let (stream_tx, stream_rx) = promised_stream::<Self>();
    let (packet_tx, packet_rx) = packet_stream::<Self>();
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut promised_listeners = Vec::with_capacity(opts.bind_addrs.len());
    let mut sockets = Vec::with_capacity(opts.bind_addrs.len());
    let bind_port = opts.bind_port.unwrap_or(0);
    for &addr in opts.bind_addrs.iter() {
      let addr = SocketAddr::new(addr, bind_port);
      let (local_addr, ln) = match stream_layer.bind(addr).await {
        Ok(ln) => (ln.local_addr().unwrap(), ln),
        Err(e) => return Err(NetTransportError::ListenPromised(addr, e)),
      };
      promised_listeners.push((Arc::new(ln), local_addr));
      // If the config port given was zero, use the first TCP listener
      // to pick an available port and then apply that to everything
      // else.
      let addr = if bind_port == 0 { local_addr } else { addr };

      let (local_addr, packet_socket) =
        <<<A::Runtime as Runtime>::Net as Net>::UdpSocket as UdpSocket>::bind(addr)
          .await
          .map(|ln| (addr, ln))
          .map_err(|e| NetTransportError::ListenPacket(addr, e))?;
      sockets.push((Arc::new(packet_socket), local_addr));
    }

    let wg = AsyncWaitGroup::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for ((promised_ln, promised_addr), (socket, socket_addr)) in
      promised_listeners.iter().zip(sockets.iter())
    {
      wg.add(2);
      PromisedProcessor::<A, Self, S> {
        wg: wg.clone(),
        stream_tx: stream_tx.clone(),
        ln: promised_ln.clone(),
        shutdown: shutdown.clone(),
        shutdown_rx: shutdown_rx.clone(),
        local_addr: *promised_addr,
      }
      .run();

      PacketProcessor::<A, Self> {
        wg: wg.clone(),
        packet_tx: packet_tx.clone(),
        label: opts.label.clone(),
        offload_size: opts.offload_size,
        #[cfg(feature = "encryption")]
        encryptor: encryptor.clone(),
        socket: socket.clone(),
        local_addr: *socket_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
        shutdown_rx: shutdown_rx.clone(),
        skip_inbound_label_check: opts.skip_inbound_label_check,
      }
      .run();
    }

    // find final advertise address
    let advertise_addr = match opts.advertise_address {
      Some(addr) => addr,
      None => {
        let addr = if opts.bind_addrs[0].is_unspecified() {
          local_ip_address::local_ip().map_err(|e| match e {
            local_ip_address::Error::LocalIpAddressNotFound => NetTransportError::NoPrivateIP,
            e => NetTransportError::NoInterfaceAddresses(e),
          })?
        } else {
          promised_listeners[0].1.ip()
        };

        // Use the port we are bound to.
        SocketAddr::new(addr, promised_listeners[0].1.port())
      }
    };

    Ok(Self {
      advertise_addr,
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      promised_listeners: promised_listeners.into_iter().map(|(ln, _)| ln).collect(),
      sockets: sockets.into_iter().map(|(ln, _)| ln).collect(),
      stream_layer,
      #[cfg(feature = "encryption")]
      encryptor,
      resolver,
      shutdown_tx,
      _marker: PhantomData,
    })
  }

  async fn retry(
    resolver: A,
    stream_layer: S,
    limit: usize,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A, W>> {
    let mut i = 0;
    let resolver = Arc::new(resolver);
    let stream_layer = Arc::new(stream_layer);
    let opts = Arc::new(opts);
    #[cfg(feature = "encryption")]
    let keyring = if S::is_secure() || opts.encryption_algo.is_none() {
      None
    } else {
      match (opts.primary_key, &opts.secret_keys) {
        (None, Some(keys)) if !keys.is_empty() => {
          tracing::warn!(target: "showbiz", "using first key in keyring as primary key");
          let mut iter = keys.iter().copied();
          let pk = iter.next().unwrap();
          let keyring = SecretKeyring::with_keys(pk, iter);
          Some(keyring)
        }
        (Some(pk), None) => Some(SecretKeyring::new(pk)),
        (Some(pk), Some(keys)) => Some(SecretKeyring::with_keys(pk, keys.iter().copied())),
        _ => None,
      }
    }
    .map(|kr| Encryptor::new(opts.encryption_algo.unwrap(), kr));
    loop {
      #[cfg(feature = "metrics")]
      let transport = {
        Self::new_in(
          resolver.clone(),
          stream_layer.clone(),
          opts.clone(),
          #[cfg(feature = "encryption")]
          keyring.clone(),
        )
        .await
      };

      match transport {
        Ok(t) => {
          if let Some(0) | None = opts.bind_port {
            let port = t.advertise_addr.port();
            tracing::warn!(target:  "showbiz", "using dynamic bind port {port}");
          }
          return Ok(t);
        }
        Err(e) => {
          tracing::debug!(target="showbiz", err=%e, "fail to create transport");
          if i == limit - 1 {
            return Err(e);
          }
          i += 1;
        }
      }
    }
  }

  fn fix_packet_overhead(&self) -> usize {
    let mut overhead = self.opts.label.encoded_overhead();
    overhead += 1 + CHECKSUM_SIZE;

    #[cfg(feature = "compression")]
    if self.opts.compressor.is_some() {
      overhead += 1 + core::mem::size_of::<u32>();
    }

    #[cfg(feature = "encryption")]
    if let Some(encryptor) = &self.encryptor {
      if self.opts.gossip_verify_outgoing {
        overhead += encryptor.encrypt_overhead();
      }
    }

    overhead
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

impl<I, A, S, W> Transport for NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire,
{
  type Error = NetTransportError<Self::Resolver, Self::Wire>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

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

  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.opts.address
  }

  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  fn max_payload_size(&self) -> usize {
    MAX_PACKET_SIZE.min(self.opts.max_payload_size)
  }

  fn packet_overhead(&self) -> usize {
    PACKET_OVERHEAD
  }

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
    let mut conn = BufReader::new(conn).peekable();
    let mut stream_label = label::remove_label_header(&mut conn).await.map_err(|e| {
      tracing::error!(target: "showbiz.net.promised", remote = %from, err=%e, "failed to receive and remove the stream label header");
      Self::Error::Connection(ConnectionError::promised_read(e))
    })?.unwrap_or_else(Label::empty);

    let label = &self.opts.label;

    if self.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(target: "showbiz.net.promised", "unexpected double stream label header");
        return Err(LabelError::duplicate(label.cheap_clone(), stream_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      stream_label = label.cheap_clone();
    }

    if stream_label.ne(&self.opts.label) {
      tracing::error!(target: "showbiz.net.promised", local_label=%label, remote_label=%stream_label, "discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), stream_label).into());
    }

    let readed = stream_label.encoded_overhead();

    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    return self
      .read_from_promised_without_compression_and_encryption(conn)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    return self
      .read_from_promised_with_compression_without_encryption(conn)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    return self
      .read_from_promised_with_encryption_without_compression(conn, from)
      .await
      .map(|(read, msg)| (readed + read, stream_label, msg));

    #[cfg(all(feature = "compression", feature = "encryption"))]
    self
      .read_from_promised_with_compression_and_encryption(conn, stream_label, from)
      .await
      .map(|(read, msg)| (readed + read, msg))
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    target: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    self.send_by_promised(conn, target, msg).await
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, Instant), Self::Error> {
    let start = Instant::now();
    let encoded_size = W::encoded_len(&packet);
    self
      .send_batch(
        addr,
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
  ) -> Result<(usize, Instant), Self::Error> {
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
          addr,
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
        futures::future::join_all(batches.into_iter().map(|b| self.send_batch(addr, b))).await;

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
    timeout: Duration,
  ) -> Result<Self::Stream, Self::Error> {
    let connector = <Self::Runtime as Runtime>::timeout(timeout, self.stream_layer.connect(*addr));
    match connector.await {
      Ok(Ok(mut conn)) => {
        if self.opts.label.is_empty() {
          Ok(conn)
        } else {
          conn
            .add_label_header(&self.opts.label)
            .await
            .map_err(|e| Self::Error::Connection(ConnectionError::promised_write(e)))?;
          Ok(conn)
        }
      }
      Ok(Err(e)) => Err(Self::Error::Connection(ConnectionError {
        kind: ConnectionKind::Promised,
        error_kind: ConnectionErrorKind::Dial,
        error: e,
      })),
      Err(_) => Err(NetTransportError::Connection(ConnectionError {
        kind: ConnectionKind::Promised,
        error_kind: ConnectionErrorKind::Dial,
        error: Error::new(ErrorKind::TimedOut, "timeout"),
      })),
    }
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
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }

  fn block_shutdown(&self) -> Result<(), Self::Error> {
    use pollster::FutureExt as _;
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();
    let wg = self.wg.clone();

    wg.wait().block_on();
    Ok(())
  }
}

struct PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  wg: AsyncWaitGroup,
  stream_tx: StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, S::Stream>,
  ln: Arc<S::Listener>,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<A, T, S> PromisedProcessor<A, T, S>
where
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Resolver = A, Stream = S::Stream, Runtime = A::Runtime>,
  S: StreamLayer,
{
  fn run(self) {
    let Self {
      wg,
      stream_tx,
      ln,
      shutdown,
      local_addr,
      ..
    } = self;

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    <T::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      let mut loop_delay = Duration::ZERO;
      loop {
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = ln.accept().fuse() => {
            match rst {
              Ok((conn, remote_addr)) => {
                // No error, reset loop delay
                loop_delay = Duration::ZERO;
                if let Err(e) = stream_tx
                  .send(remote_addr, conn)
                  .await
                {
                  tracing::error!(target:  "showbiz.net.transport", local_addr=%local_addr, err = %e, "failed to send TCP connection");
                }
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
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

                tracing::error!(target:  "showbiz.net.transport", local_addr=%local_addr, err = %e, "error accepting TCP connection");
                <T::Runtime as Runtime>::sleep(loop_delay).await;
                continue;
              }
            }
          }
        }
      }
    });
  }
}

struct PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
{
  wg: AsyncWaitGroup,
  packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  socket: Arc<<<T::Runtime as Runtime>::Net as Net>::UdpSocket>,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  label: Label,
  #[cfg(feature = "encryption")]
  encryptor: Option<Encryptor>,
  offload_size: usize,
  skip_inbound_label_check: bool,
  #[cfg(feature = "metrics")]
  metric_labels: Arc<showbiz_utils::MetricLabels>,
}

impl<A, T> PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
{
  fn run(self) {
    let Self {
      wg,
      packet_tx,
      socket,
      shutdown,
      local_addr,
      ..
    } = self;

    <T::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      tracing::info!(
        target: "showbiz.net.transport",
        "udp listening on {local_addr}"
      );

      loop {
        // Do a blocking read into a fresh buffer. Grab a time stamp as
        // close as possible to the I/O.
        let mut buf = BytesMut::new();
        buf.resize(PACKET_BUF_SIZE, 0);
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = socket.recv_from(&mut buf).fuse() => {
            match rst {
              Ok((n, addr)) => {
                // Check the length - it needs to have at least one byte to be a
                // proper message.
                if n < 1 {
                  tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = "UDP packet too short (0 bytes)");
                  continue;
                }
                buf.truncate(n);
                let start = Instant::now();
                let msg = match Self::handle_remote_bytes(
                  buf,
                  &self.label,
                  self.skip_inbound_label_check,
                  #[cfg(feature = "encryption")]
                  self.encryptor.as_ref(),
                  #[cfg(any(feature = "compression", feature = "encryption"))]
                  self.offload_size,
                ).await {
                  Ok(msg) => msg,
                  Err(e) => {
                    tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = %e, "fail to handle UDP packet");
                    continue;
                  }
                };

                #[cfg(feature = "metrics")]
                {
                  metrics::counter!("showbiz.packet.bytes.processing", self.metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);
                }

                if let Err(e) = packet_tx.send(Packet::new(msg, addr, start)).await {
                  tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = %e, "failed to send packet");
                }

                #[cfg(feature = "metrics")]
                metrics::counter!("showbiz.packet.received", self.metric_labels.iter()).increment(n as u64);
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                  break;
                }

                tracing::error!(target: "showbiz.net.transport", peer=%local_addr, err = %e, "error reading UDP packet");
                continue;
              }
            };
          }
        }
      }
    });
  }

  async fn handle_remote_bytes(
    mut buf: BytesMut,
    label: &Label,
    skip_inbound_label_check: bool,
    #[cfg(feature = "encryption")] encryptor: Option<&Encryptor>,
    #[cfg(any(feature = "encryption", feature = "compression"))] offload_size: usize,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    let mut packet_label = buf.remove_label_header()?.unwrap_or_else(Label::empty);

    if skip_inbound_label_check {
      if !packet_label.is_empty() {
        return Err(LabelError::duplicate(label.cheap_clone(), packet_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      packet_label = label.cheap_clone();
    }

    if packet_label.ne(label) {
      tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    let mut buf: Bytes = if ENCRYPT_TAG.contains(&buf[0]) {
      #[cfg(not(feature = "encryption"))]
      return Err(NetTransportError::EncryptionDisabled);

      let encryptor = match encryptor {
        Some(encryptor) => encryptor,
        None => {
          return Err(SecurityError::Disabled.into());
        }
      };

      let buf_len = buf.len();
      if buf_len > offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        {
          let keys = encryptor.kr.keys().await;
          let keyring = encryptor.kr.clone();
          let label = label.cheap_clone();
          rayon::spawn(move || {
            match keyring
              .decrypt_payload(&mut buf, keys, packet_label.as_bytes())
              .map_err(NetTransportError::Security)
            {
              Ok(_) => {}
              Err(e) => {
                if tx.send(Err(e)).is_err() {
                  tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send decryption result back to main thread");
                }
                return;
              }
            };

            if CHECKSUM_TAG.contains(&buf[0]) {
              let checksumer = match Checksumer::try_from(buf[0]) {
                Ok(checksumer) => checksumer,
                Err(e) => {
                  if tx.send(Err(e.into())).is_err() {
                    tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                  }
                  return;
                }
              };
              buf.advance(1);
              let expected = NetworkEndian::read_u32(&buf[..checksum::CHECKSUM_SIZE]);
              buf.advance(checksum::CHECKSUM_SIZE);
              let actual = checksumer.checksum(&buf);
              if actual != expected {
                if tx
                  .send(Err(NetTransportError::IO(Error::new(
                    ErrorKind::InvalidData,
                    "checksum mismatch",
                  ))))
                  .is_err()
                {
                  tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                }
                return;
              }
            }

            if COMPRESS_TAG.contains(&buf[0]) {
              #[cfg(not(feature = "compression"))]
              {
                if tx
                  .send(Err(NetTransportError::CompressionDisabled))
                  .is_err()
                {
                  tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                }
                return;
              }

              let compress_tag = buf[0];
              buf.advance(1);
              let compressor = match Compressor::try_from(compress_tag)
                .map_err(NetTransportError::UnknownCompressor)
              {
                Ok(compressor) => compressor,
                Err(e) => {
                  if tx.send(Err(e)).is_err() {
                    tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
                  }
                  return;
                }
              };

              if tx
                .send(
                  compressor
                    .decompress(&buf)
                    .map(Bytes::from)
                    .map_err(NetTransportError::Decompress),
                )
                .is_err()
              {
                tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send back to main thread");
              }
              return;
            }

            if tx.send(Ok(buf.freeze())).is_err() {
              tracing::error!(target: "showbiz.net.packet", local_label=%label, remote_label=%packet_label, "failed to send decryption result back to main thread");
            }
          });
        }

        match rx.await {
          Ok(Ok(buf)) => buf,
          Ok(Err(e)) => return Err(e),
          Err(_) => return Err(NetTransportError::ComputationTaskFailed),
        }
      } else {
        {
          let keys = encryptor.kr.keys().await;
          encryptor
            .kr
            .decrypt_payload(&mut buf, keys, packet_label.as_bytes())
            .map_err(NetTransportError::Security)?;
        }

        Self::check_checksum_and_decompress(buf, offload_size).await?
      }
    } else {
      Self::check_checksum_and_decompress(buf, offload_size).await?
    };

    if buf[0] == Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG {
      buf.advance(1);
      let num_msgs = buf[0] as usize;
      buf.advance(1);
      let mut msgs = OneOrMore::with_capacity(num_msgs);
      while msgs.len() != num_msgs {
        let msg_len = NetworkEndian::read_u16(&buf[..2]) as usize;
        buf.advance(2);
        let msg_bytes = buf.split_to(msg_len);
        let msg = <T::Wire as Wire>::decode_message(&msg_bytes).map_err(NetTransportError::Wire)?;
        msgs.push(msg);
      }
      Ok(msgs)
    } else {
      <T::Wire as Wire>::decode_message(&buf)
        .map(Into::into)
        .map_err(NetTransportError::Wire)
    }
  }

  async fn check_checksum_and_decompress(
    mut buf: BytesMut,
    offload_size: usize,
  ) -> Result<Bytes, NetTransportError<T::Resolver, T::Wire>> {
    if CHECKSUM_TAG.contains(&buf[0]) {
      let checksumer = Checksumer::try_from(buf[0])?;
      buf.advance(1);
      let expected = NetworkEndian::read_u32(&buf[..checksum::CHECKSUM_SIZE]);
      buf.advance(checksum::CHECKSUM_SIZE);
      let actual = checksumer.checksum(&buf);
      if actual != expected {
        return Err(NetTransportError::IO(Error::new(
          ErrorKind::InvalidData,
          "checksum mismatch",
        )));
      }
    }

    Ok(if COMPRESS_TAG.contains(&buf[0]) {
      #[cfg(not(feature = "compression"))]
      return Err(NetTransportError::CompressionDisabled);

      let compress_tag = buf[0];
      buf.advance(1);
      let compressor = Compressor::try_from(compress_tag)?;

      if buf.len() > offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        rayon::spawn(move || {
          let buf = compressor
            .decompress(&buf)
            .map_err(NetTransportError::Decompress);
          if tx.send(buf).is_err() {
            tracing::error!(target: "showbiz.net.packet", "failed to send back to main thread");
          }
        });
        match rx.await {
          Ok(Ok(buf)) => buf.into(),
          Ok(Err(e)) => return Err(e),
          Err(_) => return Err(NetTransportError::ComputationTaskFailed),
        }
      } else {
        compressor.decompress(&buf)?.into()
      }
    } else {
      buf.freeze()
    })
  }
}
