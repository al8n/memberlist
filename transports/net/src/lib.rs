use std::{
  borrow::Cow, io::{Error, ErrorKind}, marker::PhantomData, net::{IpAddr, SocketAddr}, sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  }, time::{Duration, Instant}
};

use agnostic::Runtime;
use bytes::{Buf, Bytes, BytesMut};
use byteorder::{ByteOrder, NetworkEndian};
use compressor::UnknownCompressor;
use futures::FutureExt;
use label::remove_label_from_bytes;
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
use showbiz_utils::net::CIDRsPolicy;
use wg::AsyncWaitGroup;

const CHECKSUM_TAG: core::ops::RangeInclusive<u8> = 44..=64;
const ENCRYPT_TAG: core::ops::RangeInclusive<u8> = 65..=85;
const COMPRESS_TAG: core::ops::RangeInclusive<u8> = 86..=126;
const LABEL_TAG: u8 = 127;


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
use security::{SecretKey, SecretKeyring, SecretKeys};

/// Errors for the net transport.
pub mod error;
use error::*;

mod stream;
pub use stream::*;
mod label;
pub use label::Label;

mod checksum;
pub use checksum::Checksumer;

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
  /// Returns when there is a unknown compressor.
  #[cfg(feature = "compression")]
  UnknownCompressor(#[from] UnknownCompressor),
  /// Returns when encode/decode error.
  #[error("wire error: {0}")]
  Wire(W::Error),
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

  #[cfg(feature = "metrics")]
  metric_labels: Option<Arc<showbiz_utils::MetricLabels>>,
}

pub struct NetTransport<
  I,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  P: PacketLayer,
  S: StreamLayer,
  W,
  C = checksum::Crc32,
> {
  opts: Arc<NetTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  promised_listeners: SmallVec<Arc<S::Listener>>,
  sockets: SmallVec<Arc<P::Stream>>,
  stream_layer: Arc<S>,
  packet_layer: Arc<P>,
  #[cfg(feature = "encryption")]
  keyring: Option<SecretKeyring>,
  wg: AsyncWaitGroup,
  resolver: Arc<A>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  _marker: PhantomData<(W, C)>,
}

impl<I, A, P, S, W, C> NetTransport<I, A, P, S, W, C>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  P: PacketLayer,
  S: StreamLayer,
  W: Wire,
  C: Checksumer,
{
  /// Creates a new net transport.
  pub async fn new(
    resolver: A,
    packet_layer: P,
    stream_layer: S,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A>> {
    match opts.bind_port {
      Some(0) | None => Self::retry(resolver, packet_layer, stream_layer, 10, opts).await,
      _ => Self::retry(resolver, packet_layer, stream_layer, 1, opts).await,
    }
  }

  async fn new_in(
    resolver: Arc<A>,
    packet_layer: Arc<P>,
    stream_layer: Arc<S>,
    opts: Arc<NetTransportOptions<I, A>>,
    #[cfg(feature = "encryption")]
    keyring: Option<SecretKeyring>,
  ) -> Result<Self, NetTransportError<A>> {
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

      let (local_addr, packet_socket) = packet_layer
        .bind(addr)
        .await
        .map(|ln| (addr, ln))
        .map_err(|e| NetTransportError::ListenPacket(addr, e))?;
      sockets.push((Arc::new(packet_socket), local_addr));
    }

    let wg = AsyncWaitGroup::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Fire them up now that we've been able to create them all.
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

      PacketProcessor::<A, Self, P> {
        wg: wg.clone(),
        packet_tx: packet_tx.clone(),
        label: opts.label.clone(),
        offload_size: opts.offload_size,
        compressor: opts.compressor,
        keyring: keyring.clone(),
        socket: socket.clone(),
        local_addr: *socket_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
        shutdown_rx: shutdown_rx.clone(),
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
      packet_layer,
      #[cfg(feature = "encryption")]
      keyring,
      resolver,
      shutdown_tx,
      _marker: PhantomData,
    })
  }

  async fn retry(
    resolver: A,
    packet_layer: P,
    stream_layer: S,
    limit: usize,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A>> {
    let mut i = 0;
    let resolver = Arc::new(resolver);
    let packet_layer = Arc::new(packet_layer);
    let stream_layer = Arc::new(stream_layer);
    let opts = Arc::new(opts);
    #[cfg(feature = "encryption")]
    let keyring = if P::is_secure() && S::is_secure() {
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
    };
    loop {
      #[cfg(feature = "metrics")]
      let transport = {
        Self::new_in(
          resolver.clone(),
          packet_layer.clone(),
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
}

impl<I, A, P, S, W, C> Transport for NetTransport<I, A, P, S, W, C>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  P: PacketLayer,
  S: StreamLayer,
  W: Wire,
  C: Checksumer,
{
  type Error = NetTransportError<Self::Resolver, Self::Wire>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

  // async fn write_to(&self, b: &[u8], addr: SocketAddr) -> Result<Instant, TransportError<Self>> {
  //   if let Some(label) = &self.label {
  //     // TODO: is there a better way to avoid allocating and copying?
  //     if !label.is_empty() {
  //       let b =
  //         Label::add_label_header_to_packet(b, label.as_bytes()).map_err(TransportError::Encode)?;
  //       return PacketStream::send_to(&self.udp_listener, addr, &b)
  //         .await
  //         .map(|_| Instant::now());
  //     }
  //   }

  //   PacketStream::send_to(&self.udp_listener, addr, b)
  //     .await
  //     .map(|_| Instant::now())
  // }

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
    self.opts.max_payload_size
  }

  fn packet_overhead(&self) -> usize {
    todo!()
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
    conn: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      showbiz_core::types::Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    todo!()
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    target: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    msg: showbiz_core::types::Message<
      Self::Id,
      <Self::Resolver as AddressResolver>::ResolvedAddress,
    >,
  ) -> Result<usize, Self::Error> {
    todo!()
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: showbiz_core::types::Message<
      Self::Id,
      <Self::Resolver as AddressResolver>::ResolvedAddress,
    >,
  ) -> Result<(usize, Instant), Self::Error> {
    todo!()
  }

  async fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<
      showbiz_core::types::Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    >,
  ) -> Result<(usize, Instant), Self::Error> {
    todo!()
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
          conn.add_label_header(&self.opts.label).await.map_err(|e| {
            Self::Error::Connection(ConnectionError {
              kind: ConnectionKind::Promised,
              error_kind: ConnectionErrorKind::Write,
              error: e,
            })
          })?;
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

struct PacketProcessor<A, T, P, C = checksum::Crc32>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
  P: PacketLayer,
{
  wg: AsyncWaitGroup,
  packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  socket: Arc<P::Stream>,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
  label: Label,
  compressor: Option<Compressor>,
  keyring: Option<SecretKeyring>,
  offload_size: usize,
  #[cfg(feature = "metrics")]
  metric_labels: Arc<showbiz_utils::MetricLabels>,
  _checksum: PhantomData<C>,
}

impl<A, T, P, C> PacketProcessor<A, T, P, C>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
  P: PacketLayer,
  C: Checksumer,
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
                  buf.freeze(),
                  &self.label,
                  #[cfg(feature = "compression")]
                  self.compressor.as_ref(),
                  #[cfg(feature = "encryption")]
                  self.keyring.as_ref(),
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
    mut buf: Bytes,
    label: &Label,
    #[cfg(feature = "compression")] compressor: Option<&Compressor>,
    #[cfg(feature = "encryption")] keyring: Option<&SecretKeyring>,
    #[cfg(any(feature = "encryption", feature = "compression"))] offload_size: usize,
  ) -> Result<
    Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    T::Error,
  > {
    if buf[0] == LABEL_TAG {
      buf.advance(1);
      match remove_label_from_bytes(&mut buf)? {
        Some(stream_label) => {
          if stream_label.ne(label) {
            tracing::warn!(target: "showbiz.net.packet", local_label=%label, remote_label=%stream_label, packet=?buf.as_ref(), "label does not match, drop packet");
            return Err(NetTransportError::IO(Error::new(ErrorKind::InvalidData, "label mismatch")));
          }
        }
        None => {
          if !label.is_empty() {
            tracing::warn!(target: "showbiz.net.packet", local_label=%label, remote_label = "", packet=?buf.as_ref(), "label does not match, drop packet");
            return Err(NetTransportError::IO(Error::new(ErrorKind::InvalidData, "label mismatch")));
          }
        },
      }
    }

    if CHECKSUM_TAG.contains(&buf[0]) {
      buf.advance(1);
      let expected = NetworkEndian::read_u32(&buf[..checksum::CHECKSUM_SIZE]);
      buf.advance(checksum::CHECKSUM_SIZE);

      let mut cks = C::new();
      cks.update(&buf);
      let checksum = cks.finalize();

      if checksum != expected {
        return Err(NetTransportError::IO(Error::new(
          ErrorKind::InvalidData,
          "checksum mismatch",
        )));
      }
    }

    if COMPRESS_TAG.contains(&buf[0]) {
      #[cfg(not(feature = "compression"))]
      return Err(NetTransportError::IO(Error::new(
        ErrorKind::InvalidData,
        "compression not enabled",
      )));

      let compress_tag = buf[0];
      buf.advance(1);
      let compressor = Compressor::try_from(compress_tag)?;
      let buf_len = buf.len();
      if buf_len > offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        #[cfg(feature = "encryption")]
        let keyring = keyring.clone();
        rayon::spawn(move || {
          let buf: Bytes = match compressor.decompress(&buf).map_err(NetTransportError::Decompress) {
            Ok(buf) => buf.into(),
            Err(e) => {
              tx.send(Err(e)).unwrap();
              return;
            }
          };

          if ENCRYPT_TAG.contains(&buf[0]) {
            #[cfg(not(feature = "encryption"))]
            return tx.send(Err(NetTransportError::IO(Error::new(
              ErrorKind::InvalidData,
              "encryption not enabled",
            )))).unwrap();

            let encrypt_tag = buf[0];
            buf.advance(1);
            let keyring = match keyring {
              Some(keyring) => keyring,
              None => {
                tx.send(Err(NetTransportError::IO(Error::new(ErrorKind::InvalidData, "encryption not enabled")))).unwrap();
                return;
              }
            };

            match keyring.decrypt(&buf).map_err(NetTransportError::Wire) {
              Ok(buf) => tx.send(Ok(buf)).unwrap(),
              Err(e) => {
                tx.send(Err(e)).unwrap();
                return;
              }
            };
          } else {
            tx.send(Ok(buf)).unwrap();
          }
        });
        buf = rx.await.unwrap()?;
      } else {
        buf = compressor.decompress(&buf)?.into();
      }
    } else if ENCRYPT_TAG.contains(&buf[0]) {
      #[cfg(not(feature = "encryption"))]
      return Err(NetTransportError::IO(Error::new(
        ErrorKind::InvalidData,
        "encryption not enabled",
      )));

      let encrypt_tag = buf[0];
      buf.advance(1);

      let keyring = keyring.ok_or_else(|| {
        NetTransportError::IO(Error::new(ErrorKind::InvalidData, "encryption not enabled"))
      })?;
      let buf_len = buf.len();
      if buf_len > offload_size {
        let (tx, rx) = futures::channel::oneshot::channel();
        rayon::spawn(move || {
          let buf = keyring.decrypt(&buf).map_err(NetTransportError::Wire);
          tx.send(buf).unwrap();
        });
        buf = rx.await.unwrap()?.into();
      } else {
        buf = keyring.decrypt(&buf)?.into();
      } 
    }

    <T::Wire as Wire>::decode_message(&buf)
      .map_err(NetTransportError::Wire)
  }
}
