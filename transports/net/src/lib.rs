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

use agnostic::Runtime;
use bytes::BytesMut;
use futures::FutureExt;
use showbiz_core::{
  transport::{
    resolver::AddressResolver,
    stream::{
      packet_stream, promised_stream, PacketProducer, PacketSubscriber, StreamProducer,
      StreamSubscriber,
    },
    Id, Transport, TransportError, Wire,
  },
  types::{Message, Packet},
  CheapClone,
};
use showbiz_utils::net::CIDRsPolicy;
use wg::AsyncWaitGroup;

/// Compress/decompress related.
pub mod compressor;
use compressor::Compressor;

/// Encrypt/decrypt related.
pub mod security;
use security::{SecretKey, SecretKeyring};

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

#[derive(thiserror::Error)]
pub enum NetTransportError<A: AddressResolver> {
  /// Connection error.
  #[error("connection error: {0}")]
  Connection(#[from] ConnectionError),
  /// IO error.
  #[error("io error: {0}")]
  IO(#[from] std::io::Error),
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  #[error("failed to get interface addresses {0}")]
  NoInterfaceAddresses(#[from] local_ip_address::Error),
  #[error("at least one bind address is required")]
  EmptyBindAddrs,
  #[error("the ip {0} is blocked")]
  BlockedIp(IpAddr),
  #[error("failed to resize UDP buffer {0}")]
  ResizeUdpBuffer(std::io::Error),
  #[error("failed to start packet listener on {0}: {1}")]
  ListenPacket(SocketAddr, std::io::Error),
  #[error("failed to start promised listener on {0}: {1}")]
  ListenPromised(SocketAddr, std::io::Error),
  #[error("failed to resolve address {addr}: {err}")]
  Resolve { addr: A::Address, err: A::Error },
  #[error("custom error: {0}")]
  Custom(std::borrow::Cow<'static, str>),
}

impl<A: AddressResolver> core::fmt::Debug for NetTransportError<A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A: AddressResolver> TransportError for NetTransportError<A> {
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
  #[viewit(getter(
    style = "ref",
    result(type = "&[IpAddr]", converter(fn = "Vec::as_slice"))
  ))]
  bind_addrs: Vec<IpAddr>,
  bind_port: Option<u16>,

  label: Label,
  max_payload_size: usize,

  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization. This is only available starting at protocol version 1.
  compressor: Option<Compressor>,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  offload_size: usize,

  /// Policy for Classless Inter-Domain Routing (CIDR).
  ///
  /// By default, allow any connection
  #[viewit(getter(style = "ref", const,))]
  #[cfg_attr(feature = "serde", serde(default))]
  cidrs_policy: CIDRsPolicy,

  /// Used to initialize the primary encryption key in a keyring.
  ///
  /// **Note: This field will not be used when network layer is secure**
  ///
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  secret_key: Option<SecretKey>,

  /// Holds all of the encryption keys used internally. It is
  /// automatically initialized using the SecretKey and SecretKeys values.
  ///
  /// **Note: This field will not be used if the network layer is secure.**
  #[viewit(getter(
    style = "ref",
    result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeyring>")
  ))]
  secret_keyring: Option<SecretKeyring>,

  #[cfg_attr(feature = "serde", serde(skip))]
  #[cfg(feature = "metrics")]
  metric_labels: Option<Arc<Vec<metrics::Label>>>,
}

pub struct NetTransport<
  I,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  P: PacketLayer,
  S: StreamLayer,
  W,
> {
  opts: Arc<NetTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  promised_listeners: Vec<Arc<S::Listener>>,
  sockets: Vec<Arc<P::Stream>>,
  stream_layer: Arc<S>,
  packet_layer: Arc<P>,
  wg: AsyncWaitGroup,
  resolver: Arc<A>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  _marker: PhantomData<W>,
}

impl<I, A, P, S, W> NetTransport<I, A, P, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  P: PacketLayer,
  S: StreamLayer,
  W: Wire,
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
    for &addr in &opts.bind_addrs {
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
        keyring: opts.secret_keyring.clone(),
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
    loop {
      #[cfg(feature = "metrics")]
      let transport = {
        Self::new_in(
          resolver.clone(),
          packet_layer.clone(),
          stream_layer.clone(),
          opts.clone(),
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

impl<I, A, P, S, W> Transport for NetTransport<I, A, P, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  P: PacketLayer,
  S: StreamLayer,
  W: Wire,
{
  type Error = NetTransportError<Self::Resolver>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

  // async fn new(
  //   label: Option<Label>,
  //   resolver: Self::Resolver,
  //   opts: Self::Options,
  // ) -> Result<Self, TransportError<Self>>
  // where
  //   Self: Sized,
  // {
  //   #[cfg(feature = "metrics")]
  //   {
  //     Self::new_in(label, opts, None).await
  //   }

  //   #[cfg(not(feature = "metrics"))]
  //   {
  //     Self::new_in(label, opts).await
  //   }
  // }

  // #[cfg(feature = "metrics")]
  // async fn with_metric_labels(
  //   label: Option<Label>,
  //   resolver: Self::Resolver,
  //   opts: Self::Options,
  //   metric_labels: Arc<Vec<crate::metrics::Label>>,
  // ) -> Result<Self, TransportError<Self>>
  // where
  //   Self: Sized,
  // {
  //   Self::new_in(label, opts, Some(metric_labels)).await
  // }

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
    packets: Vec<
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

struct PacketProcessor<A, T, P>
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
  metric_labels: Arc<Vec<metrics::Label>>,
}

impl<A, T, P> PacketProcessor<A, T, P>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
  P: PacketLayer,
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
                let msg = match Self::handle_remote_bytes(&buf) {
                  Ok(msg) => msg,
                  Err(e) => {
                    tracing::error!(target: "showbiz.packet", local=%local_addr, from=%addr, err = %e, "fail to handle UDP packet");
                    continue;
                  }
                };

                #[cfg(feature = "metrics")]
                metrics::counter!("showbiz.packet.bytes.processing", self.metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);

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

  fn handle_remote_bytes(
    buf: &[u8],
  ) -> Result<
    Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    NetTransportError<T::Resolver>,
  > {
    todo!()
  }
}
