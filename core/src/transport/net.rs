use std::{
  collections::VecDeque,
  io::{self, Error, ErrorKind},
  marker::PhantomData,
  net::SocketAddr,
  pin::Pin,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  task::{Context, Poll},
  time::{Duration, Instant},
};

#[cfg(feature = "tokio-compat")]
use agnostic::io::ReadBuf;

use crate::{
  async_trait, futures_util,
  transport::{
    stream::{
      connection_stream, packet_stream, ConnectionProducer, ConnectionSubscriber, PacketProducer,
      PacketSubscriber,
    },
    ConnectionError, ConnectionErrorKind, ConnectionKind, ConnectionTimeout, PacketConnection,
    ReliableConnection, Transport, TransportError, UnreliableConnection,
  },
  types::Packet,
};
use agnostic::{
  net::{Net, TcpListener, TcpStream, UdpSocket},
  Runtime,
};
use bytes::BytesMut;
use wg::AsyncWaitGroup;

/// Used to buffer incoming packets during read
/// operations.
const UDP_PACKET_BUF_SIZE: usize = 65536;

/// A large buffer size that we attempt to set UDP
/// sockets to in order to handle a large volume of messages.
const UDP_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum NetTransportError {
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  #[error("failed to get interface addresses {0}")]
  NoInterfaceAddresses(#[from] local_ip_address::Error),
  #[error("at least one bind address is required")]
  EmptyBindAddrs,
  #[error("failed to resize UDP buffer {0}")]
  ResizeUdpBuffer(std::io::Error),
  #[error("failed to start UDP listener on {0}: {1}")]
  ListenUdp(SocketAddr, std::io::Error),
  #[error("failed to start TCP listener on {0}: {1}")]
  ListenTcp(SocketAddr, std::io::Error),
}

#[derive(Debug, Clone)]
pub struct Crc32(crc32fast::Hasher);

impl crate::checksum::Checksumer for Crc32 {
  fn new() -> Self {
    Self(crc32fast::Hasher::new())
  }

  fn update(&mut self, data: &[u8]) {
    self.0.update(data);
  }

  fn finalize(self) -> u32 {
    self.0.finalize()
  }
}

/// Used to configure a net transport.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetTransportOptions {
  /// A list of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(getter(
    style = "ref",
    result(type = "&[SocketAddr]", converter(fn = "Vec::as_slice"))
  ))]
  bind_addrs: Vec<SocketAddr>,
}

#[repr(transparent)]
pub struct Udp<R: Runtime> {
  conn: <R::Net as Net>::UdpSocket,
}

impl<R: Runtime> Udp<R> {
  fn new(conn: <R::Net as Net>::UdpSocket) -> Self {
    Self { conn }
  }
}

#[async_trait::async_trait]
impl<R: Runtime> PacketConnection for Udp<R> {
  async fn send_to(&self, addr: SocketAddr, buffer: &[u8]) -> io::Result<usize> {
    self.conn.send_to(buffer, addr).await
  }

  async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    self.conn.recv_from(buffer).await
  }
}

impl<R: Runtime> ConnectionTimeout for Udp<R> {
  fn set_write_timeout(&self, timeout: Option<Duration>) {
    self.conn.set_write_timeout(timeout)
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.conn.write_timeout()
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    self.conn.set_read_timeout(timeout)
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.conn.read_timeout()
  }
}

#[repr(transparent)]
pub struct Tcp<R: Runtime> {
  conn: <R::Net as Net>::TcpStream,
}

impl<R: Runtime> futures_util::io::AsyncRead for Tcp<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<std::io::Result<usize>> {
    futures_util::io::AsyncRead::poll_read(Pin::new(&mut self.conn), cx, buf)
  }
}

impl<R: Runtime> futures_util::io::AsyncWrite for Tcp<R> {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    futures_util::io::AsyncWrite::poll_write(Pin::new(&mut self.conn), cx, buf)
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    futures_util::io::AsyncWrite::poll_flush(Pin::new(&mut self.conn), cx)
  }

  fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    futures_util::io::AsyncWrite::poll_close(Pin::new(&mut self.conn), cx)
  }
}

#[cfg(feature = "tokio-compat")]
impl<R: Runtime> agnostic::io::TokioAsyncRead for Tcp<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<std::io::Result<()>> {
    agnostic::io::TokioAsyncRead::poll_read(Pin::new(&mut self.conn), cx, buf)
  }
}

#[cfg(feature = "tokio-compat")]
impl<R: Runtime> agnostic::io::TokioAsyncWrite for Tcp<R> {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<Result<usize, std::io::Error>> {
    agnostic::io::TokioAsyncWrite::poll_write(Pin::new(&mut self.conn), cx, buf)
  }

  fn poll_flush(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Result<(), std::io::Error>> {
    agnostic::io::TokioAsyncWrite::poll_flush(Pin::new(&mut self.conn), cx)
  }

  fn poll_shutdown(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Result<(), std::io::Error>> {
    agnostic::io::TokioAsyncWrite::poll_shutdown(Pin::new(&mut self.conn), cx)
  }
}

impl<R: Runtime> ConnectionTimeout for Tcp<R> {
  fn set_write_timeout(&self, timeout: Option<Duration>) {
    self.conn.set_write_timeout(timeout)
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.conn.write_timeout()
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    self.conn.set_read_timeout(timeout)
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.conn.read_timeout()
  }
}

pub struct NetTransport<R: Runtime> {
  opts: Arc<NetTransportOptions>,
  packet_rx: PacketSubscriber,
  stream_rx: ConnectionSubscriber<Self>,
  tcp_addr: SocketAddr,
  udp_listener: Arc<UnreliableConnection<Self>>,
  wg: AsyncWaitGroup,
  shutdown: Arc<AtomicBool>,
  _marker: PhantomData<R>,
}

impl<R: Runtime> NetTransport<R> {
  async fn new_in(
    opts: <Self as Transport>::Options,
    #[cfg(feature = "metrics")] metrics_labels: Option<Arc<Vec<crate::metrics::Label>>>,
  ) -> Result<Self, TransportError<Self>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addrs.is_empty() {
      return Err(TransportError::Other(NetTransportError::EmptyBindAddrs));
    }

    let (stream_tx, stream_rx) = connection_stream();
    let (packet_tx, packet_rx) = packet_stream();

    let mut tcp_listeners = VecDeque::with_capacity(opts.bind_addrs.len());
    let mut udp_listeners = VecDeque::with_capacity(opts.bind_addrs.len());
    let tcp_addr = opts.bind_addrs[0];
    for &addr in &opts.bind_addrs {
      tcp_listeners.push_back((
        <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr)
          .await
          .map_err(|e| TransportError::Other(NetTransportError::ListenTcp(addr, e)))?,
        addr,
      ));

      let udp_ln = <<R::Net as Net>::UdpSocket as agnostic::net::UdpSocket>::bind(addr)
        .await
        .map_err(|e| NetTransportError::ListenUdp(addr, e))
        .and_then(|udp| {
          <<R::Net as Net>::UdpSocket as agnostic::net::UdpSocket>::set_read_buffer(
            &udp,
            UDP_RECV_BUF_SIZE,
          )
          .map(|_| UnreliableConnection::new(Udp::new(udp), addr))
          .map_err(NetTransportError::ResizeUdpBuffer)
        })
        .map_err(TransportError::Other)?;
      udp_listeners.push_back((udp_ln, addr));
    }

    let wg = AsyncWaitGroup::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Fire them up now that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for _ in 0..opts.bind_addrs.len() - 1 {
      wg.add(2);
      let (tcp_ln, remote_addr) = tcp_listeners.pop_back().unwrap();
      TcpProcessor {
        remote_addr,
        wg: wg.clone(),
        stream_tx: stream_tx.clone(),
        ln: tcp_ln,
        shutdown: shutdown.clone(),
      }
      .run();

      let (udp_ln, remote_addr) = udp_listeners.pop_back().unwrap();
      UdpProcessor {
        wg: wg.clone(),
        packet_tx: packet_tx.clone(),
        ln: udp_ln,
        remote_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metrics_labels: metrics_labels.clone().unwrap_or_default(),
        _marker: std::marker::PhantomData,
      }
      .run();
    }

    wg.add(2);
    let (tcp_ln, remote_addr) = tcp_listeners.pop_back().unwrap();
    TcpProcessor {
      remote_addr,
      wg: wg.clone(),
      stream_tx,
      ln: tcp_ln,
      shutdown: shutdown.clone(),
    }
    .run();
    let (udp_ln, remote_addr) = udp_listeners.pop_back().unwrap();
    let udp_ln = Arc::new(udp_ln);

    UdpProcessor {
      wg: wg.clone(),
      packet_tx,
      ln: udp_ln.clone(),
      remote_addr,
      shutdown: shutdown.clone(),
      #[cfg(feature = "metrics")]
      metrics_labels: metrics_labels.clone().unwrap_or_default(),
      _marker: std::marker::PhantomData,
    }
    .run();

    Ok(Self {
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      tcp_addr,
      udp_listener: udp_ln,
      _marker: PhantomData,
    })
  }
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl<R: Runtime> Transport for NetTransport<R> {
  type Error = NetTransportError;

  type Checksumer = Crc32;

  type Connection = Tcp<R>;

  type UnreliableConnection = Udp<R>;

  type Options = Arc<NetTransportOptions>;

  type Runtime = R;

  #[cfg(feature = "nightly")]
  fn new<'a>(
    opts: Self::Options,
  ) -> impl Future<Output = Result<Self, TransportError<Self>>> + Send + 'a
  where
    Self: Sized,
  {
    #[cfg(feature = "metrics")]
    {
      Self::new_in(opts, None)
    }

    #[cfg(not(feature = "metrics"))]
    {
      Self::new_in(opts)
    }
  }

  #[cfg(all(feature = "metrics", feature = "nightly"))]
  fn with_metrics_labels(
    opts: Self::Options,
    metrics_labels: std::sync::Arc<Vec<crate::metrics::Label>>,
  ) -> impl Future<Output = Result<Self, TransportError<Self>>> + Send + 'static
  where
    Self: Sized,
  {
    Self::new_in(opts, Some(metrics_labels))
  }

  #[cfg(feature = "nightly")]
  fn write_to<'a>(
    &'a self,
    b: &'a [u8],
    addr: SocketAddr,
  ) -> impl Future<Output = Result<Instant, TransportError<Self>>> + Send + 'a {
    async move {
      UnreliableConnection::send_to(&self.udp_listener, addr, b)
        .await
        .map(|_| Instant::now())
    }
  }

  #[cfg(feature = "nightly")]
  fn dial_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> impl Future<Output = Result<ReliableConnection<Self>, TransportError<Self>>> + '_ {
    async move {
      match <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect_timeout(addr, timeout)
        .await
      {
        Ok(conn) => Ok(ReliableConnection::new(
          Tcp {
            conn,
            timeout: None,
          },
          addr,
        )),
        Err(_) => Err(TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Dial,
          error: Error::new(ErrorKind::TimedOut, "timeout"),
        })),
      }
    }
  }

  #[cfg(feature = "nightly")]
  fn shutdown(&self) -> impl Future<Output = Result<(), TransportError<Self>>> + Send + '_ {
    async move {
      // This will avoid log spam about errors when we shut down.
      self.shutdown.store(true, Ordering::SeqCst);

      // Block until all the listener threads have died.
      self.wg.wait().await;
      Ok(())
    }
  }

  #[cfg(not(feature = "nightly"))]
  async fn new(opts: Self::Options) -> Result<Self, TransportError<Self>>
  where
    Self: Sized,
  {
    #[cfg(feature = "metrics")]
    {
      Self::new_in(opts, None).await
    }

    #[cfg(not(feature = "metrics"))]
    {
      Self::new_in(opts).await
    }
  }

  #[cfg(all(feature = "metrics", not(feature = "nightly")))]
  async fn with_metrics_labels(
    opts: Self::Options,
    metrics_labels: Arc<Vec<crate::metrics::Label>>,
  ) -> Result<Self, TransportError<Self>>
  where
    Self: Sized,
  {
    Self::new_in(opts, Some(metrics_labels)).await
  }

  fn final_advertise_addr(
    &self,
    addr: Option<SocketAddr>,
  ) -> Result<SocketAddr, TransportError<Self>> {
    match addr {
      Some(addr) => Ok(addr),
      None => {
        if self.opts.bind_addrs[0].ip().is_unspecified() {
          local_ip_address::local_ip()
            .map_err(|e| match e {
              local_ip_address::Error::LocalIpAddressNotFound => {
                TransportError::Other(Self::Error::NoPrivateIP)
              }
              e => TransportError::Other(Self::Error::NoInterfaceAddresses(e)),
            })
            .map(|ip| SocketAddr::new(ip, self.tcp_addr.port()))
        } else {
          Ok(self.tcp_addr)
        }
      }
    }
  }

  #[cfg(not(feature = "nightly"))]
  async fn write_to(&self, b: &[u8], addr: SocketAddr) -> Result<Instant, TransportError<Self>> {
    UnreliableConnection::send_to(&self.udp_listener, addr, b)
      .await
      .map(|_| Instant::now())
  }

  #[cfg(not(feature = "nightly"))]
  async fn dial_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> Result<ReliableConnection<Self>, TransportError<Self>> {
    match <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect_timeout(addr, timeout)
      .await
    {
      Ok(conn) => Ok(ReliableConnection::new(Tcp { conn }, addr)),
      Err(_) => Err(TransportError::Connection(ConnectionError {
        kind: ConnectionKind::Reliable,
        error_kind: ConnectionErrorKind::Dial,
        error: Error::new(ErrorKind::TimedOut, "timeout"),
      })),
    }
  }

  fn packet(&self) -> PacketSubscriber {
    self.packet_rx.clone()
  }

  fn stream(&self) -> ConnectionSubscriber<Self> {
    self.stream_rx.clone()
  }

  #[cfg(not(feature = "nightly"))]
  async fn shutdown(&self) -> Result<(), TransportError<Self>> {
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }

  fn block_shutdown(&self) -> Result<(), TransportError<Self>> {
    R::block_on(self.shutdown())
  }
}

struct TcpProcessor<T: Transport, R: Runtime> {
  remote_addr: SocketAddr,
  wg: AsyncWaitGroup,
  stream_tx: ConnectionProducer<T>,
  ln: <R::Net as agnostic::net::Net>::TcpListener,
  shutdown: Arc<AtomicBool>,
}

impl<T, R> TcpProcessor<T, R>
where
  T: Transport<Connection = Tcp<R>>,
  R: Runtime,
{
  pub(super) fn run(self) {
    let Self {
      remote_addr,
      wg,
      stream_tx,
      ln,
      shutdown,
      ..
    } = self;

    /// The initial delay after an `accept()` error before attempting again
    const BASE_DELAY: Duration = Duration::from_millis(5);

    /// the maximum delay after an `accept()` error before attempting again.
    /// In the case that tcpListen() is error-looping, it will delay the shutdown check.
    /// Therefore, changes to `MAX_DELAY` may have an effect on the latency of shutdown.
    const MAX_DELAY: Duration = Duration::from_secs(1);

    R::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      let mut loop_delay = Duration::ZERO;

      loop {
        match ln.accept().await {
          Ok((conn, _)) => {
            // No error, reset loop delay
            loop_delay = Duration::ZERO;

            if let Err(e) = stream_tx
              .send(ReliableConnection::new(Tcp { conn }, remote_addr))
              .await
            {
              tracing::error!(target = "showbiz", err = %e, "failed to send tcp connection");
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

            tracing::error!(target = "showbiz", err = %e, "error accepting TCP connection");
            R::sleep(loop_delay).await;
            continue;
          }
        }
      }
    });
  }
}

struct UdpProcessor<T, L>
where
  T: Transport,
  L: AsRef<UnreliableConnection<T>>,
{
  wg: AsyncWaitGroup,
  packet_tx: PacketProducer,
  ln: L,
  remote_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  #[cfg(feature = "metrics")]
  metrics_labels: Arc<Vec<crate::metrics::Label>>,
  _marker: PhantomData<T>,
}

impl<T, L> UdpProcessor<T, L>
where
  T: Transport,
  L: AsRef<UnreliableConnection<T>> + Send + Sync + 'static,
{
  #[cfg(feature = "async")]
  pub(super) fn run(self) {
    let Self {
      wg,
      packet_tx,
      ln,
      shutdown,
      remote_addr,
      ..
    } = self;

    <T::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      let ln = ln.as_ref();
      loop {
        // Do a blocking read into a fresh buffer. Grab a time stamp as
        // close as possible to the I/O.
        let mut buf = BytesMut::with_capacity(UDP_PACKET_BUF_SIZE);
        buf.fill(0);
        match ln.recv_from(&mut buf).await {
          Ok((n, addr)) => {
            // Check the length - it needs to have at least one byte to be a
            // proper message.
            if n < 1 {
              tracing::error!(target = "showbiz", peer=%remote_addr, from=%addr, err = "UDP packet too short (0 bytes)");
              continue;
            }

            buf.truncate(n);

            if let Err(e) = packet_tx.send(Packet::new(buf, addr, Instant::now())).await {
              tracing::error!(target = "showbiz", peer=%remote_addr, from=%addr, err = %e, "failed to send packet");
            }

            #[cfg(feature = "metrics")]
            {
              static UDP_RECEIVED: std::sync::Once = std::sync::Once::new();

              #[inline]
              fn incr_udp_received<'a>(
                val: u64,
                labels: impl Iterator<Item = &'a metrics::Label>
                  + metrics::IntoLabels,
              ) {
                UDP_RECEIVED.call_once(|| {
                  metrics::register_counter!("showbiz.udp.received");
                });
                metrics::counter!("showbiz.udp.received", val, labels);
              }

              incr_udp_received(n as u64, self.metrics_labels.iter());
            }
          }
          Err(e) => {
            if shutdown.load(Ordering::SeqCst) {
              break;
            }

            tracing::error!(target = "showbiz", peer=%remote_addr, err = %e, "error reading UDP packet");
            continue;
          }
        };
      }
    });
  }
}
