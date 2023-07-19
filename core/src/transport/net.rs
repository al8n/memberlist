use std::{
  collections::VecDeque,
  io::{self, Error, ErrorKind},
  marker::PhantomData,
  net::{IpAddr, SocketAddr},
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
use arc_swap::ArcSwapOption;
use futures_util::FutureExt;

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
  Label,
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
    result(type = "&[IpAddr]", converter(fn = "Vec::as_slice"))
  ))]
  bind_addrs: Vec<IpAddr>,
  bind_port: Option<u16>,
}

impl super::TransportOptions for NetTransportOptions {
  fn from_addr(addr: IpAddr, port: Option<u16>) -> Self {
    Self {
      bind_addrs: vec![addr],
      bind_port: port,
    }
  }

  fn from_addrs(addrs: impl Iterator<Item = IpAddr>, port: Option<u16>) -> Self {
    Self {
      bind_addrs: addrs.collect(),
      bind_port: port,
    }
  }
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

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl<R: Runtime> PacketConnection for Udp<R> {
  #[cfg(not(feature = "nightly"))]
  async fn send_to(&self, addr: SocketAddr, buffer: &[u8]) -> io::Result<usize> {
    self.conn.send_to(buffer, addr).await
  }

  #[cfg(not(feature = "nightly"))]
  async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    self.conn.recv_from(buffer).await
  }

  #[cfg(feature = "nightly")]
  fn send_to<'a>(
    &'a self,
    addr: SocketAddr,
    buf: &'a [u8],
  ) -> impl futures_util::Future<Output = Result<usize, std::io::Error>> + Send + 'a {
    self.conn.send_to(buf, addr)
  }

  #[cfg(feature = "nightly")]
  fn recv_from<'a>(
    &'a self,
    buf: &'a mut [u8],
  ) -> impl futures_util::Future<Output = Result<(usize, SocketAddr), std::io::Error>> + Send + 'a
  {
    self.conn.recv_from(buf)
  }

  fn poll_recv_from(
    &self,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<(usize, SocketAddr)>> {
    self.conn.poll_recv_from(cx, buf)
  }

  fn poll_send_to(
    &self,
    cx: &mut Context<'_>,
    buf: &[u8],
    target: SocketAddr,
  ) -> Poll<io::Result<usize>> {
    self.conn.poll_send_to(cx, buf, target)
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
  label: Option<Label>,
  packet_rx: PacketSubscriber,
  stream_rx: ConnectionSubscriber<Self>,
  tcp_bind_addr: SocketAddr,
  #[allow(dead_code)]
  tcp_listener: Arc<<R::Net as Net>::TcpListener>,
  udp_listener: Arc<UnreliableConnection<Self>>,
  wg: AsyncWaitGroup,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: ArcSwapOption<async_channel::Sender<()>>,
  _marker: PhantomData<R>,
}

impl<R: Runtime> NetTransport<R> {
  async fn new_in(
    label: Option<Label>,
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
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut tcp_listeners = VecDeque::with_capacity(opts.bind_addrs.len());
    let mut udp_listeners = VecDeque::with_capacity(opts.bind_addrs.len());
    let bind_port = opts.bind_port.unwrap_or(0);
    for &addr in &opts.bind_addrs {
      let addr = SocketAddr::new(addr, bind_port);
      let (local_addr, ln) =
        match <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr).await {
          Ok(ln) => (ln.local_addr().unwrap(), ln),
          Err(e) => return Err(TransportError::Other(NetTransportError::ListenTcp(addr, e))),
        };
      tcp_listeners.push_back((ln, local_addr));
      // If the config port given was zero, use the first TCP listener
      // to pick an available port and then apply that to everything
      // else.
      let addr = if bind_port == 0 { local_addr } else { addr };

      let (local_addr, udp_ln) =
        <<R::Net as Net>::UdpSocket as agnostic::net::UdpSocket>::bind(addr)
          .await
          .map_err(|e| NetTransportError::ListenUdp(addr, e))
          .and_then(|udp| {
            <<R::Net as Net>::UdpSocket as agnostic::net::UdpSocket>::set_read_buffer(
              &udp,
              UDP_RECV_BUF_SIZE,
            )
            .map(|_| (addr, UnreliableConnection::new(Udp::new(udp))))
            .map_err(NetTransportError::ResizeUdpBuffer)
          })
          .map_err(TransportError::Other)?;
      udp_listeners.push_back((udp_ln, local_addr));
    }

    let wg = AsyncWaitGroup::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Fire them up now that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for _ in 0..opts.bind_addrs.len() - 1 {
      wg.add(2);
      let (tcp_ln, local_addr) = tcp_listeners.pop_back().unwrap();
      TcpProcessor {
        wg: wg.clone(),
        stream_tx: stream_tx.clone(),
        ln: Listener::<R>(tcp_ln),
        shutdown: shutdown.clone(),
        shutdown_rx: shutdown_rx.clone(),
        local_addr,
      }
      .run();

      let (udp_ln, local_addr) = udp_listeners.pop_back().unwrap();
      UdpProcessor {
        wg: wg.clone(),
        packet_tx: packet_tx.clone(),
        ln: udp_ln,
        local_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metrics_labels: metrics_labels.clone().unwrap_or_default(),
        _marker: std::marker::PhantomData,
        shutdown_rx: shutdown_rx.clone(),
      }
      .run();
    }

    wg.add(2);
    let (tcp_ln, local_tcp_addr) = tcp_listeners.pop_back().unwrap();
    let tcp_ln = Arc::new(tcp_ln);
    TcpProcessor {
      wg: wg.clone(),
      stream_tx,
      ln: tcp_ln.clone(),
      shutdown: shutdown.clone(),
      shutdown_rx: shutdown_rx.clone(),
      local_addr: local_tcp_addr,
    }
    .run();
    let (udp_ln, local_addr) = udp_listeners.pop_back().unwrap();
    let udp_ln = Arc::new(udp_ln);
    UdpProcessor {
      wg: wg.clone(),
      packet_tx,
      ln: udp_ln.clone(),
      local_addr,
      shutdown: shutdown.clone(),
      #[cfg(feature = "metrics")]
      metrics_labels: metrics_labels.clone().unwrap_or_default(),
      shutdown_rx,
      _marker: std::marker::PhantomData,
    }
    .run();

    Ok(Self {
      label,
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      tcp_bind_addr: local_tcp_addr,
      tcp_listener: tcp_ln,
      shutdown_tx: ArcSwapOption::from_pointee(Some(shutdown_tx)),
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
    label: Option<Label>,
    opts: Self::Options,
  ) -> impl Future<Output = Result<Self, TransportError<Self>>> + Send + 'a
  where
    Self: Sized,
  {
    #[cfg(feature = "metrics")]
    {
      Self::new_in(label, opts, None)
    }

    #[cfg(not(feature = "metrics"))]
    {
      Self::new_in(label, opts)
    }
  }

  #[cfg(not(feature = "nightly"))]
  async fn new(label: Option<Label>, opts: Self::Options) -> Result<Self, TransportError<Self>>
  where
    Self: Sized,
  {
    #[cfg(feature = "metrics")]
    {
      Self::new_in(label, opts, None).await
    }

    #[cfg(not(feature = "metrics"))]
    {
      Self::new_in(label, opts).await
    }
  }

  #[cfg(all(feature = "metrics", feature = "nightly"))]
  fn with_metrics_labels(
    label: Option<Label>,
    opts: Self::Options,
    metrics_labels: std::sync::Arc<Vec<crate::metrics::Label>>,
  ) -> impl Future<Output = Result<Self, TransportError<Self>>> + Send + 'static
  where
    Self: Sized,
  {
    Self::new_in(label, opts, Some(metrics_labels))
  }

  #[cfg(all(feature = "metrics", not(feature = "nightly")))]
  async fn with_metrics_labels(
    label: Option<Label>,
    opts: Self::Options,
    metrics_labels: Arc<Vec<crate::metrics::Label>>,
  ) -> Result<Self, TransportError<Self>>
  where
    Self: Sized,
  {
    Self::new_in(label, opts, Some(metrics_labels)).await
  }

  fn final_advertise_addr(
    &self,
    addr: Option<IpAddr>,
    port: u16,
  ) -> Result<SocketAddr, TransportError<Self>> {
    match addr {
      Some(addr) => Ok(SocketAddr::new(addr, port)),
      None => {
        let addr = if self.opts.bind_addrs[0].is_unspecified() {
          local_ip_address::local_ip().map_err(|e| match e {
            local_ip_address::Error::LocalIpAddressNotFound => {
              TransportError::Other(Self::Error::NoPrivateIP)
            }
            e => TransportError::Other(Self::Error::NoInterfaceAddresses(e)),
          })?
        } else {
          self.tcp_bind_addr.ip()
        };

        // Use the port we are bound to.
        Ok(SocketAddr::new(addr, self.tcp_bind_addr.port()))
      }
    }
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

  #[cfg(not(feature = "nightly"))]
  async fn write_to(&self, b: &[u8], addr: SocketAddr) -> Result<Instant, TransportError<Self>> {
    if let Some(label) = &self.label {
      // TODO: is there a better way to avoid allocating and copying?
      if !label.is_empty() {
        let b =
          Label::add_label_header_to_packet(b, label.as_bytes()).map_err(TransportError::Encode)?;
        return UnreliableConnection::send_to(&self.udp_listener, addr, &b)
          .await
          .map(|_| Instant::now());
      }
    }

    UnreliableConnection::send_to(&self.udp_listener, addr, b)
      .await
      .map(|_| Instant::now())
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

  #[cfg(not(feature = "nightly"))]
  async fn dial_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> Result<ReliableConnection<Self>, TransportError<Self>> {
    match <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect_timeout(addr, timeout)
      .await
    {
      Ok(conn) => {
        let mut conn = ReliableConnection::new(Tcp { conn }, addr);
        if let Some(label) = &self.label {
          conn.add_label_header(label.as_bytes()).await?;
        }
        Ok(conn)
      }
      Err(e) => {
        tracing::error!(target = "showbiz.net.transport", err = %e);
        Err(TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Dial,
          error: Error::new(ErrorKind::TimedOut, "timeout"),
        }))
      }
    }
  }

  fn packet(&self) -> PacketSubscriber {
    self.packet_rx.clone()
  }

  fn stream(&self) -> ConnectionSubscriber<Self> {
    self.stream_rx.clone()
  }

  #[cfg(feature = "nightly")]
  fn shutdown(&self) -> impl Future<Output = Result<(), TransportError<Self>>> + Send + '_ {
    async move {
      // This will avoid log spam about errors when we shut down.
      self.shutdown.store(true, Ordering::SeqCst);
      self.shutdown_tx.store(None);

      // Block until all the listener threads have died.
      self.wg.wait().await;
      Ok(())
    }
  }

  #[cfg(not(feature = "nightly"))]
  async fn shutdown(&self) -> Result<(), TransportError<Self>> {
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.store(None);

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }

  fn block_shutdown(&self) -> Result<(), TransportError<Self>> {
    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.store(None);
    let wg = self.wg.clone();
    R::spawn_detach(async move { wg.wait().await });
    Ok(())
  }

  fn auto_bind_port(&self) -> u16 {
    self.tcp_bind_addr.port()
  }
}

struct Listener<R: Runtime>(<R::Net as agnostic::net::Net>::TcpListener);

impl<R: Runtime> AsRef<<R::Net as agnostic::net::Net>::TcpListener> for Listener<R> {
  fn as_ref(&self) -> &<R::Net as agnostic::net::Net>::TcpListener {
    &self.0
  }
}

struct TcpProcessor<T, R, L>
where
  R: Runtime,
  T: Transport<Runtime = R>,
  L: AsRef<<R::Net as agnostic::net::Net>::TcpListener>,
{
  wg: AsyncWaitGroup,
  stream_tx: ConnectionProducer<T>,
  ln: L,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<T, R, L> TcpProcessor<T, R, L>
where
  T: Transport<Connection = Tcp<R>, Runtime = R>,
  R: Runtime,
  L: AsRef<<R::Net as agnostic::net::Net>::TcpListener> + Send + Sync + 'static,
{
  pub(super) fn run(self) {
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
      let ln = ln.as_ref();
      loop {
        futures_util::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = ln.accept().fuse() => {
            match rst {
              Ok((conn, _)) => {
                let remote_addr = match conn.peer_addr() {
                  Ok(addr) => addr,
                  Err(err) => {
                    tracing::error!(target = "showbiz.net.transport", local_addr=%local_addr, err = %err, "failed to get TCP peer address");
                    continue;
                  }
                };
                // No error, reset loop delay
                loop_delay = Duration::ZERO;
                if let Err(e) = stream_tx
                  .send(ReliableConnection::new(Tcp { conn }, remote_addr))
                  .await
                {
                  tracing::error!(target = "showbiz.net.transport", local_addr=%local_addr, err = %e, "failed to send TCP connection");
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

                tracing::error!(target = "showbiz.net.transport", local_addr=%local_addr, err = %e, "error accepting TCP connection");
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

struct UdpProcessor<T, L>
where
  T: Transport,
  L: AsRef<UnreliableConnection<T>>,
{
  wg: AsyncWaitGroup,
  packet_tx: PacketProducer,
  ln: L,
  local_addr: SocketAddr,
  shutdown: Arc<AtomicBool>,
  shutdown_rx: async_channel::Receiver<()>,
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
      local_addr,
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
        futures_util::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = ln.recv_from(&mut buf).fuse() => {
            match rst {
              Ok((n, addr)) => {
                // Check the length - it needs to have at least one byte to be a
                // proper message.
                if n < 1 {
                  tracing::error!(target = "showbiz.net.transport", local=%local_addr, from=%addr, err = "UDP packet too short (0 bytes)");
                  continue;
                }

                buf.truncate(n);

                if let Err(e) = packet_tx.send(Packet::new(buf, addr, Instant::now())).await {
                  tracing::error!(target = "showbiz.net.transport", local=%local_addr, from=%addr, err = %e, "failed to send packet");
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

                tracing::error!(target = "showbiz.net.transport", peer=%local_addr, err = %e, "error reading UDP packet");
                continue;
              }
            };
          }
        }
      }
    });
  }
}
