use std::{
  io, marker::PhantomData, net::SocketAddr, pin::Pin, task::{Context, Poll}, sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  }, time::Duration
};

use agnostic::Runtime;
use futures::io::{AsyncRead, AsyncWrite};
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, VarInt};
use smol_str::SmolStr;

mod options;
pub use options::*;

use super::{QuicAcceptor, QuicConnection, QuicConnector, QuicStream, StreamLayer};

/// [`Quinn`] is an implementation of [`StreamLayer`] based on [`quinn`].
pub struct Quinn<R> {
  opts: QuinnOptions,
  _m: PhantomData<R>,
}

impl<R> Quinn<R> {
  /// Creates a new [`Quinn`] stream layer with the given options.
  fn new_in(opts: Options) -> Self {
    Self {
      opts: opts.into(),
      _m: PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Runtime = R;
  type Acceptor = QuinnAcceptor;
  type Connector = QuinnConnector<R>;
  type Connection = QuinnConnection;
  type Stream = QuinnStream;
  type Options = Options;

  fn max_stream_data(&self) -> usize {
    self.opts.max_stream_data.min(self.opts.max_connection_data)
  }

  async fn new(opts: Self::Options) -> io::Result<Self> {
    Ok(Self::new_in(opts))
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> io::Result<(SocketAddr, Self::Acceptor, Self::Connector)> {
    let server_name = self.opts.server_name.clone();

    let client_config = self.opts.client_config.clone();
    let sock = std::net::UdpSocket::bind(addr)?;
    let auto_port = addr.port() == 0;

    let endpoint = Arc::new(Endpoint::new(
      self.opts.endpoint_config.clone(),
      Some(self.opts.server_config.clone()),
      sock,
      Arc::new(R::quinn()),
    )?);

    let local_addr = endpoint.local_addr()?;
    if auto_port {
      tracing::info!(
        "memberlist_quic.endpoint: binding to dynamic addr {}",
        local_addr
      );
    }

    let acceptor = Self::Acceptor {
      endpoint: endpoint.clone(),
      local_addr,
      max_open_streams: self.opts.max_open_streams,
    };

    let connector = Self::Connector {
      server_name,
      endpoint,
      local_addr,
      client_config,
      max_open_streams: self.opts.max_open_streams,
      connect_timeout: self.opts.connect_timeout,
      _marker: PhantomData,
    };
    Ok((local_addr, acceptor, connector))
  }
}

/// [`QuinnAcceptor`] is an implementation of [`QuicAcceptor`] based on [`quinn`].
pub struct QuinnAcceptor {
  endpoint: Arc<Endpoint>,
  local_addr: SocketAddr,
  max_open_streams: usize,
}

impl Clone for QuinnAcceptor {
  fn clone(&self) -> Self {
    Self {
      endpoint: self.endpoint.clone(),
      local_addr: self.local_addr,
      max_open_streams: self.max_open_streams,
    }
  }
}

impl QuicAcceptor for QuinnAcceptor {
  type Connection = QuinnConnection;

  async fn accept(&mut self) -> io::Result<(Self::Connection, SocketAddr)> {
    let conn = self
      .endpoint
      .accept()
      .await
      .ok_or(io::Error::new(io::ErrorKind::Other, "endpoint closed"))?
      .await?;
    let remote_addr = conn.remote_address();

    Ok((
      QuinnConnection::new(conn, self.local_addr, remote_addr, self.max_open_streams),
      remote_addr,
    ))
  }

  async fn close(&mut self) -> io::Result<()> {
    Endpoint::close(&self.endpoint, VarInt::from(0u32), b"close acceptor");
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

impl Drop for QuinnAcceptor {
  fn drop(&mut self) {
    Endpoint::close(&self.endpoint, VarInt::from(0u32), b"close acceptor");
  }
}

/// [`QuinnConnector`] is an implementation of [`QuicConnector`] based on [`quinn`].
pub struct QuinnConnector<R> {
  server_name: SmolStr,
  endpoint: Arc<Endpoint>,
  client_config: ClientConfig,
  connect_timeout: Duration,
  local_addr: SocketAddr,
  max_open_streams: usize,
  _marker: PhantomData<R>,
}

impl<R> QuicConnector for QuinnConnector<R>
where
  R: Runtime,
{
  type Connection = QuinnConnection;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Connection> {
    let connecting = self
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)
      .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
    let conn = R::timeout(self.connect_timeout, connecting)
      .await
      .map_err(io::Error::from)??;
    Ok(QuinnConnection::new(
      conn,
      self.local_addr,
      addr,
      self.max_open_streams,
    ))
  }

  async fn close(&self) -> io::Result<()> {
    Endpoint::close(&self.endpoint, VarInt::from(0u32), b"close connector");
    Ok(())
  }

  async fn wait_idle(&self) -> io::Result<()> {
    self.endpoint.wait_idle().await;
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

impl<R> Drop for QuinnConnector<R> {
  fn drop(&mut self) {
    Endpoint::close(&self.endpoint, VarInt::from(0u32), b"close connector");
  }
}

/// [`QuinnStream`] is an implementation of [`QuicStream`] based on [`quinn`].
pub struct QuinnStream {
  send: SendStream,
  recv: RecvStream,
  local_id: Arc<AtomicUsize>,
}

impl QuinnStream {
  #[inline]
  fn new(send: SendStream, recv: RecvStream, local_id: Arc<AtomicUsize>) -> Self {
    Self {
      send,
      recv,
      local_id,
    }
  }
}

impl Drop for QuinnStream {
  fn drop(&mut self) {
    self.local_id.fetch_sub(1, Ordering::AcqRel);
  }
}

impl QuicStream for QuinnStream {
  async fn read_packet(&mut self) -> std::io::Result<bytes::Bytes> {
    // TODO(al8n): make size limit configurable?
    self.recv.read_to_end(u32::MAX as usize).await
      .map(Into::into)
      .map_err(|e| {
        match e {
          quinn::ReadToEndError::Read(e) => std::io::Error::from(e),
          quinn::ReadToEndError::TooLong => std::io::Error::new(std::io::ErrorKind::InvalidData, "packet too large"),
        }
      })
  }
}

impl AsyncRead for QuinnStream {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<std::io::Result<usize>> {
    let recv = Pin::new(&mut self.recv);
    AsyncRead::poll_read(recv, cx, buf)
  }
}

impl AsyncWrite for QuinnStream {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    let send = Pin::new(&mut self.send);
    AsyncWrite::poll_write(send, cx, buf)
  }

  fn poll_flush(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<io::Result<()>> {
    let send = Pin::new(&mut self.send);
    AsyncWrite::poll_flush(send, cx)
  }

  fn poll_close(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<io::Result<()>> {
    let send = Pin::new(&mut self.send);
    AsyncWrite::poll_close(send, cx)
  }
}

/// A connection based on [`quinn`].
pub struct QuinnConnection {
  conn: Connection,
  local_addr: SocketAddr,
  remote_addr: SocketAddr,
  current_opening_streams: Arc<AtomicUsize>,
  max_open_streams: usize,
}

impl QuinnConnection {
  #[inline]
  fn new(
    conn: Connection,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    max_open_streams: usize,
  ) -> Self {
    Self {
      conn,
      local_addr,
      remote_addr,
      current_opening_streams: Arc::new(AtomicUsize::new(0)),
      max_open_streams,
    }
  }
}

impl QuicConnection for QuinnConnection {
  type Stream = QuinnStream;

  async fn accept_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    let (send, recv) = self.conn.accept_bi().await?;
    self.current_opening_streams.fetch_add(1, Ordering::AcqRel);
    Ok((
      QuinnStream::new(send, recv, self.current_opening_streams.clone()),
      self.remote_addr,
    ))
  }

  async fn open_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    let (send, recv) = self.conn.open_bi().await?;
    self.current_opening_streams.fetch_add(1, Ordering::AcqRel);
    Ok((
      QuinnStream::new(send, recv, self.current_opening_streams.clone()),
      self.remote_addr,
    ))
  }

  async fn close(&self) -> io::Result<()> {
    self.conn.close(0u32.into(), b"close connection");
    Ok(())
  }

  async fn is_closed(&self) -> bool {
    self.conn.close_reason().is_some()
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  fn is_full(&self) -> bool {
    self.current_opening_streams.load(Ordering::Acquire) >= self.max_open_streams
  }
}
