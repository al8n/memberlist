use std::{
  marker::PhantomData,
  net::SocketAddr,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::{net::Net, Runtime};
use bytes::Bytes;
use futures::{AsyncReadExt, AsyncWriteExt};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};
use peekable::future::{AsyncPeekExt, AsyncPeekable};
use quinn::{ClientConfig, ConnectError, Connection, Endpoint, RecvStream, SendStream, VarInt};
use smol_str::SmolStr;

mod error;
pub use error::*;

mod options;
pub use options::*;

use super::{QuicAcceptor, QuicConnection, QuicConnector, QuicStream, StreamLayer};

/// [`Quinn`] is an implementation of [`StreamLayer`] based on [`quinn`].
pub struct Quinn<R> {
  opts: QuinnOptions,
  _marker: PhantomData<R>,
}

impl<R> Quinn<R> {
  /// Creates a new [`Quinn`] stream layer with the given options.
  fn new_in(opts: Options) -> Self {
    Self {
      opts: opts.into(),
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Error = QuinnError;
  type Acceptor = QuinnAcceptor<R>;
  type Connector = QuinnConnector<R>;
  type Connection = QuinnConnection<R>;
  type Stream = QuinnStream<R>;
  type Options = Options;

  fn max_stream_data(&self) -> usize {
    self.opts.max_stream_data.min(self.opts.max_connection_data)
  }

  async fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    Ok(Self::new_in(opts))
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> std::io::Result<(SocketAddr, Self::Acceptor, Self::Connector)> {
    let server_name = self.opts.server_name.clone();

    let client_config = self.opts.client_config.clone();
    let sock = std::net::UdpSocket::bind(addr)?;
    let auto_port = addr.port() == 0;

    let endpoint = Arc::new(Endpoint::new(
      self.opts.endpoint_config.clone(),
      Some(self.opts.server_config.clone()),
      sock,
      Arc::new(<R::Net as Net>::Quinn::default()),
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
      _marker: PhantomData,
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
pub struct QuinnAcceptor<R> {
  endpoint: Arc<Endpoint>,
  local_addr: SocketAddr,
  max_open_streams: usize,
  _marker: PhantomData<R>,
}

impl<R> Clone for QuinnAcceptor<R> {
  fn clone(&self) -> Self {
    Self {
      endpoint: self.endpoint.clone(),
      local_addr: self.local_addr,
      max_open_streams: self.max_open_streams,
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> QuicAcceptor for QuinnAcceptor<R> {
  type Error = QuinnError;
  type Connection = QuinnConnection<R>;

  async fn accept(&mut self) -> Result<(Self::Connection, SocketAddr), Self::Error> {
    let conn = self
      .endpoint
      .accept()
      .await
      .ok_or(ConnectError::EndpointStopping)?
      .await?;
    let remote_addr = conn.remote_address();
    Ok((
      QuinnConnection::new(conn, self.local_addr, remote_addr, self.max_open_streams),
      remote_addr,
    ))
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    Endpoint::close(&self.endpoint, VarInt::from(0u32), b"close acceptor");
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
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

impl<R: Runtime> QuicConnector for QuinnConnector<R> {
  type Error = QuinnError;
  type Connection = QuinnConnection<R>;

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection, Self::Error> {
    let connecting =
      self
        .endpoint
        .connect_with(self.client_config.clone(), addr, &self.server_name)?;
    let conn = R::timeout(self.connect_timeout, connecting)
      .await
      .map_err(|_| QuinnConnectionError::DialTimeout)??;
    Ok(QuinnConnection::new(
      conn,
      self.local_addr,
      addr,
      self.max_open_streams,
    ))
  }

  async fn close(&self) -> Result<(), Self::Error> {
    Endpoint::close(&self.endpoint, VarInt::from(0u32), b"close connector");
    Ok(())
  }

  async fn wait_idle(&self) -> Result<(), Self::Error> {
    self.endpoint.wait_idle().await;
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`QuinnStream`] is an implementation of [`QuicStream`] based on [`quinn`].
pub struct QuinnStream<R> {
  send: SendStream,
  recv: AsyncPeekable<RecvStream>,
  read_deadline: Option<Instant>,
  write_deadline: Option<Instant>,
  local_id: Arc<AtomicUsize>,
  _marker: PhantomData<R>,
}

impl<R> QuinnStream<R> {
  #[inline]
  fn new(send: SendStream, recv: RecvStream, local_id: Arc<AtomicUsize>) -> Self {
    Self {
      send,
      recv: recv.peekable(),
      read_deadline: None,
      write_deadline: None,
      local_id,
      _marker: PhantomData,
    }
  }
}

impl<R> Drop for QuinnStream<R> {
  fn drop(&mut self) {
    self.local_id.fetch_sub(1, Ordering::AcqRel);
  }
}

impl<R: Runtime> QuicStream for QuinnStream<R> {
  type Error = QuinnError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let sent = src.len();
    let fut = async {
      self
        .send
        .write_all(&src)
        .await
        .map(|_| sent)
        .map_err(Into::into)
    };

    match self.write_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::write_timeout())?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn flush(&mut self) -> Result<(), Self::Error> {
    self
      .send
      .flush()
      .await
      .map_err(|e| QuinnError::Write(e.into()))
  }

  async fn finish(&mut self) -> Result<(), Self::Error> {
    let fut = async { self.send.finish().await.map(|_| ()).map_err(Into::into) };

    match self.write_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::write_timeout())?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async {
      self
        .recv
        .read_exact(buf)
        .await
        .map_err(|e| QuinnReadStreamError::from(e).into())
    };

    match self.read_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::read_timeout())?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async {
      self
        .recv
        .read(buf)
        .await
        .map_err(|e| QuinnReadStreamError::from(e).into())
    };

    match self.read_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::read_timeout())?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async {
      self
        .recv
        .peek_exact(buf)
        .await
        .map_err(|e| QuinnReadStreamError::from(e).into())
    };

    match self.read_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::read_timeout())?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.send.finish().await.map_err(QuinnBiStreamError::from)?;
    self
      .recv
      .get_mut()
      .1
      .stop(VarInt::from_u32(0))
      .map_err(|_| {
        QuinnBiStreamError::Read(QuinnReadStreamError::Read(quinn::ReadError::UnknownStream)).into()
      })
  }
}

impl<R: Runtime> futures::AsyncRead for QuinnStream<R> {
  fn poll_read(
    mut self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut [u8],
  ) -> std::task::Poll<std::io::Result<usize>> {
    use futures::Future;

    let fut = self.recv.read(buf);
    futures::pin_mut!(fut);
    fut.poll(cx)
  }
}

impl<R: Runtime> TimeoutableReadStream for QuinnStream<R> {
  fn set_read_deadline(&mut self, deadline: Option<Instant>) {
    self.read_deadline = deadline;
  }

  fn read_deadline(&self) -> Option<Instant> {
    self.read_deadline
  }
}

impl<R: Runtime> TimeoutableWriteStream for QuinnStream<R> {
  fn set_write_deadline(&mut self, deadline: Option<Instant>) {
    self.write_deadline = deadline;
  }

  fn write_deadline(&self) -> Option<Instant> {
    self.write_deadline
  }
}

/// A connection based on [`quinn`].
pub struct QuinnConnection<R> {
  conn: Connection,
  local_addr: SocketAddr,
  remote_addr: SocketAddr,
  current_opening_streams: Arc<AtomicUsize>,
  max_open_streams: usize,
  _marker: PhantomData<R>,
}

impl<R> QuinnConnection<R> {
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
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> QuicConnection for QuinnConnection<R> {
  type Error = QuinnError;

  type Stream = QuinnStream<R>;

  async fn accept_bi(&self) -> Result<(Self::Stream, SocketAddr), Self::Error> {
    let (send, recv) = self.conn.accept_bi().await?;
    self.current_opening_streams.fetch_add(1, Ordering::AcqRel);
    Ok((
      QuinnStream::new(send, recv, self.current_opening_streams.clone()),
      self.remote_addr,
    ))
  }

  async fn open_bi(&self) -> Result<(Self::Stream, SocketAddr), Self::Error> {
    let (send, recv) = self.conn.open_bi().await?;
    self.current_opening_streams.fetch_add(1, Ordering::AcqRel);
    Ok((
      QuinnStream::new(send, recv, self.current_opening_streams.clone()),
      self.remote_addr,
    ))
  }

  async fn open_bi_with_deadline(
    &self,
    deadline: Instant,
  ) -> Result<(Self::Stream, SocketAddr), Self::Error> {
    let fut = async {
      let (send, recv) = self.conn.open_bi().await?;
      self.current_opening_streams.fetch_add(1, Ordering::AcqRel);
      Ok((
        QuinnStream::new(send, recv, self.current_opening_streams.clone()),
        self.remote_addr,
      ))
    };

    R::timeout_at(deadline, fut)
      .await
      .map_err(|_| Self::Error::connection_timeout())?
  }

  async fn close(&self) -> Result<(), Self::Error> {
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
