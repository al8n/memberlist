use std::{io, marker::PhantomData, net::SocketAddr, path::PathBuf, time::Duration};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{AsyncReadExt, AsyncWriteExt};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};
use peekable::future::{AsyncPeekExt, AsyncPeekable};
use s2n_quic::{
  provider::limits::Limits,
  stream::{BidirectionalStream, ReceiveStream, SendStream},
  Client, Server,
};
pub use s2n_quic_transport::connection::limits::ValidationError;

use super::{
  QuicBiAcceptor, QuicBiStream, QuicConnector, QuicReadStream, QuicUniAcceptor, QuicWriteStream,
  StreamLayer,
};

mod error;
pub use error::*;
mod options;
pub use options::*;

/// A QUIC stream layer based on [`s2n`](::s2n_quic).
pub struct S2n<R> {
  limits: Limits,
  max_stream_data: usize,
  cert: PathBuf,
  key: PathBuf,
  _marker: PhantomData<R>,
}

impl<R> S2n<R> {
  /// Creates a new [`S2n`] stream layer with the given options.
  pub fn new(opts: Options) -> Result<Self, ValidationError> {
    Ok(Self {
      limits: Limits::try_from(&opts)?,
      max_stream_data: opts.data_window as usize,
      cert: opts.cert_path,
      key: opts.key_path,
      _marker: PhantomData,
    })
  }
}

impl<R: Runtime> StreamLayer for S2n<R> {
  type Error = S2nError;
  type BiAcceptor = S2nBiAcceptor<R>;
  type UniAcceptor = S2nUniAcceptor<R>;
  type Connector = S2nConnector<R>;

  type Stream = S2nBiStream<R>;
  type ReadStream = S2nReadStream<R>;
  type WriteStream = S2nWriteStream<R>;

  fn max_stream_data(&self) -> usize {
    self.max_stream_data
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> io::Result<(
    (SocketAddr, Self::BiAcceptor, Self::UniAcceptor),
    Self::Connector,
  )> {
    let auto_server_port = addr.port() == 0;
    let srv = Server::builder()
      .with_limits(self.limits)
      .map_err(invalid_data)?
      .with_io(addr)
      .map_err(invalid_data)?
      .with_tls((self.cert.as_path(), self.key.as_path()))
      .map_err(invalid_data)?
      .start()
      .map_err(invalid_data)?;
    let client_addr = SocketAddr::new(addr.ip(), 0);
    let client = Client::builder()
      .with_limits(self.limits)
      .map_err(invalid_data)?
      .with_tls((self.cert.as_path(), self.key.as_path()))
      .map_err(invalid_data)?
      .with_io(client_addr)?
      .start()
      .map_err(invalid_data)?;

    let actual_client_addr = client.local_addr()?;
    tracing::info!(target: "memberlist.transport.quic", "bind client to dynamic address {}", actual_client_addr);

    let actual_local_addr = srv.local_addr()?;
    if auto_server_port {
      tracing::info!(target: "memberlist.transport.quic", "bind server to dynamic address {}", actual_local_addr);
    }
    let (bi, uni) = futures::lock::BiLock::new(srv);
    let bi = Self::BiAcceptor {
      server: bi,
      local_addr: actual_local_addr,
      _marker: PhantomData,
    };
    let uni = Self::UniAcceptor {
      server: uni,
      local_addr: actual_local_addr,
      _marker: PhantomData,
    };

    let connector = Self::Connector {
      client,
      local_addr: actual_client_addr,
      _marker: PhantomData,
    };
    Ok(((actual_local_addr, bi, uni), connector))
  }
}

/// [`S2nBiAcceptor`] is an implementation of [`QuicBiAcceptor`] based on [`s2n_quic`].
pub struct S2nBiAcceptor<R> {
  server: futures::lock::BiLock<Server>,
  local_addr: SocketAddr,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicBiAcceptor for S2nBiAcceptor<R> {
  type Error = S2nError;
  type BiStream = S2nBiStream<R>;

  async fn accept_bi(&self) -> Result<(Self::BiStream, SocketAddr), Self::Error> {
    let mut server = self.server.lock().await;
    let mut conn = server.accept().await.ok_or(Self::Error::Closed)?;
    conn.keep_alive(true)?;
    let remote = conn.remote_addr()?;
    conn
      .accept_bidirectional_stream()
      .await
      .map_err(Into::into)
      .and_then(|conn| {
        conn
          .map(|conn| (S2nBiStream::<R>::new(conn), remote))
          .ok_or(Self::Error::Closed)
      })
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nUniAcceptor`] is an implementation of [`QuicUniAcceptor`] based on [`s2n_quic`].
pub struct S2nUniAcceptor<R> {
  server: futures::lock::BiLock<Server>,
  local_addr: SocketAddr,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicUniAcceptor for S2nUniAcceptor<R> {
  type Error = S2nError;
  type ReadStream = S2nReadStream<R>;

  async fn accept_uni(&self) -> Result<(Self::ReadStream, SocketAddr), Self::Error> {
    let mut server = self.server.lock().await;
    let mut conn = server.accept().await.ok_or(Self::Error::Closed)?;

    let remote = conn.remote_addr()?;
    conn
      .accept_receive_stream()
      .await
      .map_err(Into::into)
      .and_then(|conn| {
        conn
          .map(|conn| (S2nReadStream::new(conn.peekable()), remote))
          .ok_or(Self::Error::Closed)
      })
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nConnector`] is an implementation of [`QuicConnector`] based on [`s2n_quic`].
pub struct S2nConnector<R> {
  client: Client,
  local_addr: SocketAddr,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicConnector for S2nConnector<R> {
  type Error = S2nError;
  type BiStream = S2nBiStream<R>;
  type WriteStream = S2nWriteStream<R>;

  async fn open_bi(&self, addr: SocketAddr) -> Result<Self::BiStream, Self::Error> {
    let mut conn = self.client.connect(addr.into()).await?;
    conn.keep_alive(true)?;
    conn
      .open_bidirectional_stream()
      .await
      .map(|conn| S2nBiStream::<R>::new(conn))
      .map_err(Into::into)
  }

  async fn open_bi_with_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> Result<Self::BiStream, Self::Error> {
    let fut = async {
      let mut conn = self.client.connect(addr.into()).await?;
      conn.keep_alive(true)?;
      conn
        .open_bidirectional_stream()
        .await
        .map(|conn| S2nBiStream::<R>::new(conn))
        .map_err(Into::into)
    };
    if timeout == Duration::ZERO {
      fut.await
    } else {
      R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?
    }
  }

  async fn open_uni(&self, addr: SocketAddr) -> Result<Self::WriteStream, Self::Error> {
    self
      .client
      .connect(addr.into())
      .await?
      .open_send_stream()
      .await
      .map(S2nWriteStream::new)
      .map_err(Into::into)
  }

  async fn open_uni_with_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> Result<Self::WriteStream, Self::Error> {
    let fut = async {
      self
        .client
        .connect(addr.into())
        .await?
        .open_send_stream()
        .await
        .map(S2nWriteStream::new)
        .map_err(Into::into)
    };
    if timeout == Duration::ZERO {
      fut.await
    } else {
      R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?
    }
  }

  async fn close(&self) -> Result<(), Self::Error> {
    // TODO: figure out how to close a client
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nBiStream`] is an implementation of [`QuicBiStream`] based on [`s2n_quic`].
pub struct S2nBiStream<R> {
  recv_stream: AsyncPeekable<ReceiveStream>,
  send_stream: SendStream,
  read_timeout: Option<Duration>,
  write_timeout: Option<Duration>,
  _marker: PhantomData<R>,
}

impl<R> S2nBiStream<R> {
  fn new(stream: BidirectionalStream) -> Self {
    let (recv, send) = stream.split();
    Self {
      recv_stream: recv.peekable(),
      send_stream: send,
      read_timeout: None,
      write_timeout: None,
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> TimeoutableReadStream for S2nBiStream<R> {
  fn set_read_timeout(&mut self, timeout: Option<Duration>) {
    self.read_timeout = timeout;
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.read_timeout
  }
}

impl<R: Runtime> TimeoutableWriteStream for S2nBiStream<R> {
  fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    self.write_timeout = timeout;
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.write_timeout
  }
}

impl<R: Runtime> QuicBiStream for S2nBiStream<R> {
  type Error = S2nError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let len = src.len();
    let fut = async {
      self.send_stream.write_all(&src).await?;
      self
        .send_stream
        .flush()
        .await
        .map(|_| len)
        .map_err(Into::into)
    };

    match self.write_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn flush(&mut self) -> Result<(), Self::Error> {
    self.send_stream.flush().await.map_err(Into::into)
  }

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async { self.recv_stream.read(buf).await.map_err(Into::into) };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.recv_stream.read_exact(buf).await.map_err(Into::into) };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn peek(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async { self.recv_stream.peek(buf).await.map_err(Into::into) };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.recv_stream.peek_exact(buf).await.map_err(Into::into) };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.send_stream.close().await.map_err(Into::into)
  }
}

/// [`S2nReadStream`] is an implementation of [`QuicReadStream`] based on [`s2n_quic`].
pub struct S2nReadStream<R> {
  stream: AsyncPeekable<ReceiveStream>,
  timeout: Option<Duration>,
  _marker: PhantomData<R>,
}

impl<R> S2nReadStream<R> {
  const fn new(stream: AsyncPeekable<ReceiveStream>) -> Self {
    Self {
      stream,
      timeout: None,
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> TimeoutableReadStream for S2nReadStream<R> {
  fn set_read_timeout(&mut self, timeout: Option<Duration>) {
    self.timeout = timeout;
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.timeout
  }
}

impl<R: Runtime> QuicReadStream for S2nReadStream<R> {
  type Error = S2nError;

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async { self.stream.read(buf).await.map_err(Into::into) };

    match self.timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await,
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.stream.read_exact(buf).await.map_err(Into::into) };

    match self.timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await,
    }
  }

  async fn peek(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async { self.stream.peek(buf).await.map_err(Into::into) };

    match self.timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await,
    }
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.stream.peek_exact(buf).await.map_err(Into::into) };

    match self.timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await,
    }
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    Ok(())
  }
}

/// [`S2nWriteStream`] is an implementation of [`QuicWriteStream`] based on [`s2n_quic`].
pub struct S2nWriteStream<R> {
  stream: SendStream,
  timeout: Option<Duration>,
  _marker: PhantomData<R>,
}

impl<R> S2nWriteStream<R> {
  const fn new(stream: SendStream) -> Self {
    Self {
      stream,
      timeout: None,
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> TimeoutableWriteStream for S2nWriteStream<R> {
  fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    self.timeout = timeout;
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.timeout
  }
}

impl<R: Runtime> QuicWriteStream for S2nWriteStream<R> {
  type Error = S2nError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let fut = async {
      let len = src.len();
      self
        .stream
        .write_all(&src)
        .await
        .map(|_| len)
        .map_err(Into::into)
    };

    match self.timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| S2nError::Timeout)?,
      None => fut.await,
    }
  }

  async fn flush(&mut self) -> Result<(), Self::Error> {
    self.stream.flush().await.map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.stream.close().await.map_err(Into::into)
  }
}

fn invalid_data<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
