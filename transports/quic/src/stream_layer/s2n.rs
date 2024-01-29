use std::{io, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{AsyncReadExt, AsyncWriteExt, Future};
use memberlist_core::transport::TimeoutableStream;
use peekable::future::{AsyncPeekExt, AsyncPeekable};
use s2n_quic::{
  stream::{BidirectionalStream, ReceiveStream, SendStream},
  Client, Server,
};

use super::{
  QuicBiAcceptor, QuicBiStream, QuicConnector, QuicReadStream, QuicUniAcceptor, QuicWriteStream,
  StreamLayer,
};

mod error;
pub use error::*;
mod options;
pub use options::*;

/// A trait which is used to create a s2n [`Server`].
pub trait ServerCreator: Send + Sync + 'static {
  /// Build a server
  fn build(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Server>> + Send;
}

/// A trait which is used to create a s2n [`Client`].
pub trait ClientCreator: Send + Sync + 'static {
  /// Build a client
  fn build(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Client>> + Send;
}

/// A QUIC stream layer based on [`s2n`](::s2n_quic).
pub struct S2n<SC, CC, R> {
  server_creator: Arc<SC>,
  client_creator: Arc<CC>,
  handshake_timeout: Duration,
  _marker: PhantomData<R>,
}

impl<SC, CC, R: Runtime> StreamLayer for S2n<SC, CC, R>
where
  SC: ServerCreator,
  CC: ClientCreator,
{
  type Error = S2nError;
  type BiAcceptor = S2nBiAcceptor<R>;
  type UniAcceptor = S2nUniAcceptor<R>;
  type Connector = S2nConnector<R>;

  type Stream = S2nBiStream<R>;
  type ReadStream = S2nReadStream;
  type WriteStream = S2nWriteStream;

  fn max_stream_data(&self) -> usize {
    todo!()
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> io::Result<((Self::BiAcceptor, Self::UniAcceptor), Self::Connector)> {
    let srv = self.server_creator.build(addr).await?;
    let client = self.client_creator.build(addr).await?;
    let (bi, uni) = futures::lock::BiLock::new(srv);
    let bi = Self::BiAcceptor {
      server: bi,
      handshake_timeout: self.handshake_timeout,
      _marker: PhantomData,
    };
    let uni = Self::UniAcceptor {
      server: uni,
      handshake_timeout: self.handshake_timeout,
      _marker: PhantomData,
    };

    let connector = Self::Connector {
      client,
      _marker: PhantomData,
    };
    Ok(((bi, uni), connector))
  }
}

/// [`S2nBiAcceptor`] is an implementation of [`QuicBiAcceptor`] based on [`s2n_quic`].
pub struct S2nBiAcceptor<R> {
  server: futures::lock::BiLock<Server>,
  handshake_timeout: Duration,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicBiAcceptor for S2nBiAcceptor<R> {
  type Error = S2nError;
  type BiStream = S2nBiStream<R>;

  async fn accept_bi(&self) -> Result<(Self::BiStream, SocketAddr), Self::Error> {
    let fut = async {
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
    };

    R::timeout(self.handshake_timeout, fut)
      .await
      .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?
  }
}

/// [`S2nUniAcceptor`] is an implementation of [`QuicUniAcceptor`] based on [`s2n_quic`].
pub struct S2nUniAcceptor<R> {
  server: futures::lock::BiLock<Server>,
  handshake_timeout: Duration,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicUniAcceptor for S2nUniAcceptor<R> {
  type Error = S2nError;
  type ReadStream = S2nReadStream;

  async fn accept_uni(&self) -> Result<(Self::ReadStream, SocketAddr), Self::Error> {
    let fut = async {
      let mut server = self.server.lock().await;
      let mut conn = server.accept().await.ok_or(Self::Error::Closed)?;

      let remote = conn.remote_addr()?;
      conn
        .accept_receive_stream()
        .await
        .map_err(Into::into)
        .and_then(|conn| {
          conn
            .map(|conn| (S2nReadStream(conn.peekable()), remote))
            .ok_or(Self::Error::Closed)
        })
    };

    R::timeout(self.handshake_timeout, fut)
      .await
      .map_err(|_| Self::Error::Timeout)?
  }
}

/// [`S2nConnector`] is an implementation of [`QuicConnector`] based on [`s2n_quic`].
pub struct S2nConnector<R> {
  client: Client,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicConnector for S2nConnector<R> {
  type Error = S2nError;
  type BiStream = S2nBiStream<R>;
  type WriteStream = S2nWriteStream;

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
      .map(S2nWriteStream)
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
        .map(S2nWriteStream)
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

impl<R: Runtime> TimeoutableStream for S2nBiStream<R> {
  fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    self.write_timeout = timeout;
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.write_timeout
  }

  fn set_read_timeout(&mut self, timeout: Option<Duration>) {
    self.read_timeout = timeout;
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.read_timeout
  }
}

impl<R: Runtime> QuicBiStream for S2nBiStream<R> {
  type Error = S2nError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let len = src.len();
    let fut = async {
      self.send_stream.write_all(&src).await?;
      self.send_stream.flush().await.map(|_| len).map_err(Into::into)
    };

    match self.write_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
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
pub struct S2nReadStream(AsyncPeekable<ReceiveStream>);

impl QuicReadStream for S2nReadStream {
  type Error = S2nError;

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self.0.read(buf).await.map_err(Into::into)
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.read_exact(buf).await.map_err(Into::into)
  }

  async fn peek(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self.0.peek(buf).await.map_err(Into::into)
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.peek_exact(buf).await.map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    Ok(())
  }
}

/// [`S2nWriteStream`] is an implementation of [`QuicWriteStream`] based on [`s2n_quic`].
pub struct S2nWriteStream(SendStream);

impl QuicWriteStream for S2nWriteStream {
  type Error = S2nError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let len = src.len();
    self.0.write_all(&src).await?;
    self.0.flush().await.map(|_| len).map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.0.close().await.map_err(Into::into)
  }
}
