use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use agnostic::{net::Net, Runtime};
use bytes::Bytes;
use futures::AsyncReadExt;
use memberlist_core::transport::TimeoutableStream;
use peekable::future::{AsyncPeekable, AsyncPeekExt};
use quinn::{ClientConfig, ConnectError, Endpoint, RecvStream, SendStream, VarInt};
use smol_str::SmolStr;

mod error;
pub use error::*;

mod options;
pub use options::*;

use super::{
  QuicBiAcceptor, QuicBiStream, QuicConnector, QuicReadStream, QuicUniAcceptor, QuicWriteStream,
  StreamLayer,
};

/// [`Quinn`] is an implementation of [`StreamLayer`] based on [`quinn`].
pub struct Quinn<R> {
  opts: QuinnOptions,
  _marker: PhantomData<R>,
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Error = QuinnError;
  type BiAcceptor = QuinnAcceptor<R>;
  type UniAcceptor = QuinnAcceptor<R>;
  type Connector = QuinnConnector<R>;
  type Stream = QuinnBiStream<R>;
  type ReadStream = QuinnReadStream;
  type WriteStream = QuinnWriteStream;

  fn max_stream_data(&self) -> usize {
    self.opts.max_stream_data.min(self.opts.max_connection_data)
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> std::io::Result<((Self::BiAcceptor, Self::UniAcceptor), Self::Connector)> {
    let server_name = self.opts.server_name.clone();

    let client_config = self.opts.client_config.clone();
    let sock = socket2::Socket::new(
      socket2::Domain::for_address(addr),
      socket2::Type::DGRAM,
      Some(socket2::Protocol::UDP),
    )?;
    sock.set_nonblocking(true)?;

    let endpoint = Arc::new(Endpoint::new(
      self.opts.endpoint_config.clone(),
      Some(self.opts.server_config.clone()),
      sock.into(),
      Arc::new(<R::Net as Net>::Quinn::default()),
    )?);

    let bi_acceptor = Self::BiAcceptor {
      endpoint: endpoint.clone(),
      handshake_timeout: self.opts.handshake_timeout,
      _marker: PhantomData,
    };

    let uni_acceptor = Self::UniAcceptor {
      endpoint: endpoint.clone(),
      handshake_timeout: self.opts.handshake_timeout,
      _marker: PhantomData,
    };

    let connector = Self::Connector {
      server_name,
      endpoint,
      client_config,
      _marker: PhantomData,
    };
    Ok(((bi_acceptor, uni_acceptor), connector))
  }
}

/// [`QuinnAcceptor`] is an implementation of [`QuicAcceptor`] based on [`quinn`].
pub struct QuinnAcceptor<R> {
  endpoint: Arc<Endpoint>,
  handshake_timeout: Duration,
  _marker: PhantomData<R>,
}

impl<R> Clone for QuinnAcceptor<R> {
  fn clone(&self) -> Self {
    Self {
      endpoint: self.endpoint.clone(),
      handshake_timeout: self.handshake_timeout,
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> QuicBiAcceptor for QuinnAcceptor<R> {
  type Error = QuinnError;
  type BiStream = QuinnBiStream<R>;

  async fn accept_bi(&self) -> Result<(Self::BiStream, SocketAddr), Self::Error> {
    let conn = self.endpoint.accept().await.unwrap();
    conn.remote_address();

    let fut = async {
      let conn = self
        .endpoint
        .accept()
        .await
        .ok_or(ConnectError::EndpointStopping)?;

      let remote = conn.remote_address();
      conn
        .await?
        .accept_bi()
        .await
        .map(|(send, recv)| (QuinnBiStream::new(send, recv), remote))
        .map_err(Into::into)
    };
    R::timeout(self.handshake_timeout, fut)
      .await
      .map_err(|_| Self::Error::Timeout)?
  }
}

impl<R: Runtime> QuicUniAcceptor for QuinnAcceptor<R> {
  type Error = QuinnError;
  type ReadStream = QuinnReadStream;

  async fn accept_uni(&self) -> Result<(Self::ReadStream, SocketAddr), Self::Error> {
    let fut = async {
      let conn = self
        .endpoint
        .accept()
        .await
        .ok_or(ConnectError::EndpointStopping)?;
      let remote = conn.remote_address();
      conn
        .await?
        .accept_uni()
        .await
        .map(|s| (QuinnReadStream(s.peekable()), remote))
        .map_err(Into::into)
    };

    R::timeout(self.handshake_timeout, fut)
      .await
      .map_err(|_| Self::Error::Timeout)?
  }
}

/// [`QuinnListener`] is an implementation of [`Listener`] based on [`quinn`].
pub struct QuinnConnector<R> {
  server_name: SmolStr,
  endpoint: Arc<Endpoint>,
  client_config: ClientConfig,
  _marker: PhantomData<R>,
}

impl<R: Runtime> QuicConnector for QuinnConnector<R> {
  type Error = QuinnError;
  type BiStream = QuinnBiStream<R>;
  type WriteStream = QuinnWriteStream;

  async fn open_bi(&self, addr: SocketAddr) -> Result<Self::BiStream, Self::Error> {
    self
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)?
      .await?
      .open_bi()
      .await
      .map(|(send, recv)| QuinnBiStream::new(send, recv))
      .map_err(Into::into)
  }

  async fn open_bi_with_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> Result<Self::BiStream, Self::Error> {
    let fut = async {
      self
        .endpoint
        .connect_with(self.client_config.clone(), addr, &self.server_name)?
        .await?
        .open_bi()
        .await
        .map(|(send, recv)| QuinnBiStream::new(send, recv))
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
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)?
      .await?
      .open_uni()
      .await
      .map(QuinnWriteStream)
      .map_err(Into::into)
  }
}

/// [`QuinnReadStream`] is an implementation of [`QuicReadStream`] based on [`quinn`].
pub struct QuinnReadStream(AsyncPeekable<RecvStream>);

impl QuicReadStream for QuinnReadStream {
  type Error = QuinnReadStreamError;

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self
      .0
      .read(buf)
      .await
      .map_err(Into::into)
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.read_exact(buf).await.map_err(Into::into)
  }

  async fn peek(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self
      .0
      .peek(buf)
      .await
      .map_err(Into::into)
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.peek_exact(buf).await.map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    // self
    //   .0
    //   .stop(VarInt::from_u32(0))
    //   .map_err(|_| QuinnReadStreamError::Read(quinn::ReadError::UnknownStream))
    todo!()
  }
}

/// [`QuinnWriteStream`] is an implementation of [`QuicWriteStream`] based on [`quinn`].
pub struct QuinnWriteStream(SendStream);

impl QuicWriteStream for QuinnWriteStream {
  type Error = QuinnWriteStreamError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let sent = src.len();
    self
      .0
      .write_all(&src)
      .await
      .map(|_| sent)
      .map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.0.finish().await.map_err(Into::into)
  }
}

/// [`QuinnBiStream`] is an implementation of [`QuicBiStream`] based on [`quinn`].
pub struct QuinnBiStream<R> {
  send: SendStream,
  recv: AsyncPeekable<RecvStream>,
  read_timeout: Option<Duration>,
  write_timeout: Option<Duration>,
  _marker: PhantomData<R>,
}

impl<R> QuinnBiStream<R> {
  #[inline]
  const fn new(send: SendStream, recv: RecvStream) -> Self {
    Self {
      send,
      recv: recv.peekable(),
      read_timeout: None,
      write_timeout: None,
      _marker: PhantomData,
    }
  }
}

impl<R: Runtime> QuicBiStream for QuinnBiStream<R> {
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

    match self.write_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.recv.read_exact(buf).await.map_err(|e| QuinnReadStreamError::from(e).into()) };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
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

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.recv.peek_exact(buf).await.map_err(|e| QuinnReadStreamError::from(e).into()) };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn peek(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async {
      self
        .recv
        .peek(buf)
        .await
        .map_err(|e| QuinnReadStreamError::from(e).into())
    };

    match self.read_timeout {
      Some(timeout) => R::timeout(timeout, fut)
        .await
        .map_err(|_| Self::Error::Timeout)?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.send.finish().await.map_err(QuinnBiStreamError::from)?;
    // self.recv.stop(VarInt::from_u32(0)).map_err(|_| {
    //   QuinnBiStreamError::Read(QuinnReadStreamError::Read(quinn::ReadError::UnknownStream)).into()
    // })
    todo!()
  }
}

impl<R: Runtime> TimeoutableStream for QuinnBiStream<R> {
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
