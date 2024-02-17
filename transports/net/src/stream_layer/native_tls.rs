use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::Duration,
};

pub use ::native_tls;
use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use async_native_tls::TlsStream as AsyncNativeTlsStream;
pub use async_native_tls::{self, TlsAcceptor, TlsConnector};
use futures::{AsyncRead, AsyncWrite};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};

use super::{Listener, PromisedStream, StreamLayer};

/// Tls stream layer
pub struct NativeTls<R> {
  acceptor: Arc<TlsAcceptor>,
  connector: TlsConnector,
  domain: String,
  _marker: std::marker::PhantomData<R>,
}

impl<R> NativeTls<R> {
  /// Create a new tcp stream layer
  #[inline]
  pub fn new(
    server_name: impl Into<String>,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
  ) -> Self {
    Self {
      acceptor: Arc::new(acceptor),
      connector,
      domain: server_name.into(),
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for NativeTls<R> {
  type Listener = NativeTlsListener<R>;
  type Stream = NativeTlsStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(addr).await?;
    let stream = self
      .connector
      .connect(self.domain.clone(), conn)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
    Ok(NativeTlsStream {
      stream,
      read_timeout: None,
      write_timeout: None,
    })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let acceptor = self.acceptor.clone();
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .map(|ln| NativeTlsListener { ln, acceptor })
  }

  async fn cache_stream(&self, _addr: SocketAddr, _stream: Self::Stream) {}

  fn is_secure() -> bool {
    true
  }
}

/// [`Listener`] of the TLS stream layer
pub struct NativeTlsListener<R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  acceptor: Arc<TlsAcceptor>,
}

impl<R: Runtime> Listener for NativeTlsListener<R> {
  type Stream = NativeTlsStream<R>;

  async fn accept(&self) -> io::Result<(Self::Stream, std::net::SocketAddr)> {
    let (conn, addr) = self.ln.accept().await?;
    let stream = TlsAcceptor::accept(&self.acceptor, conn)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
    Ok((
      NativeTlsStream {
        stream,
        read_timeout: None,
        write_timeout: None,
      },
      addr,
    ))
  }

  fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
    self.ln.local_addr()
  }
}

/// TLS connection of the TCP stream layer.
#[pin_project::pin_project]
pub struct NativeTlsStream<R: Runtime> {
  #[pin]
  stream: AsyncNativeTlsStream<<R::Net as Net>::TcpStream>,
  read_timeout: Option<Duration>,
  write_timeout: Option<Duration>,
}

impl<R: Runtime> AsyncRead for NativeTlsStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for NativeTlsStream<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    self.project().stream.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().stream.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().stream.poll_close(cx)
  }
}

impl<R: Runtime> TimeoutableReadStream for NativeTlsStream<R> {
  fn set_read_timeout(&mut self, timeout: Option<Duration>) {
    self.read_timeout = timeout;
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.read_timeout
  }
}

impl<R: Runtime> TimeoutableWriteStream for NativeTlsStream<R> {
  fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    self.write_timeout = timeout;
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.write_timeout
  }
}

impl<R: Runtime> PromisedStream for NativeTlsStream<R> {}
