use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::Instant,
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

/// The options for the native-tls stream layer.
#[derive(Debug)]
#[viewit::viewit(getters(style = "ref"), setters(prefix = "with"))]
pub struct NativeTlsOptions {
  /// The acceptor for the server.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the TLS acceptor."),),
    setter(attrs(doc = "Set the TLS acceptor. (Builder pattern)"),)
  )]
  acceptor: TlsAcceptor,
  /// The connector for the client.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the TLS connector."),),
    setter(attrs(doc = "Set the TLS connector. (Builder pattern)"),)
  )]
  connector: TlsConnector,
  /// The server name for the client.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the server name."),),
    setter(attrs(doc = "Set the server name. (Builder pattern)"),)
  )]
  server_name: String,
}

impl NativeTlsOptions {
  /// Creates a new instance.
  #[inline]
  pub fn new(
    server_name: impl Into<String>,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
  ) -> Self {
    Self {
      acceptor,
      connector,
      server_name: server_name.into(),
    }
  }
}

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
  fn new_in(
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
  type Options = NativeTlsOptions;

  #[inline]
  async fn new(options: Self::Options) -> io::Result<Self> {
    Ok(Self::new_in(
      options.server_name,
      options.acceptor,
      options.connector,
    ))
  }

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(addr).await?;
    let local_addr = conn.local_addr()?;
    let peer_addr = conn.peer_addr()?;
    let stream = self
      .connector
      .connect(self.domain.clone(), conn)
      .await
      .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
    Ok(NativeTlsStream {
      stream,
      read_deadline: None,
      write_deadline: None,
      local_addr,
      peer_addr,
    })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let acceptor = self.acceptor.clone();
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .and_then(|ln| {
        ln.local_addr().map(|local_addr| NativeTlsListener {
          local_addr,
          ln,
          acceptor,
        })
      })
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
  local_addr: SocketAddr,
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
        read_deadline: None,
        write_deadline: None,
        local_addr: self.local_addr,
        peer_addr: addr,
      },
      addr,
    ))
  }

  async fn shutdown(&self) -> io::Result<()> {
    TcpListener::shutdown(&self.ln).await
  }

  fn local_addr(&self) -> std::net::SocketAddr {
    self.local_addr
  }
}

/// TLS connection of the TCP stream layer.
#[pin_project::pin_project]
pub struct NativeTlsStream<R: Runtime> {
  #[pin]
  stream: AsyncNativeTlsStream<<R::Net as Net>::TcpStream>,
  read_deadline: Option<Instant>,
  write_deadline: Option<Instant>,
  local_addr: SocketAddr,
  peer_addr: SocketAddr,
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
  fn set_read_deadline(&mut self, deadline: Option<Instant>) {
    self.read_deadline = deadline;
  }

  fn read_deadline(&self) -> Option<Instant> {
    self.read_deadline
  }
}

impl<R: Runtime> TimeoutableWriteStream for NativeTlsStream<R> {
  fn set_write_deadline(&mut self, deadline: Option<Instant>) {
    self.write_deadline = deadline;
  }

  fn write_deadline(&self) -> Option<Instant> {
    self.write_deadline
  }
}

impl<R: Runtime> PromisedStream for NativeTlsStream<R> {
  #[inline]
  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  #[inline]
  fn peer_addr(&self) -> SocketAddr {
    self.peer_addr
  }
}
