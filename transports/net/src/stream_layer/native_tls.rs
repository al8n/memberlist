use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

pub use ::native_tls;
use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use async_native_tls::TlsStream as AsyncNativeTlsStream;
pub use async_native_tls::{self, TlsAcceptor, TlsConnector};
use futures::{lock::BiLock, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use peekable::future::AsyncPeekable;

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
  type Runtime = R;
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
    Ok(NativeTlsStream::new(stream, peer_addr, local_addr))
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
    Ok((NativeTlsStream::new(stream, addr, self.local_addr), addr))
  }

  async fn shutdown(&self) -> io::Result<()> {
    Ok(())
  }

  fn local_addr(&self) -> std::net::SocketAddr {
    self.local_addr
  }
}

/// TLS connection of the TCP stream layer.
#[pin_project::pin_project]
pub struct NativeTlsStream<R: Runtime> {
  #[pin]
  stream: AsyncPeekable<AsyncNativeTlsStream<<R::Net as Net>::TcpStream>>,
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

/// A [`ProtoReader`](memberlist_core::proto::ProtoReader) for the TLS stream layer.
pub struct NativeTlsProtoReader<R: Runtime> {
  reader: BiLock<AsyncPeekable<NativeTlsStream<R>>>,
}

impl<R: Runtime> memberlist_core::proto::ProtoReader for NativeTlsProtoReader<R> {
  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.reader.lock().await.peek(buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut reader = self.reader.lock().await;
    AsyncReadExt::read(&mut *reader, buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    let mut reader = self.reader.lock().await;
    AsyncReadExt::read_exact(&mut *reader, buf).await
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    let mut reader = self.reader.lock().await;
    AsyncReadExt::read_exact(&mut *reader, buf).await
  }
}

/// A [`ProtoWriter`](memberlist_core::proto::ProtoWriter) for the TLS stream layer.
pub struct NativeTlsProtoWriter<R: Runtime> {
  writer: BiLock<AsyncPeekable<NativeTlsStream<R>>>,
}

impl<R: Runtime> memberlist_core::proto::ProtoWriter for NativeTlsProtoWriter<R> {
  async fn close(&mut self) -> std::io::Result<()> {
    let mut writer = self.writer.lock().await;
    AsyncWriteExt::close(&mut *writer).await
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    let mut writer = self.writer.lock().await;
    AsyncWriteExt::write_all(&mut *writer, payload).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    let mut writer = self.writer.lock().await;
    AsyncWriteExt::flush(&mut *writer).await
  }
}

impl<R: Runtime> memberlist_core::transport::Connection for NativeTlsStream<R> {
  type Reader = NativeTlsProtoReader<R>;

  type Writer = NativeTlsProtoWriter<R>;

  fn split(self) -> (Self::Reader, Self::Writer) {
    let (reader, writer) = BiLock::new(AsyncPeekable::new(self));
    (Self::Reader { reader }, Self::Writer { writer })
  }

  async fn close(&mut self) -> std::io::Result<()> {
    self.stream.close().await
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    self.stream.write_all(payload).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    self.stream.flush().await
  }

  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.stream.peek(buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.stream.peek_exact(buf).await
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.stream.read_exact(buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.stream.read(buf).await
  }
}

impl<R: Runtime> PromisedStream for NativeTlsStream<R> {
  type Instant = R::Instant;

  #[inline]
  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  #[inline]
  fn peer_addr(&self) -> SocketAddr {
    self.peer_addr
  }
}

impl<R: Runtime> NativeTlsStream<R> {
  #[inline]
  fn new(
    stream: AsyncNativeTlsStream<<R::Net as Net>::TcpStream>,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
  ) -> Self {
    Self {
      stream: AsyncPeekable::new(stream),
      local_addr,
      peer_addr,
    }
  }
}
