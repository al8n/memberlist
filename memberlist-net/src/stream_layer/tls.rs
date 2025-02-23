use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use agnostic::{
  Runtime,
  net::{Net, TcpListener, TcpStream},
};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, lock::BiLock};
pub use futures_rustls::{
  TlsAcceptor, TlsConnector, client, pki_types::ServerName, rustls, server,
};
use peekable::future::{AsyncPeekExt, AsyncPeekable};
use rustls::{SignatureScheme, client::danger::ServerCertVerifier};

use super::{Listener, PromisedStream, StreamLayer};

/// A certificate verifier that does not verify the server certificate.
/// This is useful for testing. Do not use in production.
#[derive(Debug, Default)]
pub struct NoopCertificateVerifier;

impl NoopCertificateVerifier {
  /// Constructs a new `NoopCertificateVerifier`.
  pub fn new() -> Arc<Self> {
    Arc::new(Self)
  }
}

impl ServerCertVerifier for NoopCertificateVerifier {
  fn verify_server_cert(
    &self,
    _end_entity: &rustls::pki_types::CertificateDer<'_>,
    _intermediates: &[rustls::pki_types::CertificateDer<'_>],
    _server_name: &rustls::pki_types::ServerName<'_>,
    _ocsp_response: &[u8],
    _now: rustls::pki_types::UnixTime,
  ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
    Ok(rustls::client::danger::ServerCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    _message: &[u8],
    _cert: &rustls::pki_types::CertificateDer<'_>,
    _dss: &rustls::DigitallySignedStruct,
  ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
    Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
  }

  fn verify_tls13_signature(
    &self,
    _message: &[u8],
    _cert: &rustls::pki_types::CertificateDer<'_>,
    _dss: &rustls::DigitallySignedStruct,
  ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
    Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
  }

  fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
    vec![
      SignatureScheme::RSA_PKCS1_SHA1,
      SignatureScheme::ECDSA_SHA1_Legacy,
      SignatureScheme::RSA_PKCS1_SHA256,
      SignatureScheme::ECDSA_NISTP256_SHA256,
      SignatureScheme::RSA_PKCS1_SHA384,
      SignatureScheme::ECDSA_NISTP384_SHA384,
      SignatureScheme::RSA_PKCS1_SHA512,
      SignatureScheme::ECDSA_NISTP521_SHA512,
      SignatureScheme::RSA_PSS_SHA256,
      SignatureScheme::RSA_PSS_SHA384,
      SignatureScheme::RSA_PSS_SHA512,
      SignatureScheme::ED25519,
      SignatureScheme::ED448,
    ]
  }
}

/// The options for the tls stream layer.
#[viewit::viewit(getters(style = "ref"), setters(prefix = "with"))]
pub struct TlsOptions {
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
  /// The server name
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the server name."),),
    setter(attrs(doc = "Set the server name. (Builder pattern)"),)
  )]
  server_name: ServerName<'static>,
}

impl TlsOptions {
  /// Constructs a new `TlsOptions`.
  #[inline]
  pub const fn new(
    server_name: ServerName<'static>,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
  ) -> Self {
    Self {
      acceptor,
      connector,
      server_name,
    }
  }
}

/// Tls stream layer
pub struct Tls<R> {
  domain: ServerName<'static>,
  acceptor: Arc<TlsAcceptor>,
  connector: TlsConnector,
  _marker: std::marker::PhantomData<R>,
}

impl<R> Tls<R> {
  /// Create a new tcp stream layer
  #[inline]
  fn new_in(domain: ServerName<'static>, acceptor: TlsAcceptor, connector: TlsConnector) -> Self {
    Self {
      domain,
      acceptor: Arc::new(acceptor),
      connector,
      _marker: std::marker::PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Tls<R> {
  type Listener = TlsListener<R>;
  type Stream = TlsStream<R>;
  type Options = TlsOptions;
  type Runtime = R;

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
    let stream = self.connector.connect(self.domain.clone(), conn).await?;
    Ok(TlsStream::client(stream, addr, local_addr))
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let acceptor = self.acceptor.clone();
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .and_then(|ln| {
        ln.local_addr().map(|local_addr| TlsListener {
          ln,
          acceptor,
          local_addr,
        })
      })
  }

  fn is_secure() -> bool {
    true
  }
}

/// [`Listener`] of the TLS stream layer
pub struct TlsListener<R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  acceptor: Arc<TlsAcceptor>,
  local_addr: SocketAddr,
}

impl<R: Runtime> Listener for TlsListener<R> {
  type Stream = TlsStream<R>;

  async fn accept(&self) -> io::Result<(Self::Stream, std::net::SocketAddr)> {
    let (conn, addr) = self.ln.accept().await?;
    let stream = TlsAcceptor::accept(&self.acceptor, conn).await?;
    Ok((TlsStream::server(stream, addr, self.local_addr), addr))
  }

  fn local_addr(&self) -> std::net::SocketAddr {
    self.local_addr
  }

  async fn shutdown(&self) -> io::Result<()> {
    Ok(())
  }
}

#[pin_project::pin_project]
enum TlsStreamKind<R: Runtime> {
  Client {
    #[pin]
    stream: AsyncPeekable<client::TlsStream<<R::Net as Net>::TcpStream>>,
  },
  Server {
    #[pin]
    stream: AsyncPeekable<server::TlsStream<<R::Net as Net>::TcpStream>>,
  },
}

impl<R: Runtime> AsyncRead for TlsStreamKind<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client { stream } => Pin::new(stream).poll_read(cx, buf),
      Self::Server { stream } => Pin::new(stream).poll_read(cx, buf),
    }
  }
}

impl<R: Runtime> AsyncWrite for TlsStreamKind<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client { stream } => Pin::new(stream).poll_write(cx, buf),
      Self::Server { stream } => Pin::new(stream).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client { stream } => Pin::new(stream).poll_flush(cx),
      Self::Server { stream } => Pin::new(stream).poll_flush(cx),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client { stream } => Pin::new(stream).poll_close(cx),
      Self::Server { stream } => Pin::new(stream).poll_close(cx),
    }
  }
}

/// [`PromisedStream`] of the TLS stream layer.
#[pin_project::pin_project]
pub struct TlsStream<R: Runtime> {
  #[pin]
  stream: TlsStreamKind<R>,
  local_addr: SocketAddr,
  peer_addr: SocketAddr,
}

impl<R: Runtime> AsyncRead for TlsStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for TlsStream<R> {
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
pub struct TlsProtoReader<R: Runtime> {
  reader: BiLock<TlsStream<R>>,
}

impl<R: Runtime> memberlist_core::proto::ProtoReader for TlsProtoReader<R> {
  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut reader = self.reader.lock().await;
    match reader.stream {
      TlsStreamKind::Client { ref mut stream } => stream.peek(buf).await,
      TlsStreamKind::Server { ref mut stream } => stream.peek(buf).await,
    }
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut reader = self.reader.lock().await;
    AsyncReadExt::read(&mut *reader, buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    let mut reader = self.reader.lock().await;
    match reader.stream {
      TlsStreamKind::Client { ref mut stream } => stream.peek_exact(buf).await,
      TlsStreamKind::Server { ref mut stream } => stream.peek_exact(buf).await,
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    let mut reader = self.reader.lock().await;
    AsyncReadExt::read_exact(&mut *reader, buf).await
  }
}

/// A [`ProtoWriter`](memberlist_core::proto::ProtoWriter) for the TLS stream layer.
pub struct TlsProtoWriter<R: Runtime> {
  writer: BiLock<TlsStream<R>>,
}

impl<R: Runtime> memberlist_core::proto::ProtoWriter for TlsProtoWriter<R> {
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

impl<R: Runtime> memberlist_core::transport::Connection for TlsStream<R> {
  type Reader = TlsProtoReader<R>;

  type Writer = TlsProtoWriter<R>;

  fn split(self) -> (Self::Reader, Self::Writer) {
    let (reader, writer) = BiLock::new(self);
    (Self::Reader { reader }, Self::Writer { writer })
  }

  async fn close(&mut self) -> std::io::Result<()> {
    AsyncWriteExt::close(self).await
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    AsyncWriteExt::write_all(self, payload).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    AsyncWriteExt::flush(self).await
  }

  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    match &mut self.stream {
      TlsStreamKind::Client { stream } => stream.peek(buf).await,
      TlsStreamKind::Server { stream } => stream.peek(buf).await,
    }
  }

  fn consume_peek(&mut self) {
    let _ = match &mut self.stream {
      TlsStreamKind::Client { stream } => stream.consume(),
      TlsStreamKind::Server { stream } => stream.consume(),
    };
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    AsyncReadExt::read_exact(self, buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    AsyncReadExt::read(self, buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    match &mut self.stream {
      TlsStreamKind::Client { stream } => stream.peek_exact(buf).await,
      TlsStreamKind::Server { stream } => stream.peek_exact(buf).await,
    }
  }
}

impl<R: Runtime> PromisedStream for TlsStream<R> {
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

impl<R: Runtime> TlsStream<R> {
  #[inline]
  fn client(
    stream: client::TlsStream<<R::Net as Net>::TcpStream>,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
  ) -> Self {
    Self {
      stream: TlsStreamKind::Client {
        stream: stream.peekable(),
      },
      local_addr,
      peer_addr,
    }
  }

  #[inline]
  fn server(
    stream: server::TlsStream<<R::Net as Net>::TcpStream>,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
  ) -> Self {
    Self {
      stream: TlsStreamKind::Server {
        stream: stream.peekable(),
      },
      local_addr,
      peer_addr,
    }
  }
}
