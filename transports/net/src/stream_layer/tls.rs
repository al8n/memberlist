use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use futures::{AsyncRead, AsyncWrite};
pub use futures_rustls::{
  client, pki_types::ServerName, rustls, server, TlsAcceptor, TlsConnector,
};
use memberlist_core::transport::TimeoutableStream;
use rustls::{client::danger::ServerCertVerifier, SignatureScheme};

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
  pub fn new(domain: ServerName<'static>, acceptor: TlsAcceptor, connector: TlsConnector) -> Self {
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

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    let conn = <<R::Net as Net>::TcpStream as TcpStream>::connect(addr).await?;
    let stream = self.connector.connect(self.domain.clone(), conn).await?;
    Ok(TlsStream {
      stream: TlsStreamKind::Client(stream),
    })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let acceptor = self.acceptor.clone();
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .map(|ln| TlsListener { ln, acceptor })
  }

  fn is_secure() -> bool {
    true
  }
}

/// [`Listener`] of the TLS stream layer
pub struct TlsListener<R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  acceptor: Arc<TlsAcceptor>,
}

impl<R: Runtime> Listener for TlsListener<R> {
  type Stream = TlsStream<R>;

  async fn accept(&self) -> io::Result<(Self::Stream, std::net::SocketAddr)> {
    let (conn, addr) = self.ln.accept().await?;
    let stream = TlsAcceptor::accept(&self.acceptor, conn).await?;
    Ok((
      TlsStream {
        stream: TlsStreamKind::Server(stream),
      },
      addr,
    ))
  }

  fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
    self.ln.local_addr()
  }
}

#[pin_project::pin_project]
enum TlsStreamKind<R: Runtime> {
  Client(#[pin] client::TlsStream<<R::Net as Net>::TcpStream>),
  Server(#[pin] server::TlsStream<<R::Net as Net>::TcpStream>),
}

impl<R: Runtime> AsyncRead for TlsStreamKind<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_read(cx, buf),
      Self::Server(s) => Pin::new(s).poll_read(cx, buf),
    }
  }
}

impl<R: Runtime> AsyncWrite for TlsStreamKind<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_write(cx, buf),
      Self::Server(s) => Pin::new(s).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_flush(cx),
      Self::Server(s) => Pin::new(s).poll_flush(cx),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client(s) => Pin::new(s).poll_close(cx),
      Self::Server(s) => Pin::new(s).poll_close(cx),
    }
  }
}

/// [`PromisedStream`] of the TLS stream layer.
#[pin_project::pin_project]
pub struct TlsStream<R: Runtime> {
  #[pin]
  stream: TlsStreamKind<R>,
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

impl<R: Runtime> TimeoutableStream for TlsStream<R> {
  fn set_write_timeout(&self, timeout: Option<Duration>) {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.set_write_timeout(timeout),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.set_write_timeout(timeout),
    }
  }

  fn write_timeout(&self) -> Option<Duration> {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.write_timeout(),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.write_timeout(),
    }
  }

  fn set_read_timeout(&self, timeout: Option<Duration>) {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.set_read_timeout(timeout),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.set_read_timeout(timeout),
    }
  }

  fn read_timeout(&self) -> Option<Duration> {
    match self {
      Self {
        stream: TlsStreamKind::Client(s),
      } => s.get_ref().0.read_timeout(),
      Self {
        stream: TlsStreamKind::Server(s),
      } => s.get_ref().0.read_timeout(),
    }
  }
}

impl<R: Runtime> PromisedStream for TlsStream<R> {}
