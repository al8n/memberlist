use std::{
  io,
  net::SocketAddr,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::Instant,
};

use agnostic::{
  net::{Net, TcpListener, TcpStream},
  Runtime,
};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
pub use futures_rustls::{
  client, pki_types::ServerName, rustls, server, TlsAcceptor, TlsConnector,
};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};
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
    let local_addr = conn.local_addr()?;
    let stream = self.connector.connect(self.domain.clone(), conn).await?;
    Ok(TlsStream {
      stream: TlsStreamKind::Client {
        stream,
        read_deadline: None,
        write_deadline: None,
      },
      peer_addr: addr,
      local_addr,
    })
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

  async fn cache_stream(&self, _addr: SocketAddr, mut stream: Self::Stream) {
    // TODO(al8n): It seems that futures-rustls has a bug
    // client side dial remote successfully and finish send bytes successfully,
    // and then drop the connection immediately.
    // But, server side can't accept the connection, and reports `BrokenPipe` error.
    //
    // Therefore, if we remove the below code, the send unit tests in
    // - transports/net/tests/main/tokio/send.rs
    // - transports/net/tests/main/async_std/send.rs
    // - transport/net/tests/main/smol/send.rs
    //
    // I am also not sure if this bug can happen in real environment,
    // so just keep it here and not feature-gate it by `cfg(test)`.
    //
    // To reproduce the bug, you can comment out the below code and run the send unit tests
    // on a docker container or a virtual machine with few CPU cores.
    R::spawn_detach(async move {
      let _ = stream.flush().await;
      let _ = stream.close().await;
      R::sleep(std::time::Duration::from_millis(100)).await;
    });
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
    Ok((
      TlsStream {
        stream: TlsStreamKind::Server {
          stream,
          read_deadline: None,
          write_deadline: None,
        },
        peer_addr: addr,
        local_addr: self.local_addr,
      },
      addr,
    ))
  }

  fn local_addr(&self) -> std::net::SocketAddr {
    self.local_addr
  }
}

#[pin_project::pin_project]
enum TlsStreamKind<R: Runtime> {
  Client {
    #[pin]
    stream: client::TlsStream<<R::Net as Net>::TcpStream>,
    read_deadline: Option<Instant>,
    write_deadline: Option<Instant>,
  },
  Server {
    #[pin]
    stream: server::TlsStream<<R::Net as Net>::TcpStream>,
    read_deadline: Option<Instant>,
    write_deadline: Option<Instant>,
  },
}

impl<R: Runtime> AsyncRead for TlsStreamKind<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client { stream, .. } => Pin::new(stream).poll_read(cx, buf),
      Self::Server { stream, .. } => Pin::new(stream).poll_read(cx, buf),
    }
  }
}

impl<R: Runtime> AsyncWrite for TlsStreamKind<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client { stream, .. } => Pin::new(stream).poll_write(cx, buf),
      Self::Server { stream, .. } => Pin::new(stream).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client { stream, .. } => Pin::new(stream).poll_flush(cx),
      Self::Server { stream, .. } => Pin::new(stream).poll_flush(cx),
    }
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Self::Client { stream, .. } => Pin::new(stream).poll_close(cx),
      Self::Server { stream, .. } => Pin::new(stream).poll_close(cx),
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

impl<R: Runtime> TlsStream<R> {
  /// Queues a close_notify warning alert to be sent in the next `write_tls` call. This informs the peer that the connection is being closed.
  pub fn send_close_notify(&mut self) {
    match &mut self.stream {
      TlsStreamKind::Client { stream, .. } => {
        let (_, state) = stream.get_mut();
        state.send_close_notify();
      }
      TlsStreamKind::Server { stream, .. } => {
        let (_, state) = stream.get_mut();
        state.send_close_notify();
      }
    }
  }
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

impl<R: Runtime> TimeoutableReadStream for TlsStream<R> {
  fn set_read_deadline(&mut self, deadline: Option<Instant>) {
    match self {
      Self {
        stream: TlsStreamKind::Client { read_deadline, .. },
        ..
      } => *read_deadline = deadline,
      Self {
        stream: TlsStreamKind::Server { read_deadline, .. },
        ..
      } => *read_deadline = deadline,
    }
  }

  fn read_deadline(&self) -> Option<Instant> {
    match self {
      Self {
        stream: TlsStreamKind::Client { read_deadline, .. },
        ..
      } => *read_deadline,
      Self {
        stream: TlsStreamKind::Server { read_deadline, .. },
        ..
      } => *read_deadline,
    }
  }
}

impl<R: Runtime> TimeoutableWriteStream for TlsStream<R> {
  fn set_write_deadline(&mut self, deadline: Option<Instant>) {
    match self {
      Self {
        stream: TlsStreamKind::Client { write_deadline, .. },
        ..
      } => *write_deadline = deadline,
      Self {
        stream: TlsStreamKind::Server { write_deadline, .. },
        ..
      } => *write_deadline = deadline,
    }
  }

  fn write_deadline(&self) -> Option<Instant> {
    match self {
      Self {
        stream: TlsStreamKind::Client { write_deadline, .. },
        ..
      } => *write_deadline,
      Self {
        stream: TlsStreamKind::Server { write_deadline, .. },
        ..
      } => *write_deadline,
    }
  }
}

impl<R: Runtime> PromisedStream for TlsStream<R> {
  #[inline]
  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  #[inline]
  fn peer_addr(&self) -> SocketAddr {
    self.peer_addr
  }
}
