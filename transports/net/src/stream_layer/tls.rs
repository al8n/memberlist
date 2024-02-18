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
  Runtime, Timeoutable,
};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
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
    let stream = self.connector.connect(self.domain.clone(), conn).await?;
    Ok(TlsStream {
      stream: TlsStreamKind::Client {
        stream,
        read_timeout: None,
        write_timeout: None,
      },
    })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let acceptor = self.acceptor.clone();
    <<R::Net as Net>::TcpListener as TcpListener>::bind(addr)
      .await
      .map(|ln| TlsListener { ln, acceptor })
  }

  async fn cache_stream(&self, _addr: SocketAddr, mut _stream: Self::Stream) {
    let _ = _stream.flush().await;
    let _ = _stream.close().await;
    // _stream.send_close_notify();
    // // TODO(al8n): It seems that futures-rustls has a bug
    // // client side dial remote successfully and finish send bytes successfully,
    // // and then drop the connection immediately.
    // // But, server side can't accept the connection, and reports `BrokenPipe` error.
    // //
    // // Therefore, if we remove the below code, the send unit tests in
    // // - transports/net/tests/main/tokio/send.rs
    // // - transports/net/tests/main/async_std/send.rs
    // // - transport/net/tests/main/smol/send.rs
    // //
    // // I am also not sure if this bug can happen in real environment,
    // // so just keep it here and not feature-gate it by `cfg(test)`.
    // //
    // // To reproduce the bug, you can comment out the below code and run the send unit tests
    // // on a docker container or a virtual machine with few CPU cores.
    // R::spawn_detach(async move {
    //   let _ = _stream.flush().await;
    //   R::sleep(std::time::Duration::from_millis(100)).await;
    //   drop(_stream);
    // });
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
        stream: TlsStreamKind::Server {
          stream,
          read_timeout: None,
          write_timeout: None,
        },
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
  Client {
    #[pin]
    stream: client::TlsStream<<R::Net as Net>::TcpStream>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
  },
  Server {
    #[pin]
    stream: server::TlsStream<<R::Net as Net>::TcpStream>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
  },
}

impl<R: Runtime> AsyncRead for TlsStreamKind<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client {
        stream,
        read_timeout,
        ..
      } => {
        if let Some(timeout) = read_timeout {
          let fut = R::timeout(*timeout, stream.read(buf));
          futures::pin_mut!(fut);
          return match fut.poll_elapsed(cx) {
            Poll::Ready(res) => match res {
              Ok(res) => Poll::Ready(res),
              Err(err) => Poll::Ready(Err(err.into())),
            },
            Poll::Pending => Poll::Pending,
          };
        }
        Pin::new(stream).poll_read(cx, buf)
      }
      Self::Server {
        stream,
        read_timeout,
        ..
      } => {
        if let Some(timeout) = read_timeout {
          let fut = R::timeout(*timeout, stream.read(buf));
          futures::pin_mut!(fut);
          return match fut.poll_elapsed(cx) {
            Poll::Ready(res) => match res {
              Ok(res) => Poll::Ready(res),
              Err(err) => Poll::Ready(Err(err.into())),
            },
            Poll::Pending => Poll::Pending,
          };
        }
        Pin::new(stream).poll_read(cx, buf)
      }
    }
  }
}

impl<R: Runtime> AsyncWrite for TlsStreamKind<R> {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Self::Client {
        stream,
        write_timeout,
        ..
      } => {
        if let Some(timeout) = write_timeout {
          let fut = R::timeout(*timeout, stream.write(buf));
          futures::pin_mut!(fut);
          return match fut.poll_elapsed(cx) {
            Poll::Ready(res) => match res {
              Ok(res) => Poll::Ready(res),
              Err(err) => Poll::Ready(Err(err.into())),
            },
            Poll::Pending => Poll::Pending,
          };
        }

        Pin::new(stream).poll_write(cx, buf)
      }
      Self::Server {
        stream,
        write_timeout,
        ..
      } => {
        if let Some(timeout) = write_timeout {
          let fut = R::timeout(*timeout, stream.write(buf));
          futures::pin_mut!(fut);
          return match fut.poll_elapsed(cx) {
            Poll::Ready(res) => match res {
              Ok(res) => Poll::Ready(res),
              Err(err) => Poll::Ready(Err(err.into())),
            },
            Poll::Pending => Poll::Pending,
          };
        }
        Pin::new(stream).poll_write(cx, buf)
      }
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
      },
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
  fn set_read_timeout(&mut self, timeout: Option<Duration>) {
    match self {
      Self {
        stream: TlsStreamKind::Client { read_timeout, .. },
      } => *read_timeout = timeout,
      Self {
        stream: TlsStreamKind::Server { read_timeout, .. },
      } => *read_timeout = timeout,
    }
  }

  fn read_timeout(&self) -> Option<Duration> {
    match self {
      Self {
        stream: TlsStreamKind::Client { read_timeout, .. },
      } => *read_timeout,
      Self {
        stream: TlsStreamKind::Server { read_timeout, .. },
      } => *read_timeout,
    }
  }
}

impl<R: Runtime> TimeoutableWriteStream for TlsStream<R> {
  fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    match self {
      Self {
        stream: TlsStreamKind::Client { write_timeout, .. },
      } => *write_timeout = timeout,
      Self {
        stream: TlsStreamKind::Server { write_timeout, .. },
      } => *write_timeout = timeout,
    }
  }

  fn write_timeout(&self) -> Option<Duration> {
    match self {
      Self {
        stream: TlsStreamKind::Client { write_timeout, .. },
      } => *write_timeout,
      Self {
        stream: TlsStreamKind::Server { write_timeout, .. },
      } => *write_timeout,
    }
  }
}

impl<R: Runtime> PromisedStream for TlsStream<R> {}
