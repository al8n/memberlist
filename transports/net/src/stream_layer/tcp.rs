use std::{
  io,
  marker::PhantomData,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
  time::Duration,
};

use agnostic::{
  net::{Net, TcpListener as _},
  Runtime, Timeoutable,
};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};

use super::{Listener, PromisedStream, StreamLayer};

/// Tcp stream layer.
#[repr(transparent)]
pub struct Tcp<R>(PhantomData<R>);

impl<R> Clone for Tcp<R> {
  #[inline(always)]
  fn clone(&self) -> Self {
    *self
  }
}

impl<R> Copy for Tcp<R> {}

impl<R> Default for Tcp<R> {
  #[inline(always)]
  fn default() -> Self {
    Self(PhantomData)
  }
}

impl<R> Tcp<R> {
  /// Creates a new instance.
  #[inline(always)]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<R: Runtime> StreamLayer for Tcp<R> {
  type Listener = TcpListener<R>;
  type Stream = TcpStream<R>;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect(addr)
      .await
      .map(|stream| TcpStream {
        stream,
        read_timeout: None,
        write_timeout: None,
      })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr)
      .await
      .map(TcpListener)
  }

  fn cache_stream(&self, _addr: SocketAddr, _stream: Self::Stream) {
    // Do nothing
  }

  fn is_secure() -> bool {
    false
  }
}

/// [`Listener`] of the TCP stream layer
#[repr(transparent)]
pub struct TcpListener<R: Runtime>(<R::Net as Net>::TcpListener);

impl<R: Runtime> Listener for TcpListener<R> {
  type Stream = TcpStream<R>;

  async fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    self.0.accept().await.map(|(conn, addr)| {
      (
        TcpStream {
          stream: conn,
          read_timeout: None,
          write_timeout: None,
        },
        addr,
      )
    })
  }

  fn local_addr(&self) -> io::Result<SocketAddr> {
    self.0.local_addr()
  }
}

/// [`PromisedStream`] of the TCP stream layer
#[pin_project::pin_project]
pub struct TcpStream<R: Runtime> {
  #[pin]
  stream: <R::Net as Net>::TcpStream,
  read_timeout: Option<Duration>,
  write_timeout: Option<Duration>,
}

impl<R: Runtime> AsyncRead for TcpStream<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    if let Some(timeout) = self.read_timeout {
      let fut = R::timeout(timeout, self.stream.read(buf));
      futures::pin_mut!(fut);
      return match fut.poll_elapsed(cx) {
        Poll::Ready(res) => match res {
          Ok(res) => Poll::Ready(res),
          Err(err) => Poll::Ready(Err(err.into())),
        },
        Poll::Pending => Poll::Pending,
      };
    }
    self.project().stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for TcpStream<R> {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    if let Some(timeout) = self.write_timeout {
      let fut = R::timeout(timeout, self.stream.write(buf));
      futures::pin_mut!(fut);
      return match fut.poll_elapsed(cx) {
        Poll::Ready(res) => match res {
          Ok(res) => Poll::Ready(res),
          Err(err) => Poll::Ready(Err(err.into())),
        },
        Poll::Pending => Poll::Pending,
      };
    }
    self.project().stream.poll_write(cx, buf)
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().stream.poll_flush(cx)
  }

  fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    self.project().stream.poll_close(cx)
  }
}

impl<R: Runtime> TimeoutableReadStream for TcpStream<R> {
  fn set_read_timeout(&mut self, timeout: Option<Duration>) {
    self.read_timeout = timeout;
  }

  fn read_timeout(&self) -> Option<Duration> {
    self.read_timeout
  }
}

impl<R: Runtime> TimeoutableWriteStream for TcpStream<R> {
  fn set_write_timeout(&mut self, timeout: Option<Duration>) {
    self.write_timeout = timeout;
  }

  fn write_timeout(&self) -> Option<Duration> {
    self.write_timeout
  }
}

impl<R: Runtime> PromisedStream for TcpStream<R> {}
