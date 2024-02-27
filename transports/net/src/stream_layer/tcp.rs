use std::{
  io,
  marker::PhantomData,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
  time::Instant,
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
        read_deadline: None,
        write_deadline: None,
      })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr)
      .await
      .map(TcpListener)
  }

  async fn cache_stream(&self, _addr: SocketAddr, _stream: Self::Stream) {
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
          read_deadline: None,
          write_deadline: None,
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
  read_deadline: Option<Instant>,
  write_deadline: Option<Instant>,
}

impl<R: Runtime> AsyncRead for TcpStream<R> {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    if let Some(timeout) = self.read_deadline {
      let fut = R::timeout_at(timeout, self.stream.read(buf));
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
    if let Some(timeout) = self.write_deadline {
      let fut = R::timeout_at(timeout, self.stream.write(buf));
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
  fn set_read_deadline(&mut self, deadline: Option<Instant>) {
    self.read_deadline = deadline;
  }

  fn read_deadline(&self) -> Option<Instant> {
    self.read_deadline
  }
}

impl<R: Runtime> TimeoutableWriteStream for TcpStream<R> {
  fn set_write_deadline(&mut self, deadline: Option<Instant>) {
    self.write_deadline = deadline;
  }

  fn write_deadline(&self) -> Option<Instant> {
    self.write_deadline
  }
}

impl<R: Runtime> PromisedStream for TcpStream<R> {}
