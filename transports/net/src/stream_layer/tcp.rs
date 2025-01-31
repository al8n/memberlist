use std::{
  io,
  marker::PhantomData,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
};

use agnostic::{
  net::{Net, TcpListener as _, TcpStream as _},
  Runtime,
};
use futures::{AsyncRead, AsyncWrite};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};

use super::{Listener, PromisedStream, StreamLayer};

/// Tcp stream layer.
#[repr(transparent)]
pub struct Tcp<R>(PhantomData<R>);

impl<R> Clone for Tcp<R> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}

impl<R> Copy for Tcp<R> {}

impl<R> Default for Tcp<R> {
  #[inline]
  fn default() -> Self {
    Self(PhantomData)
  }
}

impl<R> Tcp<R> {
  /// Creates a new instance.
  #[inline]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<R: Runtime> StreamLayer for Tcp<R> {
  type Runtime = R;
  type Listener = TcpListener<R>;
  type Stream = TcpStream<R>;
  type Options = ();

  #[inline]
  async fn new(_: Self::Options) -> io::Result<Self> {
    Ok(Self::default())
  }

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Stream> {
    <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::connect(addr)
      .await
      .and_then(|stream| {
        Ok(TcpStream {
          local_addr: stream.local_addr()?,
          peer_addr: addr,
          stream,
          read_deadline: None,
          write_deadline: None,
        })
      })
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    <<R::Net as Net>::TcpListener as agnostic::net::TcpListener>::bind(addr)
      .await
      .and_then(|ln| {
        ln.local_addr()
          .map(|local_addr| TcpListener { ln, local_addr })
      })
  }

  async fn cache_stream(&self, _addr: SocketAddr, _stream: Self::Stream) {
    // Do nothing
  }

  fn is_secure() -> bool {
    false
  }
}

/// [`Listener`] of the TCP stream layer
pub struct TcpListener<R: Runtime> {
  ln: <R::Net as Net>::TcpListener,
  local_addr: SocketAddr,
}

impl<R: Runtime> Listener for TcpListener<R> {
  type Stream = TcpStream<R>;

  async fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    self.ln.accept().await.map(|(conn, addr)| {
      (
        TcpStream {
          stream: conn,
          read_deadline: None,
          write_deadline: None,
          local_addr: self.local_addr,
          peer_addr: addr,
        },
        addr,
      )
    })
  }

  async fn shutdown(&self) -> io::Result<()> {
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`PromisedStream`] of the TCP stream layer
#[pin_project::pin_project]
pub struct TcpStream<R: Runtime> {
  #[pin]
  stream: <R::Net as Net>::TcpStream,
  read_deadline: Option<R::Instant>,
  write_deadline: Option<R::Instant>,
  local_addr: SocketAddr,
  peer_addr: SocketAddr,
}

impl<R: Runtime> AsyncRead for TcpStream<R> {
  fn poll_read(
    self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<io::Result<usize>> {
    self.project().stream.poll_read(cx, buf)
  }
}

impl<R: Runtime> AsyncWrite for TcpStream<R> {
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

impl<R: Runtime> TimeoutableReadStream for TcpStream<R> {
  type Instant = R::Instant;

  fn set_read_deadline(&mut self, deadline: Option<Self::Instant>) {
    self.read_deadline = deadline;
  }

  fn read_deadline(&self) -> Option<Self::Instant> {
    self.read_deadline
  }
}

impl<R: Runtime> TimeoutableWriteStream for TcpStream<R> {
  type Instant = R::Instant;

  fn set_write_deadline(&mut self, deadline: Option<Self::Instant>) {
    self.write_deadline = deadline;
  }

  fn write_deadline(&self) -> Option<Self::Instant> {
    self.write_deadline
  }
}

impl<R: Runtime> PromisedStream for TcpStream<R> {
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
