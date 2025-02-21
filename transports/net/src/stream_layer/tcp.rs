use std::{io, marker::PhantomData, net::SocketAddr};

use agnostic::{
  net::{Net, TcpListener as _, TcpStream as _},
  Runtime,
};
use futures::{AsyncReadExt, AsyncWriteExt};
use peekable::future::AsyncPeekable;

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
        let local_addr = stream.local_addr()?;
        let (reader, writer) = stream.into_split();

        Ok(TcpStream {
          local_addr,
          peer_addr: addr,
          reader: AsyncPeekable::new(reader),
          writer,
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
      let (reader, writer) = conn.into_split();

      (
        TcpStream {
          writer,
          reader: AsyncPeekable::new(reader),
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
  writer: <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::OwnedWriteHalf,
  #[pin]
  reader: AsyncPeekable<<<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::OwnedReadHalf>,
  local_addr: SocketAddr,
  peer_addr: SocketAddr,
}

impl<R: Runtime> memberlist_core::transport::Connection for TcpStream<R> {
  type Reader =
    AsyncPeekable<<<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::OwnedReadHalf>;

  type Writer = <<R::Net as Net>::TcpStream as agnostic::net::TcpStream>::OwnedWriteHalf;

  #[inline]
  fn split(self) -> (Self::Reader, Self::Writer) {
    (self.reader, self.writer)
  }

  async fn close(&mut self) -> std::io::Result<()> {
    AsyncWriteExt::close(&mut self.writer).await
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    AsyncWriteExt::write_all(&mut self.writer, payload).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    AsyncWriteExt::flush(&mut self.writer).await
  }

  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.reader.peek(buf).await
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    AsyncReadExt::read_exact(&mut self.reader, buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    AsyncReadExt::read(&mut self.reader, buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.reader.peek_exact(buf).await
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
