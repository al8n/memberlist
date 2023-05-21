use std::{
  net::SocketAddr,
  time::{Duration, Instant},
};

use showbiz_types::{Address, Packet};

#[cfg(feature = "async")]
pub use r#async::*;

#[cfg(feature = "async")]
mod r#async {
  use super::*;
  use async_channel::Receiver;

  /// Compressor is used to compress and decompress data from a transport connection.
  #[async_trait::async_trait]
  pub trait Compressor {
    /// The error type returned by the compressor.
    type Error: std::error::Error;

    /// Compress data from a slice, returning compressed data.
    fn compress(&self, buf: &[u8]) -> Vec<u8>;

    /// Compress data from a slice, writing the compressed data to the given writer.
    async fn compress_to_writer<W: futures_util::io::AsyncWrite>(
      &self,
      buf: &[u8],
      writer: W,
    ) -> Result<(), Self::Error>;

    /// Decompress data from a slice, returning uncompressed data.
    fn decompress(src: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Decompress data from a reader, returning the bytes readed and the uncompressed data.
    async fn decompress_from_reader<R: futures_util::io::AsyncRead>(
      reader: R,
    ) -> Result<(usize, Vec<u8>), Self::Error>;
  }

  #[async_trait::async_trait]
  pub trait Connection:
    futures_util::io::AsyncRead + futures_util::io::AsyncWrite + Send + Sync + 'static
  {
    fn set_timeout(&mut self, timeout: Option<Duration>);

    fn timeout(&self) -> Option<Duration>;

    fn remote_address(&self) -> std::io::Result<SocketAddr>;
  }

  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  #[async_trait::async_trait]
  pub trait Transport: Send + Sync + 'static {
    type Error: std::error::Error + From<std::io::Error> + Send + Sync + 'static;
    type Connection: Connection + std::marker::Unpin;
    type Options;

    /// Creates a new transport instance with the given options
    async fn new(opts: Self::Options) -> Result<Self, Self::Error>
    where
      Self: Sized;

    /// Given the user's configured values (which
    /// might be empty) and returns the desired IP and port to advertise to
    /// the rest of the cluster.
    fn final_advertise_addr(&self, addr: Option<SocketAddr>) -> Result<SocketAddr, Self::Error>;

    /// A packet-oriented interface that fires off the given
    /// payload to the given address in a connectionless fashion. This should
    /// return a time stamp that's as close as possible to when the packet
    /// was transmitted to help make accurate RTT measurements during probes.
    ///
    /// This is similar to net.PacketConn, though we didn't want to expose
    /// that full set of required methods to keep assumptions about the
    /// underlying plumbing to a minimum. We also treat the address here as a
    /// string, similar to Dial, so it's network neutral, so this usually is
    /// in the form of "host:port".
    async fn write_to(&self, b: &[u8], addr: SocketAddr) -> Result<Instant, Self::Error>;

    /// Used to create a connection that allows us to perform
    /// two-way communication with a peer. This is generally more expensive
    /// than packet connections so is used for more infrequent operations
    /// such as anti-entropy or fallback probes if the packet-oriented probe
    /// failed.
    async fn dial_timeout(
      &self,
      addr: SocketAddr,
      timeout: Duration,
    ) -> Result<Self::Connection, Self::Error>;

    fn packet(&self) -> &Receiver<Packet>;

    /// Returns a receiver that can be read to handle incoming stream
    /// connections from other peers. How this is set up for listening is
    /// left as an exercise for the concrete transport implementations.
    fn stream(&self) -> &Receiver<Self::Connection>;

    /// Called when memberlist is shutting down; this gives the
    /// transport a chance to clean up any listeners.
    async fn shutdown(self) -> Result<(), Self::Error>;

    async fn write_to_address(&self, b: &[u8], addr: &Address) -> Result<Instant, Self::Error>;

    async fn dial_address_timeout(
      &self,
      addr: &Address,
      timeout: Duration,
    ) -> Result<Self::Connection, Self::Error>;
  }

  #[cfg(feature = "async")]
  macro_rules! bail {
    ($this:ident.$fn: ident($cx:ident, $buf:ident, $timer: expr)) => {{
      let timeout = $this.timeout;
      let conn_pin = Pin::new(&mut $this.conn);

      if let Some(timeout) = timeout {
        let timer = $timer(timeout);
        futures_util::pin_mut!(timer);

        // bias towards the read operation
        match conn_pin.$fn($cx, $buf) {
          Poll::Ready(result) => Poll::Ready(result),
          Poll::Pending => match timer.poll($cx) {
            Poll::Ready(_) => {
              Poll::Ready(Err(Error::new(ErrorKind::TimedOut, "deadline has elapsed")))
            }
            Poll::Pending => Poll::Pending,
          },
        }
      } else {
        conn_pin.$fn($cx, $buf)
      }
    }};
  }

  #[cfg(feature = "smol")]
  pub mod smol {
    use ::smol::{
      io::{Error, ErrorKind},
      net::TcpStream,
      Timer,
    };
    use futures_util::future::{Fuse, FutureExt};
    use futures_util::io::{AsyncRead, AsyncWrite};
    use std::{
      future::Future,
      net::SocketAddr,
      pin::Pin,
      task::{Context, Poll},
      time::Duration,
    };

    #[derive(Debug)]
    pub struct TransportConnection {
      timeout: Option<Duration>,
      conn: TcpStream,
    }

    impl TransportConnection {
      #[inline]
      pub const fn new(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }

      #[inline]
      pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
      }
    }

    impl From<TcpStream> for TransportConnection {
      fn from(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }
    }

    fn timer(timeout: Duration) -> Fuse<Timer> {
      Timer::after(timeout).fuse()
    }

    impl AsyncRead for TransportConnection {
      fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
      ) -> Poll<std::io::Result<usize>> {
        bail!(self.poll_read(cx, buf, timer))
      }
    }

    impl AsyncWrite for TransportConnection {
      fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
      ) -> Poll<std::io::Result<usize>> {
        bail!(self.poll_write(cx, buf, timer))
      }

      fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.conn).poll_flush(cx)
      }

      fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.conn).poll_close(cx)
      }
    }

    #[async_trait::async_trait]
    impl super::Connection for TransportConnection {
      fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
      }

      fn timeout(&self) -> Option<Duration> {
        self.timeout
      }

      fn remote_address(&self) -> std::io::Result<SocketAddr> {
        self.conn.peer_addr()
      }
    }
  }

  #[cfg(feature = "async-std")]
  pub mod async_std {
    use std::{
      future::Future,
      net::SocketAddr,
      pin::Pin,
      task::{Context, Poll},
      time::Duration,
    };

    use ::async_std::{
      io::{Error, ErrorKind},
      net::TcpStream,
    };
    #[cfg(not(target_arch = "wasm32"))]
    use async_io::Timer;

    use futures_util::io::{AsyncRead, AsyncWrite};
    use futures_util::{future::Fuse, FutureExt};
    #[cfg(target_arch = "wasm32")]
    use gloo_timers::future::sleep as timer;

    #[derive(Debug)]
    pub struct TransportConnection {
      timeout: Option<Duration>,
      conn: TcpStream,
    }

    impl TransportConnection {
      #[inline]
      pub const fn new(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }

      #[inline]
      pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
      }
    }

    impl From<TcpStream> for TransportConnection {
      fn from(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn timer(timeout: Duration) -> Fuse<Timer> {
      Timer::after(timeout).fuse()
    }

    impl AsyncRead for TransportConnection {
      fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
      ) -> Poll<std::io::Result<usize>> {
        bail!(self.poll_read(cx, buf, timer))
      }
    }

    impl AsyncWrite for TransportConnection {
      fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
      ) -> Poll<std::io::Result<usize>> {
        bail!(self.poll_write(cx, buf, timer))
      }

      fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.conn).poll_flush(cx)
      }

      fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.conn).poll_close(cx)
      }
    }

    #[async_trait::async_trait]
    impl super::Connection for TransportConnection {
      fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
      }

      fn timeout(&self) -> Option<Duration> {
        self.timeout
      }

      fn remote_address(&self) -> std::io::Result<SocketAddr> {
        self.conn.peer_addr()
      }
    }
  }

  #[cfg(feature = "tokio")]
  pub mod tokio {
    use std::{
      future::Future,
      net::SocketAddr,
      pin::Pin,
      task::{Context, Poll},
      time::Duration,
    };

    use tokio::io::{Error, ErrorKind, ReadBuf};
    #[cfg(not(target_arch = "wasm32"))]
    use tokio::net::TcpStream;

    #[cfg(target_arch = "wasm32")]
    use wasi_tokio::net::TcpStream;

    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    #[derive(Debug)]
    pub struct TransportConnection {
      timeout: Option<Duration>,
      conn: TcpStream,
    }

    impl TransportConnection {
      #[inline]
      pub const fn new(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }

      #[inline]
      pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
      }
    }

    impl From<TcpStream> for TransportConnection {
      fn from(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }
    }

    impl futures_util::io::AsyncRead for TransportConnection {
      fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
      ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut (&mut self.conn).compat()).poll_read(cx, buf)
      }
    }

    impl futures_util::io::AsyncWrite for TransportConnection {
      fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
      ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut (&mut self.conn).compat_write()).poll_write(cx, buf)
      }

      fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut (&mut self.conn).compat_write()).poll_flush(cx)
      }

      fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut (&mut self.conn).compat_write()).poll_close(cx)
      }
    }

    impl tokio::io::AsyncRead for TransportConnection {
      fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
      ) -> Poll<std::io::Result<()>> {
        bail!(self.poll_read(cx, buf, tokio::time::sleep))
      }
    }

    impl tokio::io::AsyncWrite for TransportConnection {
      fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
      ) -> Poll<Result<usize, std::io::Error>> {
        bail!(self.poll_write(cx, buf, tokio::time::sleep))
      }

      fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
      ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.conn).poll_flush(cx)
      }

      fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
      ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.conn).poll_shutdown(cx)
      }
    }

    #[async_trait::async_trait]
    impl super::Connection for TransportConnection {
      fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
      }

      fn timeout(&self) -> Option<Duration> {
        self.timeout
      }

      fn remote_address(&self) -> std::io::Result<SocketAddr> {
        self.conn.peer_addr()
      }
    }
  }
}

#[cfg(not(feature = "async"))]
pub use sync::*;

#[cfg(not(feature = "async"))]
mod sync {
  use crossbeam_channel::Receiver;

  use super::*;

  /// Compressor is used to compress and decompress data from a transport connection.
  pub trait Compressor {
    /// The error type returned by the compressor.
    type Error: std::error::Error;

    /// Compress data from a slice, returning compressed data.
    fn compress(&self, buf: &[u8]) -> Vec<u8>;

    /// Compress data from a slice, writing the compressed data to the given writer.
    fn compress_to_writer<W: std::io::Write>(
      &self,
      buf: &[u8],
      writer: W,
    ) -> Result<(), Self::Error>;

    /// Decompress data from a slice, returning uncompressed data.
    fn decompress(src: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Decompress data from a reader, returning the bytes readed and the uncompressed data.
    fn decompress_from_reader<R: std::io::Read>(reader: R)
      -> Result<(usize, Vec<u8>), Self::Error>;
  }

  pub trait Connection: std::io::Read + std::io::Write + Send + Sync + 'static {
    fn set_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()>;

    fn timeout(&self) -> std::io::Result<Option<Duration>>;

    fn remote_address(&self) -> std::io::Result<SocketAddr>;
  }

  impl Connection for std::net::TcpStream {
    fn set_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
      self
        .set_write_timeout(timeout)
        .and_then(|_| self.set_read_timeout(timeout))
    }

    fn timeout(&self) -> std::io::Result<Option<Duration>> {
      self.write_timeout()
    }

    fn remote_address(&self) -> std::io::Result<SocketAddr> {
      self.peer_addr()
    }
  }

  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  pub trait Transport {
    type Error: std::error::Error + Send + Sync + 'static;
    type Connection: Connection;
    type Options;

    /// Creates a new transport instance with the given options
    fn new(opts: Self::Options) -> Result<Self, Self::Error>
    where
      Self: Sized;

    /// Given the user's configured values (which
    /// might be empty) and returns the desired IP and port to advertise to
    /// the rest of the cluster.
    fn final_advertise_addr(&self, addr: Option<SocketAddr>) -> Result<SocketAddr, Self::Error>;

    /// A packet-oriented interface that fires off the given
    /// payload to the given address in a connectionless fashion. This should
    /// return a time stamp that's as close as possible to when the packet
    /// was transmitted to help make accurate RTT measurements during probes.
    ///
    /// This is similar to net.PacketConn, though we didn't want to expose
    /// that full set of required methods to keep assumptions about the
    /// underlying plumbing to a minimum. We also treat the address here as a
    /// string, similar to Dial, so it's network neutral, so this usually is
    /// in the form of "host:port".
    fn write_to(&self, b: &[u8], addr: SocketAddr) -> Result<Instant, Self::Error>;

    /// Used to create a connection that allows us to perform
    /// two-way communication with a peer. This is generally more expensive
    /// than packet connections so is used for more infrequent operations
    /// such as anti-entropy or fallback probes if the packet-oriented probe
    /// failed.
    fn dial_timeout(
      &self,
      addr: SocketAddr,
      timeout: Duration,
    ) -> Result<Self::Connection, Self::Error>;

    fn packet_rx(&self) -> &Receiver<Packet>;

    /// Returns a receiver that can be read to handle incoming stream
    /// connections from other peers. How this is set up for listening is
    /// left as an exercise for the concrete transport implementations.
    fn stream_rx(&self) -> &Receiver<Self::Connection>;

    /// Called when memberlist is shutting down; this gives the
    /// transport a chance to clean up any listeners.
    fn shutdown(self) -> Result<(), Self::Error>;

    fn write_to_address(&self, b: &[u8], addr: &Address) -> Result<Instant, Self::Error>;

    fn dial_address_timeout(
      &self,
      addr: &Address,
      timeout: Duration,
    ) -> Result<Self::Connection, Self::Error>;
  }
}
