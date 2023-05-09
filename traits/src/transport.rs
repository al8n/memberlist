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

  #[async_trait::async_trait]
  pub trait Connection: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn set_timeout(&mut self, timeout: Option<Duration>);

    fn timeout(&self) -> Option<Duration>;

    async fn write(&mut self, b: &[u8]) -> Result<usize, Self::Error>;

    async fn write_all(&mut self, b: &[u8]) -> Result<(), Self::Error>;

    async fn read(&mut self, b: &mut [u8]) -> Result<usize, Self::Error>;

    async fn read_exact(&mut self, b: &mut [u8]) -> Result<(), Self::Error>;
  }

  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  #[async_trait::async_trait]
  pub trait Transport: Send + Sync + 'static {
    type Error: std::error::Error
      + From<<Self::Connection as Connection>::Error>
      + Send
      + Sync
      + 'static;
    type Connection: Connection;
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

    fn packet_rx(&self) -> &Receiver<Packet>;

    /// Returns a receiver that can be read to handle incoming stream
    /// connections from other peers. How this is set up for listening is
    /// left as an exercise for the concrete transport implementations.
    fn stream_rx(&self) -> &Receiver<Self::Connection>;

    /// Called when memberlist is shutting down; this gives the
    /// transport a chance to clean up any listeners.
    async fn shutdown(self) -> Result<(), Self::Error>;
  }

  #[async_trait::async_trait]
  pub trait NodeAwareTransport: Transport {
    async fn write_to_address(&self, b: &[u8], addr: Address) -> Result<Instant, Self::Error>;

    async fn dial_address_timeout(
      &self,
      addr: Address,
      timeout: Duration,
    ) -> Result<Self::Connection, Self::Error>;
  }

  #[cfg(feature = "async")]
  macro_rules! async_bail {
    ($this:ident.$fn_name: ident ($src: ident) $(.map($map: expr))?) => {
      match $this.timeout {
        Some(t) => match timeout(t, $this.conn.$fn_name($src)).await {
          Ok(r) => r$(. map($map))?,
          Err(e) => Err(Error::new(ErrorKind::TimedOut, e)),
        },
        None => $this.conn.$fn_name($src).await$(. map($map))?,
      }
    };
  }

  #[cfg(feature = "smol")]
  pub use _smol::SmolConnection;

  #[cfg(feature = "smol")]
  mod _smol {
    use std::{future::Future, time::Duration};

    use futures_util::future::FutureExt;
    use futures_util::select;
    use smol::{
      io::{AsyncReadExt, AsyncWriteExt, Error, ErrorKind},
      net::TcpStream,
      Timer,
    };

    #[inline]
    async fn timeout<F, T>(duration: Duration, f: F) -> Result<T, Error>
    where
      F: Future<Output = T>,
    {
      select! {
        result = f.fuse() => {
          Ok(result)
        }
        _ = Timer::after(duration).fuse() => {
          Err(Error::new(ErrorKind::TimedOut, "deadline has elapsed"))
        }
      }
    }
    #[derive(Debug)]
    pub struct SmolConnection {
      timeout: Option<Duration>,
      conn: TcpStream,
    }

    impl SmolConnection {
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

    impl From<TcpStream> for SmolConnection {
      fn from(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }
    }

    #[async_trait::async_trait]
    impl super::Connection for SmolConnection {
      type Error = Error;

      fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
      }

      fn timeout(&self) -> Option<Duration> {
        self.timeout
      }

      async fn write(&mut self, src: &[u8]) -> Result<usize, Self::Error> {
        async_bail!(self.write(src))
      }

      async fn write_all(&mut self, src: &[u8]) -> Result<(), Self::Error> {
        async_bail!(self.write_all(src))
      }

      async fn read(&mut self, src: &mut [u8]) -> Result<usize, Self::Error> {
        async_bail!(self.read(src))
      }

      async fn read_exact(&mut self, b: &mut [u8]) -> Result<(), Self::Error> {
        async_bail!(self.read_exact(b))
      }
    }
  }

  #[cfg(feature = "async-std")]
  pub use _async_std::AsyncConnection;

  #[cfg(feature = "async-std")]
  mod _async_std {
    use std::time::Duration;

    use async_std::{
      future::timeout,
      io::{Error, ErrorKind, ReadExt, WriteExt},
      net::TcpStream,
    };

    #[derive(Debug)]
    pub struct AsyncConnection {
      timeout: Option<Duration>,
      conn: TcpStream,
    }

    impl AsyncConnection {
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

    impl From<TcpStream> for AsyncConnection {
      fn from(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }
    }

    #[async_trait::async_trait]
    impl super::Connection for AsyncConnection {
      type Error = Error;

      fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
      }

      fn timeout(&self) -> Option<Duration> {
        self.timeout
      }

      async fn write(&mut self, src: &[u8]) -> Result<usize, Self::Error> {
        async_bail!(self.write(src))
      }

      async fn write_all(&mut self, src: &[u8]) -> Result<(), Self::Error> {
        async_bail!(self.write_all(src))
      }

      async fn read(&mut self, src: &mut [u8]) -> Result<usize, Self::Error> {
        async_bail!(self.read(src))
      }

      async fn read_exact(&mut self, b: &mut [u8]) -> Result<(), Self::Error> {
        async_bail!(self.read_exact(b))
      }
    }
  }

  #[cfg(feature = "tokio")]
  pub use _tokio::TokioConnection;

  #[cfg(feature = "tokio")]
  mod _tokio {
    use std::time::Duration;

    use tokio::{
      io::{AsyncReadExt, AsyncWriteExt, Error, ErrorKind},
      net::TcpStream,
      time::timeout,
    };

    #[derive(Debug)]
    pub struct TokioConnection {
      timeout: Option<Duration>,
      conn: TcpStream,
    }

    impl TokioConnection {
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

    impl From<TcpStream> for TokioConnection {
      fn from(conn: TcpStream) -> Self {
        Self {
          timeout: None,
          conn,
        }
      }
    }

    #[async_trait::async_trait]
    impl super::Connection for TokioConnection {
      type Error = Error;

      fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
      }

      fn timeout(&self) -> Option<Duration> {
        self.timeout
      }

      async fn write(&mut self, src: &[u8]) -> Result<usize, Self::Error> {
        async_bail!(self.write(src))
      }

      async fn write_all(&mut self, src: &[u8]) -> Result<(), Self::Error> {
        async_bail!(self.write_all(src))
      }

      async fn read(&mut self, src: &mut [u8]) -> Result<usize, Self::Error> {
        async_bail!(self.read(src))
      }

      async fn read_exact(&mut self, b: &mut [u8]) -> Result<(), Self::Error> {
        async_bail!(self.read_exact(b).map(|_| ()))
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

  pub trait Connection: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Self::Error>;

    fn timeout(&self) -> Result<Option<Duration>, Self::Error>;

    fn write(&mut self, src: &[u8]) -> Result<usize, Self::Error>;

    fn write_all(&mut self, src: &[u8]) -> Result<(), Self::Error>;

    fn read(&mut self, src: &mut [u8]) -> Result<usize, Self::Error>;

    fn read_exact(&mut self, b: &mut [u8]) -> Result<(), Self::Error>;
  }

  impl Connection for std::net::TcpStream {
    type Error = std::io::Error;

    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), Self::Error> {
      self
        .set_write_timeout(timeout)
        .and_then(|_| self.set_read_timeout(timeout))
    }

    fn timeout(&self) -> Result<Option<Duration>, Self::Error> {
      self.write_timeout()
    }

    fn read(&mut self, src: &mut [u8]) -> Result<usize, Self::Error> {
      std::io::Read::read(self, src)
    }

    fn read_exact(&mut self, b: &mut [u8]) -> Result<(), Self::Error> {
      std::io::Read::read_exact(self, b)
    }

    fn write(&mut self, src: &[u8]) -> Result<usize, Self::Error> {
      std::io::Write::write(self, src)
    }

    fn write_all(&mut self, src: &[u8]) -> Result<(), Self::Error> {
      std::io::Write::write_all(self, src)
    }
  }

  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  pub trait Transport {
    type Error: std::error::Error
      + From<<Self::Connection as Connection>::Error>
      + Send
      + Sync
      + 'static;
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
  }

  pub trait NodeAwareTransport: Transport {
    fn write_to_address(&self, b: &[u8], addr: Address) -> Result<Instant, Self::Error>;

    fn dial_address_timeout(
      &self,
      addr: Address,
      timeout: Duration,
    ) -> Result<Self::Connection, Self::Error>;
  }
}
