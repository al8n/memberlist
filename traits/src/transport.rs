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
  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  #[async_trait::async_trait]
  pub trait Transport {
    type Error: std::error::Error + Send + Sync + 'static;
    type Conn;
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
    ) -> Result<Self::Conn, Self::Error>;

    fn packet_rx(&self) -> &Receiver<Packet>;

    /// Returns a receiver that can be read to handle incoming stream
    /// connections from other peers. How this is set up for listening is
    /// left as an exercise for the concrete transport implementations.
    fn stream_rx(&self) -> &Receiver<Self::Conn>;

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
    ) -> Result<Self::Conn, Self::Error>;
  }
}

#[cfg(not(feature = "async"))]
pub use sync::*;

#[cfg(not(feature = "async"))]
mod sync {
  use crossbeam_channel::Receiver;

  use super::*;
  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  pub trait Transport {
    type Error: std::error::Error + Send + Sync + 'static;
    type Conn;
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
    fn dial_timeout(&self, addr: SocketAddr, timeout: Duration) -> Result<Self::Conn, Self::Error>;

    fn packet_rx(&self) -> &Receiver<Packet>;

    /// Returns a receiver that can be read to handle incoming stream
    /// connections from other peers. How this is set up for listening is
    /// left as an exercise for the concrete transport implementations.
    fn stream_rx(&self) -> &Receiver<Self::Conn>;

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
    ) -> Result<Self::Conn, Self::Error>;
  }
}
