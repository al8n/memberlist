use std::{future::Future, io, net::SocketAddr};

use agnostic::RuntimeLite;

/// `StreamLayer` implementations based on TCP.
pub mod tcp;

/// `StreamLayer` implementations based on [`rustls`](https://crates.io/crates/rustls).
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub mod tls;

/// `StreamLayer` implementations based on [`native-tls`](https://crates.io/crates/native-tls).
#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
pub mod native_tls;

/// Represents a network listener.
///
/// This trait defines the operations required for a network listener that can bind to an address,
/// accept incoming connections, and query its local address.
pub trait Listener: Send + Sync + 'static {
  /// The type of the network stream associated with this listener.
  type Stream: PromisedStream;

  /// Accepts an incoming connection.
  fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

  /// Retrieves the local socket address of the listener.
  fn local_addr(&self) -> SocketAddr;

  /// Shuts down the listener.
  fn shutdown(&self) -> impl Future<Output = io::Result<()>> + Send;
}

/// Represents a network connection.
///
/// This trait encapsulates functionality for a network connection that supports asynchronous
/// read/write operations and can be split into separate read and write halves.
pub trait PromisedStream:
  memberlist_core::transport::Connection + Unpin + Send + Sync + 'static
{
  /// The instant type
  type Instant: agnostic::time::Instant + Send + Sync + 'static;

  /// Returns the address of the local endpoint of the connection.
  fn local_addr(&self) -> SocketAddr;

  /// Returns the address of the remote endpoint of the connection.
  fn peer_addr(&self) -> SocketAddr;
}

/// A trait defining the necessary components for a stream-based network layer.
/// This layer must promise a reliable, ordered, and bi-directional stream of data, e.g. TCP, QUIC, gRPC and etc.
///
/// This trait is used in conjunction with [`NetTransport`](super::NetTransport) to provide
/// an abstraction over the underlying network stream. It specifies the types for listeners
/// and connections that operate on this stream.
pub trait StreamLayer: Send + Sync + 'static {
  /// The runtime for this stream layer
  type Runtime: RuntimeLite;

  /// The listener type for the network stream.
  type Listener: Listener<Stream = Self::Stream>;

  /// The connection type for the network stream.
  type Stream: PromisedStream<Instant = <Self::Runtime as RuntimeLite>::Instant>;

  /// The options type for constructing the stream layer.
  type Options: Send + Sync + 'static;

  /// Creates a new instance of the stream layer with the given options.
  fn new(options: Self::Options) -> impl Future<Output = io::Result<Self>> + Send
  where
    Self: Sized;

  /// Establishes a connection to a specified socket address.
  fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Stream>> + Send;

  /// Binds the listener to a given socket address.
  fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Listener>> + Send;

  /// Indicates whether the connection is secure.
  ///
  /// This method returns `true` if the connection is considered secure, which means
  /// no additional encryption is applied for the promised stream by the transport layer.
  ///
  /// # Returns
  /// `true` if the connection is secure (e.g., TLS), `false` otherwise (e.g., TCP).
  fn is_secure() -> bool;
}
