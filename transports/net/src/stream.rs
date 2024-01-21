use std::{future::Future, io, net::SocketAddr};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use showbiz_core::transport::TimeoutableStream;

use crate::{Label, LABEL_TAG};

/// Represents a network listener.
///
/// This trait defines the operations required for a network listener that can bind to an address,
/// accept incoming connections, and query its local address.
pub trait Listener: Send + Sync + 'static {
  /// The type of the network stream associated with this listener.
  type Stream: PromisedConnection;

  /// Accepts an incoming connection.
  fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

  /// Retrieves the local socket address of the listener.
  fn local_addr(&self) -> io::Result<SocketAddr>;
}

/// Represents a network connection.
///
/// This trait encapsulates functionality for a network connection that supports asynchronous
/// read/write operations and can be split into separate read and write halves.
pub trait PromisedConnection:
  TimeoutableStream + AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static
{
}

impl<T: PromisedConnection> PromisedConnectionExt for T {}

/// A trait defining the necessary components for a stream-based network layer.
/// This layer must promise a reliable, ordered, and bi-directional stream of data, e.g. TCP, QUIC, gRPC and etc.
///
/// This trait is used in conjunction with [`NetTransport`](super::NetTransport) to provide
/// an abstraction over the underlying network stream. It specifies the types for listeners
/// and connections that operate on this stream.
pub trait StreamLayer: Send + Sync + 'static {
  /// The listener type for the network stream.
  type Listener: Listener<Stream = Self::Stream>;

  /// The connection type for the network stream.
  type Stream: PromisedConnection;

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

/// Packet connection. e.g. UDP connection.
pub trait PacketConnection: TimeoutableStream + Unpin + Send + Sync + 'static {
  /// Sends a packet to the specified socket address.
  ///
  /// This method asynchronously sends the provided buffer as a packet to the given address.
  ///
  /// # Arguments
  /// * `addr` - A reference to the `SocketAddr` where the packet should be sent.
  /// * `buf` - A byte slice reference representing the packet's contents.
  ///
  /// # Returns
  /// A `Future` that resolves to a `Result` indicating the outcome of the send operation.
  /// On success, it contains the number of bytes sent.
  fn send_to(
    &self,
    addr: &SocketAddr,
    buf: &[u8],
  ) -> impl Future<Output = Result<usize, std::io::Error>> + Send;

  /// Receives a packet, storing its contents into the provided buffer.
  ///
  /// This method asynchronously waits for a packet and stores its contents into the given buffer.
  ///
  /// # Arguments
  /// * `buf` - A mutable byte slice where the received packet's contents will be stored.
  ///
  /// # Returns
  /// A `Future` that resolves to a `Result` containing the size of the received data and the
  /// sender's socket address. If an error occurs, the `Result` will contain the `std::io::Error`.
  fn recv_from(
    &self,
    buf: &mut [u8],
  ) -> impl Future<Output = Result<(usize, SocketAddr), std::io::Error>> + Send;
}

/// A trait for managing packet-based network communications.
/// This layer can be unreliable stream of data, e.g. UDP. If the connection is not reliable,
/// the transport layer will apply a checksum to each packet used to detect errors.
///
/// This trait abstracts the operations necessary for handling packet-based network layers,
/// such as UDP. It includes methods for binding to a socket address, sending and receiving packets,
/// and determining certain properties of the connection like security and reliability.
pub trait PacketLayer: Send + Sync + 'static {
  /// The connection type for the network stream.
  type Stream: PacketConnection;

  /// Binds the listener to a given socket address.
  fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Stream>> + Send;

  /// Indicates whether the connection is secure.
  ///
  /// This method returns `true` if the connection is considered secure, which means
  /// no additional encryption is applied for the promised stream by the transport layer.
  ///
  /// # Returns
  /// `true` if the connection is secure (e.g., QUIC), `false` otherwise (e.g., UDP).
  fn is_secure() -> bool;

  /// Returns true if the connection is promised. If the connection is promised,
  /// the transport layer will apply a checksum.
  ///
  /// e.g.
  /// - QUIC is promised
  /// - UDP is not promised
  fn is_promised() -> bool;
}
