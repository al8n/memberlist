use std::{future::Future, io, net::SocketAddr};

use agnostic::RuntimeLite;
use bytes::Bytes;

/// QUIC stream layer based on [`quinn`](::quinn).
#[cfg(feature = "quinn")]
#[cfg_attr(docsrs, doc(cfg(feature = "quinn")))]
pub mod quinn;

/// QUIC stream layer based on [`s2n`](::s2n_quic).
#[cfg(feature = "s2n")]
#[cfg_attr(docsrs, doc(cfg(feature = "s2n")))]
pub mod s2n;

/// A trait for QUIC bidirectional streams.
pub trait QuicStream:
  memberlist_core::transport::Connection + Unpin + Send + Sync + 'static
{
  /// The send stream type.
  type SendStream: memberlist_core::proto::ProtoWriter;

  /// Read a packet from the the stream.
  fn read_packet(&mut self) -> impl Future<Output = std::io::Result<Bytes>> + Send;
}

/// A trait for QUIC stream layers.
pub trait StreamLayer: Sized + Send + Sync + 'static {
  /// The runtime for this stream layer.
  type Runtime: RuntimeLite;

  /// The acceptor type.
  type Acceptor: QuicAcceptor<Connection = Self::Connection>;

  /// The connector type.
  type Connector: QuicConnector<Connection = Self::Connection>;

  /// The bidirectional stream type.
  type Stream: QuicStream;

  /// The connection type.
  type Connection: QuicConnection<Stream = Self::Stream>;

  /// The options type used to construct the stream layer.
  type Options: Send + Sync + 'static;

  /// Max unacknowledged data in bytes that may be send on a single stream.
  fn max_stream_data(&self) -> usize;

  /// Creates a new stream layer.
  fn new(options: Self::Options) -> impl Future<Output = std::io::Result<Self>> + Send;

  /// Binds to a local address. The `BiAcceptor` and `UniAcceptor` must bind to
  /// the same address.
  fn bind(
    &self,
    addr: SocketAddr,
  ) -> impl Future<Output = std::io::Result<(SocketAddr, Self::Acceptor, Self::Connector)>> + Send;
}

/// A trait for QUIC acceptors. The stream layer will use this trait to
/// accept a bi-directional connection from a remote peer.
#[auto_impl::auto_impl(Box)]
pub trait QuicAcceptor: Send + Sync + 'static {
  /// The connection type.
  type Connection: QuicConnection;

  /// Accepts a connection from a remote peer.
  fn accept(&mut self) -> impl Future<Output = io::Result<(Self::Connection, SocketAddr)>> + Send;

  /// Close the acceptor.
  fn close(&mut self) -> impl Future<Output = io::Result<()>> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;
}

/// A trait for QUIC connectors. The stream layer will use this trait to
/// establish a connection to a remote peer.
#[auto_impl::auto_impl(Box, Arc)]
pub trait QuicConnector: Send + Sync + 'static {
  /// The bidirectional stream type.
  type Connection: QuicConnection;

  /// Connects to a remote peer.
  fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Connection>> + Send;

  /// Closes the connector.
  fn close(&self) -> impl Future<Output = io::Result<()>> + Send;

  /// Wait for all connections on the endpoint to be cleanly shut down
  ///
  /// Waiting for this condition before exiting ensures that a good-faith effort is made to notify
  /// peers of recent connection closes, whereas exiting immediately could force them to wait out
  /// the idle timeout period.
  ///
  /// Does not proactively close existing connections or cause incoming connections to be
  /// rejected. Consider calling `close()` if that is desired.
  fn wait_idle(&self) -> impl Future<Output = io::Result<()>> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;
}

/// A connection between local and remote peer.
#[auto_impl::auto_impl(Box, Arc)]
pub trait QuicConnection: Send + Sync + 'static {
  /// The bidirectional stream type.
  type Stream: QuicStream;

  /// Accepts a bidirectional stream from a remote peer.
  fn accept_bi(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

  /// Opens a bidirectional stream to a remote peer.
  fn open_bi(&self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

  /// Opens a unidirectional stream to a remote peer.
  fn open_uni(
    &self,
  ) -> impl Future<
    Output = io::Result<(
      <Self::Stream as memberlist_core::transport::Connection>::Writer,
      SocketAddr,
    )>,
  > + Send;

  /// Closes the connection.
  fn close(&self) -> impl Future<Output = io::Result<()>> + Send;

  /// Returns `true` if the connection is closed.
  fn is_closed(&self) -> impl Future<Output = bool> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;
}
