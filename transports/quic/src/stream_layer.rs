use std::{future::Future, net::SocketAddr};

use bytes::Bytes;
use memberlist_core::transport::TimeoutableStream;

/// QUIC stream layer based on [`quinn`](::quinn).
#[cfg(feature = "quinn")]
#[cfg_attr(docsrs, doc(cfg(feature = "quinn")))]
pub mod quinn;

/// QUIC stream layer based on [`s2n`](::s2n_quic).
#[cfg(feature = "s2n")]
#[cfg_attr(docsrs, doc(cfg(feature = "s2n")))]
pub mod s2n;

/// A error trait for quic stream layer.
#[auto_impl::auto_impl(Box, Arc)]
pub trait QuicError: std::error::Error + Send + Sync + 'static {
  /// Returns `true` if the error is caused by the remote peer.
  fn is_remote_failure(&self) -> bool;
}

/// A trait for QUIC bidirectional streams.
#[auto_impl::auto_impl(Box)]
pub trait QuicStream: TimeoutableStream + futures::AsyncRead + Send + Sync + 'static {
  /// The error type for the stream.
  type Error: QuicError;

  /// Sends data to the remote peer.
  fn write_all(&mut self, src: Bytes) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Flushes the stream.
  fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Receives data from the remote peer.
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Receives exact data from the remote peer.
  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Peek data from the remote peer.
  fn peek(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Peek exact data from the remote peer.
  fn peek_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Shut down the send stream gracefully.
  ///
  /// No new data may be written after calling this method. Completes when the peer has acknowledged all sent data, retransmitting data as needed.
  fn finish(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC stream layers.
#[auto_impl::auto_impl(Box, Arc)]
pub trait StreamLayer: Sized + Send + Sync + 'static {
  /// The error type.
  type Error: QuicError
    + From<<Self::Stream as QuicStream>::Error>
    + From<<Self::Connection as QuicConnection>::Error>
    + From<<Self::Acceptor as QuicAcceptor>::Error>
    + From<<Self::Connector as QuicConnector>::Error>;

  /// The acceptor type.
  type Acceptor: QuicAcceptor<Connection = Self::Connection>;

  /// The connector type.
  type Connector: QuicConnector<Connection = Self::Connection>;

  /// The bidirectional stream type.
  type Stream: QuicStream;

  /// The connection type.
  type Connection: QuicConnection<Stream = Self::Stream>;

  /// Max unacknowledged data in bytes that may be send on a single stream.
  fn max_stream_data(&self) -> usize;

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
  /// The error type.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The connection type.
  type Connection: QuicConnection;

  /// Accepts a connection from a remote peer.
  fn accept(
    &mut self,
  ) -> impl Future<Output = Result<(Self::Connection, SocketAddr), Self::Error>> + Send;

  /// Close the acceptor.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;
}

/// A trait for QUIC connectors. The stream layer will use this trait to
/// establish a connection to a remote peer.
#[auto_impl::auto_impl(Box, Arc)]
pub trait QuicConnector: Send + Sync + 'static {
  /// The error type.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The bidirectional stream type.
  type Connection: QuicConnection;

  /// Connects to a remote peer.
  fn connect(
    &self,
    addr: SocketAddr,
  ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

  /// Connects to a remote peer with timeout.
  fn connect_with_timeout(
    &self,
    addr: SocketAddr,
    timeout: std::time::Duration,
  ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

  /// Closes the connector.
  fn close(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Wait for all connections on the endpoint to be cleanly shut down
  ///
  /// Waiting for this condition before exiting ensures that a good-faith effort is made to notify
  /// peers of recent connection closes, whereas exiting immediately could force them to wait out
  /// the idle timeout period.
  ///
  /// Does not proactively close existing connections or cause incoming connections to be
  /// rejected. Consider calling `close()` if that is desired.
  fn wait_idle(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;
}

/// A connection between local and remote peer.
#[auto_impl::auto_impl(Box, Arc)]
pub trait QuicConnection: Send + Sync + 'static {
  /// The error type.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The bidirectional stream type.
  type Stream: QuicStream;

  /// Accepts a bidirectional stream from a remote peer.
  fn accept_bi(
    &self,
  ) -> impl Future<Output = Result<(Self::Stream, SocketAddr), Self::Error>> + Send;

  /// Opens a bidirectional stream to a remote peer.
  fn open_bi(&self)
    -> impl Future<Output = Result<(Self::Stream, SocketAddr), Self::Error>> + Send;

  /// Opens a bidirectional stream to a remote peer.
  fn open_bi_with_timeout(
    &self,
    timeout: std::time::Duration,
  ) -> impl Future<Output = Result<(Self::Stream, SocketAddr), Self::Error>> + Send;

  /// Closes the connection.
  fn close(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns `true` if the connection is closed.
  fn is_closed(&self) -> impl Future<Output = bool> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;

  /// Returns if the connection reached the maximum number of opened streams.
  fn is_full(&self) -> bool;
}
