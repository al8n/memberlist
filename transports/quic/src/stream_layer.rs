use std::{future::Future, net::SocketAddr, time::Duration};

use bytes::Bytes;
use memberlist_core::transport::{
  TimeoutableReadStream, TimeoutableStream, TimeoutableWriteStream,
};

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

/// A trait for QUIC read stream.
#[auto_impl::auto_impl(Box)]
pub trait QuicReadStream: TimeoutableReadStream + Send + Sync + 'static {
  /// The error type for the stream.
  type Error: QuicError;

  /// Receives data from the remote peer.
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Receives exact data from the remote peer.
  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Peek data from the remote peer.
  fn peek(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Peek exact data from the remote peer.
  fn peek_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC write stream.
#[auto_impl::auto_impl(Box)]
pub trait QuicWriteStream: TimeoutableWriteStream + Send + Sync + 'static {
  /// The error type for the stream.
  type Error: QuicError;

  /// Sends data to the remote peer.
  fn write_all(&mut self, src: Bytes) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Flushes the stream.
  fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC bidirectional streams.
#[auto_impl::auto_impl(Box)]
pub trait QuicStream: TimeoutableStream + Send + Sync + 'static {
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

  /// Receives all data from the remote peer.
  fn read_to_end(&mut self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;

  /// Peek data from the remote peer.
  fn peek(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Peek exact data from the remote peer.
  fn peek_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC stream layers.
#[auto_impl::auto_impl(Box, Arc)]
pub trait StreamLayer: Sized + Send + Sync + 'static {
  /// The error type.
  type Error: QuicError
    + From<<Self::Stream as QuicStream>::Error>
    + From<<Self::WriteStream as QuicWriteStream>::Error>
    + From<<Self::ReadStream as QuicReadStream>::Error>
    + From<<Self::Acceptor as QuicAcceptor>::Error>
    + From<<Self::Connector as QuicConnector>::Error>;

  /// The acceptor type.
  type Acceptor: QuicAcceptor<BiStream = Self::Stream>;

  /// The connector type.
  type Connector: QuicConnector<Stream = Self::Stream>;

  /// The bidirectional stream type.
  type Stream: QuicStream;

  /// The unidirectional write stream type.
  type WriteStream: QuicWriteStream;

  /// The unidirectional read stream type.
  type ReadStream: QuicReadStream;

  /// Max unacknowledged data in bytes that may be send on a single stream.
  fn max_stream_data(&self) -> usize;

  /// Binds to a local address. The `BiAcceptor` and `UniAcceptor` must bind to
  /// the same address.
  fn bind(
    &self,
    addr: SocketAddr,
  ) -> impl Future<
    Output = std::io::Result<(
      SocketAddr,
      Self::Acceptor,
      Self::Connector,
    )>,
  > + Send;
}

/// A trait for QUIC acceptors. The stream layer will use this trait to
/// accept a bi-directional connection from a remote peer.
#[auto_impl::auto_impl(Box)]
pub trait QuicAcceptor: Send + Sync + 'static {
  /// The error type.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The bidirectional stream type.
  type BiStream: QuicStream;

  /// Accepts a bidirectional connection from a remote peer.
  fn accept_bi(
    &mut self,
  ) -> impl Future<Output = Result<(Self::BiStream, SocketAddr), Self::Error>> + Send;

  /// Returns the local address.
  fn local_addr(&self) -> SocketAddr;
}

/// A trait for QUIC acceptors. The stream layer will use this trait to
/// accept a uni-directional connection from a remote peer.
#[auto_impl::auto_impl(Box, Arc)]
pub trait QuicUniAcceptor: Send + Sync + 'static {
  /// The error type.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The unidirectional read stream type.
  type ReadStream: QuicReadStream;

  /// Accepts a unidirectional read stream from a remote peer.
  fn accept_uni(
    &self,
  ) -> impl Future<Output = Result<(Self::ReadStream, SocketAddr), Self::Error>> + Send;

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
  type Stream: QuicStream;


  /// Opens a bidirectional connection to a remote peer.
  fn open_bi(
    &self,
    addr: SocketAddr,
  ) -> impl Future<Output = Result<Self::Stream, Self::Error>> + Send;

  /// Opens a bidirectional connection to a remote peer with timeout.
  fn open_bi_with_timeout(
    &self,
    addr: SocketAddr,
    timeout: Duration,
  ) -> impl Future<Output = Result<Self::Stream, Self::Error>> + Send;

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
