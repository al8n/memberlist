use std::{future::Future, net::SocketAddr};

use bytes::Bytes;

/// QUIC stream layer based on [`quinn`](::quinn).
#[cfg(feature = "quinn")]
#[cfg_attr(docsrs, doc(cfg(feature = "quinn")))]
pub mod quinn;

/// QUIC stream layer based on [`s2n`](::s2n).
#[cfg(feature = "s2n")]
#[cfg_attr(docsrs, doc(cfg(feature = "s2n")))]
pub mod s2n;

/// A error trait for quic stream layer.
pub trait QuicError: std::error::Error + Send + Sync + 'static {
  /// Returns `true` if the error is caused by the remote peer.
  fn is_remote_failure(&self) -> bool;
}

/// A trait for QUIC read stream.
pub trait QuicReadStream: Send + Sync + 'static {
  /// The error type for the stream.
  type Error: QuicError;

  /// Receives data from the remote peer.
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Receives exact data from the remote peer.
  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC write stream.
pub trait QuicWriteStream: Send + Sync + 'static {
  /// The error type for the stream.
  type Error: QuicError;

  /// Sends data to the remote peer.
  fn write_all(&mut self, src: Bytes) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC bidirectional streams.
pub trait QuicBiStream: Send + Sync + 'static {
  /// The error type for the stream.
  type Error: QuicError;

  /// Sends data to the remote peer.
  fn write_all(&mut self, src: Bytes) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Receives data from the remote peer.
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// Receives exact data from the remote peer.
  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Closes the stream.
  fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for QUIC stream layers.
pub trait StreamLayer: Sized + Send + Sync + 'static {
  /// The error type.
  type Error: QuicError;

  /// The listener type.
  type Listener: Listener<BiStream = Self::Stream>;

  /// The bidirectional stream type.
  type Stream: QuicBiStream;

  /// Max unacknowledged data in bytes that may be send on a single stream.
  fn max_stream_data(&self) -> usize;

  /// Binds to a local address.
  fn bind(&self, addr: SocketAddr) -> impl Future<Output = std::io::Result<Self::Listener>> + Send;
}

pub trait Listener {
  /// The error type.
  type Error: std::error::Error + Send + Sync + 'static;

  /// The bidirectional stream type.
  type BiStream: QuicBiStream;

  /// The unidirectional read stream type.
  type ReadStream: QuicReadStream;

  /// The unidirectional write stream type.
  type WriteStream: QuicWriteStream;

  /// Accepts a bidirectional connection from a remote peer.
  fn accept_bi(&mut self) -> impl Future<Output = Result<Self::BiStream, Self::Error>> + Send;

  /// Opens a bidirectional connection to a remote peer.
  fn open_bi(
    &self,
    addr: SocketAddr,
  ) -> impl Future<Output = Result<Self::BiStream, Self::Error>> + Send;

  /// Accepts a unidirectional read stream from a remote peer.
  fn accept_uni(&mut self) -> impl Future<Output = Result<Self::ReadStream, Self::Error>> + Send;

  /// Opens a unidirectional write stream to a remote peer.
  fn open_uni(
    &self,
    addr: SocketAddr,
  ) -> impl Future<Output = Result<Self::WriteStream, Self::Error>> + Send;
}
