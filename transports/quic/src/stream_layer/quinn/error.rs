use quinn::{ConnectError, ConnectionError, WriteError};

use super::super::QuicError;

/// Error type for quinn stream layer.
#[derive(Debug, thiserror::Error)]
pub enum QuinnError {
  /// Connection error.
  #[error(transparent)]
  Connection(#[from] QuinnConnectionError),
  /// Read error.
  #[error(transparent)]
  Read(#[from] QuinnReadStreamError),
  /// Write error.
  #[error(transparent)]
  Write(#[from] QuinnWriteStreamError),
}

impl QuinnError {
  pub(super) fn read_timeout() -> Self {
    Self::Read(QuinnReadStreamError::Timeout)
  }

  pub(super) fn write_timeout() -> Self {
    Self::Write(QuinnWriteStreamError::Timeout)
  }

  pub(super) fn connection_timeout() -> Self {
    Self::Connection(QuinnConnectionError::DialTimeout)
  }
}

impl From<WriteError> for QuinnError {
  fn from(err: WriteError) -> Self {
    Self::Write(err.into())
  }
}

impl From<quinn::ReadError> for QuinnError {
  fn from(err: quinn::ReadError) -> Self {
    Self::Read(err.into())
  }
}

impl From<quinn::ReadExactError> for QuinnError {
  fn from(err: quinn::ReadExactError) -> Self {
    Self::Read(err.into())
  }
}

impl From<ConnectError> for QuinnError {
  fn from(err: ConnectError) -> Self {
    Self::Connection(err.into())
  }
}

impl From<ConnectionError> for QuinnError {
  fn from(err: ConnectionError) -> Self {
    Self::Connection(err.into())
  }
}

impl From<QuinnBiStreamError> for QuinnError {
  fn from(err: QuinnBiStreamError) -> Self {
    match err {
      QuinnBiStreamError::Write(err) => Self::Write(err),
      QuinnBiStreamError::Read(err) => Self::Read(err),
    }
  }
}

impl QuicError for QuinnError {
  fn is_remote_failure(&self) -> bool {
    match self {
      Self::Connection(err) => err.is_remote_failure(),
      Self::Read(err) => err.is_remote_failure(),
      Self::Write(err) => err.is_remote_failure(),
    }
  }
}

/// Connection error type for quinn stream layer.
#[derive(Debug, thiserror::Error)]
pub enum QuinnConnectionError {
  /// Dialing a remote peer failed.
  #[error(transparent)]
  Connect(#[from] ConnectError),
  /// Error on an established connection.
  #[error(transparent)]
  Connection(#[from] ConnectionError),

  /// Returned when establish connection but failed because of timeout.
  #[error("timeout")]
  DialTimeout,
}

impl QuicError for QuinnConnectionError {
  fn is_remote_failure(&self) -> bool {
    match self {
      Self::Connect(err) => is_connect_error_remote_failure(err),
      Self::Connection(err) => is_connection_error_remote_failure(err),
      Self::DialTimeout => true,
    }
  }
}

/// Read error type for quinn read.
#[derive(Debug, thiserror::Error)]
pub enum QuinnReadStreamError {
  /// Error reading from the stream.
  #[error(transparent)]
  Read(#[from] quinn::ReadError),
  /// Error reading exact data from the stream.
  #[error(transparent)]
  ReadExact(#[from] quinn::ReadExactError),
  /// Error reading all data from the stream.
  #[error(transparent)]
  ReadToEnd(#[from] quinn::ReadToEndError),

  /// IO error.
  #[error(transparent)]
  IO(#[from] std::io::Error),

  /// I/O operation timeout
  #[error("timeout")]
  Timeout,
}

impl QuicError for QuinnReadStreamError {
  fn is_remote_failure(&self) -> bool {
    match self {
      Self::Read(err) => is_read_error_remote_failure(err),
      Self::ReadExact(err) => match err {
        quinn::ReadExactError::FinishedEarly => true,
        quinn::ReadExactError::ReadError(err) => is_read_error_remote_failure(err),
      },
      Self::ReadToEnd(err) => match err {
        quinn::ReadToEndError::TooLong => false,
        quinn::ReadToEndError::Read(err) => is_read_error_remote_failure(err),
      },
      Self::IO(_) => true,
      Self::Timeout => true,
    }
  }
}

/// Write error type for quinn write.
#[derive(Debug, thiserror::Error)]
pub enum QuinnWriteStreamError {
  /// Error writing to the stream.
  #[error(transparent)]
  Write(#[from] quinn::WriteError),

  /// I/O error
  #[error(transparent)]
  IO(#[from] std::io::Error),

  /// I/O operation timeout
  #[error("timeout")]
  Timeout,
}

impl QuicError for QuinnWriteStreamError {
  fn is_remote_failure(&self) -> bool {
    match self {
      Self::Write(err) => is_write_error_remote_failure(err),
      Self::IO(_) => true,
      Self::Timeout => true,
    }
  }
}

/// Error type for [`QuinnBiStream`].
#[derive(Debug, thiserror::Error)]
pub enum QuinnBiStreamError {
  /// Error writing to the stream.
  #[error(transparent)]
  Write(#[from] QuinnWriteStreamError),
  /// Error reading from the stream.
  #[error(transparent)]
  Read(#[from] QuinnReadStreamError),
}

impl From<quinn::ReadError> for QuinnBiStreamError {
  fn from(err: quinn::ReadError) -> Self {
    Self::Read(err.into())
  }
}

impl From<quinn::ReadExactError> for QuinnBiStreamError {
  fn from(err: quinn::ReadExactError) -> Self {
    Self::Read(err.into())
  }
}

impl From<quinn::WriteError> for QuinnBiStreamError {
  fn from(err: quinn::WriteError) -> Self {
    Self::Write(err.into())
  }
}

impl QuicError for QuinnBiStreamError {
  fn is_remote_failure(&self) -> bool {
    match self {
      Self::Write(err) => err.is_remote_failure(),
      Self::Read(err) => err.is_remote_failure(),
    }
  }
}

#[inline]
const fn is_read_error_remote_failure(err: &quinn::ReadError) -> bool {
  match err {
    quinn::ReadError::Reset(_) => true,
    quinn::ReadError::ConnectionLost(_) => false,
    quinn::ReadError::UnknownStream => false,
    quinn::ReadError::IllegalOrderedRead => true,
    quinn::ReadError::ZeroRttRejected => true,
  }
}

#[inline]
const fn is_connection_error_remote_failure(err: &ConnectionError) -> bool {
  match err {
    ConnectionError::VersionMismatch => true,
    ConnectionError::TransportError(_) => true,
    ConnectionError::ConnectionClosed(_) => true,
    ConnectionError::ApplicationClosed(_) => true,
    ConnectionError::Reset => true,
    ConnectionError::TimedOut => true,
    ConnectionError::LocallyClosed => false,
  }
}

#[inline]
const fn is_connect_error_remote_failure(err: &ConnectError) -> bool {
  match err {
    ConnectError::EndpointStopping => false,
    ConnectError::TooManyConnections => false,
    ConnectError::InvalidDnsName(_) => false,
    ConnectError::InvalidRemoteAddress(_) => false,
    ConnectError::NoDefaultClientConfig => false,
    ConnectError::UnsupportedVersion => false,
  }
}

#[inline]
const fn is_write_error_remote_failure(err: &WriteError) -> bool {
  match err {
    WriteError::Stopped(_) => true,
    WriteError::ConnectionLost(_) => false,
    WriteError::UnknownStream => false,
    WriteError::ZeroRttRejected => true,
  }
}
