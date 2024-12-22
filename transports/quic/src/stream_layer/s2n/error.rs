use crate::QuicError;

use s2n_quic::{connection::Error as ConnectionError, stream::Error as StreamError};

pub use s2n_quic_transport::connection::limits::ValidationError;

/// Error type for s2n stream layer.
#[derive(Debug, thiserror::Error)]
pub enum S2nError {
  /// Validation error.
  #[error("{0}")]
  Validation(#[from] ValidationError),

  /// Connection error.
  #[error(transparent)]
  Connection(#[from] ConnectionError),

  /// Stream error.
  #[error(transparent)]
  Stream(#[from] StreamError),

  /// IO error.
  #[error(transparent)]
  IO(#[from] std::io::Error),

  /// Timeout.
  #[error("timeout")]
  Timeout,

  /// Closed.
  #[error("endpoint closed")]
  Closed,
}

impl QuicError for S2nError {
  fn is_remote_failure(&self) -> bool {
    match self {
      Self::Validation(_) => false,
      Self::Connection(err) => match err {
        ConnectionError::Closed { .. } => false,
        ConnectionError::Transport { .. } => true,
        ConnectionError::Application { .. } => true,
        ConnectionError::StatelessReset { .. } => true,
        ConnectionError::IdleTimerExpired { .. } => false,
        ConnectionError::NoValidPath { .. } => true,
        ConnectionError::StreamIdExhausted { .. } => false,
        ConnectionError::MaxHandshakeDurationExceeded { .. } => true,
        ConnectionError::ImmediateClose { .. } => false,
        ConnectionError::EndpointClosing { .. } => false,
        ConnectionError::Unspecified { .. } => false,
        _ => false,
      },
      Self::Closed => false,
      Self::IO(_) => true,
      Self::Stream(err) => match err {
        StreamError::InvalidStream { .. } => false,
        StreamError::StreamReset { .. } => true,
        StreamError::SendAfterFinish { .. } => false,
        StreamError::MaxStreamDataSizeExceeded { .. } => false,
        StreamError::ConnectionError { .. } => true,
        StreamError::NonReadable { .. } => true,
        StreamError::NonWritable { .. } => false,
        StreamError::SendingBlocked { .. } => false,
        StreamError::NonEmptyOutput { .. } => false,
        _ => false,
      },
      Self::Timeout => true,
    }
  }
}
