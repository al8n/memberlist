//! Error types surfaced by the [`Endpoint`].

use std::borrow::Cow;

use thiserror::Error;

/// Errors returned by [`Endpoint`](crate::endpoint::Endpoint) operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
  /// The endpoint has already left the cluster (or never joined).
  #[error("endpoint is not running (already left or shut down)")]
  NotRunning,

  /// The endpoint received a message of an unexpected type for the current
  /// state (e.g. a `PushPull` arriving on the UDP path).
  #[error("unexpected message type: {0}")]
  UnexpectedMessage(&'static str),

  /// An incoming message had a state value the local node doesn't recognise.
  /// The payload is the raw state identifier received from the wire.
  #[error("unknown peer state: {0}")]
  UnknownPeerState(Cow<'static, str>),
}

/// Error from a per-stream reliable-exchange state machine.
///
/// `Clone` so a fatal error can be both stored in the terminal
/// `StreamPhase::Failed` and returned to the driver from the same
/// `handle_data` call. All variants are value types.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
  /// The stream deadline elapsed before the exchange completed.
  #[error("stream timed out")]
  Timeout,
  /// The driver reported that the dial failed.
  /// The payload is the free-form OS/network error description.
  #[error("dial failed: {0}")]
  DialFailed(Cow<'static, str>),
  /// The remote peer sent an unexpected or malformed message.
  /// The payload is a free-form description of what was unexpected.
  #[error("unexpected message from peer: {0}")]
  UnexpectedMessage(Cow<'static, str>),
  /// The peer sent bytes that could not be decoded.
  /// The payload is the free-form wire-decode error reason.
  #[error("decode error: {0}")]
  Decode(Cow<'static, str>),
  /// The peer closed the stream before sending a response.
  #[error("peer closed stream unexpectedly")]
  PeerClosed,
}
