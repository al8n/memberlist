//! Errors surfaced by `memberlist-reactor`.

use std::net::SocketAddr;

/// An error from a `memberlist-reactor` operation.
///
/// Grows as the driver is built out (transport / machine variants are added
/// where those operations are wired). Kept `#[non_exhaustive]` so additions are
/// not breaking.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
  /// The memberlist has shut down; the operation cannot proceed.
  #[error("memberlist has shut down")]
  Shutdown,

  /// The node has left or never started; the operation cannot proceed.
  #[error("memberlist is not running")]
  NotRunning,

  /// Address resolution failed.
  #[error("address resolution failed: {0}")]
  Resolve(String),

  /// A join dispatched push/pulls to seeds but contacted none of them.
  #[error("join contacted none of {0} seed(s)")]
  JoinFailed(usize),

  /// The resolved advertise address is not a usable unicast contact
  /// (unspecified, multicast, or broadcast); peers could not reach this node.
  #[error("invalid advertise address {0}: must be a concrete unicast address")]
  InvalidAdvertise(SocketAddr),

  /// The configured `close_timeout` is zero. The stream driver bounds each
  /// per-bridge graceful-close drain `write` with this timeout; a zero timeout
  /// fires immediately, so a graceful close abandons (RSTs) its queued push/pull
  /// response bytes instead of draining them, truncating reliable exchanges.
  /// Rejected at construction (stream backends only; QUIC has no bridges),
  /// mirroring the smoltcp driver's `ZeroCloseTimeout` rejection.
  #[error(
    "close_timeout must be nonzero: a zero timeout immediately RSTs a graceful reliable close, truncating queued push/pull bytes"
  )]
  ZeroCloseTimeout,

  /// A directed ping timed out (no ack received within the probe deadline).
  #[error("ping timed out")]
  PingTimeout,

  /// A reliable directed send failed: one or more stream exchanges did not
  /// complete successfully.
  #[error("reliable send failed")]
  SendFailed,

  /// A directed user-message payload exceeds the per-packet wire limit.
  #[error("payload too large: {0}")]
  PayloadTooLarge(String),

  /// An I/O error.
  #[error(transparent)]
  Io(#[from] std::io::Error),

  /// An encryption keyring was rejected (a key's algorithm backend is
  /// missing), so the node refuses to start rather than run plaintext on an
  /// encrypted-cluster configuration.
  #[error("encryption: {0}")]
  Encryption(memberlist_proto::EncryptionError),

  /// The cluster label supplied to
  /// [`MemberlistOptions::with_label`](crate::MemberlistOptions::with_label)
  /// violates the wire constraints: it exceeds the 253-byte maximum or is not
  /// valid UTF-8. Rejected at the setter so the error surfaces at configuration
  /// time rather than at construction.
  #[error("invalid cluster label: {0}")]
  InvalidLabel(memberlist_proto::LabelError),
}
