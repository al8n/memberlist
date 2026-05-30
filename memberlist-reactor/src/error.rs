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

  /// An I/O error.
  #[error(transparent)]
  Io(#[from] std::io::Error),
}
