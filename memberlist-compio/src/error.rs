//! Error types for memberlist-compio.

use std::io;

/// Payload for [`MemberlistError::JoinAllFailed`]: every dispatched
/// outbound push/pull exchange from a synchronous
/// [`join_with`](crate::Memberlist::join_with) terminated without
/// `PushPullOutcome::Succeeded`, OR the per-call deadline elapsed
/// before any exchange could succeed.
#[derive(Debug)]
pub struct JoinAllFailed {
  requested: usize,
  contacted: usize,
}

impl JoinAllFailed {
  /// Build a new payload from the per-call seed counts.
  #[inline]
  pub(crate) fn new(requested: usize, contacted: usize) -> Self {
    Self {
      requested,
      contacted,
    }
  }

  /// Number of seed addresses the call requested (post-resolution).
  #[inline]
  pub fn requested(&self) -> usize {
    self.requested
  }

  /// Number of seeds actually contacted before the call resolved.
  /// Always `0` when this payload is observed inside a
  /// [`MemberlistError::JoinAllFailed`] — the variant fires precisely
  /// when no seed was contacted.
  #[inline]
  pub fn contacted(&self) -> usize {
    self.contacted
  }
}

impl core::fmt::Display for JoinAllFailed {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "join failed: contacted {} of {} seed(s)",
      self.contacted, self.requested
    )
  }
}

/// Errors returned by [`Memberlist`](crate::Memberlist) operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MemberlistError {
  /// I/O error from the OS, socket, or compio runtime.
  #[error("I/O error: {0}")]
  Io(#[from] io::Error),

  /// Encryption codec error from memberlist-wire.
  #[error("encryption: {0}")]
  Encryption(#[from] memberlist_wire::EncryptionError),

  /// Frame decode error from memberlist-wire.
  #[error("frame: {0}")]
  Frame(#[from] memberlist_wire::FrameError),

  /// Address resolution failed (DNS error, etc.).
  #[error("address resolution: {0}")]
  Resolve(io::Error),

  /// The synchronous [`join_with`](crate::Memberlist::join_with) attempt
  /// resolved without any dispatched outbound push/pull exchange
  /// terminating with `PushPullOutcome::Succeeded` — either every
  /// exchange terminated `Failed` (dial failure, frame/record-layer
  /// rejection, peer hung up before the response was decoded), or
  /// the per-call deadline elapsed before any could succeed.
  #[error("{0}")]
  JoinAllFailed(JoinAllFailed),

  /// The driver task has shut down and is no longer accepting commands.
  #[error("driver shut down")]
  Shutdown,

  /// Sending a command to the driver failed because the channel is closed.
  #[error("send to driver failed (channel closed)")]
  CommandSend,

  /// The driver's reply channel was dropped before a reply arrived.
  #[error("driver reply channel closed")]
  ReplyClosed,
}

/// Convenience [`Result`] for [`MemberlistError`].
pub type Result<T, E = MemberlistError> = core::result::Result<T, E>;
