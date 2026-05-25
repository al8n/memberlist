//! Internal command channel — user-facing Memberlist handle sends;
//! driver task receives and dispatches.

use crate::error::Result;
use memberlist_wire::{CompressionOptions, EncryptionOptions};
use std::{net::SocketAddr, time::Instant};

/// Semantic of a [`Command::Join`] dispatch.
///
/// The driver routes both kinds through the same start_push_pull
/// fan-out (one outbound exchange per resolved address). The kind
/// only affects WHEN the reply is sent and WHAT count it reports:
/// - `Dispatch`: reply immediately with the number of push/pull
///   exchanges queued (fire-and-forget; the caller does not wait
///   for any exchange to terminate).
/// - `WaitForCompletion`: reply once every dispatched exchange has
///   terminated (`PushPullCompleted` observed for its `ExchangeId`)
///   OR the deadline elapses, whichever comes first. The reply
///   carries the count of exchanges whose outcome was
///   `PushPullOutcome::Succeeded`; zero successes surface as
///   `JoinAllFailed`.
pub(crate) enum JoinKind {
  /// Reply immediately with the dispatched-exchange count.
  Dispatch,
  /// Reply once every dispatched exchange has terminated OR the
  /// deadline expires.
  WaitForCompletion {
    /// Wall-clock instant past which the driver replies with
    /// whatever success count it has accumulated (zero successes
    /// surface as `JoinAllFailed`).
    deadline: Instant,
  },
}

/// Payload for [`Command::Join`].
pub(crate) struct JoinCmd {
  /// Pre-resolved socket addresses of the peers to join.
  pub(crate) addrs: Vec<SocketAddr>,
  /// Dispatch semantic — see [`JoinKind`].
  pub(crate) kind: JoinKind,
  /// One-shot reply channel for the join result.
  pub(crate) reply: flume::Sender<Result<usize>>,
}

/// Payload for [`Command::Leave`].
pub(crate) struct LeaveCmd {
  /// One-shot reply channel for the leave result.
  pub(crate) reply: flume::Sender<Result<()>>,
}

/// Payload for [`Command::UpdateNodeMetadata`].
pub(crate) struct UpdateNodeMetadataCmd {
  /// New raw metadata bytes for the local node.
  pub(crate) meta: Vec<u8>,
  /// One-shot reply channel for the update result.
  pub(crate) reply: flume::Sender<Result<()>>,
}

/// Payload for [`Command::SetCompressionOptions`].
pub(crate) struct SetCompressionOptionsCmd {
  /// New compression configuration to apply in place.
  pub(crate) opts: CompressionOptions,
  /// One-shot reply channel for the reconfiguration result.
  pub(crate) reply: flume::Sender<Result<()>>,
}

/// Payload for [`Command::SetEncryptionOptions`].
pub(crate) struct SetEncryptionOptionsCmd {
  /// New encryption configuration to apply in place.
  pub(crate) opts: EncryptionOptions,
  /// One-shot reply channel for the reconfiguration result.
  pub(crate) reply: flume::Sender<Result<()>>,
}

/// Payload for [`Command::Shutdown`].
pub(crate) struct ShutdownCmd {
  /// One-shot reply channel for the shutdown acknowledgement.
  pub(crate) reply: flume::Sender<Result<()>>,
}

/// Commands sent from the public Memberlist handle to the driver task.
pub(crate) enum Command {
  /// Join a set of peers (addresses already resolved).
  Join(JoinCmd),
  /// Leave the cluster gracefully.
  Leave(LeaveCmd),
  /// Update the local node's metadata.
  UpdateNodeMetadata(UpdateNodeMetadataCmd),
  /// Update the compression options (in-place reconfiguration).
  SetCompressionOptions(SetCompressionOptionsCmd),
  /// Update the encryption options (in-place reconfiguration).
  SetEncryptionOptions(SetEncryptionOptionsCmd),
  /// Cleanly shut down the driver task.
  Shutdown(ShutdownCmd),
}
