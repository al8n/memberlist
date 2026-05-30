//! Internal command channel — user-facing Memberlist handle sends;
//! driver task receives and dispatches.

use crate::error::Result;
use bytes::Bytes;
use memberlist_machine::Instant;
use memberlist_wire::{CompressionOptions, EncryptionOptions};
use std::net::SocketAddr;

/// Payload for [`JoinKind::WaitForCompletion`].
pub(crate) struct WaitForCompletionArgs {
  /// Wall-clock instant past which the driver replies with whatever success
  /// count it has accumulated (zero successes surface as `JoinAllFailed`).
  pub(crate) deadline: Instant,
}

/// Semantic of a [`Command::Join`] dispatch.
///
/// The driver routes both kinds through the same start_push_pull
/// fan-out (one outbound exchange per resolved address). The kind
/// only affects WHEN the reply is sent and WHAT count it reports:
/// - `Dispatch`: reply immediately with the number of push/pull
///   exchanges queued (fire-and-forget; the caller does not wait
///   for any exchange to terminate).
/// - `WaitForCompletion`: reply once every dispatched exchange has
///   terminated (`ExchangeCompleted` observed for its `ExchangeId`
///   with `kind == ExchangeKind::PushPull`) OR the deadline elapses,
///   whichever comes first. The reply carries the count of exchanges
///   whose outcome was `ExchangeOutcome::Succeeded`; zero successes
///   surface as `JoinAllFailed`.
pub(crate) enum JoinKind {
  /// Reply immediately with the dispatched-exchange count.
  Dispatch,
  /// Reply once every dispatched exchange has terminated OR the
  /// deadline expires.
  WaitForCompletion(WaitForCompletionArgs),
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

/// Payload for [`Command::QueueUserBroadcast`].
pub(crate) struct QueueUserBroadcastCmd {
  /// Application bytes to disseminate cluster-wide via gossip.
  data: Bytes,
  /// One-shot reply channel for the enqueue acknowledgement.
  reply: flume::Sender<Result<()>>,
}

impl QueueUserBroadcastCmd {
  /// Construct from the broadcast bytes and a reply channel.
  pub(crate) const fn new(data: Bytes, reply: flume::Sender<Result<()>>) -> Self {
    Self { data, reply }
  }

  /// The application bytes to broadcast.
  pub(crate) const fn data(&self) -> &Bytes {
    &self.data
  }

  /// The reply channel for the enqueue acknowledgement.
  pub(crate) const fn reply(&self) -> &flume::Sender<Result<()>> {
    &self.reply
  }
}

/// Payload for [`Command::SetLocalState`].
pub(crate) struct SetLocalStateCmd {
  /// Application push/pull local-state snapshot bytes.
  state: Bytes,
  /// One-shot reply channel for the set acknowledgement.
  reply: flume::Sender<Result<()>>,
}

impl SetLocalStateCmd {
  /// Construct from the snapshot bytes and a reply channel.
  pub(crate) const fn new(state: Bytes, reply: flume::Sender<Result<()>>) -> Self {
    Self { state, reply }
  }

  /// The local-state snapshot bytes.
  pub(crate) const fn state(&self) -> &Bytes {
    &self.state
  }

  /// The reply channel for the set acknowledgement.
  pub(crate) const fn reply(&self) -> &flume::Sender<Result<()>> {
    &self.reply
  }
}

/// Payload for [`Command::SetAckPayload`].
pub(crate) struct SetAckPayloadCmd {
  /// Application payload bytes attached to outbound probe acks.
  payload: Bytes,
  /// One-shot reply channel for the set acknowledgement.
  reply: flume::Sender<Result<()>>,
}

impl SetAckPayloadCmd {
  /// Construct from the ack-payload bytes and a reply channel.
  pub(crate) const fn new(payload: Bytes, reply: flume::Sender<Result<()>>) -> Self {
    Self { payload, reply }
  }

  /// The ack-payload bytes.
  pub(crate) const fn payload(&self) -> &Bytes {
    &self.payload
  }

  /// The reply channel for the set acknowledgement.
  pub(crate) const fn reply(&self) -> &flume::Sender<Result<()>> {
    &self.reply
  }
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
  /// Queue an application user-broadcast for cluster-wide gossip.
  QueueUserBroadcast(QueueUserBroadcastCmd),
  /// Set the application push/pull local-state snapshot.
  SetLocalState(SetLocalStateCmd),
  /// Set the application payload attached to outbound probe acks.
  SetAckPayload(SetAckPayloadCmd),
  /// Cleanly shut down the driver task.
  Shutdown(ShutdownCmd),
}
