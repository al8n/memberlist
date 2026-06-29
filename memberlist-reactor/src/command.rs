//! Driver commands — a `Memberlist` handle pushes these onto the shared queue;
//! the driver drains and dispatches them, replying on each command's oneshot.

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
#[cfg(checksum)]
use memberlist_proto::ChecksumOptions;
#[cfg(compression)]
use memberlist_proto::CompressionOptions;
#[cfg(encryption)]
use memberlist_proto::EncryptionOptions;
use memberlist_proto::Node;

use crate::error::Error;
use futures_channel::oneshot::Sender;
use smallvec::SmallVec;

/// Address-set reply for [`Command::Join`].
///
/// A waiting join (`wait == true`) replies `Ok(set)` with the contacted seed
/// addresses on success, or `Err((reached_so_far, err))` — the partial-success
/// tuple mirrored from the serf driver. A fire-and-forget join (`wait == false`)
/// replies `Ok(set)` with the dispatched seed set, whose length is the
/// dispatched-exchange count. `JoinFailed` carries an EMPTY reached set — it is
/// the all-failed case. A `Shutdown` error, however, can carry a NON-EMPTY
/// reached-so-far set.
///
/// The reached set is EXACT in both cases — not best-effort. A normal join's set
/// is precisely the seeds whose push/pull `Succeeded`. A shutdown-raced join's
/// set is ALSO precise: it is exactly the seeds the coordinator would authorize
/// as `Succeeded` — i.e. whose peer-FIN was processed (the bridge produced the
/// `BridgeInbound::Eof` for the exchange) — at the freeze instant. The shutdown
/// teardown freezes every bridge's reads, then drains the bridge inbound channel
/// to all-senders-gone, so every already-read completion is folded — whether it
/// was queued in the channel or parked on a saturated `inbound_tx.send` — before
/// the join is reaped. A seed whose peer-FIN had not yet been read off the socket
/// at the freeze instant is genuinely in-flight and is correctly absent from the
/// set.
pub(crate) type JoinReply = Result<SmallVec<[SocketAddr; 1]>, (SmallVec<[SocketAddr; 1]>, Error)>;

/// A command from a `Memberlist` handle to its backend driver.
///
/// `I` is the node-identity type; `Ping` carries a full `Node<I, SocketAddr>`
/// and drives the parameterisation. All other variants carry only `SocketAddr`
/// or `Bytes` data and are unaffected by the type parameter.
pub(crate) enum Command<I> {
  /// Contact the resolved seed addresses and merge their state.
  Join(JoinCmd),
  /// Gracefully leave the cluster, then stop.
  Leave(LeaveCmd),
  /// Stop the driver and release its socket.
  Shutdown(ShutdownCmd),
  /// Probe a specific node and return the round-trip time.
  Ping(PingCmd<I>),
  /// Send one or more unreliable directed user messages via gossip.
  SendUser(SendUserCmd),
  /// Send one or more reliable directed user messages via the stream plane.
  SendReliable(SendReliableCmd),
  /// Reconfigure the gossip compression policy in place.
  #[cfg(compression)]
  SetCompressionOptions(SetCompressionOptionsCmd),
  /// Reconfigure the gossip (unreliable) checksum policy in place.
  #[cfg(checksum)]
  SetChecksumOptions(SetChecksumOptionsCmd),
  /// Reconfigure the gossip encryption policy in place.
  #[cfg(encryption)]
  SetEncryptionOptions(SetEncryptionOptionsCmd),
  /// Replace this node's advertised metadata in place.
  UpdateNodeMetadata(UpdateNodeMetadataCmd),
  /// Queue an application user-broadcast for cluster-wide gossip.
  QueueUserBroadcast(QueueUserBroadcastCmd),
  /// Set the push/pull application local-state snapshot.
  SetLocalState(SetLocalStateCmd),
  /// Set the payload attached to outbound probe acks.
  SetAckPayload(SetAckPayloadCmd),
}

/// Payload of [`Command::Join`].
pub(crate) struct JoinCmd {
  /// Already-resolved seed addresses to contact (one push/pull each).
  pub(crate) addrs: Vec<SocketAddr>,
  /// Wait for every dispatched exchange to complete (replying the contacted
  /// address set), or reply immediately with the dispatched seed set.
  pub(crate) wait: bool,
  /// Replies with the reached address set, or the partial-success tuple. See
  /// [`JoinReply`].
  pub(crate) reply: Sender<JoinReply>,
}

/// Payload of [`Command::Leave`].
pub(crate) struct LeaveCmd {
  /// Replies once the leave has reached the wire (or on timeout/shutdown).
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::Shutdown`].
pub(crate) struct ShutdownCmd {
  /// Replies once the driver has stopped and released its socket.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::Ping`].
pub(crate) struct PingCmd<I> {
  /// The node to probe (id + wire address).
  pub(crate) node: Node<I, SocketAddr>,
  /// Replies with the round-trip time, or an error.
  pub(crate) reply: Sender<Result<Duration, Error>>,
}

/// Payload of [`Command::SendUser`].
pub(crate) struct SendUserCmd {
  /// Destination wire address.
  pub(crate) to: SocketAddr,
  /// One or more unreliable user-message payloads to direct to `to`.
  pub(crate) payloads: Vec<Bytes>,
  /// Replies with `Ok(())` on dispatch, or an error.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::SendReliable`].
pub(crate) struct SendReliableCmd {
  /// Destination wire address.
  pub(crate) to: SocketAddr,
  /// One or more reliable user-message payloads to deliver to `to`.
  pub(crate) payloads: Vec<Bytes>,
  /// Replies with `Ok(())` once all exchanges complete, or an error.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::SetCompressionOptions`].
#[cfg(compression)]
pub(crate) struct SetCompressionOptionsCmd {
  /// The new compression policy to apply.
  pub(crate) opts: CompressionOptions,
  /// Replies with `Ok(())` once applied, or `Err(NotRunning)`.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::SetChecksumOptions`].
#[cfg(checksum)]
pub(crate) struct SetChecksumOptionsCmd {
  /// The new gossip (unreliable) checksum policy to apply.
  pub(crate) opts: ChecksumOptions,
  /// Replies with `Ok(())` once applied, or `Err(NotRunning)`.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::SetEncryptionOptions`].
#[cfg(encryption)]
pub(crate) struct SetEncryptionOptionsCmd {
  /// The new encryption policy to apply (validated before applying).
  pub(crate) opts: EncryptionOptions,
  /// Replies with `Ok(())` once applied, `Err(NotRunning)`, or a
  /// keyring-validation error.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::UpdateNodeMetadata`].
pub(crate) struct UpdateNodeMetadataCmd {
  /// The new metadata bytes, validated against the meta cap on apply.
  pub(crate) meta: Vec<u8>,
  /// Replies with `Ok(())` once applied, `Err(NotRunning)`, or a size error.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::QueueUserBroadcast`].
pub(crate) struct QueueUserBroadcastCmd {
  /// The user-broadcast bytes to gossip cluster-wide.
  pub(crate) data: Bytes,
  /// Replies with `Ok(())` once queued, `Err(NotRunning)`, or a size error.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::SetLocalState`].
pub(crate) struct SetLocalStateCmd {
  /// The push/pull application state snapshot.
  pub(crate) state: Bytes,
  /// Replies with `Ok(())` once set, `Err(NotRunning)`, or a size error.
  pub(crate) reply: Sender<Result<(), Error>>,
}

/// Payload of [`Command::SetAckPayload`].
pub(crate) struct SetAckPayloadCmd {
  /// The payload attached to outbound probe acks.
  pub(crate) payload: Bytes,
  /// Replies with `Ok(())` once set, `Err(NotRunning)`, or a size error.
  pub(crate) reply: Sender<Result<(), Error>>,
}
