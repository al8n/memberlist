//! Driver commands — a `Memberlist` handle pushes these onto the shared queue;
//! the driver drains and dispatches them, replying on each command's oneshot.

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use flume::Sender;
use memberlist_proto::Node;

use crate::error::Error;

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
}

/// Payload of [`Command::Join`].
pub(crate) struct JoinCmd {
  /// Already-resolved seed addresses to contact (one push/pull each).
  pub(crate) addrs: Vec<SocketAddr>,
  /// Wait for every dispatched exchange to complete (replying the contacted
  /// count), or reply immediately with the dispatched count.
  pub(crate) wait: bool,
  /// Replies with the number of seeds contacted, or an error.
  pub(crate) reply: Sender<Result<usize, Error>>,
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
