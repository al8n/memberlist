//! Driver commands — a `Memberlist` handle pushes these onto the shared queue;
//! the driver drains and dispatches them, replying on each command's oneshot.

use std::net::SocketAddr;

use flume::Sender;

use crate::error::Error;

/// A command from a `Memberlist` handle to its backend driver.
pub(crate) enum Command {
  /// Contact the resolved seed addresses and merge their state.
  Join(JoinCmd),
  /// Gracefully leave the cluster, then stop.
  Leave(LeaveCmd),
  /// Stop the driver and release its socket.
  Shutdown(ShutdownCmd),
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
