use std::{net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use showbiz_traits::Transport;
use showbiz_types::{NodeStateType, SmolStr};

use crate::showbiz::Showbiz;

#[cfg(feature = "async")]
mod r#async;

#[cfg(feature = "sync")]
mod sync;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Assumed header overhead
const COMPOUND_HEADER_OVERHEAD: usize = 2;

/// Assumed overhead per entry in compound header
const COMPOUND_OVERHEAD: usize = 2;

const USER_MSG_OVERHEAD: usize = 1;

/// Warn if a UDP packet takes this long to process
const BLOCKING_WARNING: Duration = Duration::from_millis(10);

const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;
/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: usize = 128;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
  LZW,
}

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Ping {
  seq_no: u32,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  node: SmolStr,

  /// Source address, used for a direct reply
  source_addr: SocketAddr,
  /// Source name, used for a direct reply
  source_node: SmolStr,
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct IndirectPingRequest {
  seq_no: u32,
  target: SocketAddr,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect against an agent
  /// restart with a new name.
  node: SmolStr,

  /// true if we'd like a nack back
  nack: bool,

  /// Source address, used for a direct reply
  source_addr: SocketAddr,
  /// Source name, used for a direct reply
  source_node: SmolStr,
}

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct AckResponse {
  seq_no: u32,
  payload: bytes::Bytes,
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub(crate) struct NackResponse {
  seq_no: u32,
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub(crate) struct ErrorResponse {
  err: SmolStr,
}

/// suspect is broadcast when we suspect a node is dead
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Suspect {
  incarnation: u32,
  node: SmolStr,
  from: SmolStr,
}

/// Alive is broadcast when we know a node is alive.
/// Overloaded for nodes joining
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Alive {
  incarnation: u32,
  node: SmolStr,
  addr: SocketAddr,
  meta: Bytes,
  // The versions of the protocol/delegate that are being spoken, order:
  // pmin, pmax, pcur, dmin, dmax, dcur
  vsn: [u8; 6],
}

/// Dead is broadcast when we confirm a node is dead
/// Overloaded for nodes leaving
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Dead {
  incarnation: u32,
  node: SmolStr,
  from: SmolStr, // Include who is suspecting
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct PushPullHeader {
  nodes: usize,
  user_state_len: usize, // Encodes the byte lengh of user state
  join: bool,            // Is this a join request or a anti-entropy run
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct UserMsgHeader {
  user_msg_len: usize, // Encodes the byte lengh of user state
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct PushNodeState {
  name: SmolStr,
  addr: SocketAddr,
  meta: Bytes,
  incarnation: u32,
  state: NodeStateType,
  vsn: [u8; 6],
}

impl Showbiz {
  fn stream_listen(&self) {}
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Compress {
  algo: CompressionType,
  buf: Bytes,
}
