use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

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

/// Ping request sent directly to node
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Ping {
  seq_no: u32,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect again an agent
  /// restart with a new name.
  node: String,

  /// Source address, used for a direct reply
  source_addr: SocketAddr,
  /// Source name, used for a direct reply
  source_node: String,
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct IndirectPingRequest {
  seq_no: u32,
  target: SocketAddr,
  /// Node is sent so the target can verify they are
  /// the intended recipient. This is to protect against an agent
  /// restart with a new name.
  node: String,

  /// true if we'd like a nack back
  nack: bool,

  /// Source address, used for a direct reply
  source_addr: SocketAddr,
  /// Source name, used for a direct reply
  source_node: String,
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
  err: String,
}
