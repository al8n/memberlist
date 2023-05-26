use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;

use showbiz_traits::Transport;

use crate::{showbiz::Showbiz, types::*};

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
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

#[viewit::viewit]
pub(crate) struct RemoteNodeState {
  join: bool,
  push_states: Vec<PushNodeState>,
  user_state: Bytes,
}

// impl Showbiz {
//   fn stream_listen(&self) {}
// }
