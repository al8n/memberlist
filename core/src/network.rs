use std::{
  net::{Ipv4Addr, SocketAddr, SocketAddrV4},
  sync::Arc,
  time::Duration,
};

use bytes::Bytes;
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use showbiz_traits::Transport;
use showbiz_types::{hidden::*, MessageType, NodeState, SmolStr};

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
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionAlgo {
  #[default]
  LZW = 0,
  None = 1,
}

impl CompressionAlgo {
  pub fn is_none(&self) -> bool {
    matches!(self, Self::None)
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct Compress {
  algo: CompressionAlgo,
  buf: Bytes,
}

#[viewit::viewit]
pub(crate) struct RemoteNodeState {
  join: bool,
  push_states: Vec<PushNodeState>,
  user_state: Bytes,
}

// impl Showbiz {
//   fn stream_listen(&self) {}
// }
