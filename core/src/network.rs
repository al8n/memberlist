use std::time::Duration;

use bytes::Bytes;

use crate::{showbiz::Showbiz, transport::Transport, types::*};

mod r#async;
// #[cfg(feature = "test")]
// pub use r#async::tests::*;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

#[viewit::viewit]
pub(crate) struct RemoteServerState<'a, I, A> {
  join: bool,
  push_states: Vec<&'a PushServerState<I, A>>,
  user_state_pos: usize,
  src: Bytes,
}
