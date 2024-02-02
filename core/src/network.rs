use std::time::Duration;

use bytes::Bytes;

use crate::{memberlist::Memberlist, transport::Transport, types::*};

mod r#async;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;
