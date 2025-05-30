use std::{borrow::Cow, future::Future};

use crate::proto::Meta;
use bytes::Bytes;

/// Used to manage node related events.
#[auto_impl::auto_impl(Box, Arc)]
pub trait NodeDelegate: Send + Sync + 'static {
  /// Used to retrieve meta-data about the current node
  /// when broadcasting an alive message. It's length is limited to
  /// the given byte size. This metadata is available in the NodeState structure.
  fn node_meta(&self, limit: usize) -> impl Future<Output = Meta> + Send {
    let _ = limit;
    async move { Meta::empty() }
  }

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  fn notify_message(&self, msg: Cow<'_, [u8]>) -> impl Future<Output = ()> + Send {
    async move {
      let _ = msg;
    }
  }

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  ///
  /// The `encoded_len` function accepts a bytes, and will return
  /// the same bytes back and the encoded length of the message.
  fn broadcast_messages<F>(
    &self,
    limit: usize,
    encoded_len: F,
  ) -> impl Future<Output = impl Iterator<Item = Bytes> + Send> + Send
  where
    F: Fn(Bytes) -> (usize, Bytes) + Send + Sync + 'static,
  {
    let _ = limit;
    let _ = encoded_len;
    async move { std::iter::empty() }
  }

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See `merge_remote_state` as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  fn local_state(&self, join: bool) -> impl Future<Output = Bytes> + Send {
    let _ = join;
    async move { Bytes::new() }
  }

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's `local_state` call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  fn merge_remote_state(&self, buf: &[u8], join: bool) -> impl Future<Output = ()> + Send {
    let _ = buf;
    let _ = join;
    async move {}
  }
}
