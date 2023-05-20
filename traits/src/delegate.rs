use std::marker::PhantomData;

use bytes::Bytes;

/// The trait that clients must implement if they want to hook
/// into the gossip layer of Memberlist. All the methods must be thread-safe,
/// as they can and generally will be called concurrently.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait Delegate: Send + Sync + 'static {
  type Error: std::error::Error + Send + Sync + 'static;

  /// Used to retrieve meta-data about the current node
  /// when broadcasting an alive message. It's length is limited to
  /// the given byte size. This metadata is available in the Node structure.
  fn node_meta(&self, limit: usize) -> Bytes;

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(not(feature = "async"))]
  fn notify_msg(&self, msg: Bytes) -> Result<(), Self::Error>;

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(feature = "async")]
  async fn notify_msg(&self, msg: Bytes) -> Result<(), Self::Error>;

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Bytes>;

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See MergeRemoteState as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(feature = "async")]
  async fn local_state(&self, join: bool) -> Result<Bytes, Self::Error>;

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See MergeRemoteState as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(not(feature = "async"))]
  fn local_state(&self, join: bool) -> Result<Bytes, Self::Error>;

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's LocalState call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(feature = "async")]
  async fn merge_remote_state(&self, buf: Bytes, join: bool) -> Result<(), Self::Error>;

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's LocalState call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(not(feature = "async"))]
  fn merge_remote_state(&self, buf: Bytes, join: bool) -> Result<(), Self::Error>;
}

/// No-op implementation of Delegate
#[derive(Debug, Default, Clone, Copy)]
pub struct VoidDelegate<E: std::error::Error + Send + Sync + 'static>(PhantomData<E>);

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<E> Delegate for VoidDelegate<E>
where
  E: std::error::Error + Send + Sync + 'static,
{
  type Error = E;

  #[inline(always)]
  fn node_meta(&self, _limit: usize) -> Bytes {
    Bytes::new()
  }

  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn notify_msg(&self, _msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn notify_msg(&self, _msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  #[inline(always)]
  fn get_broadcasts(&self, _overhead: usize, _limit: usize) -> Vec<Bytes> {
    Vec::new()
  }

  #[inline(always)]
  #[cfg(feature = "async")]
  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  #[inline(always)]
  #[cfg(not(feature = "async"))]
  fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn merge_remote_state(&self, _buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn merge_remote_state(&self, _buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    Ok(())
  }
}
