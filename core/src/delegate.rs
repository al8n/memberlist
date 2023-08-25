use std::sync::Arc;

use bytes::Bytes;

use crate::types::Message;
use crate::types::{Node, NodeId};

#[cfg(any(test, feature = "test"))]
mod mock;
#[cfg(any(test, feature = "test"))]
pub use mock::*;

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
pub trait Delegate: Send + Sync + 'static {
  /// The error type of the delegate
  type Error: std::error::Error + Send + Sync + 'static;

  /// Used to retrieve meta-data about the current node
  /// when broadcasting an alive message. It's length is limited to
  /// the given byte size. This metadata is available in the Node structure.
  fn node_meta(&self, limit: usize) -> Bytes;

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(not(feature = "nightly"))]
  async fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error>;

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  #[cfg(not(feature = "nightly"))]
  async fn get_broadcasts(
    &self,
    overhead: usize,
    limit: usize,
  ) -> Result<Vec<Message>, Self::Error>;

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See MergeRemoteState as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(not(feature = "nightly"))]
  async fn local_state(&self, join: bool) -> Result<Bytes, Self::Error>;

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's LocalState call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(not(feature = "nightly"))]
  async fn merge_remote_state(&self, buf: &[u8], join: bool) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(not(feature = "nightly"))]
  async fn notify_join(&self, node: Arc<Node>) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have left the cluster
  #[cfg(not(feature = "nightly"))]
  async fn notify_leave(&self, node: Arc<Node>) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(not(feature = "nightly"))]
  async fn notify_update(&self, node: Arc<Node>) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "nightly"))]
  async fn notify_alive(&self, peer: Arc<Node>) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "nightly"))]
  async fn notify_conflict(&self, existing: Arc<Node>, other: Arc<Node>)
    -> Result<(), Self::Error>;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, peers: Vec<Arc<Node>>) -> Result<(), Self::Error>;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(not(feature = "nightly"))]
  async fn ack_payload(&self) -> Result<Bytes, Self::Error>;

  /// Invoked when an ack for a ping is received
  #[cfg(not(feature = "nightly"))]
  async fn notify_ping_complete(
    &self,
    node: Arc<Node>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error>;

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(feature = "nightly")]
  fn notify_user_msg<'a>(
    &'a self,
    msg: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  #[cfg(feature = "nightly")]
  fn get_broadcasts<'a>(
    &'a self,
    overhead: usize,
    limit: usize,
  ) -> impl Future<Output = Result<Vec<Message>, Self::Error>> + Send + 'a;

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See MergeRemoteState as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(feature = "nightly")]
  fn local_state<'a>(
    &'a self,
    join: bool,
  ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'a;

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's LocalState call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(feature = "nightly")]
  fn merge_remote_state<'a>(
    &'a self,
    buf: Bytes,
    join: bool,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(feature = "nightly")]
  fn notify_join<'a>(
    &'a self,
    node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when a node is detected to have left the cluster
  #[cfg(feature = "nightly")]
  fn notify_leave<'a>(
    &'a self,
    node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(feature = "nightly")]
  fn notify_update<'a>(
    &'a self,
    node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when a name conflict is detected
  #[cfg(feature = "nightly")]
  fn notify_alive<'a>(
    &'a self,
    peer: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when a name conflict is detected
  #[cfg(feature = "nightly")]
  fn notify_conflict<'a>(
    &'a self,
    existing: Arc<Node>,
    other: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(feature = "nightly")]
  fn notify_merge<'a>(
    &'a self,
    peers: Vec<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(feature = "nightly")]
  fn ack_payload<'a>(&'a self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'a;

  /// Invoked when an ack for a ping is received
  #[cfg(feature = "nightly")]
  fn notify_ping_complete<'a>(
    &'a self,
    node: Arc<Node>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

  /// Invoked when we want to send a ping message to target by reliable connection. Return true if the target node does not expect ping message from reliable connection.
  fn disable_reliable_pings(&self, target: &NodeId) -> bool;
}

// impl<D: Delegate> Delegate for Arc<D> {

// }

#[derive(Debug, Copy, Clone)]
pub struct VoidDelegateError;

impl std::fmt::Display for VoidDelegateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "void delegate error")
  }
}

impl std::error::Error for VoidDelegateError {}

#[derive(Debug, Copy, Clone)]
pub struct VoidDelegate;

impl Default for VoidDelegate {
  fn default() -> Self {
    Self
  }
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl Delegate for VoidDelegate {
  type Error = VoidDelegateError;

  fn node_meta(&self, _limit: usize) -> Bytes {
    Bytes::new()
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_user_msg(&self, _msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn get_broadcasts(
    &self,
    _overhead: usize,
    _limit: usize,
  ) -> Result<Vec<Message>, Self::Error> {
    Ok(Vec::new())
  }

  #[cfg(not(feature = "nightly"))]
  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  #[cfg(not(feature = "nightly"))]
  async fn merge_remote_state(&self, _buf: &[u8], _join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_join(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_leave(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_update(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_alive(&self, _peer: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_conflict(
    &self,
    _existing: Arc<Node>,
    _other: Arc<Node>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, _peers: Vec<Arc<Node>>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_ping_complete(
    &self,
    _node: Arc<Node>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(feature = "nightly")]
  fn notify_user_msg<'a>(
    &'a self,
    _msg: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn get_broadcasts<'a>(
    &'a self,
    _overhead: usize,
    _limit: usize,
  ) -> impl Future<Output = Result<Vec<Message>, Self::Error>> + Send + 'a {
    async move { Ok(Vec::new()) }
  }

  #[cfg(feature = "nightly")]
  fn local_state<'a>(
    &'a self,
    _join: bool,
  ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'a {
    async move { Ok(Bytes::new()) }
  }

  #[cfg(feature = "nightly")]
  fn merge_remote_state<'a>(
    &'a self,
    _buf: Bytes,
    _join: bool,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_join<'a>(
    &'a self,
    _node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_leave<'a>(
    &'a self,
    _node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_update<'a>(
    &'a self,
    _node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_alive<'a>(
    &'a self,
    _peer: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_conflict<'a>(
    &'a self,
    _existing: Arc<Node>,
    _other: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_merge<'a>(
    &'a self,
    _peers: Vec<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn ack_payload<'a>(&'a self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'a {
    async move {}
  }

  #[cfg(feature = "nightly")]
  fn notify_ping_complete<'a>(
    &'a self,
    _node: Arc<Node>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[inline]
  fn disable_reliable_pings(&self, _node: &NodeId) -> bool {
    false
  }
}
