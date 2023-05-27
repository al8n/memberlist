use std::sync::Arc;

use bytes::Bytes;

use crate::types::{Message, Node};

#[cfg_attr(feature = "async", async_trait::async_trait)]
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
  #[cfg(not(feature = "async"))]
  fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error>;

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(feature = "async")]
  async fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error>;

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  #[cfg(feature = "async")]
  async fn get_broadcasts(
    &self,
    overhead: usize,
    limit: usize,
  ) -> Result<Vec<Message>, Self::Error>;

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  #[cfg(not(feature = "async"))]
  fn get_broadcasts(&self, overhead: usize, limit: usize) -> Result<Vec<Message>, Self::Error>;

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

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(not(feature = "async"))]
  fn notify_join(&self, node: &Node) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(feature = "async")]
  async fn notify_join(&self, node: &Node) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have left the cluster
  #[cfg(not(feature = "async"))]
  fn notify_leave(&self, node: Arc<Node>) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have left the cluster
  #[cfg(feature = "async")]
  async fn notify_leave(&self, node: Arc<Node>) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(not(feature = "async"))]
  fn notify_update(&self, node: &Node) -> Result<(), Self::Error>;

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(feature = "async")]
  async fn notify_update(&self, node: &Node) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_conflict(&self, existing: bool, other: &Node) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_conflict(&self, existing: bool, other: &Node) -> Result<(), Self::Error>;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(not(feature = "async"))]
  fn notify_merge(&self, peers: Vec<Node>) -> Result<(), Self::Error>;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(feature = "async")]
  async fn notify_merge(&self, peers: Vec<Node>) -> Result<(), Self::Error>;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(not(feature = "async"))]
  fn ack_payload(&self) -> Result<Bytes, Self::Error>;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(feature = "async")]
  async fn ack_payload(&self) -> Result<Bytes, Self::Error>;

  /// Invoked when an ack for a ping is received
  #[cfg(not(feature = "async"))]
  fn notify_ping_complete(
    &self,
    node: &Node,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error>;

  /// Invoked when an ack for a ping is received
  #[cfg(feature = "async")]
  async fn notify_ping_complete(
    &self,
    node: &Node,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error>;
}

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

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl Delegate for VoidDelegate {
  type Error = VoidDelegateError;

  /// Used to retrieve meta-data about the current node
  /// when broadcasting an alive message. It's length is limited to
  /// the given byte size. This metadata is available in the Node structure.
  fn node_meta(&self, limit: usize) -> Bytes {
    Bytes::new()
  }

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(not(feature = "async"))]
  fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  #[cfg(feature = "async")]
  async fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  #[cfg(feature = "async")]
  async fn get_broadcasts(
    &self,
    overhead: usize,
    limit: usize,
  ) -> Result<Vec<Message>, Self::Error> {
    Ok(Vec::new())
  }

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  #[cfg(not(feature = "async"))]
  fn get_broadcasts(&self, overhead: usize, limit: usize) -> Result<Vec<Message>, Self::Error> {
    Ok(Vec::new())
  }

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See MergeRemoteState as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(feature = "async")]
  async fn local_state(&self, join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See MergeRemoteState as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(not(feature = "async"))]
  fn local_state(&self, join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's LocalState call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(feature = "async")]
  async fn merge_remote_state(&self, buf: Bytes, join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's LocalState call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  #[cfg(not(feature = "async"))]
  fn merge_remote_state(&self, buf: Bytes, join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(not(feature = "async"))]
  fn notify_join(&self, node: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(feature = "async")]
  async fn notify_join(&self, node: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a node is detected to have left the cluster
  #[cfg(not(feature = "async"))]
  fn notify_leave(&self, node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a node is detected to have left the cluster
  #[cfg(feature = "async")]
  async fn notify_leave(&self, node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(not(feature = "async"))]
  fn notify_update(&self, node: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(feature = "async")]
  async fn notify_update(&self, node: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_conflict(&self, existing: bool, other: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_conflict(&self, existing: bool, other: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(not(feature = "async"))]
  fn notify_merge(&self, peers: Vec<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(feature = "async")]
  async fn notify_merge(&self, peers: Vec<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(not(feature = "async"))]
  fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(feature = "async")]
  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  /// Invoked when an ack for a ping is received
  #[cfg(not(feature = "async"))]
  fn notify_ping_complete(
    &self,
    node: &Node,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  /// Invoked when an ack for a ping is received
  #[cfg(feature = "async")]
  async fn notify_ping_complete(
    &self,
    node: &Node,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}
