use std::sync::Arc;

use bytes::Bytes;
use futures::Future;
use nodecraft::{Address, CheapClone, Id};

use crate::types::{Server, SmallVec};

#[cfg(any(test, feature = "test"))]
mod mock;
#[cfg(any(test, feature = "test"))]
pub use mock::*;

pub trait Delegate: Send + Sync + 'static {
  /// The error type of the delegate
  type Error: std::error::Error + Send + Sync + 'static;

  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;

  /// Used to retrieve meta-data about the current node
  /// when broadcasting an alive message. It's length is limited to
  /// the given byte size. This metadata is available in the Server structure.
  fn node_meta(&self, limit: usize) -> impl Future<Output = Bytes> + Send;

  /// Called when a user-data message is received.
  /// Care should be taken that this method does not block, since doing
  /// so would block the entire UDP packet receive loop. Additionally, the byte
  /// slice may be modified after the call returns, so it should be copied if needed
  fn notify_message(&self, msg: Bytes) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Called when user data messages can be broadcast.
  /// It can return a list of buffers to send. Each buffer should assume an
  /// overhead as provided with a limit on the total byte size allowed.
  /// The total byte size of the resulting data to send must not exceed
  /// the limit. Care should be taken that this method does not block,
  /// since doing so would block the entire UDP packet receive loop.
  ///
  /// The `encoded_len` function accepts a user data message, and will return
  /// the same message back and the encoded length of the message calculated by
  /// [`Transport::Wire`].
  ///
  /// [`Transport::Wire`]: trait.Transport.html#associatedtype.Wire
  fn broadcast_messages<F>(
    &self,
    overhead: usize,
    limit: usize,
    encoded_len: F,
  ) -> impl Future<Output = Result<Vec<Bytes>, Self::Error>> + Send
  where
    F: Fn(Bytes) -> (usize, Bytes) + Send;

  /// Used for a TCP Push/Pull. This is sent to
  /// the remote side in addition to the membership information. Any
  /// data can be sent here. See `merge_remote_state` as well. The `join`
  /// boolean indicates this is for a join instead of a push/pull.
  fn local_state(&self, join: bool) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;

  /// Invoked after a TCP Push/Pull. This is the
  /// state received from the remote side and is the result of the
  /// remote side's `local_state` call. The 'join'
  /// boolean indicates this is for a join instead of a push/pull.
  fn merge_remote_state(
    &self,
    buf: Bytes,
    join: bool,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when a node is detected to have joined the cluster
  fn notify_join(
    &self,
    node: Arc<Server<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when a node is detected to have left the cluster
  fn notify_leave(
    &self,
    node: Arc<Server<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  fn notify_update(
    &self,
    node: Arc<Server<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when a name conflict is detected
  fn notify_alive(
    &self,
    peer: Arc<Server<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when a name conflict is detected
  fn notify_conflict(
    &self,
    existing: Arc<Server<Self::Id, Self::Address>>,
    other: Arc<Server<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  fn notify_merge(
    &self,
    peers: SmallVec<Arc<Server<Self::Id, Self::Address>>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  fn ack_payload(&self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;

  /// Invoked when an ack for a ping is received
  fn notify_ping_complete(
    &self,
    node: Arc<Server<Self::Id, Self::Address>>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Invoked when we want to send a ping message to target by promised connection. Return true if the target node does not expect ping message from promised connection.
  fn disable_promised_pings(&self, target: &Self::Id) -> bool;
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
pub struct VoidDelegate<I, A>(core::marker::PhantomData<(I, A)>);

impl<I, A> Default for VoidDelegate<I, A> {
  fn default() -> Self {
    Self(core::marker::PhantomData)
  }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> Delegate for VoidDelegate<I, A> {
  type Error = VoidDelegateError;
  type Id = I;
  type Address = A;

  async fn node_meta(&self, _limit: usize) -> Bytes {
    Bytes::new()
  }

  async fn notify_message(&self, _msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn broadcast_messages<F>(
    &self,
    _overhead: usize,
    _limit: usize,
    _encoded_len: F,
  ) -> Result<Vec<Bytes>, Self::Error>
  where
    F: Fn(Bytes) -> (usize, Bytes) + Send,
  {
    Ok(Vec::new())
  }

  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  async fn merge_remote_state(&self, _buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_join(
    &self,
    _node: Arc<Server<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_leave(
    &self,
    _node: Arc<Server<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_update(
    &self,
    _node: Arc<Server<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_alive(
    &self,
    _peer: Arc<Server<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_conflict(
    &self,
    _existing: Arc<Server<Self::Id, Self::Address>>,
    _other: Arc<Server<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_merge(
    &self,
    _peers: SmallVec<Arc<Server<Self::Id, Self::Address>>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  async fn notify_ping_complete(
    &self,
    _node: Arc<Server<Self::Id, Self::Address>>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  #[inline]
  fn disable_promised_pings(&self, _node: &Self::Id) -> bool {
    false
  }
}
