use std::{future::Future, sync::Arc};

use bytes::Bytes;
use nodecraft::{CheapClone, Id};

use crate::types::NodeState;

/// Used to notify an observer how long it took for a ping message to
/// complete a round trip.  It can also be used for writing arbitrary byte slices
/// into ack messages. Note that in order to be meaningful for RTT estimates, this
/// delegate does not apply to indirect pings, nor fallback pings sent over promised connection.
#[auto_impl::auto_impl(Box, Arc)]
pub trait PingDelegate: Send + Sync + 'static {
  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  fn ack_payload(&self) -> impl Future<Output = Bytes> + Send;

  /// Invoked when an ack for a ping is received
  fn notify_ping_complete(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> impl Future<Output = ()> + Send;

  /// Invoked when we want to send a ping message to target by promised connection.
  /// Return true if the target node does not expect ping message from promised connection.
  fn disable_reliable_pings(&self, _target: &Self::Id) -> bool {
    false
  }
}
