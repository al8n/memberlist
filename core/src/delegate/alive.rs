use std::{future::Future, sync::Arc};

use nodecraft::{CheapClone, Id};

use crate::types::NodeState;

/// Used to involve a client in processing
/// a node "alive" message. When a node joins, either through
/// a packet gossip or promised push/pull, we update the state of
/// that node via an alive message. This can be used to filter
/// a node out and prevent it from being considered a peer
/// using application specific logic.
#[auto_impl::auto_impl(Box, Arc)]
pub trait AliveDelegate: Send + Sync + 'static {
  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;

  /// The error type of the delegate
  type Error: std::error::Error + Send + Sync + 'static;

  /// Invoked when a message about a live
  /// node is received from the network.  Returning a non-nil
  /// error prevents the node from being considered a peer.
  fn notify_alive(
    &self,
    peer: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
