use std::{future::Future, sync::Arc};

use memberlist_utils::SmallVec;
use nodecraft::{CheapClone, Id};

use crate::types::NodeState;

/// Used to involve a client in
/// a potential cluster merge operation. Namely, when
/// a node does a promised push/pull (as part of a join),
/// the delegate is involved and allowed to cancel the join
/// based on custom logic. The merge delegate is NOT invoked
/// as part of the push-pull anti-entropy.
#[auto_impl::auto_impl(Box, Arc)]
pub trait MergeDelegate: Send + Sync + 'static {
  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;

  /// The error type of the delegate
  type Error: std::error::Error + Send + Sync + 'static;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer. If
  /// the return value is `Err`, the merge is canceled.
  fn notify_merge(
    &self,
    peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
