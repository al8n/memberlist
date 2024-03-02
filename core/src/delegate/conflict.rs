use std::{future::Future, sync::Arc};

use nodecraft::{CheapClone, Id};

use crate::types::NodeState;

/// Used to inform a client that
/// a node has attempted to join which would result in a
/// name conflict. This happens if two clients are configured
/// with the same name but different addresses.
#[auto_impl::auto_impl(Box, Arc)]
pub trait ConflictDelegate: Send + Sync + 'static {
  /// The id type of the delegate
  type Id: Id;

  /// The address type of the delegate
  type Address: CheapClone + Send + Sync + 'static;

  /// Invoked when a name conflict is detected
  fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send;
}
