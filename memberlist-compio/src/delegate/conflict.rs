//! `ConflictDelegate` — async conflict hook.

use std::sync::Arc;

use memberlist_proto::typed::NodeState;

// `async fn` in trait is intentional: compio is `!Send`-first, so we want
// the bare-future desugaring, not `-> impl Future + Send`.
#[allow(async_fn_in_trait)]
/// Async hook fired when a peer's name conflicts with an existing peer.
pub trait ConflictDelegate: 'static {
  /// Node identifier type.
  type Id;
  /// Address type.
  type Address;

  /// Called with the existing peer and the conflicting incoming peer.
  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
    let _ = (existing, other); // Unused: default no-op; override to handle.
  }
}
