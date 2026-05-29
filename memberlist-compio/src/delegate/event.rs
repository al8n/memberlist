//! `EventDelegate` — async event hooks (join/leave/update).

use std::sync::Arc;

use memberlist_wire::typed::NodeState;

// `async fn` in trait is intentional: compio is `!Send`-first, so we want
// the bare-future desugaring, not `-> impl Future + Send`.
#[allow(async_fn_in_trait)]
/// Async event hooks. Methods fire on the driver thread after the machine
/// applies the corresponding state transition. Default impls are no-ops;
/// override what you need.
pub trait EventDelegate: 'static {
  /// Node identifier type.
  type Id;
  /// Address type.
  type Address;

  /// Called when a peer transitions to alive (newly joined).
  async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    let _ = node; // Unused: default no-op; override to handle.
  }

  /// Called when a peer leaves (graceful) or is reaped (dead-after-suspect).
  async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    let _ = node; // Unused: default no-op; override to handle.
  }

  /// Called when a peer's metadata is updated.
  async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    let _ = node; // Unused: default no-op; override to handle.
  }
}
