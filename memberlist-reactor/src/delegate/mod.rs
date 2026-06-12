//! The observation [`Delegate`] — async hooks the driver's observation task
//! invokes as membership and application events surface.

use std::{future::Future, marker::PhantomData, sync::Arc, time::Duration};

use bytes::Bytes;
use memberlist_proto::typed::NodeState;

/// Observation hooks, invoked off the driver loop by the observation task as
/// events surface.
///
/// Every hook has a default no-op body — implement only the ones you need. Hooks
/// are written `-> impl Future<Output = ()> + Send + '_` (not `async fn`) so the
/// observation task can run on a multi-threaded runtime; the delegate as a whole
/// is `Send + Sync + 'static`.
pub trait Delegate: Send + Sync + 'static {
  /// Node-identity type.
  type Id;
  /// Resolved-address type.
  type Address;

  /// A peer became alive — newly joined, or returned from dead/left.
  fn notify_join(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let _ = node;
    async {}
  }

  /// A peer left gracefully or was reaped after suspicion.
  fn notify_leave(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let _ = node;
    async {}
  }

  /// A peer's metadata changed (same id + address).
  fn notify_update(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let _ = node;
    async {}
  }

  /// Two peers claim the same id with different advertised addresses.
  fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let _ = (existing, other);
    async {}
  }

  /// A probe ping round-trip completed, with its RTT and the peer's ack payload.
  fn notify_ping_complete(
    &self,
    peer_id: Self::Id,
    peer_addr: Self::Address,
    rtt: Duration,
    payload: Bytes,
  ) -> impl Future<Output = ()> + Send + '_ {
    let _ = (peer_id, peer_addr, rtt, payload);
    async {}
  }

  /// An application user-data packet arrived (delivered here, not via the event
  /// stream).
  fn notify_user_msg(&self, msg: Bytes) -> impl Future<Output = ()> + Send + '_ {
    let _ = msg;
    async {}
  }

  /// A peer's push/pull application state arrived; `join` marks a join-time
  /// exchange.
  fn merge_remote_state(&self, state: Bytes, join: bool) -> impl Future<Output = ()> + Send + '_ {
    let _ = (state, join);
    async {}
  }
}

/// A no-op [`Delegate`] for callers that observe membership through the snapshot
/// or the [`EventStream`](crate::EventStream) only.
pub struct VoidDelegate<I, A>(PhantomData<fn(I, A)>);

impl<I, A> VoidDelegate<I, A> {
  /// Creates a no-op delegate.
  #[must_use]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<I, A> Default for VoidDelegate<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> Delegate for VoidDelegate<I, A>
where
  I: Send + Sync + 'static,
  A: Send + Sync + 'static,
{
  type Id = I;
  type Address = A;
}

#[cfg(test)]
mod tests;
