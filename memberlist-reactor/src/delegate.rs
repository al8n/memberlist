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
mod tests {
  use std::{
    net::SocketAddr,
    pin::pin,
    task::{Context, Poll, Waker},
    time::Duration,
  };

  use memberlist_proto::typed::{NodeState, State};
  use smol_str::SmolStr;

  use super::*;

  /// Dependency-free single-poll driver for the trivially-ready default hooks.
  fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(fut);
    loop {
      if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
        return out;
      }
    }
  }

  fn node() -> Arc<NodeState<SmolStr, SocketAddr>> {
    Arc::new(NodeState::new(
      SmolStr::new("peer"),
      SocketAddr::from(([127, 0, 0, 1], 1)),
      State::Alive,
    ))
  }

  /// Every default `Delegate` hook is a no-op that completes immediately; drive
  /// each one through `VoidDelegate` so the default bodies are exercised.
  #[test]
  fn void_delegate_default_hooks_are_noops() {
    let d = VoidDelegate::<SmolStr, SocketAddr>::new();
    let n = node();
    block_on(d.notify_join(n.clone()));
    block_on(d.notify_leave(n.clone()));
    block_on(d.notify_update(n.clone()));
    block_on(d.notify_conflict(n.clone(), n.clone()));
    block_on(d.notify_ping_complete(
      SmolStr::new("peer"),
      SocketAddr::from(([127, 0, 0, 1], 1)),
      Duration::from_millis(3),
      Bytes::from_static(b"ack"),
    ));
    block_on(d.notify_user_msg(Bytes::from_static(b"data")));
    block_on(d.merge_remote_state(Bytes::from_static(b"state"), true));
  }

  /// `VoidDelegate` is `Default` and the `Default::default` path matches `new`.
  #[test]
  fn void_delegate_default_constructs() {
    let _ = VoidDelegate::<SmolStr, SocketAddr>::default();
    let _ = VoidDelegate::<u64, SocketAddr>::new();
  }

  /// A custom delegate may override a subset of hooks; the un-overridden hooks
  /// fall back to the default no-op bodies (driven here for coverage).
  #[test]
  fn custom_delegate_partial_override_uses_defaults() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting {
      joins: Arc<AtomicUsize>,
    }

    impl Delegate for Counting {
      type Id = SmolStr;
      type Address = SocketAddr;

      fn notify_join(
        &self,
        _node: Arc<NodeState<SmolStr, SocketAddr>>,
      ) -> impl Future<Output = ()> + Send + '_ {
        let joins = self.joins.clone();
        async move {
          joins.fetch_add(1, Ordering::Relaxed);
        }
      }
    }

    let joins = Arc::new(AtomicUsize::new(0));
    let d = Counting {
      joins: joins.clone(),
    };
    let n = node();
    block_on(d.notify_join(n.clone()));
    assert_eq!(joins.load(Ordering::Relaxed), 1, "the override ran");
    // The remaining hooks fall back to the trait defaults.
    block_on(d.notify_leave(n.clone()));
    block_on(d.notify_update(n));
    block_on(d.notify_user_msg(Bytes::from_static(b"x")));
    assert_eq!(
      joins.load(Ordering::Relaxed),
      1,
      "default hooks do not touch the override's counter"
    );
  }
}
