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
