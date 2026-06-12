use super::*;
use std::net::SocketAddr;

use memberlist_proto::typed::State;
use smol_str::SmolStr;

/// A type that adopts the trait's DEFAULT `notify_conflict` (no override),
/// so awaiting it exercises the default no-op body.
struct DefaultConflict;

impl ConflictDelegate for DefaultConflict {
  type Id = SmolStr;
  type Address = SocketAddr;
}

fn block_on<F: core::future::Future>(mut fut: F) -> F::Output {
  use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
  fn raw() -> RawWaker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
      raw()
    }
    RawWaker::new(
      core::ptr::null(),
      &RawWakerVTable::new(clone, no_op, no_op, no_op),
    )
  }
  // SAFETY: every vtable entry is a no-op / re-derives the same raw waker;
  // there is no shared state.
  let waker = unsafe { Waker::from_raw(raw()) };
  let mut cx = Context::from_waker(&waker);
  let mut fut = unsafe { core::pin::Pin::new_unchecked(&mut fut) };
  loop {
    match fut.as_mut().poll(&mut cx) {
      Poll::Ready(v) => return v,
      Poll::Pending => {}
    }
  }
}

// The default `notify_conflict` is a no-op that completes immediately when
// awaited — driving it covers the default trait-method body.
#[test]
fn default_notify_conflict_is_a_noop_that_completes() {
  let d = DefaultConflict;
  let existing = Arc::new(NodeState::new(
    SmolStr::new("dup"),
    "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
    State::Alive,
  ));
  let other = Arc::new(NodeState::new(
    SmolStr::new("dup"),
    "127.0.0.1:2".parse::<SocketAddr>().unwrap(),
    State::Alive,
  ));
  block_on(d.notify_conflict(existing, other));
  // Reaching here means the default no-op future resolved.
}
