use super::*;
use std::{
  borrow::Cow,
  cell::{Cell, RefCell},
  net::SocketAddr,
  sync::Arc,
};

use bytes::Bytes;
use memberlist_proto::{
  event::{NodeConflict, Reliability, UserPacket},
  typed::{NodeState, State},
};
use smol_str::SmolStr;

use crate::delegate::{ConflictDelegate, Delegate, EventDelegate, NodeDelegate, PingDelegate};

fn sock(n: u16) -> SocketAddr {
  format!("127.0.0.1:{n}").parse().unwrap()
}

fn node(id: &str, n: u16, state: State) -> Arc<NodeState<SmolStr, SocketAddr>> {
  Arc::new(NodeState::new(SmolStr::new(id), sock(n), state))
}

/// Records which observation hook fired (and the relevant payload) so the
/// dispatcher's per-variant routing can be asserted. `!Send` (`RefCell`) —
/// the driver fires the hooks on one thread, which is exactly how the
/// dispatcher is used.
#[derive(Default)]
struct Recorder {
  log: RefCell<Vec<String>>,
}

impl NodeDelegate for Recorder {
  async fn notify_user_msg(&self, msg: Cow<'_, [u8]>) {
    self
      .log
      .borrow_mut()
      .push(format!("user:{}", String::from_utf8_lossy(msg.as_ref())));
  }

  async fn merge_remote_state(&self, buf: &[u8], join: bool) {
    self
      .log
      .borrow_mut()
      .push(format!("merge:{}:{join}", buf.len()));
  }
}

impl EventDelegate for Recorder {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_join(&self, n: Arc<NodeState<SmolStr, SocketAddr>>) {
    self.log.borrow_mut().push(format!("join:{}", n.id_ref()));
  }

  async fn notify_leave(&self, n: Arc<NodeState<SmolStr, SocketAddr>>) {
    self.log.borrow_mut().push(format!("leave:{}", n.id_ref()));
  }

  async fn notify_update(&self, n: Arc<NodeState<SmolStr, SocketAddr>>) {
    self.log.borrow_mut().push(format!("update:{}", n.id_ref()));
  }
}

impl PingDelegate for Recorder {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for Recorder {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<SmolStr, SocketAddr>>,
    other: Arc<NodeState<SmolStr, SocketAddr>>,
  ) {
    self
      .log
      .borrow_mut()
      .push(format!("conflict:{}:{}", existing.id_ref(), other.id_ref()));
  }
}

impl Delegate for Recorder {
  type Id = SmolStr;
  type Address = SocketAddr;
}

/// Drive a single future to completion on the current thread without a
/// runtime: poll with a no-op waker. The dispatcher / `yield_once` futures
/// never park on real I/O, so a busy-poll resolves them.
fn block_on<F>(mut fut: F) -> F::Output
where
  F: core::future::Future,
{
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
  // SAFETY: the vtable's functions are all no-ops / re-derive the same raw
  // waker; there is no shared state to misuse.
  let waker = unsafe { Waker::from_raw(raw()) };
  let mut cx = Context::from_waker(&waker);
  // The future is pinned to the stack for the duration of the poll loop.
  let mut fut = unsafe { core::pin::Pin::new_unchecked(&mut fut) };
  loop {
    match fut.as_mut().poll(&mut cx) {
      Poll::Ready(v) => return v,
      Poll::Pending => {}
    }
  }
}

// The membership-event variants each route to their matching hook, carrying
// the FSM-resolved node.
#[test]
fn dispatch_routes_membership_events_to_their_hooks() {
  let rec = Recorder::default();

  block_on(dispatch_event_delegate(
    &rec,
    &Event::NodeJoined(node("a", 1, State::Alive)),
  ));
  block_on(dispatch_event_delegate(
    &rec,
    &Event::NodeLeft(node("b", 2, State::Dead)),
  ));
  block_on(dispatch_event_delegate(
    &rec,
    &Event::NodeUpdated(node("c", 3, State::Alive)),
  ));
  block_on(dispatch_event_delegate(
    &rec,
    &Event::NodeConflict(NodeConflict::new(
      node("d", 4, State::Alive),
      node("d", 5, State::Alive),
    )),
  ));

  assert_eq!(
    *rec.log.borrow(),
    vec!["join:a", "leave:b", "update:c", "conflict:d:d"]
  );
}

// `UserPacket` routes to `notify_user_msg` with the payload bytes.
#[test]
fn dispatch_routes_user_packet_to_notify_user_msg() {
  let rec = Recorder::default();
  let pkt = UserPacket::new(sock(7), Bytes::from_static(b"hi"), Reliability::Unreliable);
  block_on(dispatch_event_delegate(&rec, &Event::UserPacket(pkt)));
  assert_eq!(*rec.log.borrow(), vec!["user:hi"]);
}

// A control event with no matching observation hook (the dispatcher's `_`
// arm) is a silent no-op — nothing is recorded.
#[test]
fn dispatch_ignores_events_without_a_hook() {
  let rec = Recorder::default();
  block_on(dispatch_event_delegate(
    &rec,
    &Event::LeftCluster::<SmolStr, SocketAddr>,
  ));
  assert!(rec.log.borrow().is_empty());
}

// `observation_payload_bytes` charges only the payload-bearing variants;
// membership / control events return `None`.
#[test]
fn observation_payload_bytes_charges_only_payload_variants() {
  let pkt = UserPacket::new(sock(7), Bytes::from_static(b"abcde"), Reliability::Reliable);
  assert_eq!(
    observation_payload_bytes(&Event::UserPacket::<SmolStr, SocketAddr>(pkt)),
    Some(5)
  );
  // Membership events carry no charged payload.
  assert_eq!(
    observation_payload_bytes(&Event::NodeJoined(node("a", 1, State::Alive))),
    None
  );
  assert_eq!(
    observation_payload_bytes(&Event::LeftCluster::<SmolStr, SocketAddr>),
    None
  );
}

// `add_obs_payload` adds `Some(n)` and is a no-op on `None`.
#[test]
fn add_obs_payload_accumulates_some_and_skips_none() {
  let counter = Cell::new(0u64);
  add_obs_payload(&counter, Some(10));
  add_obs_payload(&counter, None);
  add_obs_payload(&counter, Some(5));
  assert_eq!(counter.get(), 15);
}

// `yield_once` re-arms the waker and returns `Pending` exactly once, then
// resolves — it must complete (not hang) and round-trip in one re-poll.
#[test]
fn yield_once_resolves_after_one_pending() {
  block_on(async {
    yield_once().await;
  });
  // Reaching here means the future resolved.
}

// Repeated dispatch on one shared delegate accumulates each call's record —
// the dispatcher borrows the delegate by shared reference and the per-call
// hook fires every time.
#[test]
fn dispatch_handles_repeated_calls_on_shared_delegate() {
  let rec = Recorder::default();
  for i in 0..3 {
    block_on(dispatch_event_delegate(
      &rec,
      &Event::NodeJoined(node("x", 1, State::Alive)),
    ));
    assert_eq!(rec.log.borrow().len(), i + 1);
  }
}
