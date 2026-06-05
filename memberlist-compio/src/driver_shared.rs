//! Driver helpers shared by every transport backend's driver loop (the
//! stream driver in [`crate::driver`] and the QUIC driver in
//! [`crate::quic_driver`]).
//!
//! These are the parts of the observation / event hand-off path that are
//! independent of the reliable plane: the cluster-wide [`ExchangeId`] token,
//! the [`Delegate`] hook dispatcher, the cooperative yield used to drain a
//! bounded observation channel, and the observation byte-backstop accounting.
//! They live here — outside the feature-gated [`crate::driver`] /
//! [`crate::bridge`] stream-transport modules — so the QUIC driver can reuse
//! them without depending on the byte-stream plane (which is only compiled with
//! a `tcp` / `tls-*` feature).

use std::sync::atomic::AtomicU64;

use memberlist_proto::event::Event;

use crate::delegate::Delegate;

/// Coordinator-allocated handle for one in-flight reliable exchange.
///
/// The driver and the per-bridge task agree on the same opaque id without
/// the rest of the crate having to name the machine's `streams` module.
/// Sourced from the ungated [`memberlist_proto::event`] module so the QUIC
/// driver — whose reliable plane is `QuicEndpoint`, not the byte-stream
/// `streams` plane — shares the identical id type as the stream backends.
pub(crate) type ExchangeId = memberlist_proto::event::ExchangeId;

/// Fire the matching [`Delegate`] hook for one drained [`Event`].
///
/// The event-shaped hooks (`notify_join` / `notify_leave` / `notify_update`
/// / `notify_ping_complete`) run on the driver thread BEFORE the event is
/// forwarded to subscribers, so a delegate observes the transition before
/// any [`EventStream`](crate::EventStream) consumer does. The membership
/// FSM already carries the resolved `Arc<NodeState>` inside each variant,
/// so the hook borrows it (cheap `Arc` bump) with no re-projection.
///
/// Admission (`notify_alive` / `notify_merge`) is NOT fired here — those
/// are the machine's `AliveDelegate` / `MergeDelegate` predicates, supplied
/// via [`Options`](crate::Options) and run inline inside the FSM ahead of
/// the alive/merge transition. The observation [`Delegate`] is a distinct
/// concern: its hooks observe transitions the FSM has already applied.
pub(crate) async fn dispatch_event_delegate<I, A, D>(delegate: &D, ev: &Event<I, A>)
where
  D: Delegate<Id = I, Address = A>,
{
  match ev {
    Event::NodeJoined(node) => delegate.notify_join(node.clone()).await,
    Event::NodeLeft(node) => delegate.notify_leave(node.clone()).await,
    Event::NodeUpdated(node) => delegate.notify_update(node.clone()).await,
    Event::PingCompleted(payload) => {
      let node = payload.node_ref();
      delegate
        .notify_ping_complete(
          node.id_ref(),
          node.address_ref(),
          payload.rtt(),
          payload.payload_ref().clone(),
        )
        .await;
    }
    Event::NodeConflict(c) => {
      delegate
        .notify_conflict(c.existing_ref().clone(), c.other_ref().clone())
        .await;
    }
    Event::UserPacket(pkt) => {
      delegate
        .notify_user_msg(std::borrow::Cow::Borrowed(pkt.data_ref().as_ref()))
        .await;
    }
    Event::RemoteStateReceived(rs) => {
      delegate
        .merge_remote_state(rs.user_data_ref().as_ref(), rs.join())
        .await;
    }
    _ => {}
  }
}

/// Yield to the runtime exactly once.
///
/// The event drain is synchronous — no `.await` fires for membership
/// events — so on a single-threaded runtime the observation task is not
/// scheduled mid-drain. A bounded `obs_tx` would therefore overflow on a
/// single large-but-valid burst (e.g. a join push/pull carrying many members)
/// before the task drains a single event. Yielding hands the scheduler to the
/// already-woken observation task so it can drain `obs_rx` before the drain
/// continues. Runtime-agnostic (no dependency on a specific `yield_now`):
/// re-arms the waker and returns `Pending` once, so the executor runs other
/// ready tasks before re-polling this one.
pub(crate) async fn yield_once() {
  let mut yielded = false;
  core::future::poll_fn(move |cx| {
    if yielded {
      core::task::Poll::Ready(())
    } else {
      yielded = true;
      cx.waker().wake_by_ref();
      core::task::Poll::Pending
    }
  })
  .await
}

/// The observation-channel byte-backstop weight of an event: `Some(len)` for
/// the payload-bearing variants (`UserPacket` / `RemoteStateReceived`, whose
/// `Bytes` ride up to `max_stream_frame_size`), `None` for the small membership
/// / control events the count cap already bounds.
pub(crate) fn observation_payload_bytes<I, A>(ev: &Event<I, A>) -> Option<u64> {
  match ev {
    Event::UserPacket(p) => Some(p.data_ref().len() as u64),
    Event::RemoteStateReceived(r) => Some(r.user_data_ref().len() as u64),
    _ => None,
  }
}

/// Add a just-enqueued event's payload weight (if any) to the byte-backstop
/// counter. Paired with the subtract in each driver's `observation_task` on
/// dequeue.
pub(crate) fn add_obs_payload(counter: &AtomicU64, bytes: Option<u64>) {
  if let Some(b) = bytes {
    counter.fetch_add(b, std::sync::atomic::Ordering::Relaxed);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::{
    borrow::Cow,
    cell::RefCell,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
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
    let counter = AtomicU64::new(0);
    add_obs_payload(&counter, Some(10));
    add_obs_payload(&counter, None);
    add_obs_payload(&counter, Some(5));
    assert_eq!(counter.load(Ordering::Relaxed), 15);
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
}
