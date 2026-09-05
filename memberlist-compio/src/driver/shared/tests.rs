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

/// compio's default proactor capacity, and so io_uring's submission ring size.
///
/// compio does not re-export `ProactorBuilder` through its facade, so a test
/// cannot ask for a smaller ring without taking a direct dependency on
/// `compio-driver`; the default is assumed instead. If it ever changes, the
/// saturation below stops leaving the ring full — which is precisely what
/// `drop_based_teardown_does_not_complete_with_a_full_ring` detects.
const PROACTOR_CAPACITY: usize = 1024;

/// Stage `count` receives without yielding, so every entry stays in the
/// submission queue. The returned futures must be kept alive: dropping one
/// issues the very cancellation this scenario starves.
///
/// An ordinary operation push DRAINS the ring and retries when it finds it
/// full, landing in an emptied ring. The ring is therefore only full after
/// exactly `PROACTOR_CAPACITY` entries have been staged in total — counting
/// the operation under test — so callers pass `PROACTOR_CAPACITY` minus what
/// they have already staged. Staging one too many empties the ring and the
/// scenario silently tests nothing.
///
/// On a readiness-based backend there is no submission queue and this is
/// simply a set of pending receives.
async fn stage_pending_receives(
  socket: &compio::net::UdpSocket,
  count: usize,
) -> Vec<super::RecvFut<'_>> {
  let mut staged = Vec::with_capacity(count);
  for _ in 0..count {
    let mut fut: super::RecvFut<'_> = Box::pin(socket.recv_from(vec![0u8; 64]));
    // Poll once to stage the operation without awaiting it.
    let _ = futures_util::poll!(fut.as_mut());
    staged.push(fut);
  }
  staged
}

/// A driver's last receive must be COMPLETED before the socket is closed, not
/// cancelled.
///
/// io_uring's cancellation is a single best-effort submission-queue push that
/// is silently discarded when the queue is full. A receive whose cancellation
/// is lost stays in flight forever holding a reference to the socket's shared
/// descriptor, and `close` waits on that last reference — so a drop-based
/// teardown hangs under exactly the conditions staged here. Completing the
/// receive with a self-addressed datagram needs no free queue slot, so the
/// close that follows has nothing left to wait for.
#[compio::test]
async fn teardown_completes_last_receive_with_submission_queue_saturated() {
  let socket = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let filler = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

  // Arm the receive the way the driver loop does, so its entry is staged
  // before the queue fills.
  let mut recv = PendingRecv::new(&socket, 64);
  let _ = futures_util::poll!(recv.fut().as_mut());

  // The receive above already occupies one ring entry, so one short of a full
  // capacity of fillers leaves the ring exactly full.
  let staged = stage_pending_receives(&filler, PROACTOR_CAPACITY - 1).await;

  assert!(
    complete_recv_before_close(recv).await,
    "the teardown marker did not complete the pending receive",
  );
  assert!(
    compio::time::timeout(TEARDOWN_CLOSE_TIMEOUT, socket.close())
      .await
      .is_ok(),
    "close did not finish after the receive was completed",
  );

  drop(staged);
}

/// A wildcard bind is not a valid destination, so the marker is aimed at the
/// matching loopback address on the bound port.
#[compio::test]
async fn self_addressed_dest_substitutes_loopback_for_a_wildcard_bind() {
  let v4 = compio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
  let dest = self_addressed_dest(&v4).expect("bound socket has a destination");
  assert_eq!(
    dest.ip(),
    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
  );
  assert_eq!(dest.port(), v4.local_addr().unwrap().port());
  assert_ne!(dest.port(), 0);
}

/// A concrete bind keeps its own address as the marker destination.
#[compio::test]
async fn self_addressed_dest_keeps_a_concrete_bind() {
  let s = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
  assert_eq!(self_addressed_dest(&s), s.local_addr().ok());
}

/// The drop-based teardown is fine when the submission queue has room: the
/// cancellation lands, the operation ends, and the close completes.
///
/// This is the control for the baseline below — together they show that the
/// hang is caused by the full ring and not by the pending receive as such.
#[compio::test]
async fn drop_based_teardown_completes_when_the_ring_has_room() {
  let socket = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let mut recv = PendingRecv::new(&socket, 64);
  let _ = futures_util::poll!(recv.fut().as_mut());
  drop(recv);
  assert!(
    compio::time::timeout(TEARDOWN_CLOSE_TIMEOUT, socket.close())
      .await
      .is_ok(),
    "close did not finish even though the cancellation had room to be pushed",
  );
}

/// Baseline proving that the staging above really does leave the ring full.
///
/// With the ring full, the cancellation issued by dropping the armed receive
/// is discarded, the operation is stranded, and the close never completes.
/// That is the exact condition
/// `teardown_completes_last_receive_with_submission_queue_saturated` is there
/// to survive; if this stops holding, the saturation arithmetic (or
/// [`PROACTOR_CAPACITY`]) has drifted and that test is no longer covering
/// anything.
///
/// Ignored by default because it asserts an io_uring-specific hang: on kqueue,
/// IOCP, and io_uring's own polling fallback the cancellation is not a queue
/// entry and the close completes, so an always-on assertion would fail on
/// every other backend. Run it on Linux with io_uring reachable:
/// `cargo test -p memberlist-compio --lib drop_based_teardown_does_not -- --ignored`
#[compio::test]
#[ignore = "asserts an io_uring-only hang; see the doc comment for how to run it"]
async fn drop_based_teardown_does_not_complete_with_a_full_ring() {
  let socket = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let filler = compio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

  let mut recv = PendingRecv::new(&socket, 64);
  let _ = futures_util::poll!(recv.fut().as_mut());
  let staged = stage_pending_receives(&filler, PROACTOR_CAPACITY - 1).await;

  // The ring is full, so this cancellation is discarded.
  drop(recv);

  assert!(
    compio::time::timeout(TEARDOWN_CLOSE_TIMEOUT, socket.close())
      .await
      .is_err(),
    "close completed, so the cancellation was pushed: the ring was not full \
     and the saturation no longer reproduces the stranding condition",
  );

  drop(staged);
}

/// A future that never resolves and reports itself as UNTERMINATED.
///
/// `futures_util::future::pending` is fused the other way — it declares itself
/// terminated precisely because it will never produce a value — which the
/// accept helper short-circuits on. Standing in for an accept whose completion
/// has not arrived needs the opposite: an operation that is still live, so the
/// helper actually runs its attempt loop.
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
struct Unresolved;

#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
impl core::future::Future for Unresolved {
  type Output = ();

  fn poll(
    self: core::pin::Pin<&mut Self>,
    _: &mut core::task::Context<'_>,
  ) -> core::task::Poll<Self::Output> {
    core::task::Poll::Pending
  }
}

#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
impl futures_util::future::FusedFuture for Unresolved {
  fn is_terminated(&self) -> bool {
    false
  }
}

/// The accept-completion protocol gives up, it does not hang, when the
/// operation it is trying to complete never resolves.
///
/// The helper exists so a driver's shutdown cannot hang, so every await inside
/// it is bounded and an elapsed step is treated exactly like a failed one. A
/// pending-forever future stands in for the pathological case the bound is
/// there for: an accept whose completion never arrives, on a listener that is
/// otherwise perfectly dialable. Without the bound on the accept await the
/// first attempt would park here forever; with it, the attempt budget is spent
/// and the caller is handed back onto the drop-based path, whose close is
/// bounded by [`TEARDOWN_CLOSE_TIMEOUT`].
///
/// The elapsed-connect branch is not asserted here: the destination is derived
/// from the listener's own bound address, so a test cannot aim the helper at
/// something undialable without a second interface.
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
#[compio::test]
async fn teardown_accept_gives_up_when_the_operation_never_resolves() {
  let listener = compio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let never = Unresolved;
  futures_util::pin_mut!(never);

  let started = std::time::Instant::now();
  let completed = complete_accept_before_close(never, &listener).await;
  let elapsed = started.elapsed();

  assert!(
    !completed,
    "the helper claimed the accept was completed, but it never resolved",
  );
  // Each attempt spends at most one step bound on the accept; the connects are
  // loopback and immediate. Twice the nominal budget leaves room for a loaded
  // machine while still failing outright if an await has become unbounded.
  assert!(
    elapsed < TEARDOWN_STEP_TIMEOUT * (TEARDOWN_MARKER_ATTEMPTS as u32) * 2,
    "the helper took {elapsed:?}, so an await inside it is no longer bounded",
  );
}
