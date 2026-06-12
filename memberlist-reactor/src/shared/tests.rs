use std::{
  net::SocketAddr,
  sync::atomic::{AtomicBool, Ordering as AtomicOrdering},
  task::{Wake, Waker},
};

use memberlist_proto::typed::{NodeState, State};
use smol_str::SmolStr;

use super::*;
use crate::command::{Command, ShutdownCmd};

/// A waker that records, via a shared flag, whether it was woken. Lets the
/// tests assert that `push_command` / `wake_driver` actually fire the parked
/// waker. Built on the safe `std::task::Wake` trait (no `unsafe`, which the
/// crate forbids).
struct FlagWaker(Arc<AtomicBool>);

impl Wake for FlagWaker {
  fn wake(self: Arc<Self>) {
    self.0.store(true, AtomicOrdering::SeqCst);
  }
  fn wake_by_ref(self: &Arc<Self>) {
    self.0.store(true, AtomicOrdering::SeqCst);
  }
}

fn flag_waker(flag: Arc<AtomicBool>) -> Waker {
  Waker::from(Arc::new(FlagWaker(flag)))
}

fn sock(port: u16) -> SocketAddr {
  SocketAddr::from(([127, 0, 0, 1], port))
}

fn snapshot(id: &str) -> MemberlistSnapshot<SmolStr, SocketAddr> {
  let local = Arc::new(NodeState::new(SmolStr::new(id), sock(9), State::Alive));
  MemberlistSnapshot::new(vec![local.clone()], local, 1, 1, 0)
}

fn shutdown_cmd() -> Command<SmolStr> {
  let (tx, _rx) = futures_channel::oneshot::channel();
  Command::Shutdown(ShutdownCmd { reply: tx })
}

/// `push_command` enqueues and wakes the parked driver waker; `drain_commands`
/// returns every queued command and re-parks the waker.
#[test]
fn push_drains_and_wakes_parked_waker() {
  let shared = Shared::<SmolStr>::new(snapshot("me"));
  let woken = Arc::new(AtomicBool::new(false));
  let waker = flag_waker(woken.clone());

  // Park the waker (driver side), with nothing queued yet.
  assert!(
    shared.drain_commands(&waker).is_empty(),
    "no commands queued initially"
  );
  assert!(!woken.load(AtomicOrdering::SeqCst), "parking does not wake");

  // A push must enqueue and fire the parked waker.
  assert!(
    shared.push_command(shutdown_cmd()),
    "push accepted while open"
  );
  assert!(
    woken.load(AtomicOrdering::SeqCst),
    "push_command must wake the parked driver"
  );

  // Drain returns the queued command and re-parks the (new) waker.
  let drained = shared.drain_commands(&waker);
  assert_eq!(drained.len(), 1, "the one queued command is drained");
}

/// `drain_commands` keeps the existing parked waker when the next poll passes a
/// waker that `will_wake` the current one (no needless clone churn).
#[test]
fn drain_commands_reuses_equivalent_waker() {
  let shared = Shared::<SmolStr>::new(snapshot("me"));
  let woken = Arc::new(AtomicBool::new(false));
  let waker = flag_waker(woken.clone());
  let _ = shared.drain_commands(&waker);
  // Re-draining with a clone of the same waker takes the will_wake fast path.
  let same = waker.clone();
  let _ = shared.drain_commands(&same);
  // A subsequent push still wakes through the retained waker.
  assert!(shared.push_command(shutdown_cmd()));
  assert!(
    woken.load(AtomicOrdering::SeqCst),
    "retained waker still wakes"
  );
}

/// `close_and_drain` rejects further pushes (so a handle fails fast) and hands
/// back any commands still queued at driver exit.
#[test]
fn close_and_drain_rejects_subsequent_pushes() {
  let shared = Shared::<SmolStr>::new(snapshot("me"));
  assert!(shared.push_command(shutdown_cmd()), "push before close");
  let leftover = shared.close_and_drain();
  assert_eq!(leftover.len(), 1, "queued commands returned at close");
  assert!(
    !shared.push_command(shutdown_cmd()),
    "a push after close is rejected so the caller can fail fast"
  );
}

/// `wake_driver` fires the parked waker without enqueuing a command (the
/// last-handle-drop path), and is a no-op when no waker is parked.
#[test]
fn wake_driver_fires_parked_waker_only() {
  let shared = Shared::<SmolStr>::new(snapshot("me"));
  // No waker parked yet: wake_driver is a harmless no-op.
  shared.wake_driver();

  let woken = Arc::new(AtomicBool::new(false));
  let waker = flag_waker(woken.clone());
  let _ = shared.drain_commands(&waker);
  shared.wake_driver();
  assert!(
    woken.load(AtomicOrdering::SeqCst),
    "wake_driver must fire the parked waker"
  );
  // The waker was taken, so a second wake_driver finds none parked.
  woken.store(false, AtomicOrdering::SeqCst);
  shared.wake_driver();
  assert!(
    !woken.load(AtomicOrdering::SeqCst),
    "the waker is consumed on wake; a second wake_driver is a no-op"
  );
}

/// The snapshot is publishable and readable lock-free, and the drop counters
/// accumulate.
#[test]
fn publish_load_and_counters() {
  let shared = Shared::<SmolStr>::new(snapshot("first"));
  assert_eq!(
    shared.load_snapshot().local_ref().id_ref().as_str(),
    "first"
  );
  shared.publish(snapshot("second"));
  assert_eq!(
    shared.load_snapshot().local_ref().id_ref().as_str(),
    "second",
    "publish swaps the live snapshot"
  );

  assert_eq!(shared.events_dropped(), 0);
  assert_eq!(shared.observation_dropped(), 0);
  shared.add_events_dropped(3);
  shared.add_events_dropped(2);
  shared.add_observation_dropped(5);
  assert_eq!(shared.events_dropped(), 5, "event drops accumulate");
  assert_eq!(shared.observation_dropped(), 5, "obs drops accumulate");
}

/// The shutdown flag flips on `begin_shutdown`, and handle ref-counting
/// reports the last drop.
#[test]
fn shutdown_flag_and_handle_refcount() {
  let shared = Shared::<SmolStr>::new(snapshot("me"));
  assert!(!shared.is_shutdown(), "fresh shared is not shutting down");
  shared.begin_shutdown();
  assert!(shared.is_shutdown(), "begin_shutdown sets the flag");

  // Starts with one live handle. Cloning then dropping twice: the first drop is
  // not the last, the second is.
  shared.handle_cloned(); // now 2 handles
  assert!(
    !shared.handle_dropped(),
    "dropping one of two handles is not the last"
  );
  assert!(
    shared.handle_dropped(),
    "dropping the final handle reports true"
  );
}

/// A late `shutdown()` caller whose command the closed queue rejected parks on
/// the completion latch and is released only once the driver marks teardown
/// done; a caller arriving after that is ready on its first poll.
#[test]
fn shutdown_complete_latch_releases_late_caller() {
  use std::{future::Future, task::Context};

  let shared = Shared::<SmolStr>::new(snapshot("me"));
  // The driver has closed the queue (teardown underway) but not finished it.
  let _ = shared.close_and_drain();
  let woken = Arc::new(AtomicBool::new(false));
  let waker = flag_waker(woken.clone());
  let mut cx = Context::from_waker(&waker);

  let mut late = std::pin::pin!(shared.wait_shutdown_complete());
  assert!(
    late.as_mut().poll(&mut cx).is_pending(),
    "a late caller blocks while teardown is unfinished"
  );

  // The driver finishing teardown fires the latch and wakes the parked caller.
  shared.mark_shutdown_complete();
  assert!(
    woken.load(AtomicOrdering::SeqCst),
    "marking teardown complete wakes the parked caller"
  );
  assert!(
    late.as_mut().poll(&mut cx).is_ready(),
    "the latch releases the late caller once teardown is complete"
  );

  // A caller arriving after the latch has fired is ready immediately.
  let mut after = std::pin::pin!(shared.wait_shutdown_complete());
  assert!(
    after.as_mut().poll(&mut cx).is_ready(),
    "a wait after teardown completed returns at once"
  );
}
