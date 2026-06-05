//! [`Shared`] — the uniform handle-to-driver shared state: a command queue + the
//! driver's parked waker, the published membership snapshot, drop counters, and
//! the live-handle count. The machine is owned privately by the driver; handles
//! reach it only by pushing [`Command`]s and reading the snapshot.

use std::{
  collections::VecDeque,
  net::SocketAddr,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
  },
  task::Waker,
};

use arc_swap::ArcSwap;

use crate::{command::Command, snapshot::MemberlistSnapshot};

/// The lock-guarded part of [`Shared`]: the command queue handles push onto, and
/// the driver's parked waker.
struct Inner<I> {
  commands: VecDeque<Command<I>>,
  driver_waker: Option<Waker>,
  /// Set once the driver has exited; no further commands are accepted, so a
  /// handle never waits on a reply that can never come.
  closed: bool,
}

/// State shared between the `Memberlist` handle (and its clones) and the backend
/// driver task.
pub(crate) struct Shared<I> {
  inner: Mutex<Inner<I>>,
  snapshot: ArcSwap<MemberlistSnapshot<I, SocketAddr>>,
  /// Recoverable EventStream-forward drops (membership / control gaps).
  events_dropped: AtomicU64,
  /// Observation-channel drops (a slow delegate; may lose application data).
  observation_dropped: AtomicU64,
  /// Set once the driver is shutting down; handle command methods observe it.
  shutdown: AtomicBool,
  /// Count of live `Memberlist` handles. The last to drop flips `shutdown` and
  /// wakes the driver so it exits.
  handles: AtomicUsize,
}

impl<I> Shared<I> {
  /// Builds the shared state around an initial published snapshot, with one live
  /// handle.
  pub(crate) fn new(initial: MemberlistSnapshot<I, SocketAddr>) -> Self {
    Self {
      inner: Mutex::new(Inner::<I> {
        commands: VecDeque::new(),
        driver_waker: None,
        closed: false,
      }),
      snapshot: ArcSwap::from_pointee(initial),
      events_dropped: AtomicU64::new(0),
      observation_dropped: AtomicU64::new(0),
      shutdown: AtomicBool::new(false),
      handles: AtomicUsize::new(1),
    }
  }

  /// Pushes a command and wakes the driver. Returns `false` (dropping `cmd`) if
  /// the driver has already exited, so the caller can fail fast instead of
  /// awaiting a reply that will never arrive.
  pub(crate) fn push_command(&self, cmd: Command<I>) -> bool {
    let mut inner = self.inner.lock().unwrap();
    if inner.closed {
      return false;
    }
    inner.commands.push_back(cmd);
    if let Some(waker) = inner.driver_waker.take() {
      waker.wake();
    }
    true
  }

  /// Driver side: parks `waker` for the next push and returns all queued
  /// commands.
  pub(crate) fn drain_commands(&self, waker: &Waker) -> VecDeque<Command<I>> {
    let mut inner = self.inner.lock().unwrap();
    match &inner.driver_waker {
      Some(w) if w.will_wake(waker) => {}
      _ => inner.driver_waker = Some(waker.clone()),
    }
    core::mem::take(&mut inner.commands)
  }

  /// Driver side, on exit: closes the queue (rejecting further pushes) and
  /// returns any still-queued commands so the driver can fail their repliers.
  pub(crate) fn close_and_drain(&self) -> VecDeque<Command<I>> {
    let mut inner = self.inner.lock().unwrap();
    inner.closed = true;
    core::mem::take(&mut inner.commands)
  }

  /// Publishes a fresh membership snapshot for handles to read lock-free.
  pub(crate) fn publish(&self, snap: MemberlistSnapshot<I, SocketAddr>) {
    self.snapshot.store(Arc::new(snap));
  }

  /// Loads the latest published snapshot.
  pub(crate) fn load_snapshot(&self) -> Arc<MemberlistSnapshot<I, SocketAddr>> {
    self.snapshot.load_full()
  }

  /// Records `n` recoverable EventStream-forward drops.
  pub(crate) fn add_events_dropped(&self, n: u64) {
    self.events_dropped.fetch_add(n, Ordering::Relaxed);
  }

  /// Records `n` observation-channel drops.
  pub(crate) fn add_observation_dropped(&self, n: u64) {
    self.observation_dropped.fetch_add(n, Ordering::Relaxed);
  }

  /// The cumulative recoverable EventStream-forward drop count.
  pub(crate) fn events_dropped(&self) -> u64 {
    self.events_dropped.load(Ordering::Relaxed)
  }

  /// The cumulative observation-channel drop count.
  pub(crate) fn observation_dropped(&self) -> u64 {
    self.observation_dropped.load(Ordering::Relaxed)
  }

  /// Whether the driver is shutting down.
  pub(crate) fn is_shutdown(&self) -> bool {
    self.shutdown.load(Ordering::Acquire)
  }

  /// Marks the driver as shutting down.
  pub(crate) fn begin_shutdown(&self) {
    self.shutdown.store(true, Ordering::Release);
  }

  /// Registers a freshly cloned handle.
  pub(crate) fn handle_cloned(&self) {
    self.handles.fetch_add(1, Ordering::Relaxed);
  }

  /// Deregisters a dropped handle; returns `true` if it was the last one (the
  /// caller should then begin shutdown and wake the driver).
  pub(crate) fn handle_dropped(&self) -> bool {
    self.handles.fetch_sub(1, Ordering::AcqRel) == 1
  }

  /// Wakes the driver without enqueuing a command (used on last-handle drop).
  pub(crate) fn wake_driver(&self) {
    if let Some(waker) = self.inner.lock().unwrap().driver_waker.take() {
      waker.wake();
    }
  }
}

#[cfg(test)]
mod tests {
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
}
