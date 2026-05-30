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
struct Inner {
  commands: VecDeque<Command>,
  driver_waker: Option<Waker>,
  /// Set once the driver has exited; no further commands are accepted, so a
  /// handle never waits on a reply that can never come.
  closed: bool,
}

/// State shared between the `Memberlist` handle (and its clones) and the backend
/// driver task.
pub(crate) struct Shared<I> {
  inner: Mutex<Inner>,
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
      inner: Mutex::new(Inner {
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
  pub(crate) fn push_command(&self, cmd: Command) -> bool {
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
  pub(crate) fn drain_commands(&self, waker: &Waker) -> VecDeque<Command> {
    let mut inner = self.inner.lock().unwrap();
    match &inner.driver_waker {
      Some(w) if w.will_wake(waker) => {}
      _ => inner.driver_waker = Some(waker.clone()),
    }
    core::mem::take(&mut inner.commands)
  }

  /// Driver side, on exit: closes the queue (rejecting further pushes) and
  /// returns any still-queued commands so the driver can fail their repliers.
  pub(crate) fn close_and_drain(&self) -> VecDeque<Command> {
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
