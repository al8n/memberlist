//! A `!Send` single-threaded async MPSC channel for the thread-per-core driver.
//!
//! `memberlist-compio` is thread-per-core: the run loop and every task it spawns
//! (the per-bridge byte movers, the observation task) are pinned to the run-loop
//! thread by compio's `!Send` `spawn`. The driver↔task channels therefore never
//! cross a thread, so a `Send`/atomic channel (`flume`) pays an atomic per
//! `send`/`recv` for cross-thread safety that is never exercised on these paths.
//!
//! This channel is internally an `Rc<RefCell<VecDeque<T>>>` plus wakers — **no
//! atomics** — and is sound by construction: a single thread cannot race itself,
//! so each `poll` runs to completion with no other endpoint interleaving, and the
//! usual register-then-recheck waker dance is unnecessary.
//!
//! It mirrors the slice of `flume`'s API the driver-local channels use —
//! [`bounded`] / [`unbounded`], [`Sender::send_async`] / [`Sender::try_send`],
//! [`Receiver::recv_async`] / [`Receiver::try_recv`] — so swapping a driver-local
//! `flume` channel for this one is near drop-in. The genuinely cross-thread
//! channels (the `Handle` command channel, the embedder event stream, the
//! shutdown ack) stay on `flume`.
//!
//! `bounded` requires `cap >= 1`; a zero-capacity rendezvous is not supported and
//! never configured by the driver.

use std::{
  cell::{Cell, RefCell},
  collections::VecDeque,
  fmt,
  future::{Future, poll_fn},
  pin::Pin,
  rc::Rc,
  task::{Context, Poll, Waker},
};

use futures_util::future::FusedFuture;

/// Shared channel state. `!Send` (holds `Rc` / `Cell` / `RefCell`); no atomics.
struct Shared<T> {
  queue: RefCell<VecDeque<T>>,
  /// `None` = unbounded; `Some(n)` = at most `n` queued items.
  cap: Option<usize>,
  /// Live [`Sender`] count. When it reaches 0 the receiver drains what is queued
  /// and then observes `Disconnected`.
  senders: Cell<usize>,
  /// Cleared once the single [`Receiver`] is dropped; sends then fail with
  /// `Disconnected`.
  receiver_alive: Cell<bool>,
  /// The single receiver's waker, woken when an item is pushed or the last
  /// sender drops.
  recv_waker: RefCell<Option<Waker>>,
  /// Wakers of senders parked on a full bounded channel, woken (all) when a slot
  /// frees or the receiver drops. Wake-all is robust against a parked send
  /// future being dropped: a stale waker merely re-polls a task whose send is
  /// already gone.
  send_wakers: RefCell<Vec<Waker>>,
}

impl<T> Shared<T> {
  fn wake_receiver(&self) {
    let waker = self.recv_waker.borrow_mut().take();
    if let Some(w) = waker {
      w.wake();
    }
  }

  fn wake_senders(&self) {
    let wakers: Vec<Waker> = self.send_wakers.borrow_mut().drain(..).collect();
    for w in wakers {
      w.wake();
    }
  }
}

/// Create a bounded channel holding at most `cap` queued items. `send_async`
/// applies backpressure (parks) when full; `try_send` returns
/// [`TrySendError::Full`]. `cap` must be `>= 1`.
pub(crate) fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
  new_pair(Some(cap))
}

/// Create an unbounded channel. Sends never park or report `Full`.
pub(crate) fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
  new_pair(None)
}

fn new_pair<T>(cap: Option<usize>) -> (Sender<T>, Receiver<T>) {
  let shared = Rc::new(Shared {
    queue: RefCell::new(VecDeque::new()),
    cap,
    senders: Cell::new(1),
    receiver_alive: Cell::new(true),
    recv_waker: RefCell::new(None),
    send_wakers: RefCell::new(Vec::new()),
  });
  (
    Sender {
      shared: shared.clone(),
    },
    Receiver { shared },
  )
}

/// The sending half. Cloneable (MPSC).
pub(crate) struct Sender<T> {
  shared: Rc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
  fn clone(&self) -> Self {
    self.shared.senders.set(self.shared.senders.get() + 1);
    Self {
      shared: self.shared.clone(),
    }
  }
}

impl<T> Drop for Sender<T> {
  fn drop(&mut self) {
    let n = self.shared.senders.get();
    self.shared.senders.set(n - 1);
    if n == 1 {
      // Last sender gone: wake a parked receiver so it observes `Disconnected`
      // (empty queue + zero senders).
      self.shared.wake_receiver();
    }
  }
}

impl<T> Sender<T> {
  /// Push without blocking. Errors with [`TrySendError::Full`] on a full bounded
  /// channel, or [`TrySendError::Disconnected`] if the receiver is gone.
  pub(crate) fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
    let shared = &self.shared;
    if !shared.receiver_alive.get() {
      return Err(TrySendError::Disconnected(item));
    }
    {
      let mut q = shared.queue.borrow_mut();
      if shared.cap.is_some_and(|cap| q.len() >= cap) {
        return Err(TrySendError::Full(item));
      }
      q.push_back(item);
    }
    shared.wake_receiver();
    Ok(())
  }

  /// Push, parking until a slot frees on a full bounded channel (the
  /// backpressure path). Resolves to [`SendError`] carrying the item back if the
  /// receiver is gone. The returned future is `Unpin` and may be `.fuse()`d for
  /// use in a `select`.
  pub(crate) fn send_async(&self, item: T) -> impl Future<Output = Result<(), SendError<T>>> + '_ {
    let mut slot = Some(item);
    poll_fn(move |cx| {
      let shared = &self.shared;
      if !shared.receiver_alive.get() {
        let item = slot.take().expect("send future polled after completion");
        return Poll::Ready(Err(SendError(item)));
      }
      let has_room = match shared.cap {
        None => true,
        Some(cap) => shared.queue.borrow().len() < cap,
      };
      if has_room {
        let item = slot.take().expect("send future polled after completion");
        shared.queue.borrow_mut().push_back(item);
        shared.wake_receiver();
        return Poll::Ready(Ok(()));
      }
      // Full: park. Dedup the registration so repeated polls (e.g. spurious
      // wakes) cannot grow `send_wakers` without bound between recv drains.
      let mut wakers = shared.send_wakers.borrow_mut();
      if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
        wakers.push(cx.waker().clone());
      }
      Poll::Pending
    })
  }
}

/// The single receiving half.
pub(crate) struct Receiver<T> {
  shared: Rc<Shared<T>>,
}

impl<T> Drop for Receiver<T> {
  fn drop(&mut self) {
    self.shared.receiver_alive.set(false);
    // Drop any queued-but-unreceived items now: with the receiver gone they are
    // unreachable, yet a live sender clone keeps `Shared` (and those payloads)
    // alive. Take the queue out and drop it after the borrow is released.
    let drained = std::mem::take(&mut *self.shared.queue.borrow_mut());
    drop(drained);
    // Wake parked senders so their `send_async` observes `Disconnected`.
    self.shared.wake_senders();
  }
}

impl<T> Receiver<T> {
  /// Pop without blocking. [`TryRecvError::Empty`] if nothing is queued,
  /// [`TryRecvError::Disconnected`] if empty and every sender is gone.
  pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
    let shared = &self.shared;
    let popped = shared.queue.borrow_mut().pop_front();
    if let Some(item) = popped {
      shared.wake_senders();
      return Ok(item);
    }
    if shared.senders.get() == 0 {
      return Err(TryRecvError::Disconnected);
    }
    Err(TryRecvError::Empty)
  }

  /// Pop, parking until an item arrives. Resolves to [`RecvError::Disconnected`]
  /// once the queue is empty and every sender has dropped. The future is a
  /// [`FusedFuture`] (usable directly in `select_biased!`).
  pub(crate) fn recv_async(&self) -> RecvFut<'_, T> {
    RecvFut {
      receiver: self,
      done: false,
    }
  }
}

/// Future returned by [`Receiver::recv_async`]. Stores no `T`, so it is `Unpin`
/// regardless of `T`.
pub(crate) struct RecvFut<'a, T> {
  receiver: &'a Receiver<T>,
  done: bool,
}

impl<T> Future for RecvFut<'_, T> {
  type Output = Result<T, RecvError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let shared = &this.receiver.shared;
    let popped = shared.queue.borrow_mut().pop_front();
    if let Some(item) = popped {
      // A slot freed — wake parked senders (bounded backpressure).
      shared.wake_senders();
      this.done = true;
      return Poll::Ready(Ok(item));
    }
    if shared.senders.get() == 0 {
      this.done = true;
      return Poll::Ready(Err(RecvError::Disconnected));
    }
    *shared.recv_waker.borrow_mut() = Some(cx.waker().clone());
    Poll::Pending
  }
}

impl<T> FusedFuture for RecvFut<'_, T> {
  fn is_terminated(&self) -> bool {
    self.done
  }
}

/// Returned by [`Sender::send_async`] when the receiver is gone; carries the
/// unsent item back.
pub(crate) struct SendError<T>(pub(crate) T);

/// Returned by [`Sender::try_send`].
pub(crate) enum TrySendError<T> {
  /// The bounded channel is at capacity.
  Full(T),
  /// The receiver is gone.
  Disconnected(T),
}

/// Returned by [`Receiver::recv_async`] when empty and every sender has dropped.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecvError {
  Disconnected,
}

/// Returned by [`Receiver::try_recv`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TryRecvError {
  /// Nothing is queued, but senders remain.
  Empty,
  /// Empty and every sender is gone.
  Disconnected,
}

// Hand-written `Debug` (no `T: Debug` bound) — the payload is never shown.
impl<T> fmt::Debug for SendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("SendError(..)")
  }
}

impl<T> fmt::Debug for TrySendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Full(_) => f.write_str("Full(..)"),
      Self::Disconnected(_) => f.write_str("Disconnected(..)"),
    }
  }
}

impl<T> fmt::Display for SendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("sending on a disconnected channel")
  }
}

impl<T> fmt::Display for TrySendError<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Full(_) => f.write_str("sending on a full channel"),
      Self::Disconnected(_) => f.write_str("sending on a disconnected channel"),
    }
  }
}

impl fmt::Display for RecvError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("receiving on an empty and disconnected channel")
  }
}

impl fmt::Display for TryRecvError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Empty => f.write_str("receiving on an empty channel"),
      Self::Disconnected => f.write_str("receiving on an empty and disconnected channel"),
    }
  }
}

impl<T> std::error::Error for SendError<T> {}
impl<T> std::error::Error for TrySendError<T> {}
impl std::error::Error for RecvError {}
impl std::error::Error for TryRecvError {}

#[cfg(test)]
mod tests;
