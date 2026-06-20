use std::{
  future::Future,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  task::{Context, Poll, Waker},
};

use futures_util::{
  future::FusedFuture,
  task::{ArcWake, waker},
};

use super::*;

/// A waker that counts how often it is woken, so a test can assert a parked
/// future was actually woken — a missing wake would hang in production.
struct WakeCounter(AtomicUsize);

impl ArcWake for WakeCounter {
  fn wake_by_ref(arc_self: &Arc<Self>) {
    arc_self.0.fetch_add(1, Ordering::SeqCst);
  }
}

fn counting_waker() -> (Waker, Arc<WakeCounter>) {
  let c = Arc::new(WakeCounter(AtomicUsize::new(0)));
  (waker(c.clone()), c)
}

fn wake_count(c: &Arc<WakeCounter>) -> usize {
  c.0.load(Ordering::SeqCst)
}

fn poll_once<F: Future + Unpin>(fut: &mut F, w: &Waker) -> Poll<F::Output> {
  Pin::new(fut).poll(&mut Context::from_waker(w))
}

#[test]
fn unbounded_send_recv_is_fifo() {
  let (tx, rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  tx.try_send(3).unwrap();
  assert_eq!(rx.try_recv().unwrap(), 1);
  assert_eq!(rx.try_recv().unwrap(), 2);
  assert_eq!(rx.try_recv().unwrap(), 3);
  assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn bounded_try_send_reports_full() {
  let (tx, _rx) = bounded::<u32>(2);
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  assert!(matches!(tx.try_send(3), Err(TrySendError::Full(3))));
}

#[test]
fn try_send_after_receiver_drop_is_disconnected() {
  let (tx, rx) = unbounded::<u32>();
  drop(rx);
  assert!(matches!(tx.try_send(1), Err(TrySendError::Disconnected(1))));
}

#[test]
fn try_recv_drains_queue_before_reporting_disconnected() {
  let (tx, rx) = unbounded::<u32>();
  tx.try_send(1).unwrap();
  tx.try_send(2).unwrap();
  drop(tx);
  // Queued items are delivered even though every sender is gone.
  assert_eq!(rx.try_recv().unwrap(), 1);
  assert_eq!(rx.try_recv().unwrap(), 2);
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn clone_tracks_live_senders() {
  let (tx, rx) = unbounded::<u32>();
  let tx2 = tx.clone();
  drop(tx);
  // One sender remains, so the channel is still open.
  tx2.try_send(1).unwrap();
  assert_eq!(rx.try_recv().unwrap(), 1);
  drop(tx2);
  assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn recv_async_parks_then_wakes_on_send() {
  let (tx, rx) = unbounded::<u32>();
  let (w, c) = counting_waker();
  let mut fut = rx.recv_async();
  assert!(poll_once(&mut fut, &w).is_pending());
  tx.try_send(42).unwrap();
  assert!(wake_count(&c) >= 1, "a parked recv must be woken by a send");
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Ok(42)));
}

#[test]
fn recv_async_wakes_and_disconnects_on_last_sender_drop() {
  let (tx, rx) = unbounded::<u32>();
  let (w, c) = counting_waker();
  let mut fut = rx.recv_async();
  assert!(poll_once(&mut fut, &w).is_pending());
  drop(tx);
  assert!(
    wake_count(&c) >= 1,
    "dropping the last sender must wake recv"
  );
  assert_eq!(
    poll_once(&mut fut, &w),
    Poll::Ready(Err(RecvError::Disconnected))
  );
}

#[test]
fn send_async_parks_when_full_then_wakes_on_recv() {
  let (tx, rx) = bounded::<u32>(1);
  let (w, c) = counting_waker();
  tx.try_send(1).unwrap(); // fill the single slot
  let mut sfut = tx.send_async(2);
  assert!(poll_once(&mut sfut, &w).is_pending());
  // Receiving frees the slot and must wake the parked sender.
  assert_eq!(rx.try_recv().unwrap(), 1);
  assert!(
    wake_count(&c) >= 1,
    "freeing a slot must wake a parked sender"
  );
  assert!(matches!(poll_once(&mut sfut, &w), Poll::Ready(Ok(()))));
  assert_eq!(rx.try_recv().unwrap(), 2);
}

#[test]
fn send_async_wakes_and_disconnects_on_receiver_drop() {
  let (tx, rx) = bounded::<u32>(1);
  let (w, c) = counting_waker();
  tx.try_send(1).unwrap();
  let mut sfut = tx.send_async(2);
  assert!(poll_once(&mut sfut, &w).is_pending());
  drop(rx);
  assert!(
    wake_count(&c) >= 1,
    "dropping the receiver must wake parked senders"
  );
  assert!(matches!(
    poll_once(&mut sfut, &w),
    Poll::Ready(Err(SendError(2)))
  ));
}

#[test]
fn recv_fut_reports_terminated_after_ready() {
  let (tx, rx) = unbounded::<u32>();
  let (w, _c) = counting_waker();
  tx.try_send(7).unwrap();
  let mut fut = rx.recv_async();
  assert!(!fut.is_terminated());
  assert_eq!(poll_once(&mut fut, &w), Poll::Ready(Ok(7)));
  assert!(fut.is_terminated());
}

#[test]
fn dropping_a_parked_send_leaves_no_phantom_slot() {
  let (tx, rx) = bounded::<u32>(1);
  let (w, _c) = counting_waker();
  tx.try_send(10).unwrap(); // queue: [10]
  let mut sfut = tx.send_async(20);
  assert!(poll_once(&mut sfut, &w).is_pending()); // full → parks
  drop(sfut); // the unsent 20 is discarded; no slot was reserved
  assert_eq!(rx.try_recv().unwrap(), 10); // queue now empty
  tx.try_send(30).unwrap(); // slot is genuinely free
  assert_eq!(rx.try_recv().unwrap(), 30);
}

/// Increments a shared counter on drop, so a test can prove queued payloads are
/// released rather than retained.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
  fn drop(&mut self) {
    self.0.fetch_add(1, Ordering::SeqCst);
  }
}

#[test]
fn receiver_drop_releases_queued_payloads() {
  let drops = Arc::new(AtomicUsize::new(0));
  let (tx, rx) = unbounded::<DropCounter>();
  tx.try_send(DropCounter(drops.clone())).unwrap();
  tx.try_send(DropCounter(drops.clone())).unwrap();
  // Two items are queued, not yet received.
  assert_eq!(drops.load(Ordering::SeqCst), 0);
  // Drop the receiver while a sender is still alive (so `Shared` survives): the
  // queued payloads are now unreachable and must be freed at once, not retained
  // until the last sender drops.
  drop(rx);
  assert_eq!(drops.load(Ordering::SeqCst), 2);
  drop(tx);
}
