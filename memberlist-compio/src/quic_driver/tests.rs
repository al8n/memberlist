use std::collections::{HashMap, HashSet};

use memberlist_proto::Instant;

use super::{
  MemberlistError, PendingJoin, PendingLeave, min_pending_join_deadline,
  min_pending_leave_deadline, reap_pending_joins, reap_pending_leave,
};

/// `min_pending_join_deadline` returns the EARLIEST deadline across waiters,
/// and `None` when there are none. Folded into the driver's select timer so
/// the soonest-expiring synchronous join wakes the loop.
#[compio::test]
async fn min_pending_join_deadline_picks_the_earliest() {
  let mut joins: HashMap<u64, PendingJoin> = HashMap::new();
  assert_eq!(
    min_pending_join_deadline(&joins),
    None,
    "no waiters ⇒ no deadline",
  );

  let base = Instant::now();
  let earliest = base + core::time::Duration::from_secs(1);
  let latest = base + core::time::Duration::from_secs(5);
  for (key, deadline) in [(1u64, latest), (2u64, earliest)] {
    let (tx, _rx) = futures_channel::oneshot::channel();
    joins.insert(
      key,
      PendingJoin {
        pending: HashSet::new(),
        contacted: 0,
        requested: 1,
        deadline,
        reply: tx,
      },
    );
  }
  assert_eq!(
    min_pending_join_deadline(&joins),
    Some(earliest),
    "the soonest deadline wins",
  );
}

/// `min_pending_leave_deadline` surfaces the in-flight leave's deadline, or
/// `None` when no leave is parked.
#[compio::test]
async fn min_pending_leave_deadline_tracks_the_parked_leave() {
  assert_eq!(min_pending_leave_deadline(&None), None);
  let deadline = Instant::now() + core::time::Duration::from_secs(3);
  let pl = PendingLeave {
    repliers: Vec::new(),
    deadline,
  };
  assert_eq!(min_pending_leave_deadline(&Some(pl)), Some(deadline));
}

/// `PendingLeave::resolve_all` fans the single terminal outcome out to EVERY
/// joined replier (the initiator plus racing clones), and silently tolerates a
/// replier whose receiver was dropped.
#[compio::test]
async fn pending_leave_resolve_all_fans_out_to_every_replier() {
  let (tx_a, rx_a) = futures_channel::oneshot::channel::<super::Result<()>>();
  let (tx_b, rx_b) = futures_channel::oneshot::channel::<super::Result<()>>();
  // A third replier whose receiver is dropped up front — `resolve_all` must
  // not panic when its send fails.
  let (tx_c, rx_c) = futures_channel::oneshot::channel::<super::Result<()>>();
  drop(rx_c);

  let pl = PendingLeave {
    repliers: vec![tx_a, tx_b, tx_c],
    deadline: Instant::now(),
  };
  pl.resolve_all(|| Ok(())).await;

  assert!(
    matches!(rx_a.await, Ok(Ok(()))),
    "first replier resolved Ok"
  );
  assert!(
    matches!(rx_b.await, Ok(Ok(()))),
    "second replier resolved Ok"
  );
}

/// `reap_pending_leave` replies `LeaveTimeout` to every replier once the
/// deadline has elapsed and clears the slot; a not-yet-expired leave is left
/// parked untouched.
#[compio::test]
async fn reap_pending_leave_fires_only_after_the_deadline() {
  let now = Instant::now();

  // Not yet expired: the slot stays, no reply is sent.
  let (tx_live, mut rx_live) = futures_channel::oneshot::channel::<super::Result<()>>();
  let mut not_expired = Some(PendingLeave {
    repliers: vec![tx_live],
    deadline: now + core::time::Duration::from_secs(10),
  });
  reap_pending_leave(&mut not_expired, now).await;
  assert!(
    not_expired.is_some(),
    "a future-deadline leave is left parked"
  );
  assert!(
    matches!(rx_live.try_recv(), Ok(None)),
    "no reply is sent before the deadline",
  );

  // Expired: every replier gets LeaveTimeout and the slot is cleared.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let mut expired = Some(PendingLeave {
    repliers: vec![tx],
    deadline: now - core::time::Duration::from_secs(1),
  });
  reap_pending_leave(&mut expired, now).await;
  assert!(expired.is_none(), "an expired leave clears its slot");
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::LeaveTimeout))),
    "an expired leave replies LeaveTimeout",
  );
}

/// `reap_pending_joins` resolves a waiter whose `pending` set is empty: zero
/// contacts ⇒ `JoinAllFailed(requested, 0)`, any contacts ⇒ `Ok(contacted)`.
/// (The empty-`pending` branch is the degenerate zero-exchange
/// `WaitForCompletion` AND the post-completion drain.)
#[compio::test]
async fn reap_pending_joins_resolves_empty_pending_waiters() {
  let now = Instant::now();
  let mut joins: HashMap<u64, PendingJoin> = HashMap::new();

  // Waiter 1: zero contacts ⇒ JoinAllFailed carrying the requested count.
  let (tx_fail, rx_fail) = futures_channel::oneshot::channel();
  joins.insert(
    1,
    PendingJoin {
      pending: HashSet::new(),
      contacted: 0,
      requested: 3,
      deadline: now + core::time::Duration::from_secs(60),
      reply: tx_fail,
    },
  );
  // Waiter 2: two contacts ⇒ Ok(2).
  let (tx_ok, rx_ok) = futures_channel::oneshot::channel();
  joins.insert(
    2,
    PendingJoin {
      pending: HashSet::new(),
      contacted: 2,
      requested: 2,
      deadline: now + core::time::Duration::from_secs(60),
      reply: tx_ok,
    },
  );

  reap_pending_joins(&mut joins, now).await;
  assert!(joins.is_empty(), "both empty-pending waiters are reaped");

  match rx_fail.await {
    Ok(Err(MemberlistError::JoinAllFailed(e))) => {
      assert_eq!(e.requested(), 3, "carries the requested seed count");
      assert_eq!(e.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }
  assert!(
    matches!(rx_ok.await, Ok(Ok(2))),
    "a waiter with contacts resolves Ok(contacted)",
  );
}
