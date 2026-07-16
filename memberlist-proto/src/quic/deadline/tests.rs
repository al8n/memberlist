use super::*;
use core::time::Duration;

use crate::event::StreamId;
use quinn_proto::ConnectionHandle;

/// A distinct, ordered instant `secs` after the origin.
fn t(secs: u64) -> Instant {
  Instant::from_origin(Duration::from_secs(secs))
}

fn conn(n: usize) -> TimerKey {
  TimerKey::Conn(ConnectionHandle(n))
}

#[test]
fn empty_index_has_no_earliest() {
  let mut idx = DeadlineIndex::new();
  assert_eq!(idx.earliest(), None);
  assert_eq!(idx.live_key_count(), 0);
}

#[test]
fn single_key_returns_its_deadline() {
  let mut idx = DeadlineIndex::new();
  idx.set(TimerKey::Endpoint, Some(t(10)));
  assert_eq!(idx.earliest(), Some(t(10)));
  assert_eq!(idx.live_key_count(), 1);
}

#[test]
fn earliest_returns_minimum_across_keys() {
  let mut idx = DeadlineIndex::new();
  idx.set(TimerKey::Endpoint, Some(t(30)));
  idx.set(conn(0), Some(t(10)));
  idx.set(conn(1), Some(t(20)));
  idx.set(TimerKey::ImmediateDue, Some(t(25)));
  assert_eq!(idx.earliest(), Some(t(10)));
}

#[test]
fn setting_none_removes_a_key_from_the_minimum() {
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(10)));
  idx.set(conn(1), Some(t(20)));
  assert_eq!(idx.earliest(), Some(t(10)));
  // Remove the current minimum; the next minimum surfaces past the tombstone.
  idx.set(conn(0), None);
  assert_eq!(idx.earliest(), Some(t(20)));
  assert_eq!(idx.live_key_count(), 1);
  idx.set(conn(1), None);
  assert_eq!(idx.earliest(), None);
  assert_eq!(idx.live_key_count(), 0);
}

#[test]
fn updating_a_key_supersedes_its_prior_entry() {
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(10)));
  idx.set(conn(1), Some(t(50)));
  // Move conn(0) LATER than conn(1): its old entry (t=10) becomes a tombstone
  // that still sits at the heap top, and must be discarded so the true minimum
  // (conn(1) at t=50) surfaces.
  idx.set(conn(0), Some(t(90)));
  assert_eq!(idx.earliest(), Some(t(50)));
  // Move conn(0) EARLIER again; the fresh entry wins.
  idx.set(conn(0), Some(t(5)));
  assert_eq!(idx.earliest(), Some(t(5)));
}

#[test]
fn set_unchanged_deadline_is_a_noop() {
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(10)));
  // Draining the top settles the heap; the live entry is not popped.
  assert_eq!(idx.earliest(), Some(t(10)));
  idx.reset_entities_scanned();
  // Re-registering the SAME deadline must neither grow the heap nor create a
  // tombstone: a subsequent `earliest` still examines just the one live entry.
  for _ in 0..100 {
    idx.set(conn(0), Some(t(10)));
  }
  assert_eq!(idx.earliest(), Some(t(10)));
  assert_eq!(
    idx.entities_scanned(),
    1,
    "an unchanged re-set must not add heap entries to skip past"
  );
  assert_eq!(idx.live_key_count(), 1);
}

#[test]
fn removing_then_reinserting_the_same_deadline_is_live_again() {
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(10)));
  idx.set(conn(0), None);
  assert_eq!(idx.earliest(), None);
  // A fresh registration with the same deadline gets a new generation, so the
  // stale removed entry does not mask it.
  idx.set(conn(0), Some(t(10)));
  assert_eq!(idx.earliest(), Some(t(10)));
}

#[test]
fn same_streamid_in_bridge_and_dial_do_not_alias() {
  let mut idx = DeadlineIndex::new();
  let sid = StreamId::from_raw(7);
  idx.set(TimerKey::Bridge(sid), Some(t(10)));
  idx.set(TimerKey::Dial(sid), Some(t(20)));
  assert_eq!(
    idx.live_key_count(),
    2,
    "distinct variants are distinct keys"
  );
  assert_eq!(idx.earliest(), Some(t(10)));
  // Clearing the bridge leaves the dial intact.
  idx.set(TimerKey::Bridge(sid), None);
  assert_eq!(idx.earliest(), Some(t(20)));
}

#[test]
fn earliest_examines_o1_entries_regardless_of_live_key_count() {
  // Populate a large live table, settle it, then confirm one `earliest` call
  // examines a constant number of entries independent of the table size — the
  // property that keeps `poll_timeout` off the O(connections + bridges) fold.
  fn scanned_for(n: usize) -> u64 {
    let mut idx = DeadlineIndex::new();
    for i in 0..n {
      // Deadlines increase with i so the minimum is a single fixed entry.
      idx.set(conn(i), Some(t(100 + i as u64)));
    }
    // Settle (discard nothing — all entries are live), then measure the next
    // read in isolation.
    let _ = idx.earliest();
    idx.reset_entities_scanned();
    let _ = idx.earliest();
    idx.entities_scanned()
  }
  let small = scanned_for(4);
  let large = scanned_for(4096);
  assert_eq!(small, 1, "the live minimum is a single heap top");
  assert_eq!(
    large, small,
    "examination count must not grow with the number of live keys"
  );
}

#[test]
fn each_tombstone_is_discarded_at_most_once() {
  // Repeatedly moving the minimum key later strands a tombstone at the top each
  // time; across many reads each tombstone is popped exactly once, so the total
  // examination count is bounded by (pushes + reads), never quadratic.
  let mut idx = DeadlineIndex::new();
  idx.set(conn(1), Some(t(1_000_000)));
  idx.reset_entities_scanned();
  let rounds = 200u64;
  for r in 0..rounds {
    // conn(0) sits at the top, then jumps far past conn(1): its old entry is a
    // tombstone the next `earliest` must skip exactly once.
    idx.set(conn(0), Some(t(r)));
    let _ = idx.earliest();
  }
  // Each round pushes one entry and reads once; discards are amortized so the
  // cumulative examination count is linear in rounds, not quadratic.
  assert!(
    idx.entities_scanned() <= 3 * rounds,
    "amortized O(1): got {} examinations over {} rounds",
    idx.entities_scanned(),
    rounds
  );
}
