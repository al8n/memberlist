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
  // Remove the current minimum; its ordered entry is dropped and the next
  // minimum surfaces.
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
  // Move conn(0) LATER than conn(1): its prior ordered entry (t=10) is removed
  // as the new one is inserted, so the true minimum (conn(1) at t=50) surfaces
  // and no stale entry lingers.
  idx.set(conn(0), Some(t(90)));
  assert_eq!(idx.earliest(), Some(t(50)));
  assert_eq!(
    idx.entry_count(),
    2,
    "an update replaces, never accumulates"
  );
  // Move conn(0) EARLIER again; the fresh entry wins.
  idx.set(conn(0), Some(t(5)));
  assert_eq!(idx.earliest(), Some(t(5)));
  assert_eq!(idx.entry_count(), 2);
}

#[test]
fn set_unchanged_deadline_is_a_noop() {
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(10)));
  assert_eq!(idx.earliest(), Some(t(10)));
  idx.reset_entities_scanned();
  // Re-registering the SAME deadline must neither grow the index nor rewrite
  // its entry: a subsequent `earliest` still examines just the one live entry.
  for _ in 0..100 {
    idx.set(conn(0), Some(t(10)));
  }
  assert_eq!(idx.earliest(), Some(t(10)));
  assert_eq!(
    idx.entities_scanned(),
    1,
    "an unchanged re-set must not add index entries to skip past"
  );
  assert_eq!(
    idx.entry_count(),
    1,
    "an unchanged re-set must not grow storage"
  );
  assert_eq!(idx.live_key_count(), 1);
}

#[test]
fn removing_then_reinserting_the_same_deadline_is_live_again() {
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(10)));
  idx.set(conn(0), None);
  assert_eq!(idx.earliest(), None);
  // A fresh registration re-inserts the key under a new generation; the removed
  // entry was physically dropped, so nothing masks it.
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
  // Populate a large live table, then confirm one `earliest` call examines a
  // constant number of entries independent of the table size — the property
  // that keeps `poll_timeout` off the O(connections + bridges) fold.
  fn scanned_for(n: usize) -> u64 {
    let mut idx = DeadlineIndex::new();
    for i in 0..n {
      // Deadlines increase with i so the minimum is a single fixed entry.
      idx.set(conn(i), Some(t(100 + i as u64)));
    }
    idx.reset_entities_scanned();
    let _ = idx.earliest();
    idx.entities_scanned()
  }
  let small = scanned_for(4);
  let large = scanned_for(4096);
  assert_eq!(small, 1, "the live minimum is a single ordered entry");
  assert_eq!(
    large, small,
    "examination count must not grow with the number of live keys"
  );
}

#[test]
fn updates_keep_storage_proportional_to_live_keys() {
  // A non-minimum key whose deadline churns under a packet flood must not grow
  // storage: each change replaces the key's prior ordered entry rather than
  // stranding it. With one earlier key pinned as the minimum and a second key
  // moved 10_000 times, the index holds exactly the two live keys — not one
  // entry per update. The old lazy-heap-of-tombstones design left the moved
  // key's superseded entries behind (a stable earlier minimum keeps them off
  // the top), so its storage grew with the update count.
  let mut idx = DeadlineIndex::new();
  idx.set(conn(0), Some(t(1))); // the pinned, always-minimum key
  let updates = 10_000u64;
  for round in 0..updates {
    // Always LATER than conn(0), so conn(0) stays the surfaced minimum and the
    // moved key is never the first entry — the exact shape that accreted
    // tombstones before.
    idx.set(conn(1), Some(t(100 + round)));
  }
  assert_eq!(
    idx.earliest(),
    Some(t(1)),
    "the pinned key stays the minimum"
  );
  assert_eq!(
    idx.entry_count(),
    2,
    "storage must equal the live-key count (2), not grow with the {updates} updates"
  );
  assert_eq!(idx.live_key_count(), 2);
}
