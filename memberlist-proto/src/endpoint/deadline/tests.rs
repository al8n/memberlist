use super::*;
use core::time::Duration;

/// A distinct, ordered instant `secs` after the origin.
fn t(secs: u64) -> Instant {
  Instant::from_origin(Duration::from_secs(secs))
}

/// A distinct key. `u64` is `Eq + Hash + CheapClone`, the exact bound the index
/// requires of a real node id or `StreamId`, so it exercises the same code paths
/// with no test scaffolding.
fn id(n: u64) -> u64 {
  n
}

#[test]
fn empty_index_has_no_earliest() {
  let idx = DeadlineIndex::<u64>::new();
  assert_eq!(idx.earliest(), None);
  assert!(idx.is_empty());
  assert_eq!(idx.live_key_count(), 0);
  assert_eq!(idx.entry_count(), 0);
}

#[test]
fn single_suspicion_returns_its_deadline() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(1), Some(t(10)));
  assert_eq!(idx.earliest(), Some(t(10)));
  assert!(!idx.is_empty());
  assert_eq!(idx.live_key_count(), 1);
  assert_eq!(idx.current_deadline(&id(1)), Some(t(10)));
  assert!(idx.contains(&id(1)));
}

#[test]
fn earliest_returns_minimum_across_members() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(30)));
  idx.set(&id(1), Some(t(10)));
  idx.set(&id(2), Some(t(20)));
  assert_eq!(idx.earliest(), Some(t(10)));
  assert_eq!(idx.live_key_count(), 3);
  assert_eq!(idx.entry_count(), 3);
}

#[test]
fn setting_none_removes_a_member_from_the_minimum() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(10)));
  idx.set(&id(1), Some(t(20)));
  assert_eq!(idx.earliest(), Some(t(10)));
  // Remove the current minimum; its ordered entry is dropped and the next
  // minimum surfaces.
  idx.set(&id(0), None);
  assert_eq!(idx.earliest(), Some(t(20)));
  assert_eq!(idx.live_key_count(), 1);
  assert!(!idx.contains(&id(0)));
  idx.set(&id(1), None);
  assert_eq!(idx.earliest(), None);
  assert!(idx.is_empty());
  assert_eq!(idx.live_key_count(), 0);
}

#[test]
fn confirmation_accelerating_a_deadline_supersedes_its_prior_entry() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(50)));
  idx.set(&id(1), Some(t(90)));
  assert_eq!(idx.earliest(), Some(t(50)));
  // A confirmation pulls id(1)'s deadline in BELOW id(0): its prior ordered
  // entry (t=90) is removed as the new one is inserted, so the accelerated
  // deadline surfaces and no stale entry lingers.
  idx.set(&id(1), Some(t(20)));
  assert_eq!(idx.earliest(), Some(t(20)));
  assert_eq!(idx.current_deadline(&id(1)), Some(t(20)));
  assert_eq!(
    idx.entry_count(),
    2,
    "an update replaces, never accumulates"
  );
  // Direction-agnostic: moving a deadline LATER also replaces cleanly.
  idx.set(&id(1), Some(t(70)));
  assert_eq!(idx.earliest(), Some(t(50)));
  assert_eq!(idx.entry_count(), 2);
}

#[test]
fn set_unchanged_deadline_is_a_noop() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(10)));
  assert_eq!(idx.earliest(), Some(t(10)));
  // Re-registering the SAME deadline must neither grow the index nor rewrite
  // its entry.
  for _ in 0..100 {
    idx.set(&id(0), Some(t(10)));
  }
  assert_eq!(idx.earliest(), Some(t(10)));
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
  idx.set(&id(0), Some(t(10)));
  idx.set(&id(0), None);
  assert_eq!(idx.earliest(), None);
  // A fresh registration re-inserts the member under a new `seq`; the removed
  // entry was physically dropped, so nothing masks it.
  idx.set(&id(0), Some(t(10)));
  assert_eq!(idx.earliest(), Some(t(10)));
  assert_eq!(idx.entry_count(), 1);
}

#[test]
fn distinct_members_are_distinct_keys() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(7), Some(t(10)));
  idx.set(&id(8), Some(t(20)));
  assert_eq!(idx.live_key_count(), 2);
  assert_eq!(idx.earliest(), Some(t(10)));
  // Clearing one leaves the other intact.
  idx.set(&id(7), None);
  assert_eq!(idx.earliest(), Some(t(20)));
  assert!(idx.contains(&id(8)));
  assert!(!idx.contains(&id(7)));
}

#[test]
fn confirmations_keep_storage_proportional_to_live_suspicions() {
  // A non-minimum suspicion whose deadline is accelerated repeatedly (a
  // confirmation flood) must not grow storage: each change replaces the
  // member's prior ordered entry rather than stranding it. With one earlier
  // member pinned as the minimum and a second accelerated 10_000 times, the
  // index holds exactly the two live suspicions — not one entry per update.
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(1))); // the pinned, always-minimum suspicion
  let updates = 10_000u64;
  for round in 0..updates {
    // Always LATER than id(0), so id(0) stays the surfaced minimum and the
    // moved key is never the first entry — the exact shape that accretes
    // tombstones in a lazy-heap design.
    idx.set(&id(1), Some(t(100 + round)));
  }
  assert_eq!(
    idx.earliest(),
    Some(t(1)),
    "the pinned suspicion stays the minimum"
  );
  assert_eq!(
    idx.entry_count(),
    2,
    "storage must equal the live-suspicion count (2), not grow with the {updates} updates"
  );
  assert_eq!(idx.live_key_count(), 2);
}

#[test]
fn is_empty_tracks_live_suspicions() {
  let mut idx = DeadlineIndex::new();
  assert!(idx.is_empty());
  idx.set(&id(0), Some(t(10)));
  assert!(!idx.is_empty());
  idx.set(&id(1), Some(t(20)));
  assert!(!idx.is_empty());
  idx.set(&id(0), None);
  assert!(!idx.is_empty());
  idx.set(&id(1), None);
  assert!(idx.is_empty());
}

#[test]
fn clear_empties_both_maps_at_once() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(10)));
  idx.set(&id(1), Some(t(20)));
  idx.set(&id(2), Some(t(30)));
  assert_eq!(idx.entry_count(), 3);
  idx.clear();
  assert_eq!(idx.earliest(), None);
  assert!(idx.is_empty());
  assert_eq!(idx.entry_count(), 0);
  assert_eq!(idx.live_key_count(), 0);
  // Post-clear the index is fully usable again — a fresh registration surfaces
  // with no stale masking.
  idx.set(&id(5), Some(t(7)));
  assert_eq!(idx.earliest(), Some(t(7)));
  assert_eq!(idx.entry_count(), 1);
}

#[test]
fn earliest_uncounted_reads_the_minimum_without_bumping_the_scan_counter() {
  let mut idx = DeadlineIndex::new();
  idx.set(&id(0), Some(t(30)));
  idx.set(&id(1), Some(t(10)));
  idx.reset_entities_scanned();
  // The oracle read must observe the same minimum as `earliest`...
  assert_eq!(idx.earliest_uncounted(), Some(t(10)));
  // ...but must NOT count, so it cannot perturb the O(1)-scan measurement.
  assert_eq!(
    idx.entities_scanned(),
    0,
    "earliest_uncounted must not touch the scan counter"
  );
  // The counting path still counts.
  assert_eq!(idx.earliest(), Some(t(10)));
  assert_eq!(idx.entities_scanned(), 1);
}

#[test]
fn earliest_examines_o1_entries_regardless_of_live_key_count() {
  // Populate a large live table, then confirm one `earliest` call examines a
  // constant number of entries independent of the table size — the property
  // that keeps `poll_timeout` off the O(members + intents) fold.
  fn scanned_for(n: u64) -> u64 {
    let mut idx = DeadlineIndex::new();
    for i in 0..n {
      // Deadlines increase with i so the minimum is a single fixed entry.
      idx.set(&id(i), Some(t(100 + i)));
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
