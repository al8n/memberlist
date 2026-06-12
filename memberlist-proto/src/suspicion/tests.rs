use super::*;
use smol_str::SmolStr;

fn id(s: &str) -> SmolStr {
  SmolStr::new(s)
}

#[test]
fn remaining_time_matches_legacy_vector() {
  // Verbatim port of memberlist-core/src/suspicion.rs:252-313
  let cases: &[(u32, u32, Duration, Duration, Duration, Duration)] = &[
    (
      0,
      3,
      Duration::from_secs(0),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::from_secs(30),
    ),
    (
      1,
      3,
      Duration::from_secs(2),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::from_secs(14),
    ),
    (
      2,
      3,
      Duration::from_secs(3),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::from_millis(4810),
    ),
    (
      3,
      3,
      Duration::from_secs(4),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::ZERO,
    ),
    (
      4,
      3,
      Duration::from_secs(5),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::ZERO,
    ),
    (
      5,
      3,
      Duration::from_secs(10),
      Duration::from_secs(2),
      Duration::from_secs(30),
      Duration::ZERO,
    ),
  ];
  for (i, &(n, k, elapsed, min, max, expected)) in cases.iter().enumerate() {
    let got = remaining_suspicion_time(n, k, elapsed, min, max);
    assert_eq!(got, expected, "case {i}: got {got:?} expected {expected:?}");
  }
}

#[test]
fn new_with_k_zero_uses_min_immediately() {
  let now = Instant::now();
  let s = Suspicion::new(
    id("alice"),
    0,
    Duration::from_millis(500),
    Duration::from_secs(30),
    now,
  );
  assert_eq!(s.deadline(), now + Duration::from_millis(500));
  assert_eq!(s.confirmations(), 0);
  assert_eq!(s.k(), 0);
}

#[test]
fn new_with_k_positive_uses_max() {
  let now = Instant::now();
  let s = Suspicion::new(
    id("alice"),
    3,
    Duration::from_millis(500),
    Duration::from_secs(30),
    now,
  );
  assert_eq!(s.deadline(), now + Duration::from_secs(30));
  assert_eq!(s.k(), 3);
}

#[test]
fn confirm_from_original_suspector_is_ignored() {
  let now = Instant::now();
  let mut s = Suspicion::new(
    id("alice"),
    3,
    Duration::from_millis(500),
    Duration::from_secs(30),
    now,
  );
  let r = s.confirm(&id("alice"), now);
  assert_eq!(r, Confirmation::Ignored);
  assert_eq!(s.confirmations(), 0);
}

#[test]
fn confirm_duplicate_from_same_peer_is_ignored() {
  let now = Instant::now();
  let mut s = Suspicion::new(
    id("alice"),
    3,
    Duration::from_millis(500),
    Duration::from_secs(30),
    now,
  );
  let r1 = s.confirm(&id("bob"), now);
  assert!(matches!(r1, Confirmation::Accepted(_)));
  assert_eq!(s.confirmations(), 1);

  let r2 = s.confirm(&id("bob"), now);
  assert_eq!(r2, Confirmation::Ignored);
  assert_eq!(s.confirmations(), 1);
}

#[test]
fn confirm_accepts_distinct_peers_until_k() {
  let now = Instant::now();
  let mut s = Suspicion::new(
    id("alice"),
    3,
    Duration::from_millis(500),
    Duration::from_secs(30),
    now,
  );
  for peer in &["bob", "carol", "dave"] {
    let r = s.confirm(&id(peer), now);
    assert!(matches!(r, Confirmation::Accepted(_)), "{peer} rejected");
  }
  assert_eq!(s.confirmations(), 3);

  // Fourth confirm exceeds k=3 and is ignored.
  let r = s.confirm(&id("eve"), now);
  assert_eq!(r, Confirmation::Ignored);
  assert_eq!(s.confirmations(), 3);
}

#[test]
fn confirm_pulls_deadline_inward() {
  let now = Instant::now();
  let mut s = Suspicion::new(
    id("alice"),
    3,
    Duration::from_millis(500),
    Duration::from_secs(30),
    now,
  );
  let original = s.deadline();
  let r = s.confirm(&id("bob"), now);
  let Confirmation::Accepted(new_deadline) = r else {
    panic!("expected Accepted, got {r:?}");
  };
  assert!(new_deadline < original, "deadline should move earlier");
  assert_eq!(s.deadline(), new_deadline);
}
