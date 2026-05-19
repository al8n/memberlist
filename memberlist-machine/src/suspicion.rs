//! Lifeguard suspicion-timer state.

use core::time::Duration;
use std::{collections::HashSet, time::Instant};

use nodecraft::CheapClone;

/// Computes the remaining time before the suspect-timeout fires, given the
/// number of confirmations received (`n`), the required threshold (`k`),
/// the elapsed time since suspicion started, and the min/max bounds.
///
/// Identical to the formula in HashiCorp's Lifeguard paper section 4.2.
#[inline]
pub(crate) fn remaining_suspicion_time(
  n: u32,
  k: u32,
  elapsed: Duration,
  min: Duration,
  max: Duration,
) -> Duration {
  let frac = (n as f64 + 1.0).ln() / (k as f64 + 1.0).ln();
  let raw = max.as_secs_f64() - frac * (max.as_secs_f64() - min.as_secs_f64());
  let timeout_ms = (raw * 1000.0).floor();
  if timeout_ms < min.as_millis() as f64 {
    min.saturating_sub(elapsed)
  } else {
    Duration::from_millis(timeout_ms as u64).saturating_sub(elapsed)
  }
}

/// State tracking a suspect node's pending confirmations and timeout deadline.
///
/// Pure data: this struct does NOT schedule itself. The owning `Endpoint`
/// reads `deadline()` and arranges to call back into the appropriate handler
/// when the wall clock crosses it.
#[derive(Debug)]
pub struct Suspicion<I> {
  /// Number of independent confirmations required to accelerate the timer
  /// to its minimum value. If `0`, the timer is fixed at `min`.
  k: u32,
  min: Duration,
  max: Duration,
  /// When suspicion started.
  start: Instant,
  /// When the timer should fire if no further confirmations arrive.
  deadline: Instant,
  /// Set of peer ids that have already confirmed (excluding the original
  /// suspector). Used to deduplicate `confirm()` calls.
  confirmations: HashSet<I>,
  /// Cached count of confirmations beyond the original suspector. Always
  /// equals `confirmations.len() - 1` (we seed with the original suspector).
  n: u32,
}

/// Result of `Suspicion::confirm`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
  /// The confirmation was new; the deadline was advanced to `new_deadline`.
  Accepted {
    /// The freshly-pulled-in deadline.
    new_deadline: Instant,
  },
  /// The confirmation was a duplicate, the `k` threshold was already
  /// reached, or the source was the original suspector. Deadline unchanged.
  Ignored,
}

impl<I: Eq + std::hash::Hash + CheapClone> Suspicion<I> {
  /// Construct a new suspicion state. `from` is the original suspector and
  /// is excluded from confirmations (so a peer's own suspicion can't count
  /// as a confirmation if it bounces back via gossip). `now` is the current
  /// time the caller observed when transitioning the node to Suspect.
  ///
  /// If `k == 0`, the timer is fixed at `min` (no confirmations expected).
  /// Otherwise, the timer starts at `max` and accelerates toward `min` as
  /// confirmations arrive.
  pub fn new(from: I, k: u32, min: Duration, max: Duration, now: Instant) -> Self {
    let mut confirmations = HashSet::new();
    confirmations.insert(from);
    let initial_timeout = if k < 1 { min } else { max };
    Self {
      k,
      min,
      max,
      start: now,
      deadline: now + initial_timeout,
      confirmations,
      n: 0,
    }
  }

  /// Register a confirmation from `from`. Returns whether the deadline was
  /// advanced. The caller (Endpoint) is responsible for re-arming any
  /// external timer using the new deadline.
  pub fn confirm(&mut self, from: &I, now: Instant) -> Confirmation {
    if self.n >= self.k {
      return Confirmation::Ignored;
    }
    if self.confirmations.contains(from) {
      return Confirmation::Ignored;
    }
    self.confirmations.insert(from.cheap_clone());
    self.n += 1;
    let elapsed = now.saturating_duration_since(self.start);
    let remaining = remaining_suspicion_time(self.n, self.k, elapsed, self.min, self.max);
    self.deadline = now + remaining;
    Confirmation::Accepted {
      new_deadline: self.deadline,
    }
  }

  /// The current absolute deadline at which this suspicion times out.
  pub fn deadline(&self) -> Instant {
    self.deadline
  }

  /// The number of distinct confirmations received (not counting the original
  /// suspector).
  pub fn confirmations(&self) -> u32 {
    self.n
  }

  /// The threshold at which the deadline reaches `min`.
  pub fn k(&self) -> u32 {
    self.k
  }

  /// When suspicion started (used by tests and metrics).
  pub fn started_at(&self) -> Instant {
    self.start
  }
}

#[cfg(test)]
mod tests {
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
    assert!(matches!(r1, Confirmation::Accepted { .. }));
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
      assert!(
        matches!(r, Confirmation::Accepted { .. }),
        "{peer} rejected"
      );
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
    let Confirmation::Accepted { new_deadline } = r else {
      panic!("expected Accepted, got {r:?}");
    };
    assert!(new_deadline < original, "deadline should move earlier");
    assert_eq!(s.deadline(), new_deadline);
  }
}
