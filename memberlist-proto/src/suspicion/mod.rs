//! Lifeguard suspicion-timer state.

use derive_more::{IsVariant, TryUnwrap, Unwrap};

use crate::{FxHashSet, Instant};
use core::time::Duration;

use crate::CheapClone;
use core::hash::Hash;

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
  let frac = crate::mathf::ln(n as f64 + 1.0) / crate::mathf::ln(k as f64 + 1.0);
  let raw = max.as_secs_f64() - frac * (max.as_secs_f64() - min.as_secs_f64());
  let timeout_ms = crate::mathf::floor(raw * 1000.0);
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
  confirmations: FxHashSet<I>,
  /// Cached count of confirmations beyond the original suspector. Always
  /// equals `confirmations.len() - 1` (we seed with the original suspector).
  n: u32,
}

/// Result of `Suspicion::confirm`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IsVariant, Unwrap, TryUnwrap)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
pub enum Confirmation {
  /// The confirmation was new; the deadline was advanced. Carries the
  /// freshly-pulled-in deadline.
  Accepted(Instant),
  /// The confirmation was a duplicate, the `k` threshold was already
  /// reached, or the source was the original suspector. Deadline unchanged.
  Ignored,
}

impl<I> Suspicion<I> {
  /// The current absolute deadline at which this suspicion times out.
  #[inline(always)]
  pub const fn deadline(&self) -> Instant {
    self.deadline
  }

  /// The number of distinct confirmations received (not counting the original
  /// suspector).
  #[inline(always)]
  pub const fn confirmations(&self) -> u32 {
    self.n
  }

  /// The threshold at which the deadline reaches `min`.
  #[inline(always)]
  pub const fn k(&self) -> u32 {
    self.k
  }

  /// When suspicion started (used by tests and metrics).
  #[inline(always)]
  pub const fn started_at(&self) -> Instant {
    self.start
  }
}

impl<I> Suspicion<I>
where
  I: Eq + Hash,
{
  /// Construct a new suspicion state. `from` is the original suspector and
  /// is excluded from confirmations (so a peer's own suspicion can't count
  /// as a confirmation if it bounces back via gossip). `now` is the current
  /// time the caller observed when transitioning the node to Suspect.
  ///
  /// If `k == 0`, the timer is fixed at `min` (no confirmations expected).
  /// Otherwise, the timer starts at `max` and accelerates toward `min` as
  /// confirmations arrive.
  pub fn new(from: I, k: u32, min: Duration, max: Duration, now: Instant) -> Self {
    let mut confirmations = FxHashSet::default();
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
}

impl<I> Suspicion<I>
where
  I: Eq + Hash + CheapClone,
{
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
    Confirmation::Accepted(self.deadline)
  }
}

#[cfg(test)]
mod tests;
