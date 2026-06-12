//! Deterministic virtual clock in [`memberlist_proto::Instant`] time.
//!
//! `Clock::new()` anchors at a fixed, large offset past the machine-time
//! origin (not a wall-clock sample, so the harness is fully deterministic).
//! All time advancement accumulates a `Duration` from that anchor.
//! `Endpoint` only ever does `now + Duration` arithmetic internally, so
//! feeding computed `base + elapsed` is semantically identical to feeding a
//! real clock — but tests never sleep. The large anchor offset also gives
//! backward aging ([`Cluster::age_node`](crate::Cluster::age_node)) ample
//! headroom: `Instant - Duration` saturates at the origin.

use memberlist_proto::Instant;
use std::time::Duration;

/// Fixed offset of the simulation origin past the machine-time origin. Large
/// enough that any backward state-change aging a scenario performs stays well
/// above the origin (where `Instant - Duration` would otherwise saturate).
const ANCHOR_OFFSET: Duration = Duration::from_secs(86_400);

/// A deterministic virtual clock.
///
/// (Illustrative; `doctest = false` for this harness crate — the
/// behavior is verified by the `clock_advance_is_monotonic` unit test.)
///
/// ```ignore
/// use memberlist_simulation::Clock;
/// use std::time::Duration;
///
/// let mut clk = Clock::new();
/// let t0 = clk.now();
/// clk.advance(Duration::from_secs(1));
/// assert!(clk.now() > t0);
/// ```
#[derive(Debug, Clone)]
pub struct Clock {
  base: Instant,
  elapsed: Duration,
}

impl Clock {
  /// Anchor the simulation origin at a fixed offset past the machine-time
  /// origin (deterministic — no wall-clock sample).
  pub fn new() -> Self {
    Self {
      base: Instant::from_origin(ANCHOR_OFFSET),
      elapsed: Duration::ZERO,
    }
  }

  /// Current simulated time.
  pub fn now(&self) -> Instant {
    self.base + self.elapsed
  }

  /// Advance simulated time by `by`. Panics if `by` would overflow.
  pub fn advance(&mut self, by: Duration) {
    self.elapsed = self
      .elapsed
      .checked_add(by)
      .expect("Clock::advance: duration overflow");
  }

  /// Return the simulated `Instant` that is `secs` seconds after the anchor.
  ///
  /// Useful for constructing absolute deadlines in tests without arithmetic.
  pub fn at(&self, secs: u64) -> Instant {
    self.base + Duration::from_secs(secs)
  }

  /// Raw elapsed duration since clock origin.
  pub fn elapsed(&self) -> Duration {
    self.elapsed
  }
}

impl Default for Clock {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests;
