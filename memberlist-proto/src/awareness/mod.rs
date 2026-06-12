//! Lifeguard health-awareness score. Tracks the local node's estimated health,
//! used to scale timeouts during periods of self-detected degradation.

use core::time::Duration;

/// Tracks the local node's estimated health. Lower scores mean healthier;
/// zero is the minimum and means fully healthy. The score is clamped to
/// `[0, max - 1]`, where `max` is the upper threshold supplied at construction.
///
/// Used by the Lifeguard SWIM extension to slow our own probing rate when we
/// look unhealthy, so we don't kill innocent peers.
#[derive(Debug, Clone, Copy)]
pub struct Awareness {
  max: u32,
  score: u32,
}

impl Awareness {
  /// Construct an awareness tracker with the given upper threshold.
  /// The score will be clamped to `[0, max - 1]`. Panics if `max == 0`.
  #[inline(always)]
  pub const fn new(max: u32) -> Self {
    assert!(max >= 1, "Awareness::new: max must be >= 1");
    Self { max, score: 0 }
  }

  /// Record a positive health signal (e.g. successful probe response).
  /// Decreases the score by 1, saturating at 0 (lower = healthier).
  #[inline(always)]
  pub fn record_success(&mut self) {
    self.score = self.score.saturating_sub(1);
  }

  /// Record a negative health signal (e.g. failed probe, forced refute).
  /// Increases the score by `severity`, clamping to `max - 1`.
  #[inline(always)]
  pub fn record_failure(&mut self, severity: u32) {
    self.score = self.score.saturating_add(severity).min(self.max - 1);
  }

  /// Returns the current health score (`0` = fully healthy, `max - 1` = worst).
  #[inline(always)]
  pub const fn health_score(&self) -> u32 {
    self.score
  }

  /// Returns the upper threshold passed to `new`.
  #[inline(always)]
  pub const fn max(&self) -> u32 {
    self.max
  }

  /// Scales the given timeout by `(score + 1)`. Lower health → longer
  /// timeout. memberlist-core computes `timeout * (score + 1)` with an
  /// unchecked `Duration * u32` that panics on overflow; `score` is
  /// bounded by `awareness_max_multiplier` (≤7 by default) so this is
  /// unreachable for any sane `probe_interval`. As a library Sans-I/O
  /// machine we still degrade a pathologically-large configured duration
  /// to the unscaled timeout instead of panicking; behavior is otherwise
  /// identical to upstream.
  pub fn scale_timeout(&self, timeout: Duration) -> Duration {
    timeout.checked_mul(self.score + 1).unwrap_or(timeout)
  }
}

#[cfg(test)]
mod tests;
