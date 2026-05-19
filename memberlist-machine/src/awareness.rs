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
  pub const fn new(max: u32) -> Self {
    assert!(max >= 1, "Awareness::new: max must be >= 1");
    Self { max, score: 0 }
  }

  /// Record a positive health signal (e.g. successful probe response).
  /// Decreases the score by 1, saturating at 0 (lower = healthier).
  pub fn record_success(&mut self) {
    self.score = self.score.saturating_sub(1);
  }

  /// Record a negative health signal (e.g. failed probe, forced refute).
  /// Increases the score by `severity`, clamping to `max - 1`.
  pub fn record_failure(&mut self, severity: u32) {
    self.score = self.score.saturating_add(severity).min(self.max - 1);
  }

  /// Returns the current health score (`0` = fully healthy, `max - 1` = worst).
  pub const fn health_score(&self) -> u32 {
    self.score
  }

  /// Returns the upper threshold passed to `new`.
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
mod tests {
  use super::*;

  #[test]
  fn record_success_floors_at_zero() {
    let mut a = Awareness::new(8);
    assert_eq!(a.health_score(), 0);
    a.record_success();
    assert_eq!(a.health_score(), 0);
    for _ in 0..10 {
      a.record_success();
    }
    assert_eq!(a.health_score(), 0);
  }

  #[test]
  fn record_failure_ceilings_at_max_minus_one() {
    let mut a = Awareness::new(8);
    a.record_failure(100);
    assert_eq!(a.health_score(), 7);
  }

  #[test]
  fn record_signals_match_legacy_clamp_behavior() {
    // Mirrors the legacy memberlist-core/src/awareness.rs:78-105 test vector,
    // expressed in terms of record_success / record_failure instead of the
    // signed apply_delta the legacy used.
    let mut a = Awareness::new(8);
    // Sequence: no-op, -1, -10, +1, -1, +10, -1, -1, -1, -1, -1, -1, -1, -1.
    // Expected scores: 0, 0, 0, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0, 0.
    let one_sec = Duration::from_secs(1);
    let signals: &[Signal] = &[
      Signal::Noop,
      Signal::Success,
      Signal::SuccessN(10),
      Signal::Failure(1),
      Signal::Success,
      Signal::Failure(10),
      Signal::Success,
      Signal::Success,
      Signal::Success,
      Signal::Success,
      Signal::Success,
      Signal::Success,
      Signal::Success,
      Signal::Success,
    ];
    let expected_scores: &[u32] = &[0, 0, 0, 1, 0, 7, 6, 5, 4, 3, 2, 1, 0, 0];
    let expected_secs: &[u64] = &[1, 1, 1, 2, 1, 8, 7, 6, 5, 4, 3, 2, 1, 1];

    enum Signal {
      Noop,
      Success,
      SuccessN(u32),
      Failure(u32),
    }

    for (i, signal) in signals.iter().enumerate() {
      match signal {
        Signal::Noop => {}
        Signal::Success => a.record_success(),
        Signal::SuccessN(n) => {
          for _ in 0..*n {
            a.record_success();
          }
        }
        Signal::Failure(n) => a.record_failure(*n),
      }
      assert_eq!(a.health_score(), expected_scores[i], "step {i}");
      assert_eq!(
        a.scale_timeout(one_sec),
        Duration::from_secs(expected_secs[i]),
        "step {i} timeout"
      );
    }
  }

  #[test]
  fn scale_timeout_at_zero_score() {
    let a = Awareness::new(8);
    assert_eq!(
      a.scale_timeout(Duration::from_millis(500)),
      Duration::from_millis(500)
    );
  }

  #[test]
  fn scale_timeout_at_max_score() {
    let mut a = Awareness::new(8);
    a.record_failure(100);
    assert_eq!(
      a.scale_timeout(Duration::from_secs(1)),
      Duration::from_secs(8)
    );
  }

  /// A pathologically large configured duration must NOT panic on the
  /// `Duration * (score+1)` overflow (memberlist-core's unchecked mul
  /// would). Degrade to the unscaled timeout instead.
  #[test]
  fn scale_timeout_saturates_instead_of_panicking() {
    let mut a = Awareness::new(8);
    a.record_failure(100); // score = 7 → multiplier 8
    let huge = Duration::MAX;
    // 8 * Duration::MAX overflows; must return `huge` unscaled, not panic.
    assert_eq!(a.scale_timeout(huge), huge);
    // A normal duration is still scaled exactly.
    assert_eq!(
      a.scale_timeout(Duration::from_millis(10)),
      Duration::from_millis(80)
    );
  }

  #[test]
  #[should_panic(expected = "Awareness::new: max must be >= 1")]
  fn new_with_zero_max_panics() {
    Awareness::new(0);
  }
}
