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
