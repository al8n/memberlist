use super::*;

#[test]
fn clock_starts_at_base() {
  let clk = Clock::new();
  assert_eq!(clk.elapsed(), Duration::ZERO);
}

#[test]
fn clock_advance_is_monotonic() {
  let mut clk = Clock::new();
  let t0 = clk.now();
  clk.advance(Duration::from_millis(100));
  let t1 = clk.now();
  clk.advance(Duration::from_millis(50));
  let t2 = clk.now();
  assert!(t1 > t0, "t1 must be after t0");
  assert!(t2 > t1, "t2 must be after t1");
}

#[test]
fn clock_at_returns_correct_instant() {
  let clk = Clock::new();
  let t10 = clk.at(10);
  let t20 = clk.at(20);
  assert_eq!(t20 - t10, Duration::from_secs(10));
}

#[test]
fn clock_multiple_advances_accumulate() {
  let mut clk = Clock::new();
  for _ in 0..10 {
    clk.advance(Duration::from_millis(100));
  }
  assert_eq!(clk.elapsed(), Duration::from_millis(1000));
}
