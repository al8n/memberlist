use super::*;

#[test]
fn std_round_trips_including_pre_first_conversion() {
  let t = std::time::Instant::now();
  // A forward instant round-trips exactly.
  assert_eq!(Instant::from_std(t).into_std(), t);

  // An instant minted seconds earlier — potentially before the shared origin
  // was first observed — must keep its ordering and round-trip, NOT collapse
  // to the origin. This is the regression the back-dated origin guards
  // against (without it, an earlier-than-origin instant saturated to ZERO).
  let earlier = t
    .checked_sub(Duration::from_secs(5))
    .expect("monotonic clock underflow");
  assert_eq!(Instant::from_std(earlier).into_std(), earlier);
  assert!(Instant::from_std(earlier) < Instant::from_std(t));
  assert_ne!(Instant::from_std(earlier), Instant::ORIGIN);
}

#[test]
fn forward_arithmetic_saturates_instead_of_panicking() {
  // Deadlines are built as `now + interval`; an extreme `now` or interval
  // must clamp at the representable maximum, never panic.
  let near_max = Instant::from_origin(Duration::MAX - Duration::from_secs(1));
  assert_eq!(
    (near_max + Duration::from_secs(10)).since_origin(),
    Duration::MAX
  );
  let mut i = near_max;
  i += Duration::from_secs(10);
  assert_eq!(i.since_origin(), Duration::MAX);

  // into_std on an unrepresentable instant clamps rather than panicking.
  let _ = Instant::from_origin(Duration::MAX).into_std();
}

#[test]
fn duration_since_saturates_in_both_directions() {
  // The public `duration_since` is total: a reversed (out-of-order) pair
  // yields zero rather than panicking, matching `std::time::Instant`.
  let earlier = Instant::from_origin(Duration::from_secs(1));
  let later = Instant::from_origin(Duration::from_secs(5));
  assert_eq!(later.duration_since(earlier), Duration::from_secs(4));
  assert_eq!(earlier.duration_since(later), Duration::ZERO);
}
