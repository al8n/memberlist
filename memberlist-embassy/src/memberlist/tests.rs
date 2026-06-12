use super::{MAX_SOCKET_TIMEOUT, checked_socket_timeout, duration_to_ticks};
use core::time::Duration;

// 1 GHz, 1 MHz, and 1 Hz span fine through coarse tick rates. The crate compiles at
// one fixed `embassy_time::TICK_HZ`, so the parameterized helpers are what let these
// exercise a coarse rate without a second build.
const GHZ: u128 = 1_000_000_000;
const MHZ: u128 = 1_000_000;
const HZ: u128 = 1;

#[test]
fn duration_to_ticks_floors_to_whole_ticks() {
  // 1.999 s collapses to a single tick at 1 Hz.
  assert_eq!(duration_to_ticks(Duration::from_millis(1999), HZ), 1);
  // Whole seconds are exact at every rate.
  assert_eq!(duration_to_ticks(Duration::from_secs(2), HZ), 2);
  assert_eq!(duration_to_ticks(Duration::from_secs(2), MHZ), 2_000_000);
}

#[test]
fn duration_to_ticks_keeps_sub_microsecond_resolution_at_fine_rates() {
  // The nanosecond basis (not microseconds) keeps 500 ns = 500 ticks at 1 GHz.
  assert_eq!(duration_to_ticks(Duration::from_nanos(500), GHZ), 500);
  // The same 500 ns floors away to zero ticks at 1 Hz.
  assert_eq!(duration_to_ticks(Duration::from_nanos(500), HZ), 0);
}

#[test]
fn duration_to_ticks_saturates_instead_of_panicking() {
  // An absurd duration converts to a clamped tick count rather than overflowing.
  assert_eq!(
    duration_to_ticks(Duration::from_secs(u64::MAX), GHZ),
    u64::MAX
  );
}

#[test]
fn coarse_tick_rate_rejects_a_portable_valid_socket_timeout() {
  // 1.999 s exceeds the 1.5 s close and 1.0 s stream deadlines as core::Durations, but
  // all three floor to a single tick at 1 Hz, so the installed backstop would not
  // outlast the engine's own deadline.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_millis(1999),
      Duration::from_millis(1500),
      Duration::from_secs(1),
      HZ,
    ),
    None
  );
  // Three whole seconds clears both deadlines (3 ticks > 1 tick) at 1 Hz.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_secs(3),
      Duration::from_millis(1500),
      Duration::from_secs(1),
      HZ,
    ),
    Some(3)
  );
}

#[test]
fn equal_after_flooring_is_rejected() {
  // 500 ns over the close deadline disappears at 1 MHz, tying the installed microseconds.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_nanos(1_000_500),
      Duration::from_micros(1000),
      Duration::from_micros(1000),
      MHZ,
    ),
    None
  );
}

#[test]
fn fine_tick_rate_rejects_a_microsecond_floored_tie() {
  // At 1 GHz the tick count is finer than a microsecond: 1 ms + 1 ns clears the tick
  // comparison (1_000_000_001 > 1_000_000_000 ticks) but embassy-net's microsecond floor
  // installs 1000 us — equal to the deadline, the early-abort the check must prevent.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_nanos(1_000_001),
      Duration::from_millis(1),
      Duration::from_millis(1),
      GHZ,
    ),
    None
  );
  // One whole microsecond of headroom installs 1001 us and is accepted.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_micros(1001),
      Duration::from_millis(1),
      Duration::from_millis(1),
      GHZ,
    ),
    Some(1_001_000)
  );
}

#[test]
fn fine_tick_rate_rejects_a_sub_microsecond_zero_install() {
  // 800 ns floors to zero installed microseconds at 1 GHz; accepting it would install an
  // immediate-abort timeout into smoltcp.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_nanos(800),
      Duration::from_nanos(100),
      Duration::from_nanos(100),
      GHZ,
    ),
    None
  );
}

#[test]
fn sub_tick_socket_timeout_never_installs_a_zero_immediate_abort() {
  // 500 ms floors to zero ticks at 1 Hz; accepting it would install an immediate abort.
  assert_eq!(
    checked_socket_timeout(
      Duration::from_millis(500),
      Duration::from_millis(100),
      Duration::from_millis(100),
      HZ,
    ),
    None
  );
}

#[test]
fn upper_bound_is_rejected_but_exactly_max_is_accepted() {
  assert_eq!(
    checked_socket_timeout(
      MAX_SOCKET_TIMEOUT + Duration::from_nanos(1),
      Duration::from_secs(1),
      Duration::from_secs(1),
      MHZ,
    ),
    None
  );
  assert!(
    checked_socket_timeout(
      MAX_SOCKET_TIMEOUT,
      Duration::from_secs(1),
      Duration::from_secs(1),
      MHZ,
    )
    .is_some()
  );
}

#[test]
fn typical_configuration_is_accepted() {
  assert_eq!(
    checked_socket_timeout(
      Duration::from_secs(15),
      Duration::from_secs(1),
      Duration::from_millis(500),
      MHZ,
    ),
    Some(15_000_000)
  );
}
