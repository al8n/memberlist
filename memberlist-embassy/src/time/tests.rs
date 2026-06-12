use core::time::Duration;

use embassy_time::Instant as RawInstant;
use memberlist_proto::Instant as MachineInstant;

use super::{EmbassyInstant, machine_to_raw, now};

/// Two embassy-time instants 5ms apart map to two machine instants whose
/// `duration_since` is 5ms — the bridge preserves elapsed time.
#[test]
fn five_ms_gap_is_preserved_through_the_machine_instant() {
  let t0 = RawInstant::now();
  let t1 = t0 + embassy_time::Duration::from_millis(5);

  let m0 = EmbassyInstant(t0).machine();
  let m1 = EmbassyInstant(t1).machine();

  // Exact at any tick rate that resolves a millisecond (the std test driver
  // ticks at 1 MHz, so 5ms = 5000 ticks is represented exactly).
  assert_eq!(m1.duration_since(m0), Duration::from_millis(5));
  // The mapping is monotonic: the later instant is strictly greater.
  assert!(m1 > m0);
  // Out-of-order input saturates to zero rather than panicking.
  assert_eq!(m0.duration_since(m1), Duration::ZERO);
}

/// The two `From` conversions are mutual inverses: wrapping a raw reading and
/// unwrapping it yields the same `embassy_time::Instant`.
#[test]
fn raw_wrapper_conversions_round_trip() {
  let raw = RawInstant::now();
  let wrapped: EmbassyInstant = raw.into();
  let back: RawInstant = wrapped.into();
  assert_eq!(raw, back);
}

/// `machine_to_raw` is the inverse of `EmbassyInstant::machine`: a raw reading
/// mapped to the machine domain and back reconstructs the original tick (exact
/// at the std driver's 1 MHz, where every microsecond is a whole tick).
#[test]
fn machine_to_raw_inverts_machine_at_microsecond_resolution() {
  let raw = RawInstant::MIN + embassy_time::Duration::from_micros(123_456);
  let machine = EmbassyInstant(raw).machine();
  assert_eq!(machine_to_raw(machine), raw);
}

/// A machine deadline whose microsecond count overflows embassy-time's `u64`
/// tick domain saturates to `Instant::MAX` rather than wrapping to a near
/// (effectively-past) instant.
#[test]
fn machine_to_raw_saturates_on_overflow_instead_of_wrapping() {
  // `u128::MAX` microseconds cannot fit a `u64`, so the conversion takes the
  // saturating branch.
  let extreme = MachineInstant::from_origin(Duration::new(u64::MAX, 0));
  assert_eq!(machine_to_raw(extreme), RawInstant::MAX);
}

/// `now()` reads the live clock and advances monotonically: a later call is
/// never earlier than an earlier one.
#[test]
fn now_is_monotonic() {
  let a = now();
  let b = now();
  assert!(b >= a, "now() must be monotonic non-decreasing");
}
