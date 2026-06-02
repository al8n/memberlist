//! Bridge the `embassy-time` clock to the machine's portable
//! [`Instant`](memberlist_proto::Instant).
//!
//! The SWIM machine carries time as a [`Duration`](core::time::Duration) since an
//! opaque origin the driver chooses (`memberlist_proto::Instant`); only relative
//! arithmetic is meaningful. embassy-time's own [`Instant`](embassy_time::Instant)
//! counts monotonically from device boot (tick 0 = [`Instant::MIN`]), so anchoring
//! the machine origin at boot makes the mapping a plain "uptime as a `Duration`":
//! monotonic, starting at ~0, and far below `Duration::MAX`, so machine-side
//! deadline arithmetic (`now + interval`) never saturates in practice.
//!
//! Microsecond granularity is preserved (`as_micros`) rather than the smoltcp
//! driver's millisecond rounding, since embassy-time can resolve finer than a
//! millisecond depending on the configured tick rate.

use core::time::Duration;

use embassy_time::Instant as RawInstant;
use memberlist_proto::Instant as MachineInstant;

/// An [`embassy_time::Instant`] wrapper carrying the driver's runtime clock
/// reading.
///
/// The machine consumes `memberlist_proto::Instant` (a portable `Duration`-based
/// newtype), so the driver maps each raw reading through [`EmbassyInstant::machine`]
/// (or the free [`now`] helper) at the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EmbassyInstant(pub RawInstant);

impl From<RawInstant> for EmbassyInstant {
  #[inline(always)]
  fn from(raw: RawInstant) -> Self {
    Self(raw)
  }
}

impl From<EmbassyInstant> for RawInstant {
  #[inline(always)]
  fn from(wrapped: EmbassyInstant) -> Self {
    wrapped.0
  }
}

impl EmbassyInstant {
  /// Map this embassy-time reading to the machine [`Instant`](memberlist_proto::Instant),
  /// anchoring the machine origin at device boot ([`embassy_time::Instant::MIN`]).
  ///
  /// The result is the uptime since boot as a [`Duration`], so it is monotonic in
  /// lockstep with `embassy_time::Instant` and starts at ~0.
  #[inline]
  pub fn machine(self) -> MachineInstant {
    // `duration_since(MIN)` is the full uptime; embassy-time's `Duration` is in
    // ticks, so convert through microseconds for the machine's `core::Duration`.
    let since_boot = self.0.duration_since(RawInstant::MIN);
    MachineInstant::from_origin(Duration::from_micros(since_boot.as_micros()))
  }
}

/// The current machine [`Instant`](memberlist_proto::Instant) from the embassy-time
/// clock, anchored at device boot.
///
/// Reads [`embassy_time::Instant::now`] and maps it through
/// [`EmbassyInstant::machine`]. This is the `now` the driver hands the engine each
/// pump.
#[inline]
pub fn now() -> MachineInstant {
  EmbassyInstant(RawInstant::now()).machine()
}

/// Map a machine [`Instant`](memberlist_proto::Instant) deadline the engine
/// returns back to an [`embassy_time::Instant`] for [`Timer::at`](embassy_time::Timer).
///
/// The inverse of [`EmbassyInstant::machine`]: the machine instant is the uptime
/// since boot as a [`Duration`], so adding it to [`embassy_time::Instant::MIN`]
/// (tick 0 = boot) reconstructs the raw reading. embassy-time's `Duration` counts
/// ticks at the configured rate; `from_micros` truncates the machine instant's
/// microseconds to the nearest tick at or below it. A monotonic timer cannot fire
/// before its deadline, so this is acceptable only because the driver re-pumps on
/// every wake and the engine returns a fresh, still-future deadline if the timer
/// fired a tick early — the loop converges rather than spinning, since each
/// re-pump that finds no work due yet returns the same deadline and the next tick
/// crosses it.
///
/// Saturates at [`embassy_time::Instant::MAX`] for a deadline whose tick count
/// overflows embassy-time's `u64` tick domain (e.g. a machine `Instant` built
/// from a saturated/extreme timeout), so an "effectively never" deadline maps to
/// a far-future timer rather than panicking.
#[inline]
pub fn machine_to_raw(deadline: MachineInstant) -> RawInstant {
  let micros = deadline.since_origin().as_micros();
  // embassy-time `Duration::from_micros` takes a `u64`; clamp an out-of-range
  // microsecond count to the max tick instant rather than truncating the high
  // bits (which would alias an extreme deadline to a near one).
  match u64::try_from(micros) {
    Ok(us) => RawInstant::MIN
      .checked_add(embassy_time::Duration::from_micros(us))
      .unwrap_or(RawInstant::MAX),
    Err(_) => RawInstant::MAX,
  }
}

#[cfg(test)]
mod tests {
  use core::time::Duration;

  use embassy_time::Instant as RawInstant;

  use super::EmbassyInstant;

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
}
