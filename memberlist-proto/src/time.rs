//! A portable monotonic instant for the Sans-I/O machine.
//!
//! `std::time::Instant` does not exist on `no_std`, so the machine carries time
//! as a [`Duration`] since an opaque per-process origin the driver chooses. Only
//! *relative* arithmetic is meaningful — the absolute value is not a wall clock.
//! The driver maps its runtime clock to this type at the boundary: the std
//! drivers use [`Instant::now`]; an embedded driver maps its monotonic clock to
//! [`Instant::from_origin`]. This mirrors how the machine is handed `now` rather
//! than reading a clock itself.

use core::time::Duration;

/// A monotonic point in time, represented as a [`Duration`] since an opaque
/// origin chosen by the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(Duration);

impl Instant {
  /// The origin instant (zero duration since the origin).
  pub const ORIGIN: Self = Self(Duration::ZERO);

  /// Builds an instant `since_origin` after the driver's chosen origin.
  #[inline(always)]
  pub const fn from_origin(since_origin: Duration) -> Self {
    Self(since_origin)
  }

  /// The duration between this instant and the origin.
  #[inline(always)]
  pub const fn since_origin(self) -> Duration {
    self.0
  }

  /// The duration from `earlier` to `self`, saturating at zero when `earlier`
  /// is the later of the two — out-of-order input must never panic.
  #[inline(always)]
  pub fn saturating_duration_since(self, earlier: Self) -> Duration {
    self.0.saturating_sub(earlier.0)
  }

  /// The duration from `earlier` to `self`, or zero when `earlier` is the
  /// later of the two. Saturating like `std::time::Instant::duration_since`
  /// (and [`Self::saturating_duration_since`]) — every public `Instant`
  /// arithmetic path is total, so a driver handling stale or out-of-order
  /// timestamps never panics.
  #[inline(always)]
  pub fn duration_since(self, earlier: Self) -> Duration {
    self.0.saturating_sub(earlier.0)
  }

  /// `self` minus `d`, or `None` if it would precede the origin. Mirrors
  /// `std::time::Instant::checked_sub`.
  #[inline(always)]
  pub fn checked_sub(self, d: Duration) -> Option<Self> {
    self.0.checked_sub(d).map(Self)
  }

  /// The current instant from the std monotonic clock, relative to a fixed
  /// per-process origin. Std-only — embedded drivers build instants from their
  /// own clock through [`Self::from_origin`].
  #[cfg(feature = "std")]
  #[inline]
  pub fn now() -> Self {
    Self::from_std(std::time::Instant::now())
  }
}

/// How far before its first observation the shared std origin is back-dated.
///
/// `from_std` measures forward from the origin and saturates at it, so a
/// `std::time::Instant` minted *before* the origin was first established would
/// otherwise collapse to the origin and lose ordering. The std drivers always
/// establish the origin first (their opening `start_scheduling(Instant::now())`
/// precedes any foreign timer), but back-dating the origin an hour before its
/// first observation gives any instant minted in that window a faithful,
/// loss-free mapping regardless of conversion order. `checked_sub` keeps this
/// panic-free on a host whose monotonic clock cannot represent that far back
/// (uptime under the offset) — there it degrades to the un-shifted origin,
/// which is still correct because no older foreign instant can exist yet.
#[cfg(feature = "std")]
const STD_ORIGIN_BACKDATE: Duration = Duration::from_secs(3600);

/// Std-only conversions for boundaries with std APIs (e.g. quinn-proto, whose
/// timers are `std::time::Instant`). All share one per-process origin, so they
/// round-trip with [`Instant::now`] for any instant at or after the origin (see
/// `STD_ORIGIN_BACKDATE`).
#[cfg(feature = "std")]
impl Instant {
  #[inline]
  fn std_origin() -> std::time::Instant {
    use std::sync::OnceLock;
    static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
    *ORIGIN.get_or_init(|| {
      let now = std::time::Instant::now();
      now.checked_sub(STD_ORIGIN_BACKDATE).unwrap_or(now)
    })
  }

  /// Maps a `std::time::Instant` into this type, relative to the shared origin.
  ///
  /// Faithful (round-trips through [`Self::into_std`]) for any instant at or
  /// after the back-dated origin — in practice every instant the machine sees,
  /// since the origin precedes all of them (see `STD_ORIGIN_BACKDATE`). An
  /// instant older than the origin saturates to it rather than panicking.
  #[inline]
  pub fn from_std(t: std::time::Instant) -> Self {
    Self(t.saturating_duration_since(Self::std_origin()))
  }

  /// Maps back to a `std::time::Instant` relative to the shared origin. Always
  /// walks forward from the origin, so it never underflows. An instant too far
  /// out to represent as a `std::time::Instant` (e.g. a saturated extreme
  /// deadline) is clamped to a far-future instant rather than panicking — a
  /// driver treats it as "effectively never".
  #[inline]
  pub fn into_std(self) -> std::time::Instant {
    let origin = Self::std_origin();
    match origin.checked_add(self.0) {
      Some(t) => t,
      // 30 years is representable on every supported platform; the final
      // fallback to `origin` is unreachable.
      None => origin
        .checked_add(Duration::from_secs(60 * 60 * 24 * 365 * 30))
        .unwrap_or(origin),
    }
  }
}

/// `Instant + Duration` advances time, saturating at the far end of the
/// representable range so a deadline built from an extreme `now` or an extreme
/// configured timeout can never panic — the machine computes deadlines as
/// `now + interval` everywhere and must stay correct under any input.
impl core::ops::Add<Duration> for Instant {
  type Output = Self;
  #[inline(always)]
  fn add(self, rhs: Duration) -> Self {
    Self(self.0.saturating_add(rhs))
  }
}

impl core::ops::AddAssign<Duration> for Instant {
  #[inline(always)]
  fn add_assign(&mut self, rhs: Duration) {
    self.0 = self.0.saturating_add(rhs);
  }
}

/// `Instant - Duration` walks backwards in time, saturating at the origin so a
/// deadline computed before the origin can never panic.
impl core::ops::Sub<Duration> for Instant {
  type Output = Self;
  #[inline(always)]
  fn sub(self, rhs: Duration) -> Self {
    Self(self.0.saturating_sub(rhs))
  }
}

/// `Instant - Instant` is the gap between them, saturating at zero on
/// out-of-order input.
impl core::ops::Sub<Instant> for Instant {
  type Output = Duration;
  #[inline(always)]
  fn sub(self, earlier: Self) -> Duration {
    self.0.saturating_sub(earlier.0)
  }
}

#[cfg(all(test, feature = "std"))]
mod tests {
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
}
