use std::time::Duration;

#[cfg(windows)]
pub(crate) type Epoch = system_epoch::SystemTimeEpoch;

#[cfg(not(windows))]
pub(crate) type Epoch = instant_epoch::InstantEpoch;

#[cfg(windows)]
mod system_epoch {
  use super::*;
  use std::time::SystemTime;

  type SystemTimeEpochInner = SystemTime;

  #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub(crate) struct SystemTimeEpoch(SystemTimeEpochInner);

  impl core::fmt::Debug for SystemTimeEpoch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      self.0.fmt(f)
    }
  }

  impl core::ops::Sub for SystemTimeEpoch {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Duration {
      self.0.duration_since(rhs.0).unwrap()
    }
  }

  impl core::ops::Sub<Duration> for SystemTimeEpoch {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self {
      Self(self.0 - rhs)
    }
  }

  impl core::ops::SubAssign<Duration> for SystemTimeEpoch {
    fn sub_assign(&mut self, rhs: Duration) {
      self.0 -= rhs;
    }
  }

  impl core::ops::Add<Duration> for SystemTimeEpoch {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self {
      SystemTimeEpoch(self.0 + rhs)
    }
  }

  impl core::ops::AddAssign<Duration> for SystemTimeEpoch {
    fn add_assign(&mut self, rhs: Duration) {
      self.0 += rhs;
    }
  }

  impl SystemTimeEpoch {
    pub(crate) fn now() -> Self {
      Self(SystemTimeEpochInner::now())
    }

    pub(crate) fn elapsed(&self) -> Duration {
      self.0.elapsed().unwrap()
    }

    pub(crate) fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
      self.0.duration_since(earlier.0).ok()
    }
  }
}

#[cfg(not(windows))]
mod instant_epoch {
  use super::*;
  use std::time::Instant;

  type InstantEpochInner = Instant;

  #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub(crate) struct InstantEpoch(InstantEpochInner);

  impl core::fmt::Debug for InstantEpoch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      self.0.fmt(f)
    }
  }

  impl core::ops::Sub for InstantEpoch {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Duration {
      self.0 - rhs.0
    }
  }

  impl core::ops::Sub<Duration> for InstantEpoch {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self {
      Self(self.0 - rhs)
    }
  }

  impl core::ops::SubAssign<Duration> for InstantEpoch {
    fn sub_assign(&mut self, rhs: Duration) {
      self.0 -= rhs;
    }
  }

  impl core::ops::Add<Duration> for InstantEpoch {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self {
      InstantEpoch(self.0 + rhs)
    }
  }

  impl core::ops::AddAssign<Duration> for InstantEpoch {
    fn add_assign(&mut self, rhs: Duration) {
      self.0 += rhs;
    }
  }

  impl InstantEpoch {
    pub(crate) fn now() -> Self {
      Self(InstantEpochInner::now())
    }

    pub(crate) fn elapsed(&self) -> Duration {
      self.0.elapsed()
    }

    pub(crate) fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
      self.0.checked_duration_since(earlier.0)
    }
  }
}
