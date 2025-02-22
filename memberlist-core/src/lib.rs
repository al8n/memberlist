#![doc = include_str!("../../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

macro_rules! cfg_offload {
  (@if { $($item:expr)* } @else { $($else:expr)* } ) => {
    $(
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
        feature = "encryption",
      ))]
      $item
    )*

    $(
      #[cfg(not(any(
        feature = "zstd",
        feature = "lz4",
        feature = "brotli",
        feature = "snappy",
        feature = "encryption",
      )))]
      $else
    )*
  };
}

macro_rules! cfg_rayon {
  (@if { $($item:expr)* } @else { $($else:expr)* } ) => {
    $(
      #[cfg(feature = "rayon")]
      $item
    )*

    $(
      #[cfg(not(feature = "rayon"))]
      $else
    )*
  };
}

mod api;
mod awareness;
mod base;
mod broadcast;
mod network;
mod options;
mod state;
mod suspicion;

/// Trait can be implemented to hook into the memberlist lifecycle.
pub mod delegate;
/// Error related to memberlist
pub mod error;
/// The keyring implementation.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub mod keyring;
/// The types used in memberlist
pub mod proto;
/// The transimit queue implementation.
pub mod queue;
/// The transport layer for memberlist
pub mod transport;
/// The utils used in memberlist
pub mod util;

pub use agnostic_lite;
pub use base::*;
pub use broadcast::*;
pub use bytes;
pub use futures;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use metrics;
pub use network::META_MAX_SIZE;
pub use nodecraft::CheapClone;
pub use options::Options;
pub use tracing;

use std::time::Duration;

#[cfg(windows)]
type Epoch = system_epoch::SystemTimeEpoch;

#[cfg(not(windows))]
type Epoch = instant_epoch::InstantEpoch;

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

    #[cfg(any(feature = "test", test))]
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

    #[cfg(any(feature = "test", test))]
    pub(crate) fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
      self.0.checked_duration_since(earlier.0)
    }
  }
}

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if memberlist also works with their runtime.
#[cfg(feature = "test")]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests {
  use std::net::SocketAddr;

  use nodecraft::resolver::AddressResolver;
  #[cfg(not(windows))]
  use parking_lot::Mutex;
  pub use paste;

  use self::{delegate::Delegate, error::Error, transport::Transport};
  use super::*;

  /// Re-export the all unit test cases for state
  pub mod state {
    pub use crate::state::tests::*;
  }

  /// Re-export the all unit test cases for memberlist
  pub mod memberlist {
    pub use crate::base::tests::*;
  }

  #[cfg(any(feature = "test", test))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
  #[macro_export]
  #[doc(hidden)]
  macro_rules! unit_tests {
    ($runtime:ty => $run:ident($($fn:ident), +$(,)?)) => {
      $(
        ::memberlist_core::tests::paste::paste! {
          #[test]
          fn [< test_ $fn >] () {
            $run($fn::<$runtime>());
          }
        }
      )*
    };
  }

  #[cfg(any(feature = "test", test))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
  #[macro_export]
  #[doc(hidden)]
  macro_rules! unit_tests_with_expr {
    ($run:ident($(
      $(#[$outer:meta])*
      $fn:ident( $expr:expr )
    ), +$(,)?)) => {
      $(
        ::memberlist_core::tests::paste::paste! {
          #[test]
          $(#[$outer])*
          fn [< test_ $fn >] () {
            $run(async move {
              $expr
            });
          }
        }
      )*
    };
  }

  /// Any error type used for testing.
  pub type AnyError = Box<dyn std::error::Error + Send + Sync + 'static>;

  /// The kind of address
  pub enum AddressKind {
    /// V4
    V4,
    /// V6
    V6,
  }

  impl core::fmt::Display for AddressKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      match self {
        Self::V4 => write!(f, "v4"),
        Self::V6 => write!(f, "v6"),
      }
    }
  }

  impl AddressKind {
    /// Get the next address
    pub fn next(&self, network: u8) -> SocketAddr {
      match self {
        Self::V4 => next_socket_addr_v4(network),
        Self::V6 => next_socket_addr_v6(),
      }
    }
  }

  #[cfg(not(windows))]
  static IPV4_BIND_NUM: Mutex<usize> = Mutex::new(10);
  #[cfg(not(windows))]
  static IPV6_BIND_NUM: Mutex<usize> = Mutex::new(10);

  /// Returns the next socket addr v4
  pub fn next_socket_addr_v4(_network: u8) -> SocketAddr {
    #[cfg(not(windows))]
    {
      let mut mu = IPV4_BIND_NUM.lock();
      let addr: SocketAddr = format!("127.0.{}.{}:0", _network, *mu).parse().unwrap();
      *mu += 1;
      if *mu > 255 {
        *mu = 10;
      }

      addr
    }

    #[cfg(windows)]
    "127.0.0.1:0".parse().unwrap()
  }

  /// Returns the next socket addr v6
  pub fn next_socket_addr_v6() -> SocketAddr {
    #[cfg(not(windows))]
    {
      let mut mu = IPV6_BIND_NUM.lock();
      let addr: SocketAddr = format!("[fc00::1:{}]:0", *mu).parse().unwrap();
      *mu += 1;
      if *mu > 255 {
        *mu = 10;
      }

      addr
    }

    #[cfg(windows)]
    "[::1]:0".parse().unwrap()
  }

  /// Run the unit test with a given async runtime sequentially.
  pub fn run<B, F>(block_on: B, fut: F)
  where
    B: FnOnce(F) -> F::Output,
    F: std::future::Future<Output = ()>,
  {
    // initialize_tests_tracing();
    block_on(fut);
  }

  /// Initialize the tracing for the unit tests.
  pub fn initialize_tests_tracing() {
    use std::sync::Once;
    static TRACE: Once = Once::new();
    TRACE.call_once(|| {
      let filter = std::env::var("MEMBERLIST_TESTING_LOG").unwrap_or_else(|_| "info".to_owned());
      tracing::subscriber::set_global_default(
        tracing_subscriber::fmt::fmt()
          .without_time()
          .with_line_number(true)
          .with_env_filter(filter)
          .with_file(false)
          .with_target(true)
          .with_ansi(true)
          .finish(),
      )
      .unwrap();
    });
  }

  /// Returns a [`Memberlist`] but not alive self for testing purposes.
  pub async fn get_memberlist<T, D>(
    t: T,
    d: D,
    opts: Options,
  ) -> Result<Memberlist<T, D>, Error<T, D>>
  where
    D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
    T: Transport,
  {
    crate::Memberlist::new_in(t, Some(d), opts)
      .await
      .map(|(_, _, this)| this)
  }
}
