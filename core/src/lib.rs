#![doc = include_str!("../../README.md")]
#![forbid(unsafe_code)]
#![deny(warnings)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

mod awareness;
pub mod broadcast;
pub mod delegate;
pub mod error;
mod network;
pub use network::META_MAX_SIZE;
mod options;
pub use options::{DelegateVersion, Options, ProtocolVersion};
mod memberlist;
pub mod queue;
pub use memberlist::*;
mod state;
mod suspicion;
pub mod transport;
pub mod types;
pub mod util;

pub use bytes;

mod timer;
mod version;

pub use nodecraft::CheapClone;

pub use agnostic;

pub use futures;

#[cfg(feature = "metrics")]
#[doc(hidden)]
pub use metrics;

#[doc(hidden)]
pub use tracing;

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if memberlist also works with their runtime.
/// See [memberlist-wasm] for more examples about how to use these unit test fn runners.
///
/// [memberlist-wasm]: https://github.com/al8n/memberlist/blob/main/memberlist-wasm/src/lib.rs#L20
#[cfg(feature = "test")]
pub mod tests {
  use std::net::SocketAddr;

  use agnostic::Runtime;
  use nodecraft::resolver::AddressResolver;
  #[cfg(not(windows))]
  use parking_lot::Mutex;
  pub use paste;

  use self::{delegate::Delegate, error::Error, transport::Transport};
  use super::*;

  pub mod state {
    pub use crate::state::tests::*;
  }

  /// Add `test` prefix to the predefined unit test fn with a given [`Runtime`]
  #[cfg(any(feature = "test", test))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
  #[macro_export]
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

  /// Add `test` prefix to the predefined unit test fn with a given [`Runtime`]
  #[cfg(any(feature = "test", test))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "test", test))))]
  #[macro_export]
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
    initialize_tests_tracing();
    block_on(fut);
  }

  pub fn initialize_tests_tracing() {
    use std::sync::Once;
    static TRACE: Once = Once::new();
    TRACE.call_once(|| {
      let filter = std::env::var("MEMBERLIST_TESTING_LOG").unwrap_or_else(|_| "trace".to_owned());
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
    <<<T as Transport>::Runtime as Runtime>::Sleep as futures::Future>::Output: Send,
    <<<T as Transport>::Runtime as Runtime>::Interval as futures::Stream>::Item: Send,
    D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    T: Transport,
  {
    crate::Memberlist::new_in(t, Some(d), opts)
      .await
      .map(|(_, _, this)| this)
  }
}
