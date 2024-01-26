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
  pub use super::{
    network::*,
    // memberlist::tests::*,
    // state::*,
    transport::tests::*,
  };

  pub fn initialize_tests_tracing() {
    use std::sync::Once;
    static TRACE: Once = Once::new();
    TRACE.call_once(|| {
      let filter = std::env::var("SHOWBIZ_TESTING_LOG").unwrap_or_else(|_| "debug".to_owned());
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
}
