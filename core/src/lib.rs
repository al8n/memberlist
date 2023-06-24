#![forbid(unsafe_code)]
// #![deny(warnings)]
#![cfg_attr(feature = "nightly", feature(return_position_impl_trait_in_trait))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(not(feature = "async"))]
compile_error!("showbiz does not support sync currently, `async` feature must be enabled.");

mod awareness;
pub mod broadcast;
pub mod checksum;
pub mod delegate;
pub mod error;
mod network;
mod options;
pub use options::{DelegateVersion, Options, ProtocolVersion};
mod dns;
pub mod queue;
pub mod security;
mod showbiz;
pub use showbiz::*;
mod state;
mod suspicion;
pub mod transport;
mod types;
pub use types::{Address, CompressionAlgo, Domain, Label, Message, Name, Node, NodeId, Packet};
mod util;

pub use bytes;
pub use ipnet::IpNet;

#[cfg(feature = "async")]
mod timer;
mod version;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use agnostic;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use async_trait;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use futures_util;

#[cfg(feature = "metrics")]
#[doc(hidden)]
pub use metrics;

#[doc(hidden)]
pub use tracing;

/// All unit test fns are exported in the `tests` module.
/// This module is used for users want to use other async runtime,
/// and want to use the test if showbiz also works with their runtime.
/// See [showbiz-wasm] for more examples about how to use these unit test fn runners.
///
/// [showbiz-wasm]: https://github.com/al8n/showbiz/blob/main/showbiz-wasm/src/lib.rs#L20
#[cfg(all(feature = "async", feature = "test"))]
pub mod tests {
  pub use super::network::tests::*;
  pub use super::showbiz::tests::*;
  pub use super::state::tests::*;
  pub use super::transport::tests::*;
}
