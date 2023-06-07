#![forbid(unsafe_code)]
#![deny(warnings)]
#![cfg_attr(feature = "nightly", feature(return_position_impl_trait_in_trait))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

mod awareness;
mod broadcast;
pub mod checksum;
pub mod delegate;
pub mod error;
mod keyring;
pub use keyring::{SecretKey, SecretKeyring, SecretKeyringError};
mod network;
mod options;
pub use options::Options;
mod dns;
pub mod queue;
mod security;
mod showbiz;
pub use showbiz::*;
mod state;
mod suspicion;
pub mod transport;
pub mod types;
mod util;

pub use bytes;
pub use ipnet::IpNet;

#[cfg(feature = "async")]
pub mod timer;

mod version;
pub use version::*;

#[cfg(feature = "async")]
#[doc(hidden)]
pub use async_channel;

#[cfg(feature = "async")]
#[doc(hidden)]
pub use async_lock;

#[cfg(feature = "async")]
#[doc(hidden)]
pub use async_trait;

#[cfg(feature = "async")]
#[doc(hidden)]
pub use futures_util;

#[cfg(feature = "metrics")]
#[doc(hidden)]
pub use metrics;

#[doc(hidden)]
pub use tracing;
