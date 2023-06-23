#![forbid(unsafe_code)]
#![deny(warnings)]
#![cfg_attr(feature = "nightly", feature(return_position_impl_trait_in_trait))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

mod awareness;
pub mod broadcast;
pub mod checksum;
pub mod delegate;
pub mod error;
mod network;
mod options;
pub use options::{Options, ProtocolVersion, DelegateVersion};
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
