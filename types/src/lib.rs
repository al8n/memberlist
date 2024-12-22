//! Types used by the [`memberlist`](https://crates.io/crates/memberlist) crate.
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
#![deny(warnings)]
#![allow(clippy::type_complexity, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub use smallvec_wrapper::smallvec_wrapper;

pub use bytes;

mod message;
pub use message::*;

mod ack;
pub use ack::*;

mod alive;
pub use alive::*;

mod bad_state;
pub use bad_state::*;

mod err;
pub use err::*;

mod label;
pub use label::*;

mod meta;
pub use meta::*;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
mod metrics_label;
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub use metrics_label::MetricLabels;

mod cidr_policy;
pub use cidr_policy::*;

mod ping;
pub use ping::*;

mod push_pull_state;
pub use push_pull_state::*;

mod packet;
pub use packet::*;

mod server;
pub use server::*;

#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
mod secret;
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use secret::*;

mod version;
pub use version::*;

pub use smallvec_wrapper::*;

pub use nodecraft::{
  CheapClone, Node, NodeAddress, NodeAddressError, NodeId, NodeIdTransformError, NodeTransformError,
};

const MAX_ENCODED_LEN_SIZE: usize = core::mem::size_of::<u32>();
