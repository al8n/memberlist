//! Types used by the [`memberlist`](https://crates.io/crates/memberlist) crate.
#![doc(html_logo_url = "https://github.com/al8n/memberlist/blob/main/art/logo.svg")]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[macro_use]
extern crate smallvec_wrapper;

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
mod metrics_label;
#[cfg(feature = "metrics")]
pub use metrics_label::*;

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

mod secret;
pub use secret::*;

mod version;
pub use version::*;

pub use smallvec_wrapper::*;

const MAX_ENCODED_LEN_SIZE: usize = core::mem::size_of::<u32>();
