#![forbid(unsafe_code)]
#![deny(warnings)]
#![cfg_attr(feature = "nightly", feature(return_position_impl_trait_in_trait))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub mod net;
pub use net::*;
