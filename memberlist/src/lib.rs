#![doc = include_str!("../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

/// Re-export of [`agnostic`] crate.
pub mod agnostic {
  #[cfg(not(feature = "agnostic"))]
  pub use agnostic_lite::*;

  #[cfg(feature = "agnostic")]
  pub use agnostic::*;
}

pub use memberlist_core::*;

/// Re-export of [`memberlist_net`] crate.
#[cfg(feature = "memberlist-net")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist-net")))]
pub mod net {
  pub use memberlist_net::*;
}

/// Re-export of [`memberlist_quic`] crate.
#[cfg(feature = "memberlist-quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist-quic")))]
pub mod quic {
  pub use memberlist_quic::*;
}

/// [`Memberlist`] for `tokio` runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// [`Memberlist`] for `async-std` runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub mod async_std;

/// [`Memberlist`] for `smol` runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub mod smol;
