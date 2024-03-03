#![doc = include_str!("../../README.md")]
#![doc(html_logo_url = "https://github.com/al8n/memberlist/blob/main/art/logo.svg")]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub use agnostic;
pub use memberlist_core::*;

/// Re-export of [`memberlist_net`] crate.
#[cfg(feature = "memberlist-net")]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "tcp", feature = "tls", feature = "native-tls")))
)]
pub mod net {
  pub use memberlist_net::*;
}

/// Re-export of [`memberlist_quic`] crate.
#[cfg(feature = "memberlist-quic")]
#[cfg_attr(docsrs, doc(cfg(any(feature = "quinn", feature = "s2n"))))]
pub mod quic {
  pub use memberlist_quic::*;
}

/// [`Memberlist`](memberlist_core::Memberlist) for `tokio` runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// [`Memberlist`](memberlist_core::Memberlist) for `async-std` runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub mod async_std;

/// [`Memberlist`](memberlist_core::Memberlist) for `smol` runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub mod smol;
