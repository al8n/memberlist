//! The `memberlist` umbrella: the runtime-agnostic Sans-I/O core re-exported as
//! [`proto`], plus each driver behind a runtime feature.
//!
//! Enable exactly one runtime feature (`tokio`, `smol`, `compio`, `smoltcp`,
//! `embassy`, `embedded`, or the generic `reactor`) and select transports
//! (`tcp`, `tls`/`tls-rustls-ring`, `quic`/`quic-rustls-ring`) plus optional
//! `lz4`/`snappy`/`zstd`/`brotli` compression and `aes-gcm`/`chacha20-poly1305`
//! encryption.
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

/// The runtime-agnostic protocol core: the membership machine, the QUIC
/// coordinator, options, typed messages, events, and transforms.
pub use memberlist_proto as proto;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub mod smol;

#[cfg(feature = "compio")]
#[cfg_attr(docsrs, doc(cfg(feature = "compio")))]
pub mod compio;

#[cfg(feature = "smoltcp")]
#[cfg_attr(docsrs, doc(cfg(feature = "smoltcp")))]
pub mod smoltcp;

#[cfg(feature = "embassy")]
#[cfg_attr(docsrs, doc(cfg(feature = "embassy")))]
pub mod embassy;

#[cfg(feature = "reactor")]
#[cfg_attr(docsrs, doc(cfg(feature = "reactor")))]
pub mod reactor;

#[cfg(feature = "embedded")]
#[cfg_attr(docsrs, doc(cfg(feature = "embedded")))]
pub mod embedded;
