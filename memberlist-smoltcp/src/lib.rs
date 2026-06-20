#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
// `collapsible_if`: the nested `if cond { if let ... }` form is kept deliberately —
// flattening multi-level guards into one long let-chain reads worse here.
#![allow(clippy::collapsible_if, clippy::type_complexity, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

// Alias `alloc` to the name `std` so genuine-heap `std::` paths compile unchanged
// under no_std+alloc (and `#[macro_use]` brings `vec!`/`format!` crate-wide).
// Core-resident items are imported from `core::` directly, never via this alias.
#[cfg(all(not(feature = "std"), feature = "alloc"))]
#[macro_use]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("memberlist-smoltcp requires the `std` or `alloc` feature");

pub use config::Options;
pub use error::{GossipMtuTooLarge, InitError, JoinError, MediumMismatch};
pub use interface::{
  EthernetAddress, HardwareAddress, InterfaceOptions, IpAddress, IpCidr, Ipv4Address, Ipv6Address,
  Medium, Route,
};
pub use memberlist::Memberlist;
pub use resolver::{Resolver, SocketAddrResolver};
// Re-export types that appear in public `Memberlist` method signatures so
// callers do not need to depend on `memberlist-proto` directly. `ResolvedAddrs`
// and its bound are the return type a custom `Resolver` must produce, so they are
// part of this crate's public resolver API.
pub use memberlist_embedded::{
  AliveDelegate, MAX_RESOLVED_ADDRS_PER_SEED, MaybeOwned, MaybeResolved, MergeDelegate,
  ResolvedAddrs,
};
// `ControlError` is the encryption key-rotation error; it exists in the shared
// core only when an encryption backend is built in.
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use memberlist_embedded::ControlError;
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use memberlist_proto::EncryptionError;
pub use memberlist_proto::{Node, PingId, StreamId, typed::NodeState};
// CIDR peer-admission policy, installed via `Options::with_cidr_policy`.
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
// The wire transforms live in the shared `memberlist-embedded` core and are
// re-exported here so the smoltcp public API is self-contained. `TransformOptions`
// and `LabelError` always exist; the per-backend option/algorithm/key types exist
// only when their transform backend is built in.
#[cfg(checksum)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3"
  )))
)]
pub use memberlist_embedded::transform::{ChecksumAlgorithm, ChecksumOptions};
#[cfg(compression)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(
    feature = "lz4",
    feature = "snappy",
    feature = "zstd",
    feature = "brotli"
  )))
)]
pub use memberlist_embedded::transform::{CompressAlgorithm, CompressionOptions};
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use memberlist_embedded::transform::{EncryptionOptions, Keyring, SecretKey};
pub use memberlist_embedded::transform::{LabelError, TransformOptions};

mod addr;
mod config;
mod error;
mod gossip_io;
mod interface;
mod memberlist;
mod resolver;
mod stream_io;
