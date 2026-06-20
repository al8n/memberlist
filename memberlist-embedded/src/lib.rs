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
// under no_std+alloc. Core-resident items are imported from `core::` directly,
// never via this alias; heap macros are written path-qualified (`std::vec!`), so
// no crate-wide `#[macro_use]` is needed.
#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("memberlist-embedded requires the `std` or `alloc` feature");

pub mod addr;
mod cidr;
pub mod config;
pub mod engine;
pub mod error;
pub mod gossip_io;
pub mod reliable;
pub mod resolver;
pub mod stream_io;
pub mod transform;

pub use addr::socket_addr_is_routable;
pub use config::{DEFAULT_CLOSE_TIMEOUT, Options};
pub use engine::{Engine, validate_runtime_config};
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use error::ControlError;
pub use error::{GossipMtuTooLarge, InitError};
pub use gossip_io::GossipIo;
// Admission predicates a caller can install via `Engine::set_alive_delegate` /
// `set_merge_delegate`.
pub use memberlist_proto::{AliveDelegate, MaybeOwned, MaybeResolved, MergeDelegate};
// CIDR peer-admission policy, installed via `Options::with_cidr_policy`.
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
pub use reliable::{ConnState, Connection, Pool, ReliablePlane};
pub use resolver::{MAX_RESOLVED_ADDRS_PER_SEED, ResolvedAddrs};
pub use stream_io::{StreamIo, StreamIoError};
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
pub use transform::{ChecksumAlgorithm, ChecksumOptions};
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
pub use transform::{CompressAlgorithm, CompressionOptions};
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use transform::{EncryptionOptions, Keyring, SecretKey};
pub use transform::{LabelError, TransformOptions};
