//! Executor-free `no_std` memberlist driver over [smoltcp](https://docs.rs/smoltcp).
//!
//! Composes smoltcp's poll-based TCP/IP stack with the memberlist SWIM machine
//! in one synchronous super-loop the caller drives. See
//! `docs/superpowers/specs/2026-05-30-memberlist-smoltcp-driver-design.md`.
//!
//! # Building for bare metal
//!
//! Turn the default `std` feature off and the `alloc` feature on, against a
//! bare-metal target. The gossip RNG seed is drawn from
//! [`getrandom`](https://docs.rs/getrandom) when no explicit seed is configured,
//! so a bare-metal target must register a `getrandom` backend (e.g. a hardware
//! RNG); supply one, or build with the custom-backend cfg and provide the
//! symbol in the final binary:
//!
//! ```sh
//! RUSTFLAGS='--cfg getrandom_backend="custom"' \
//!   cargo build -p memberlist-smoltcp --no-default-features --features alloc \
//!   --target thumbv7em-none-eabihf
//! ```
//!
//! Pinning the interface seed via
//! [`InterfaceOptions::with_random_seed`](crate::InterfaceOptions::with_random_seed)
//! avoids the construction-time `getrandom` draw entirely — the gossip RNG then
//! derives from it. [`Memberlist::with_rng`] instead drives the gossip schedule from
//! a caller-supplied RNG, but smoltcp's interface seed (its TCP-stack ISN and
//! ephemeral-port RNG) still draws `getrandom` at construction unless that seed is
//! also pinned.
//!
//! Neither covers encryption, which is cross-transport — applied to gossip datagrams
//! and, on the bare-metal plaintext reliable plane, to stream frames. Every encrypted
//! frame draws a fresh nonce from `getrandom` at send time. The construction-time
//! keyring probe is entropy-free (it checks only that each key's AEAD backend is
//! compiled in and its cipher variant matches its tag), so an encrypted node
//! constructs on a target whose nonce backend is missing or failing and then cannot
//! encrypt outbound traffic: gossip datagrams and reliable exchanges alike fail as
//! they are sent. Register a working `getrandom` backend before enabling encryption.
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
