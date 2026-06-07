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
//! Configuring an explicit RNG seed via `EndpointOptions` avoids the entropy
//! draw entirely.
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![allow(clippy::type_complexity, unexpected_cfgs)]
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
pub use error::{GossipMtuTooLarge, InitError, MediumMismatch};
pub use interface::{
  EthernetAddress, HardwareAddress, InterfaceOptions, IpAddress, IpCidr, Ipv4Address, Ipv6Address,
  Medium, Route,
};
pub use memberlist::Memberlist;
// Re-export types that appear in public `Memberlist` method signatures so
// callers do not need to depend on `memberlist-proto` directly.
pub use memberlist_embedded::{AliveDelegate, ControlError, MergeDelegate};
pub use memberlist_proto::{EncryptionError, Node, PingId, StreamId, typed::NodeState};
// CIDR peer-admission policy, installed via `Options::with_cidr_policy`.
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
// The wire transforms live in the shared `memberlist-embedded` core and are
// re-exported here so the smoltcp public API is self-contained.
pub use memberlist_embedded::transform::{
  ChecksumAlgorithm, ChecksumOptions, CompressAlgorithm, CompressionOptions, EncryptionOptions,
  Keyring, LabelError, SecretKey, TransformOptions,
};

mod addr;
mod config;
mod error;
mod gossip_io;
mod interface;
mod memberlist;
mod stream_io;
