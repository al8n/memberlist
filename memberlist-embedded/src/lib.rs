//! The transport-agnostic driving core shared by the embedded memberlist
//! drivers.
//!
//! This `no_std` + `alloc` crate holds the protocol work that is independent of
//! any particular link layer: the gossip encode/decode pipeline, the
//! reliable-plane connection state machine, the transport-neutral address
//! screen, and the wire transforms (compression / encryption / cluster label).
//! A concrete driver — [`memberlist-smoltcp`](https://docs.rs/memberlist-smoltcp)
//! (caller-driven poll loop) or a future embassy-net driver (async executor) —
//! supplies the link-layer stack tick plus implementations of the two
//! non-blocking I/O traits [`GossipIo`] and [`StreamIo`], and this crate drives
//! the machine over them.
//!
//! # Building for bare metal
//!
//! Turn the default `std` feature off and the `alloc` feature on, against a
//! bare-metal target. When no explicit RNG seed is configured the gossip RNG
//! seed is drawn from [`getrandom`](https://docs.rs/getrandom), so a bare-metal
//! target must register a `getrandom` backend (e.g. a hardware RNG); supply
//! one, or build with the custom-backend cfg and provide the symbol in the
//! final binary:
//!
//! ```sh
//! RUSTFLAGS='--cfg getrandom_backend="custom"' \
//!   cargo build -p memberlist-embedded --no-default-features --features alloc \
//!   --target thumbv7em-none-eabihf
//! ```
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
pub mod stream_io;
pub mod transform;

pub use addr::socket_addr_is_routable;
pub use config::{DEFAULT_CLOSE_TIMEOUT, Options};
pub use engine::Engine;
pub use error::{ControlError, GossipMtuTooLarge, InitError};
pub use gossip_io::GossipIo;
// Admission predicates a caller can install via `Engine::set_alive_delegate` /
// `set_merge_delegate`.
pub use memberlist_proto::{AliveDelegate, MergeDelegate};
// CIDR peer-admission policy, installed via `Options::with_cidr_policy`.
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
pub use reliable::{ConnState, Connection, Pool, ReliablePlane};
pub use stream_io::{StreamIo, StreamIoError};
pub use transform::{
  ChecksumAlgorithm, ChecksumOptions, CompressAlgorithm, CompressionOptions, EncryptionOptions,
  Keyring, LabelError, SecretKey, TransformOptions,
};
