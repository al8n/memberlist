//! Async `no_std` memberlist driver over the [embassy-net](https://docs.rs/embassy-net)
//! network stack.
//!
//! Drives the transport-agnostic
//! [`Engine`](memberlist_embedded::Engine) shared with the
//! [`memberlist-smoltcp`](https://docs.rs/memberlist-smoltcp) driver, supplying
//! an embassy-net link layer and an `embassy-time` clock bridge. The gossip
//! plane runs over an embassy-net `UdpSocket`; the reliable plane (TCP) is
//! driven over embassy-net's TCP sockets.
//!
//! # Building for bare metal
//!
//! Turn the default `std` feature off and the `alloc` feature on, against a
//! bare-metal target. [`Memberlist::new`] seeds the gossip RNG from the platform
//! [`getrandom`] backend (a bare-metal target must register one — e.g. a hardware
//! RNG); [`Memberlist::new_with_rng`] instead takes a caller-seeded [`SmallRng`]
//! (or any [`Rng`](memberlist_proto::Rng)) so the driver acquires no entropy of
//! its own — seed it from the same authority that seeds the embassy-net stack.
//!
//! ```sh
//! cargo build -p memberlist-embassy --no-default-features --features alloc \
//!   --target thumbv7em-none-eabihf
//! ```
//!
//! Neither constructor covers encryption, which is cross-transport — applied to
//! gossip datagrams and, on embassy's plaintext reliable plane, to stream frames.
//! Every encrypted frame draws a fresh nonce from [`getrandom`] at send time. The
//! construction-time keyring probe is entropy-free (it checks only that each key's
//! AEAD backend is compiled in and its cipher variant matches its tag), so an
//! encrypted node constructs on a target whose nonce backend is missing or failing
//! and then cannot encrypt outbound traffic: gossip datagrams and reliable
//! exchanges alike fail as they are sent. Register a working [`getrandom`] backend
//! before enabling encryption.
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
// `collapsible_if`: the nested `if cond { if let ... }` form is kept deliberately —
// flattening multi-level guards into one long let-chain reads worse here.
#![allow(clippy::collapsible_if, clippy::type_complexity, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("memberlist-embassy requires the `std` or `alloc` feature");

// Always link the `alloc` crate so path-qualified `alloc::` access (the mailbox
// rings, the `Rc`/`Vec` the runner and handle use) compiles in every feature
// configuration. On a `std` build `alloc` is re-exported by `std`; on no_std it
// is the bare `alloc` crate.
extern crate alloc;

// Alias `alloc` to the name `std` so genuine-heap `std::` paths compile unchanged
// under no_std+alloc, matching the `memberlist-embedded` crate this driver builds
// on (its public API surfaces `std::sync::Arc` / `std::vec::Vec`, which resolve to
// `alloc` here). Core-resident items are imported from `core::` directly.
#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc as std;

mod config;
mod error;
mod gossip_io;
mod mailbox;
mod memberlist;
mod resolver;
mod runner;
mod shared;
mod stream_io;
mod time;
mod worker;

pub use config::Options;
pub use error::{InitError, OpError, SocketTimeoutOutOfRange};
pub use gossip_io::EmbassyGossip;
pub use memberlist::{Memberlist, NodeStateHandle};
pub use resolver::{AddressResolver, SocketAddrResolver};
pub use runner::Runner;
pub use stream_io::{EmbassyStream, SlotId};
pub use time::{EmbassyInstant, now};

// Types re-exported from the shared `memberlist-embedded` core so the embassy
// public API is self-contained. `TransformOptions`, `LabelError`, and the
// resolver/delegate types always exist; the per-backend option/algorithm/key
// types — and `ControlError`, the encryption key-rotation error — exist only
// when their transform backend is built into this crate.
pub use memberlist_embedded::{
  AliveDelegate, MAX_RESOLVED_ADDRS_PER_SEED, MaybeResolved, MergeDelegate, ResolvedAddrs,
  TransformOptions, transform::LabelError,
};
// `ControlError` is the encryption key-rotation error; it exists in the shared
// core only when an encryption backend is built in.
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use memberlist_embedded::ControlError;
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
pub use memberlist_embedded::transform::CompressionOptions;
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
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use memberlist_embedded::transform::{EncryptionOptions, Keyring, SecretKey};
pub use memberlist_proto::{EndpointOptions, Instant, Node, event};
// `EncryptionError` exists in `memberlist-proto` only when an encryption backend
// is built in.
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use memberlist_proto::EncryptionError;
// The gossip RNG that [`Memberlist::new_with_rng`] takes by value (and that
// [`Memberlist::new`] seeds for you), re-exported so a caller can name and seed it
// without taking its own `memberlist-proto` dependency.
pub use memberlist_proto::{Rng, SeedableRng, SmallRng};
// CIDR peer-admission policy, installed via `Options::with_cidr_policy`.
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
