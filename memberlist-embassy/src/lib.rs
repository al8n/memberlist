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
//! bare-metal target. The gossip RNG seed is drawn from
//! [`getrandom`](https://docs.rs/getrandom) when no explicit seed is configured,
//! so a bare-metal target must register a `getrandom` backend (e.g. a hardware
//! RNG); supply one, or build with the custom-backend cfg and provide the
//! symbol in the final binary:
//!
//! ```sh
//! RUSTFLAGS='--cfg getrandom_backend="custom"' \
//!   cargo build -p memberlist-embassy --no-default-features --features alloc \
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
mod runner;
mod shared;
mod stream_io;
mod time;
mod worker;

pub use config::Options;
pub use error::{InitError, OpError, SocketTimeoutOutOfRange};
pub use gossip_io::EmbassyGossip;
pub use memberlist::{Memberlist, NodeStateHandle};
pub use runner::Runner;
pub use stream_io::{EmbassyStream, SlotId};
pub use time::{EmbassyInstant, now};

pub use memberlist_embedded::{
  AliveDelegate, ControlError, MergeDelegate, TransformOptions,
  transform::{
    ChecksumAlgorithm, ChecksumOptions, CompressionOptions, EncryptionOptions, Keyring, LabelError,
    SecretKey,
  },
};
pub use memberlist_proto::{EncryptionError, EndpointOptions, Instant, Node, event};
