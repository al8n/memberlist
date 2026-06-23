//! Runtime-agnostic glue shared by the memberlist async driver crates.
//!
//! A driver binds `memberlist-proto`'s Sans-I/O endpoint to a real async runtime. This crate holds
//! the runtime-independent pieces both the compio and reactor drivers need — the observable
//! [`MemberlistSnapshot`] and (in later layers) the common driver error types and small pure
//! helpers — so they live in one place instead of being copied per runtime. The run loops, the
//! channel and cell substrate, and the delegate dispatch stay in each runtime crate.

mod snapshot;

pub use snapshot::MemberlistSnapshot;
