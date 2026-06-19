//! The bounded [`ResolvedAddrs`] resolution result shared by the embedded
//! drivers.

use core::net::SocketAddr;
/// Per-call cap on the candidate wire addresses a resolver may return for one
/// seed or for the advertise address.
///
/// One name needs only a handful of A/AAAA candidates to bootstrap a join, so a
/// small fixed bound is sufficient. Enforcing it through the [`ResolvedAddrs`]
/// type (rather than truncating an owned heap `Vec` after the fact) means a
/// misbehaving resolver cannot allocate an unbounded result on a constrained
/// target in the first place.
pub const MAX_RESOLVED_ADDRS_PER_SEED: usize = 8;

/// The bounded, no-heap result a resolver returns: at most
/// [`MAX_RESOLVED_ADDRS_PER_SEED`] wire addresses, stored inline on the stack.
///
/// The cap is part of the type, so resolution is bounded by construction — there
/// is no owned heap buffer for a runaway resolver to grow without limit.
pub type ResolvedAddrs = heapless::Vec<SocketAddr, MAX_RESOLVED_ADDRS_PER_SEED>;
