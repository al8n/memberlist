//! Deterministic single-threaded simulation harness for [`memberlist_proto`].
//!
//! No async runtime. No real I/O. Tests run in microseconds.
//!
//! # Quick start
//!
//! ```ignore
//! use std::net::SocketAddr;
//! use memberlist_simulation::Cluster;
//!
//! let mut c = Cluster::new();
//! let a1: SocketAddr = "127.0.0.1:7001".parse().unwrap();
//! let a2: SocketAddr = "127.0.0.1:7002".parse().unwrap();
//! c.add_node("node1".into(), a1);
//! c.add_node("node2".into(), a2);
//! c.run_until_quiescent(500);
//! // Both nodes should now see each other as Alive.
//! ```
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]

use core::net::{IpAddr, SocketAddr};

pub mod checker;
pub mod clock;
pub mod cluster;
pub mod faults;
pub mod network;
#[cfg(feature = "__quic-harness")]
pub mod quic_net;
pub mod scenarios;
#[cfg(feature = "tcp")]
pub mod tcp_net;
#[cfg(feature = "__tls-harness")]
pub mod tls_net;
#[cfg(any(feature = "__tls-harness", feature = "tcp"))]
mod virtual_tcp;
pub mod vopr;

pub use checker::{
  BoundednessChecker, CheckResult, ConvergenceChecker, IllegalPairChecker,
  IncarnationMonotonicChecker, MetaPerIncarnationChecker, NoResurrectionChecker,
  SelfIncarnationChecker, SelfLivenessChecker, Transition,
};
pub use clock::Clock;
pub use cluster::{Cluster, DecisionPolicy};
pub use faults::FaultConfig;
pub use network::Network;

// Re-export the protocol types most commonly needed by simulation tests so
// that test code only needs `memberlist_simulation` as a dependency.
pub use memberlist_proto::{
  EndpointOptions, Event, NodeConflict, PingCompleted, Reliability, UserPacket,
  typed::{Alive, Dead, Message, Meta, Node, PushNodeState, State, Suspect},
};

/// FNV-1a hash of the full advertise address (IP octets then port). The
/// simulation seeds a node's gossip RNG from its whole `SocketAddr` rather than
/// just its port, so two nodes on the same memberlist port but distinct IPs (the
/// normal production shape) get distinct gossip streams.
pub(crate) fn rng_seed_from_addr(addr: &SocketAddr) -> u64 {
  const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
  let mut acc: u64 = 0xcbf2_9ce4_8422_2325;
  let mut fold = |bytes: &[u8]| {
    for &b in bytes {
      acc ^= u64::from(b);
      acc = acc.wrapping_mul(FNV_PRIME);
    }
  };
  match addr.ip() {
    IpAddr::V4(v4) => fold(&v4.octets()),
    IpAddr::V6(v6) => fold(&v6.octets()),
  }
  fold(&addr.port().to_le_bytes());
  acc
}

/// A deterministic 32-byte quinn RNG seed derived from the full advertise
/// address (IP octets then port), so the QUIC conformance harness stays
/// bit-for-bit reproducible yet distinct for same-port/distinct-IP nodes.
#[cfg(feature = "__quic-harness")]
pub(crate) fn quinn_seed_from_addr(addr: &SocketAddr) -> [u8; 32] {
  let mut seed = [0u8; 32];
  let n = match addr.ip() {
    IpAddr::V4(v4) => {
      seed[..4].copy_from_slice(&v4.octets());
      4
    }
    IpAddr::V6(v6) => {
      seed[..16].copy_from_slice(&v6.octets());
      16
    }
  };
  seed[n..n + 2].copy_from_slice(&addr.port().to_le_bytes());
  seed
}
