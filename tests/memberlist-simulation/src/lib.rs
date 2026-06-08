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
pub mod vopr;
#[cfg(feature = "__tls-harness")]
pub mod tls_net;
#[cfg(any(feature = "__tls-harness", feature = "tcp"))]
mod virtual_tcp;

pub use checker::{BoundednessChecker, CheckResult, ConvergenceChecker, IllegalPairChecker, IncarnationMonotonicChecker, MetaPerIncarnationChecker, NoResurrectionChecker, SelfIncarnationChecker, SelfLivenessChecker, Transition};
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
