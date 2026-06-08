//! Reusable scenario helpers for integration and property tests.
//!
//! Each sub-module implements one or more scenario functions that set up a
//! [`Cluster`](crate::Cluster), drive it to completion, and return observable state.
//! Integration tests call these helpers and assert on the results.

pub mod gossip;
pub mod probe;
pub mod push_pull;
pub mod suspect_dead;
