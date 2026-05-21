//! Per-exchange connection tracking for the plain-TCP coordinator.
//!
//! Connection-per-exchange: each reliable exchange dials its own TCP
//! connection, runs exactly one `Stream`, sends a TCP FIN, and tears down.
//! Connection ↔ bridge ↔ `Stream` are 1:1, so this is a plain map of
//! in-flight bridges keyed by an exchange handle — none of the pool / slab /
//! drained-reap / `peers` machinery a multiplexed transport needs. The design
//! is structurally identical to the TLS coordinator's connection map
//! (`crate::tls::conn`), with `TlsBridge` replaced by `TcpBridge`.
//!
//! A bridge is keyed by a coordinator-allocated [`ExchangeId`] for its whole
//! lifetime — allocated during the `Handshaking` window (before the `Stream`,
//! and therefore its `StreamId`, exists) and kept unchanged once the label
//! step completes and the `Stream` is minted. The entry is never re-keyed and
//! there is no `StreamId -> ExchangeId` reverse index: the coordinator drives
//! each bridge by iterating the map, and the originating `StreamId` needed for
//! the `dial_succeeded` mint travels in the coordinator's per-exchange
//! metadata.

use std::collections::HashMap;

use super::bridge::TcpBridge;

/// A coordinator-allocated handle for one in-flight reliable exchange, stable
/// across the `Handshaking → Established` promotion (unlike `StreamId`, which
/// does not exist until the label step settles). Newtype over `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeId(u64);

impl ExchangeId {
  pub(crate) const fn new(raw: u64) -> Self {
    Self(raw)
  }
  /// The raw monotonic handle. The driver keys its per-exchange TCP
  /// connection on this value.
  pub const fn get(self) -> u64 {
    self.0
  }
}

/// The in-flight bridge map. Accessor-only.
pub(crate) struct TcpConns<I, A> {
  bridges: HashMap<ExchangeId, TcpBridge<I, A>>,
  next: u64,
}

impl<I, A> TcpConns<I, A>
where
  I: nodecraft::Id
    + memberlist_wire::Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  pub(crate) fn new() -> Self {
    Self {
      bridges: HashMap::new(),
      next: 0,
    }
  }

  /// Allocate the next exchange handle (monotonic; never reused).
  pub(crate) fn allocate(&mut self) -> ExchangeId {
    let id = ExchangeId::new(self.next);
    self.next += 1;
    id
  }

  /// Register a bridge for `id` (dial-time or accept-time).
  pub(crate) fn insert(&mut self, id: ExchangeId, bridge: TcpBridge<I, A>) {
    self.bridges.insert(id, bridge);
  }

  pub(crate) fn get_mut(&mut self, id: ExchangeId) -> Option<&mut TcpBridge<I, A>> {
    self.bridges.get_mut(&id)
  }

  /// Take a bridge out for the split-borrow pump (put back via `insert`, or
  /// drop on terminal teardown).
  pub(crate) fn remove(&mut self, id: ExchangeId) -> Option<TcpBridge<I, A>> {
    self.bridges.remove(&id)
  }

  pub(crate) fn ids(&self) -> Vec<ExchangeId> {
    self.bridges.keys().copied().collect()
  }

  pub(crate) fn len(&self) -> usize {
    self.bridges.len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use smol_str::SmolStr;
  use std::{
    net::SocketAddr,
    time::{Duration, Instant},
  };

  fn a_bridge() -> TcpBridge<SmolStr, SocketAddr> {
    let records = crate::tcp::records::RawRecords::acceptor(Some(b"c".to_vec()), false);
    TcpBridge::new(records, Instant::now() + Duration::from_secs(10))
  }

  #[test]
  fn allocate_is_monotonic_and_distinct() {
    let mut c: TcpConns<SmolStr, SocketAddr> = TcpConns::new();
    let a = c.allocate();
    let b = c.allocate();
    assert_ne!(a, b);
    assert_eq!(b.get(), a.get() + 1);
  }

  #[test]
  fn insert_then_remove_clears_the_entry() {
    let mut c: TcpConns<SmolStr, SocketAddr> = TcpConns::new();
    let id = c.allocate();
    c.insert(id, a_bridge());
    assert_eq!(c.len(), 1);
    assert!(c.get_mut(id).is_some());
    assert!(c.remove(id).is_some());
    assert_eq!(c.len(), 0, "terminal teardown removes the bridge");
    assert!(c.get_mut(id).is_none());
  }
}
