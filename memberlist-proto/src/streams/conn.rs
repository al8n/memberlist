//! Per-exchange connection tracking for the unified stream-transport
//! coordinator.
//!
//! Connection-per-exchange: each reliable exchange dials its own
//! transport connection, runs exactly one `Stream`, sends the
//! transport's close anchor (e.g. a TCP FIN or a TLS `close_notify`),
//! and tears down. Connection ↔ bridge ↔ `Stream` are 1:1, so this is
//! a plain map of in-flight bridges keyed by an exchange handle — none
//! of the pool / slab / drained-reap / `peers` machinery a multiplexed
//! transport needs.
//!
//! A bridge is keyed by a coordinator-allocated [`ExchangeId`] for its
//! whole lifetime — allocated during the `Handshaking` window (before
//! the `Stream`, and therefore its `StreamId`, exists) and kept
//! unchanged once the record-layer handshake or label step completes
//! and the `Stream` is minted. The entry is never re-keyed and there is
//! no `StreamId -> ExchangeId` reverse index: the coordinator drives
//! each bridge by iterating the map, and the originating `StreamId`
//! needed for the `dial_succeeded` mint travels in the coordinator's
//! per-exchange metadata.
//!
//! Consumed by the unified stream-transport coordinator.

use crate::FxHashMap;
#[cfg(not(feature = "std"))]
use std::vec::Vec;

pub use crate::event::ExchangeId;
use crate::streams::{bridge::StreamBridge, transport::StreamTransport};

/// The in-flight bridge map. Accessor-only.
pub(crate) struct StreamConns<I, A, R> {
  bridges: FxHashMap<ExchangeId, StreamBridge<I, A, R>>,
  next: u64,
}

impl<I, A, R> StreamConns<I, A, R>
where
  R: StreamTransport,
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: crate::Data
    + crate::CheapClone
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
      bridges: FxHashMap::default(),
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
  pub(crate) fn insert(&mut self, id: ExchangeId, bridge: StreamBridge<I, A, R>) {
    self.bridges.insert(id, bridge);
  }

  pub(crate) fn get_mut(&mut self, id: ExchangeId) -> Option<&mut StreamBridge<I, A, R>> {
    self.bridges.get_mut(&id)
  }

  /// Take a bridge out for the split-borrow pump (put back via `insert`, or
  /// drop on terminal teardown).
  pub(crate) fn remove(&mut self, id: ExchangeId) -> Option<StreamBridge<I, A, R>> {
    self.bridges.remove(&id)
  }

  pub(crate) fn ids(&self) -> Vec<ExchangeId> {
    self.bridges.keys().copied().collect()
  }

  /// Iterate the live bridges WITHOUT allocating — for the per-tick deadline scan
  /// and the periodic dirty sweep, which must not pay an O(N) `Vec` allocation on
  /// every driver poll.
  pub(crate) fn iter(&self) -> impl Iterator<Item = (ExchangeId, &StreamBridge<I, A, R>)> {
    self.bridges.iter().map(|(id, br)| (*id, br))
  }

  pub(crate) fn len(&self) -> usize {
    self.bridges.len()
  }
}
