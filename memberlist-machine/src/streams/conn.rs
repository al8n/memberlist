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

use std::collections::HashMap;

use crate::streams::{bridge::StreamBridge, transport::StreamTransport};

/// A coordinator-allocated handle for one in-flight reliable exchange, stable
/// across the `Handshaking → Established` promotion (unlike `StreamId`, which
/// does not exist until the record-layer step settles). Newtype over `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeId(u64);

impl ExchangeId {
  pub(crate) const fn new(raw: u64) -> Self {
    Self(raw)
  }
  /// The raw monotonic handle. The driver keys its per-exchange stream-transport
  /// connection on this value.
  #[inline(always)]
  pub const fn get(self) -> u64 {
    self.0
  }
}

/// The in-flight bridge map. Accessor-only.
pub(crate) struct StreamConns<I, A, R: StreamTransport> {
  bridges: HashMap<ExchangeId, StreamBridge<I, A, R>>,
  next: u64,
}

impl<I, A, R: StreamTransport> StreamConns<I, A, R>
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

  pub(crate) fn len(&self) -> usize {
    self.bridges.len()
  }
}
