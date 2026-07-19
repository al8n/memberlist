//! Incremental earliest-deadline index over keys `K`, backing the O(log n)
//! deadline terms of [`Endpoint::poll_timeout`](super::Endpoint::poll_timeout).
//!
//! Production drivers re-poll `Endpoint::poll_timeout` after every inbound
//! receive batch to re-arm the driver's wake timer. Two terms of that fold used
//! to be full table scans on *every* poll: the suspicion term
//! (`members.iter().filter_map(|m| m.suspicion()...).min()`, O(members)) and the
//! pending-dial-intent term (`pending_stream_intents.values().map(|i|
//! i.deadline).min()`, O(intents)). Corrupted or replayed traffic that forces a
//! re-poll per datagram would drag those scans behind it, and an attacker can
//! inflate the intent count — exhaust the transport's stream budget so pooled
//! dials park, each holding an intent — to amplify the per-poll cost. This index
//! keeps each such minimum incrementally: every lifecycle event registers or
//! clears the affected key's deadline, and [`DeadlineIndex::earliest`] reads the
//! live minimum in O(log n). The `Endpoint` holds one instance keyed by member
//! id (active suspicions) and one keyed by
//! [`StreamId`](crate::event::StreamId) (pending dial intents).
//!
//! **Storage is proportional to the number of LIVE keys, never the update
//! rate.** The ordered map is keyed by `(deadline, seq)`, and
//! [`DeadlineIndex::current`] is a reverse lookup from a key to the `(deadline,
//! seq)` it currently occupies. [`set`](DeadlineIndex::set) removes a key's prior
//! ordered entry before inserting its new one, so a deadline that is accelerated
//! (e.g. a suspicion confirmation) never leaves a stale entry behind — the space
//! cannot grow with the number of updates the way a lazy heap of tombstones
//! would. Every entry in [`DeadlineIndex::by_deadline`] is therefore live, and
//! `earliest` reads the first ordered key with no discard loop.
//!
//! Shape mirrors [`crate::quic::deadline`]'s `DeadlineIndex`. The differences
//! are deliberate: the value is the owned key `K` (cloned on insert via
//! [`CheapClone`], since a member id or a `StreamId` is the natural key) rather
//! than a `Copy` `TimerKey`, so `set` takes `&K`; and the collections are the
//! alloc-backed `BTreeMap` plus the crate's hashbrown-backed
//! [`FxHashMap`](crate::FxHashMap) rather than `std::collections::HashMap`,
//! because the membership machine (unlike the QUIC coordinator, whose `quic`
//! feature implies `std`) compiles under `no_std + alloc` for the embedded
//! drivers.

use core::hash::Hash;
// `std` is the crate's `extern crate alloc as std` alias under `no_std`, so this
// is the alloc-backed `BTreeMap` — no_std-safe, matching the crate convention
// (e.g. `broadcast`'s `BTreeSet`). It is NOT `std::collections::HashMap`, which
// does not exist under `alloc`; the reverse map uses the crate's `FxHashMap`.
use std::collections::BTreeMap;

use crate::{CheapClone, FxHashMap, Instant};

#[cfg(test)]
mod tests;

/// An ordered per-key deadline index with no tombstones, backing the O(log n)
/// deadline terms of [`Endpoint::poll_timeout`](super::Endpoint::poll_timeout).
/// See the module docs for the live-storage invariant.
pub(crate) struct DeadlineIndex<K> {
  /// Every live key's deadline, ordered by `(deadline, seq)` so the first entry
  /// is the global minimum. `seq` breaks ties into a total order and
  /// distinguishes a re-registration from the entry it replaced. Holds no stale
  /// entries: [`Self::set`] removes a key's prior `(deadline, seq)` before
  /// inserting the new one, so its length equals the number of live keys.
  by_deadline: BTreeMap<(Instant, u64), K>,
  /// The authoritative current deadline for each registered key, and the `seq`
  /// under which it sits in `by_deadline`. The reverse lookup that lets
  /// [`Self::set`] find and drop a key's prior ordered entry in O(log n).
  current: FxHashMap<K, (Instant, u64)>,
  /// Monotonic insertion counter; stamps each inserted entry so a
  /// re-registration keys a distinct `by_deadline` slot from the one it
  /// replaces even at an identical deadline instant. Deliberately named `seq`,
  /// not "generation": it is a private map tie-breaker and must never be
  /// confused with the member-generation token.
  seq: u64,
  /// Test-only count of ordered-map examinations by [`Self::earliest`] since the
  /// last [`Self::reset_entities_scanned`] — one per non-empty read (a single
  /// `first_key_value`). Interior-mutable because `earliest` is `&self` (its
  /// caller [`poll_timeout`](super::Endpoint::poll_timeout) is `&self`); the
  /// multi-key regression test reads it to prove `poll_timeout` consults the
  /// index in O(1) rather than scanning the live table. Never compiled into
  /// production builds.
  #[cfg(test)]
  entities_scanned: core::cell::Cell<u64>,
}

impl<K> DeadlineIndex<K> {
  /// Construct an empty index.
  pub(crate) fn new() -> Self {
    Self {
      by_deadline: BTreeMap::new(),
      current: FxHashMap::default(),
      seq: 0,
      #[cfg(test)]
      entities_scanned: core::cell::Cell::new(0),
    }
  }

  /// The live minimum deadline across all registered keys, or `None` when none
  /// is registered. Every entry is live ([`Self::set`] leaves no tombstones), so
  /// this reads the first ordered key directly — no discard loop, no table scan.
  /// Equal to the old per-term `min()` fold by the module invariant.
  pub(crate) fn earliest(&self) -> Option<Instant> {
    let earliest = self.by_deadline.first_key_value().map(|(&(d, _), _)| d);
    // Count the single ordered-map examination (only when there is a live top),
    // proving `poll_timeout` reads the index rather than folding the live table.
    #[cfg(test)]
    if earliest.is_some() {
      self.entities_scanned.set(self.entities_scanned.get() + 1);
    }
    earliest
  }

  /// Read the earliest deadline WITHOUT counting the examination. The
  /// `poll_timeout` soundness oracles use this so their `debug_assert_eq!` read
  /// does not perturb the `entities_scanned` measurement the O(1)-scan
  /// regression test takes through the production [`earliest`](Self::earliest)
  /// path.
  #[cfg(any(test, debug_assertions))]
  pub(crate) fn earliest_uncounted(&self) -> Option<Instant> {
    self.by_deadline.first_key_value().map(|(&(d, _), _)| d)
  }

  /// Whether no deadline is currently registered. The incremental equivalent of
  /// `!<table>.iter().any(...)`, used by
  /// [`Endpoint::has_pending_operation`](super::Endpoint::has_pending_operation).
  pub(crate) fn is_empty(&self) -> bool {
    self.by_deadline.is_empty()
  }

  /// Drop every registered deadline at once. Used when the owning table is
  /// bulk-cleared (`leave()` drops all pending dial intents together): leaves
  /// the index empty and consistent with the now-empty table. `seq` stays
  /// monotonic — no live entry references any prior stamp, so there is nothing
  /// to reset.
  pub(crate) fn clear(&mut self) {
    self.by_deadline.clear();
    self.current.clear();
  }

  /// Test-only count of physically stored ordered-map entries. Equal to
  /// [`Self::live_key_count`] by the no-tombstone invariant; the churn test
  /// asserts it stays `==` live keys under a flood of updates rather than
  /// growing with the number of updates.
  #[cfg(test)]
  pub(crate) fn entry_count(&self) -> usize {
    self.by_deadline.len()
  }

  /// Test-only live-key count (entries in `current`), for asserting the
  /// authority map does not leak keys.
  #[cfg(test)]
  pub(crate) fn live_key_count(&self) -> usize {
    self.current.len()
  }

  /// Test-only ordered-map-examination count since the last reset. See
  /// [`Self::entities_scanned`].
  #[cfg(test)]
  pub(crate) fn entities_scanned(&self) -> u64 {
    self.entities_scanned.get()
  }

  /// Zero the test-only ordered-map-examination count.
  #[cfg(test)]
  pub(crate) fn reset_entities_scanned(&self) {
    self.entities_scanned.set(0);
  }
}

impl<K> DeadlineIndex<K>
where
  K: Eq + Hash + CheapClone,
{
  /// Register `key`'s deadline. `Some(d)` records (or replaces) it; `None`
  /// removes it. A no-op when the key already holds exactly `d`, so
  /// re-registering an unchanged deadline neither bumps `seq` nor touches the
  /// map. A changed deadline (e.g. a suspicion confirmation accelerating the
  /// timer) drops the key's prior ordered entry before inserting the new one, so
  /// no tombstone can accumulate under a churning flood.
  ///
  /// Direction-agnostic: it makes no assumption about whether the new deadline
  /// is earlier or later than the old — it removes the old and inserts the new
  /// regardless. The membership FSM only ever pulls a suspicion deadline in and
  /// a dial intent is set once, but encoding either here would be a latent trap
  /// for any future caller.
  pub(crate) fn set(&mut self, key: &K, deadline: Option<Instant>) {
    match deadline {
      Some(d) => {
        if let Some(&(old_d, old_seq)) = self.current.get(key) {
          if old_d == d {
            // Unchanged: the existing live entry already carries `d`.
            return;
          }
          // Drop the key's prior ordered entry so storage stays proportional to
          // the live-key count, never the update count.
          self.by_deadline.remove(&(old_d, old_seq));
        }
        self.seq += 1;
        let seq = self.seq;
        self.by_deadline.insert((d, seq), key.cheap_clone());
        self.current.insert(key.cheap_clone(), (d, seq));
      }
      None => {
        // Remove both the ordered entry and the authority.
        if let Some((old_d, old_seq)) = self.current.remove(key) {
          self.by_deadline.remove(&(old_d, old_seq));
        }
      }
    }
  }

  /// Test / debug-assertion helper: whether `key` currently holds a registered
  /// deadline. Available under `debug_assertions` (not just `test`) so the
  /// `reset_nodes` reclaim path can assert, across the conformance sim's dev
  /// build, that a member being pruned no longer carries an index entry.
  #[cfg(any(test, debug_assertions))]
  pub(crate) fn contains(&self, key: &K) -> bool {
    self.current.contains_key(key)
  }

  /// Test-only: the deadline currently registered for `key`, if any. Lets a
  /// regression test assert a lifecycle event registered the exact deadline it
  /// should have.
  #[cfg(test)]
  pub(crate) fn current_deadline(&self, key: &K) -> Option<Instant> {
    self.current.get(key).map(|&(d, _)| d)
  }
}
