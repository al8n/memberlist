//! Incremental earliest-suspicion-deadline index for
//! [`Endpoint::poll_timeout`](super::Endpoint::poll_timeout).
//!
//! Production drivers re-poll `Endpoint::poll_timeout` after every inbound
//! receive batch to re-arm the driver's wake timer. The suspicion term of that
//! fold used to be `members.iter().filter_map(|m| m.suspicion()...).min()` — a
//! full member scan on *every* poll, even at zero active suspicions, so
//! corrupted or replayed traffic that forces a re-poll per datagram would drag
//! an O(members) scan behind it. This index keeps that minimum incrementally:
//! each suspicion lifecycle event registers or clears the affected member's
//! deadline, and [`SuspicionDeadlines::earliest`] reads the live minimum in
//! O(log n).
//!
//! **Storage is proportional to the number of LIVE suspicions, never the update
//! rate.** The ordered map is keyed by `(deadline, seq)`, and
//! [`SuspicionDeadlines::current`] is a reverse lookup from a member id to the
//! `(deadline, seq)` it currently occupies. [`set`](SuspicionDeadlines::set)
//! removes a member's prior ordered entry before inserting its new one, so a
//! suspicion whose deadline is accelerated by a confirmation never leaves a
//! stale entry behind — the space cannot grow with the number of confirmations
//! the way a lazy heap of tombstones would. Every entry in
//! [`SuspicionDeadlines::by_deadline`] is therefore live, and `earliest` reads
//! the first ordered key with no discard loop.
//!
//! Shape mirrors [`crate::quic::deadline`]'s `DeadlineIndex`. The differences
//! are deliberate: the value is the member id `I` (suspicion is per-member)
//! rather than a `Copy` `TimerKey`, so `set` takes `&I` and clones on insert;
//! and the collections are the alloc-backed `BTreeMap` plus the crate's
//! hashbrown-backed [`FxHashMap`](crate::FxHashMap) rather than
//! `std::collections::HashMap`, because the membership machine (unlike the
//! QUIC coordinator, whose `quic` feature implies `std`) compiles under
//! `no_std + alloc` for the embedded drivers.

use core::hash::Hash;
// `std` is the crate's `extern crate alloc as std` alias under `no_std`, so this
// is the alloc-backed `BTreeMap` — no_std-safe, matching the crate convention
// (e.g. `broadcast`'s `BTreeSet`). It is NOT `std::collections::HashMap`, which
// does not exist under `alloc`; the reverse map uses the crate's `FxHashMap`.
use std::collections::BTreeMap;

use crate::{CheapClone, FxHashMap, Instant};

#[cfg(test)]
mod tests;

/// An ordered per-member suspicion-deadline index with no tombstones, backing
/// an O(log n) suspicion term in
/// [`Endpoint::poll_timeout`](super::Endpoint::poll_timeout). See the module
/// docs for the live-storage invariant.
pub(crate) struct SuspicionDeadlines<I> {
  /// Every live suspicion's deadline, ordered by `(deadline, seq)` so the first
  /// entry is the global minimum. `seq` breaks ties into a total order and
  /// distinguishes a re-registration from the entry it replaced. Holds no stale
  /// entries: [`Self::set`] removes a member's prior `(deadline, seq)` before
  /// inserting the new one, so its length equals the number of live suspicions.
  by_deadline: BTreeMap<(Instant, u64), I>,
  /// The authoritative current deadline for each suspected member, and the
  /// `seq` under which it sits in `by_deadline`. The reverse lookup that lets
  /// [`Self::set`] find and drop a member's prior ordered entry in O(log n).
  current: FxHashMap<I, (Instant, u64)>,
  /// Monotonic insertion counter; stamps each inserted entry so a
  /// re-registration keys a distinct `by_deadline` slot from the one it
  /// replaces even at an identical deadline instant. Deliberately named `seq`,
  /// not "generation": it is a private map tie-breaker and must never be
  /// confused with the member-generation token.
  seq: u64,
}

impl<I> SuspicionDeadlines<I> {
  /// Construct an empty index.
  pub(crate) fn new() -> Self {
    Self {
      by_deadline: BTreeMap::new(),
      current: FxHashMap::default(),
      seq: 0,
    }
  }

  /// The live minimum suspicion deadline across all suspected members, or
  /// `None` when none is registered. Every entry is live ([`Self::set`] leaves
  /// no tombstones), so this reads the first ordered key directly — no discard
  /// loop, no member scan. Equal to the old
  /// `members.iter().filter_map(|m| m.suspicion().map(|s| s.deadline())).min()`
  /// fold by the module invariant.
  pub(crate) fn earliest(&self) -> Option<Instant> {
    self.by_deadline.first_key_value().map(|(&(d, _), _)| d)
  }

  /// Whether no suspicion deadline is currently registered. The incremental
  /// equivalent of `!members.iter().any(|m| m.suspicion().is_some())`, used by
  /// [`Endpoint::has_pending_operation`](super::Endpoint::has_pending_operation).
  pub(crate) fn is_empty(&self) -> bool {
    self.by_deadline.is_empty()
  }

  /// Test-only count of physically stored ordered-map entries. Equal to
  /// [`Self::live_key_count`] by the no-tombstone invariant; the churn test
  /// asserts it stays `==` live suspicions under a flood of confirmations
  /// rather than growing with the number of updates.
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
}

impl<I> SuspicionDeadlines<I>
where
  I: Eq + Hash + CheapClone,
{
  /// Register `id`'s suspicion deadline. `Some(d)` records (or replaces) it;
  /// `None` removes it. A no-op when the member already holds exactly `d`, so
  /// re-registering an unchanged deadline neither bumps `seq` nor touches the
  /// map. A changed deadline (e.g. a confirmation accelerating the timer)
  /// drops the member's prior ordered entry before inserting the new one, so no
  /// tombstone can accumulate under a confirmation flood.
  ///
  /// Direction-agnostic: it makes no assumption about whether the new deadline
  /// is earlier or later than the old — it removes the old and inserts the new
  /// regardless. The membership FSM only ever pulls a suspicion deadline in,
  /// but encoding that here would be a latent trap for any future caller.
  pub(crate) fn set(&mut self, id: &I, deadline: Option<Instant>) {
    match deadline {
      Some(d) => {
        if let Some(&(old_d, old_seq)) = self.current.get(id) {
          if old_d == d {
            // Unchanged: the existing live entry already carries `d`.
            return;
          }
          // Drop the member's prior ordered entry so storage stays proportional
          // to the live-suspicion count, never the update count.
          self.by_deadline.remove(&(old_d, old_seq));
        }
        self.seq += 1;
        let seq = self.seq;
        self.by_deadline.insert((d, seq), id.cheap_clone());
        self.current.insert(id.cheap_clone(), (d, seq));
      }
      None => {
        // Remove both the ordered entry and the authority.
        if let Some((old_d, old_seq)) = self.current.remove(id) {
          self.by_deadline.remove(&(old_d, old_seq));
        }
      }
    }
  }

  /// Test / debug-assertion helper: whether `id` currently holds a registered
  /// suspicion deadline. Available under `debug_assertions` (not just `test`)
  /// so the `reset_nodes` reclaim path can assert, across the conformance sim's
  /// dev build, that a member being pruned no longer carries an index entry.
  #[cfg(any(test, debug_assertions))]
  pub(crate) fn contains(&self, id: &I) -> bool {
    self.current.contains_key(id)
  }

  /// Test-only: the deadline currently registered for `id`, if any. Lets a
  /// regression test assert a lifecycle event registered the exact deadline it
  /// should have.
  #[cfg(test)]
  pub(crate) fn current_deadline(&self, id: &I) -> Option<Instant> {
    self.current.get(id).map(|&(d, _)| d)
  }
}
