//! Incremental earliest-deadline index for the QUIC coordinator's
//! `poll_timeout`.
//!
//! Production drivers (reactor, compio) re-poll `QuicEndpoint::poll_timeout`
//! after every inbound receive batch, so it must not fold an
//! O(connections + bridges + dials) minimum per call — corrupted or replayed
//! traffic would otherwise force that scan per batch. This index keeps the
//! minimum incrementally: each deadline source registers its next deadline
//! under a [`TimerKey`], and `earliest` reads the live minimum in amortized
//! O(1).
//!
//! The shape mirrors quinn-proto's own timer handling — a binary heap keyed by
//! an instant, with stale entries discarded on pop. A push never rewrites or
//! removes an existing heap entry, so a key whose deadline changes leaves its
//! prior entry behind as a tombstone that [`DeadlineIndex::earliest`] discards
//! when it surfaces at the top. [`DeadlineIndex::current`] is the authority: a
//! heap entry is live only while it matches the `(deadline, generation)`
//! `current` records for its key. Each pushed entry is discarded at most once,
//! so the amortized cost of a pop-then-peek is constant.

use crate::Instant;
use core::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use quinn_proto::ConnectionHandle;

use crate::event::StreamId;

#[cfg(test)]
mod tests;

/// Identifies one deadline source folded into
/// [`QuicEndpoint::poll_timeout`](super::QuicEndpoint::poll_timeout). Newtype
/// variants only — every payload is a single `Copy` identifier.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) enum TimerKey {
  /// The inner membership `Endpoint`'s next timer (probe / suspicion / gossip
  /// / push-pull schedulers), i.e. `Endpoint::poll_timeout`.
  Endpoint,
  /// One pooled quinn connection's next transport timer
  /// (`Connection::poll_timeout`: loss detection / idle / close / key discard).
  Conn(ConnectionHandle),
  /// One reliable bridge's flush/lifetime deadline (`Bridge::poll_timeout`).
  Bridge(StreamId),
  /// One pending-dial intent's own `deadline`.
  Dial(StreamId),
  /// The immediate-due anchor: an already-past `Instant` (the coordinator's
  /// `last_now`) surfaced as "wake as soon as possible" whenever a freshly
  /// sieved dial is still unattempted or a connection carries a deferred
  /// `ConnectionEvent` backlog. A singleton key.
  ImmediateDue,
}

/// One heap entry: the `deadline` registered for `key` by the push that stamped
/// it `generation`. Ordered by `deadline` (min-first once wrapped in
/// [`Reverse`] at the heap), then by `generation` for a total order.
/// `generation` is globally unique per push, so no two live entries compare
/// equal and the `Ord`/`Eq` relation is consistent.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Entry {
  deadline: Instant,
  generation: u64,
  key: TimerKey,
}

impl Ord for Entry {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .deadline
      .cmp(&other.deadline)
      .then_with(|| self.generation.cmp(&other.generation))
  }
}

impl PartialOrd for Entry {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

/// A min-heap of per-key deadlines with lazy invalidation, backing an
/// amortized-O(1) `poll_timeout`. See the module docs for the invariant.
pub(crate) struct DeadlineIndex {
  /// All pushed entries, min-deadline first. Contains tombstones (entries whose
  /// key was re-registered or removed) that `earliest` discards on pop.
  heap: BinaryHeap<Reverse<Entry>>,
  /// The authoritative current deadline for each registered key, and the
  /// `generation` of the heap entry that carries it. A heap entry is live iff
  /// `current[entry.key] == (entry.deadline, entry.generation)`.
  current: HashMap<TimerKey, (Instant, u64)>,
  /// Monotonic push counter; stamps each pushed entry so a re-registration is
  /// distinguishable from the tombstone it supersedes.
  generation: u64,
  /// Test-only count of heap entries examined by [`Self::earliest`] since the
  /// last [`Self::reset_entities_scanned`]. Proves `poll_timeout` examines
  /// O(1) entries rather than scanning the connection/bridge tables. Never
  /// compiled into production builds.
  #[cfg(test)]
  entities_scanned: u64,
}

impl DeadlineIndex {
  pub(crate) fn new() -> Self {
    Self {
      heap: BinaryHeap::new(),
      current: HashMap::new(),
      generation: 0,
      #[cfg(test)]
      entities_scanned: 0,
    }
  }

  /// Register `key`'s next deadline. `Some(d)` records (or replaces) it;
  /// `None` removes it. A no-op when the key already holds exactly `d`, so
  /// re-registering an unchanged deadline neither bumps the generation nor
  /// grows the heap — repeated polling of a stable table stays flat.
  pub(crate) fn set(&mut self, key: TimerKey, deadline: Option<Instant>) {
    match deadline {
      Some(d) => {
        // Skip when unchanged: the existing live entry already carries `d`, so
        // pushing a duplicate would only add a tombstone to discard later.
        if let Some((cur, _)) = self.current.get(&key) {
          if *cur == d {
            return;
          }
        }
        self.generation += 1;
        let generation = self.generation;
        self.current.insert(key, (d, generation));
        self.heap.push(Reverse(Entry {
          deadline: d,
          generation,
          key,
        }));
      }
      None => {
        // Drop the authority; any prior heap entry becomes a tombstone that
        // `earliest` discards lazily when it surfaces.
        self.current.remove(&key);
      }
    }
  }

  /// The live minimum deadline across all registered keys, or `None` when none
  /// is registered. Discards stale heap tops (keys whose authority no longer
  /// matches, or was removed) before reading the minimum.
  pub(crate) fn earliest(&mut self) -> Option<Instant> {
    loop {
      // Copy the top out before any pop: `peek` borrows the heap, and the
      // fields are all `Copy`.
      let (deadline, generation, key) = match self.heap.peek() {
        Some(Reverse(top)) => (top.deadline, top.generation, top.key),
        None => return None,
      };
      #[cfg(test)]
      {
        self.entities_scanned += 1;
      }
      let live = matches!(
        self.current.get(&key),
        Some(&(cur_d, cur_g)) if cur_d == deadline && cur_g == generation
      );
      if live {
        return Some(deadline);
      }
      // Tombstone: its key was re-registered or removed. Discard and continue.
      self.heap.pop();
    }
  }

  /// Test-only heap-examination count since the last reset. See
  /// [`Self::entities_scanned`].
  #[cfg(test)]
  pub(crate) fn entities_scanned(&self) -> u64 {
    self.entities_scanned
  }

  /// Zero the test-only heap-examination count.
  #[cfg(test)]
  pub(crate) fn reset_entities_scanned(&mut self) {
    self.entities_scanned = 0;
  }

  /// Test-only live-key count (entries in `current`), for asserting the
  /// authority map does not leak keys.
  #[cfg(test)]
  pub(crate) fn live_key_count(&self) -> usize {
    self.current.len()
  }
}
