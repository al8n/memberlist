//! Incremental earliest-deadline index for the QUIC coordinator's
//! `poll_timeout`.
//!
//! Production drivers (reactor, compio) re-poll `QuicEndpoint::poll_timeout`
//! after every inbound receive batch, so it must not fold an
//! O(connections + bridges + dials) minimum per call — corrupted or replayed
//! traffic would otherwise force that scan per batch. This index keeps the
//! minimum incrementally: each deadline source registers its next deadline
//! under a [`TimerKey`], and `earliest` reads the live minimum in O(log n).
//!
//! **Storage is proportional to the LIVE key count, not the update rate.**
//! quinn resets a connection's idle deadline on every authenticated packet, so
//! sustained valid traffic churns `Conn` deadlines. The ordered map is keyed by
//! `(deadline, generation)`, and [`DeadlineIndex::current`] is a reverse lookup
//! from a key to the `(deadline, generation)` it currently occupies. `set`
//! removes a key's prior ordered entry before inserting its new one, so a key
//! whose deadline moves never leaves a stale entry behind — the space cannot
//! grow with the number of updates the way a lazy heap of tombstones would.
//! Every entry in [`DeadlineIndex::by_deadline`] is therefore live, and
//! `earliest` reads the first ordered key with no discard loop.

use crate::Instant;
use std::collections::{BTreeMap, HashMap};

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

/// An ordered per-key deadline index with no tombstones, backing an
/// O(log n) `poll_timeout`. See the module docs for the live-storage invariant.
pub(crate) struct DeadlineIndex {
  /// Every live key's deadline, ordered by `(deadline, generation)` so the
  /// first entry is the global minimum. `generation` breaks ties into a total
  /// order and distinguishes a re-registration from the entry it replaced.
  /// Holds no stale entries: `set` removes a key's prior `(deadline,
  /// generation)` before inserting the new one, so its length equals the number
  /// of registered keys.
  by_deadline: BTreeMap<(Instant, u64), TimerKey>,
  /// The authoritative current deadline for each registered key, and the
  /// `generation` under which it sits in `by_deadline`. The reverse lookup that
  /// lets `set` find and drop a key's prior ordered entry in O(log n).
  current: HashMap<TimerKey, (Instant, u64)>,
  /// Monotonic insertion counter; stamps each inserted entry so a
  /// re-registration keys a distinct `by_deadline` slot from the one it
  /// replaces even at an identical deadline instant.
  generation: u64,
  /// Test-only count of ordered-map entries examined by [`Self::earliest`]
  /// since the last [`Self::reset_entities_scanned`] — now O(1) per call (a
  /// single `first_key_value`). Proves `poll_timeout` reads the index rather
  /// than scanning the connection/bridge tables. Never compiled into
  /// production builds.
  #[cfg(test)]
  entities_scanned: u64,
}

impl DeadlineIndex {
  pub(crate) fn new() -> Self {
    Self {
      by_deadline: BTreeMap::new(),
      current: HashMap::new(),
      generation: 0,
      #[cfg(test)]
      entities_scanned: 0,
    }
  }

  /// Register `key`'s next deadline. `Some(d)` records (or replaces) it;
  /// `None` removes it. A no-op when the key already holds exactly `d`, so
  /// re-registering an unchanged deadline neither bumps the generation nor
  /// touches the map — repeated polling of a stable table stays flat. A
  /// changed deadline drops the key's prior ordered entry before inserting the
  /// new one, so no tombstone can accumulate under a churning flood.
  pub(crate) fn set(&mut self, key: TimerKey, deadline: Option<Instant>) {
    match deadline {
      Some(d) => {
        if let Some(&(old_d, old_gen)) = self.current.get(&key) {
          if old_d == d {
            // Unchanged: the existing live entry already carries `d`.
            return;
          }
          // Drop the key's prior ordered entry so storage stays proportional to
          // the live-key count, never the update count.
          self.by_deadline.remove(&(old_d, old_gen));
        }
        self.generation += 1;
        let generation = self.generation;
        self.by_deadline.insert((d, generation), key);
        self.current.insert(key, (d, generation));
      }
      None => {
        // Remove both the ordered entry and the authority.
        if let Some((old_d, old_gen)) = self.current.remove(&key) {
          self.by_deadline.remove(&(old_d, old_gen));
        }
      }
    }
  }

  /// The live minimum deadline across all registered keys, or `None` when none
  /// is registered. Every entry is live (`set` leaves no tombstones), so this
  /// reads the first ordered key directly — no discard loop, no table scan.
  pub(crate) fn earliest(&mut self) -> Option<Instant> {
    let earliest = self.by_deadline.first_key_value().map(|(&(d, _), _)| d);
    // Count the single ordered-map examination (only when there is a live top),
    // matching the old lazy-heap accounting for the O(1) proof.
    #[cfg(test)]
    if earliest.is_some() {
      self.entities_scanned += 1;
    }
    earliest
  }

  /// Test-only ordered-map-examination count since the last reset. See
  /// [`Self::entities_scanned`].
  #[cfg(test)]
  pub(crate) fn entities_scanned(&self) -> u64 {
    self.entities_scanned
  }

  /// Zero the test-only ordered-map-examination count.
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

  /// Test-only count of entries physically stored in the ordered index. Equal
  /// to [`Self::live_key_count`] by the no-tombstone invariant; the space-bound
  /// test asserts it stays == live keys under a churn of updates rather than
  /// growing with the number of updates (which the old lazy-heap-of-tombstones
  /// design did).
  #[cfg(test)]
  pub(crate) fn entry_count(&self) -> usize {
    self.by_deadline.len()
  }

  /// Test-only: whether `key` currently holds a registered deadline. Lets a
  /// regression test assert a connection-mutating datagram path registered the
  /// affected `Conn`/`Bridge`/`Dial` key in the same pass rather than leaving
  /// it to an unrelated later tick.
  #[cfg(test)]
  pub(crate) fn contains_key(&self, key: TimerKey) -> bool {
    self.current.contains_key(&key)
  }
}
