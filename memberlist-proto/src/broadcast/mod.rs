//! Broadcast trait and `BroadcastQueue` — the gossip retransmit queue.
use std::{collections::BTreeSet, sync::Arc, vec::Vec};

use crate::{FxHashMap, typed::Message};
use core::{cmp::Ordering, fmt, hash::Hash};

/// Computes the per-message retransmit ceiling using the formula
/// `retransmit_mult * ceil(log10(num_nodes + 1))`.
#[inline]
pub fn retransmit_limit(retransmit_mult: u32, num_nodes: u32) -> u32 {
  let log_n = crate::mathf::ceil(crate::mathf::log10((num_nodes as f64) + 1.0)) as u32;
  retransmit_mult * log_n
}

/// Something that can be broadcast via gossip to the cluster.
///
/// Sync version of `memberlist-core::broadcast::Broadcast`: `finished()` is
/// a synchronous notification (default no-op) instead of an async fn.
pub trait Broadcast: fmt::Debug + Send + Sync + 'static {
  /// Type used to identify broadcasts so newer ones can invalidate older ones.
  type Id: Clone + Eq + Hash + fmt::Debug + fmt::Display;
  /// Type of the message payload (cloned out of the queue when sent).
  type Message: Clone + fmt::Debug + Send + Sync + 'static;

  /// Optional id used to deduplicate / invalidate. If `None`, the broadcast is
  /// not deduplicated by id (see `is_unique`).
  fn id(&self) -> Option<&Self::Id>;

  /// Returns true iff `self` makes `other` redundant (e.g. a newer state
  /// update for the same node).
  fn invalidates(&self, other: &Self) -> bool;

  /// The message payload to actually transmit.
  fn message(&self) -> &Self::Message;

  /// Encoded length of `msg` in bytes (used for fitting messages into a UDP packet).
  fn encoded_len(msg: &Self::Message) -> usize;

  /// Called when this broadcast hits its retransmit ceiling and is dropped
  /// from the queue, OR when it is invalidated by a newer broadcast. Default
  /// no-op; implementors override for "leave" / "update_node" notification.
  fn finished(&self) {}

  /// Override to `true` if every broadcast of this type is intrinsically
  /// unique and cannot invalidate any other. Skips the `invalidates` scan
  /// for messages without an id.
  fn is_unique(&self) -> bool {
    false
  }
}

#[derive(Debug)]
struct LimitedBroadcast<B> {
  /// btree-key[0]: number of transmissions attempted so far.
  transmits: usize,
  /// btree-key[1]: encoded length (newer/smaller items prioritized within tier).
  msg_len: u64,
  /// btree-key[2]: monotonic insertion id.
  id: u64,
  broadcast: Arc<B>,
}

impl<B> Clone for LimitedBroadcast<B> {
  fn clone(&self) -> Self {
    Self {
      transmits: self.transmits,
      msg_len: self.msg_len,
      id: self.id,
      broadcast: self.broadcast.clone(),
    }
  }
}

impl<B> PartialEq for LimitedBroadcast<B> {
  fn eq(&self, other: &Self) -> bool {
    self.transmits == other.transmits && self.msg_len == other.msg_len && self.id == other.id
  }
}

impl<B> Eq for LimitedBroadcast<B> {}

impl<B> PartialOrd for LimitedBroadcast<B> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<B> Ord for LimitedBroadcast<B> {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .transmits
      .cmp(&other.transmits)
      .then_with(|| other.msg_len.cmp(&self.msg_len))
      .then_with(|| other.id.cmp(&self.id))
  }
}

#[derive(Copy, Clone)]
struct Cmp {
  transmits: usize,
  msg_len: u64,
  id: u64,
}

impl<B> PartialEq<&LimitedBroadcast<B>> for Cmp {
  fn eq(&self, other: &&LimitedBroadcast<B>) -> bool {
    self.transmits == other.transmits && self.msg_len == other.msg_len && self.id == other.id
  }
}

impl<B> PartialOrd<&LimitedBroadcast<B>> for Cmp {
  fn partial_cmp(&self, other: &&LimitedBroadcast<B>) -> Option<Ordering> {
    Some(
      self
        .transmits
        .cmp(&other.transmits)
        .then_with(|| other.msg_len.cmp(&self.msg_len))
        .then_with(|| other.id.cmp(&self.id)),
    )
  }
}

/// Queue of pending broadcasts, ordered by retransmit count (lowest first ⇒
/// freshest broadcasts go out first), with a configurable retransmit ceiling
/// derived from `retransmit_mult * ceil(log10(num_nodes + 1))`.
///
/// Sync version of `memberlist-core::queue::BroadcastQueue`. The number
/// of nodes is supplied per-call rather than via a `NodeCalculator` trait.
#[derive(Debug)]
pub struct BroadcastQueue<Id, B> {
  retransmit_mult: u32,
  q: BTreeSet<LimitedBroadcast<B>>,
  m: FxHashMap<Id, LimitedBroadcast<B>>,
  id_gen: u64,
}

impl<Id, B> BroadcastQueue<Id, B> {
  /// Construct a new queue with the given retransmit multiplier.
  pub fn new(retransmit_mult: u32) -> Self {
    Self {
      retransmit_mult,
      q: BTreeSet::new(),
      m: FxHashMap::default(),
      id_gen: 0,
    }
  }

  /// Number of currently queued broadcasts.
  #[inline(always)]
  pub fn num_queued(&self) -> usize {
    self.q.len()
  }

  /// True iff the queue is empty.
  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.q.is_empty()
  }

  fn next_id(&mut self) -> u64 {
    if self.id_gen == u64::MAX {
      self.id_gen = 1;
    } else {
      self.id_gen += 1;
    }
    self.id_gen
  }

  fn transmit_range(&self) -> (usize, usize) {
    match (self.q.first(), self.q.last()) {
      (Some(min), Some(max)) => (min.transmits, max.transmits),
      _ => (0, 0),
    }
  }
}

impl<Id, B> BroadcastQueue<Id, B>
where
  Id: Hash + core::cmp::Eq + core::clone::Clone,
  B: Broadcast<Id = Id>,
{
  fn insert_into(&mut self, item: LimitedBroadcast<B>) {
    let id = item.broadcast.id().cloned();
    // `q` is the source of truth; only mirror into `m` after `q` actually
    // accepted the item. `LimitedBroadcast` Eq/Ord is
    // `(transmits, msg_len, id)`; the id is always allocated AFTER
    // invalidation/reset so a queued id is never reused while still
    // present — hence this insert cannot collide. The assert pins that
    // invariant; the `if inserted` guard keeps `m`/`q` consistent even
    // if it were ever violated in release (no `m` entry without a
    // matching `q` entry → no silent drop).
    let inserted = self.q.insert(item.clone());
    debug_assert!(
      inserted,
      "BroadcastQueue invariant violated: a queued LimitedBroadcast id \
       was reused while still queued"
    );
    if inserted {
      if let Some(id) = id {
        self.m.insert(id, item);
      }
    }
  }

  /// Enqueue a broadcast. If a broadcast with the same id is already queued,
  /// the older one is `finished()` and replaced. If `is_unique() == false` and
  /// `id() == None`, all queued broadcasts that this one `invalidates` are
  /// `finished()` and removed.
  pub fn queue_broadcast(&mut self, b: B) {
    let broadcast = Arc::new(b);
    let msg_len = B::encoded_len(broadcast.message()) as u64;

    // Invalidate/replace BEFORE allocating this item's id. memberlist-core/
    // src/queue.rs stamps the id FIRST and shares a footgun: the has-id
    // replacement (or slow-path invalidation) can empty the queue and reset
    // `id_gen = 0`, then reinsert the already-stamped item carrying the
    // PRE-reset (anomalously high) id. That deterministically (the common
    // low-traffic case: one queued entry per node, replaced) breaks the
    // monotonic-id FIFO tiebreaker — subsequently-queued newer items get
    // LOWER ids and sort as "older"/higher-priority within a
    // `(transmits, msg_len)` tier — and, once `id_gen` later climbs back
    // to a still-queued id, risks a `(transmits, msg_len, id)` collision
    // that makes `q.insert` a silent no-op while `m` was already updated
    // (m/q desync + dropped membership gossip). Allocating the id AFTER
    // invalidation/reset keeps every queued id consistent with `id_gen`,
    // which is also faithful to upstream's *intent* (monotonic insertion
    // ids; reset only an idle/empty generator).
    if let Some(bid) = broadcast.id() {
      if let Some(old) = self.m.remove(bid) {
        old.broadcast.finished();
        self.q.remove(&old);
        if self.q.is_empty() {
          // Match legacy `Inner::remove` (memberlist-core/src/queue.rs:21-32):
          // an emptied (idle) queue resets the id counter so it doesn't
          // grow unboundedly across cluster lifetime.
          self.id_gen = 0;
        }
      }
    } else if !broadcast.is_unique() {
      // Slow path: invalidate any existing entries this one supersedes.
      let invalidated: Vec<LimitedBroadcast<B>> = self
        .q
        .iter()
        .filter(|item| broadcast.invalidates(&item.broadcast))
        .cloned()
        .collect();
      for item in &invalidated {
        item.broadcast.finished();
        self.q.remove(item);
        if let Some(id) = item.broadcast.id() {
          self.m.remove(id);
        }
      }
      if self.q.is_empty() {
        self.id_gen = 0;
      }
    }

    let lb = LimitedBroadcast {
      transmits: 0,
      msg_len,
      id: self.next_id(),
      broadcast,
    };
    self.insert_into(lb);
  }

  /// Take up to `limit` bytes worth of broadcasts, prioritizing by lowest
  /// retransmit count then largest message size then oldest id. Each chosen
  /// broadcast's `transmits` counter is incremented; if the new value reaches
  /// `retransmit_limit(retransmit_mult, num_nodes)`, the broadcast is finished
  /// and removed.
  ///
  /// Each selected message is charged `encoded_len(msg) + overhead` against
  /// `limit` (the overhead is the per-message framing cost when the result
  /// is assembled into a compound datagram — the inner-length varint of each
  /// compound part). Pass `0` when the result is NOT assembled into a
  /// compound (a single message becomes a byte-identical plain frame, and
  /// the full-drain path imposes no MTU). Faithful to memberlist-core
  /// `transmitLimitedQueue.GetBroadcasts(overhead, limit)`, which charges the
  /// same per-message overhead inside the selection loop.
  ///
  /// Returns the cloned messages in the order they should be transmitted.
  pub fn take_broadcasts(
    &mut self,
    num_nodes: u32,
    overhead: usize,
    limit: usize,
  ) -> Vec<B::Message> {
    self.take_broadcasts_measured(num_nodes, overhead, limit).0
  }

  /// As [`take_broadcasts`](Self::take_broadcasts), but also returns the total
  /// bytes the selection charged (each message's encoded length plus `overhead`).
  /// The gossip assembler uses this to size the residual user-data budget without
  /// re-encoding the selected messages.
  pub fn take_broadcasts_measured(
    &mut self,
    num_nodes: u32,
    overhead: usize,
    limit: usize,
  ) -> (Vec<B::Message>, usize) {
    if self.q.is_empty() {
      return (Vec::new(), 0);
    }

    let transmit_limit: u32 = retransmit_limit(self.retransmit_mult, num_nodes);
    let (min_tr, max_tr) = self.transmit_range();
    let mut to_send: Vec<B::Message> = Vec::new();
    let mut bytes_used = 0usize;
    let mut transmits = min_tr;
    let mut reinsert: Vec<LimitedBroadcast<B>> = Vec::new();

    while transmits <= max_tr {
      let free = limit.saturating_sub(bytes_used);
      if free <= overhead {
        break; // not even one more (msg + overhead) can fit
      }
      let fit = free - overhead; // bytes available for the message's plain frame

      // Lower bound narrows the BTree scan to items whose stored msg_len
      // is <= fit, skipping the encoded_len() call on items that
      // definitely won't fit. Matches the legacy lower bound in
      // memberlist-core/src/queue.rs:162-166.
      let geq = Cmp {
        transmits,
        msg_len: fit as u64,
        id: u64::MAX,
      };
      let lt = Cmp {
        transmits: transmits + 1,
        msg_len: u64::MAX,
        id: u64::MAX,
      };

      // Use the stored `msg_len` (computed once at `queue_broadcast`; the message
      // is immutable behind the `Arc`) instead of re-encoding each candidate.
      let candidate = self
        .q
        .iter()
        .filter(|item| geq <= item && lt > item)
        .find(|item| item.msg_len <= fit as u64)
        .cloned();

      match candidate {
        Some(mut keep) => {
          bytes_used += keep.msg_len as usize + overhead;
          to_send.push(keep.broadcast.message().clone());

          self.q.remove(&keep);
          if let Some(id) = keep.broadcast.id() {
            self.m.remove(id);
          }

          // transmit_limit is u32; keep.transmits is usize. Cast for comparison.
          if (keep.transmits as u32) + 1 >= transmit_limit {
            keep.broadcast.finished();
          } else {
            keep.transmits += 1;
            reinsert.push(keep);
          }
        }
        None => {
          transmits += 1;
        }
      }
    }

    for item in reinsert {
      self.insert_into(item);
    }
    if self.q.is_empty() {
      self.id_gen = 0;
    }
    (to_send, bytes_used)
  }

  /// Take exactly ONE broadcast whose encoded plain-frame length is
  /// `<= limit` (NO per-message overhead — a lone message is emitted as a
  /// byte-identical plain `Packet`, not a compound part), using the same
  /// priority order and transmit/`finished` accounting as
  /// [`take_broadcasts`]. The gossip scheduler's lone-Packet rescue calls
  /// this so a near-MTU membership broadcast the compound-reduced drain
  /// stranded (its plain frame exceeds `compound_budget - per-part
  /// overhead` yet still fits one datagram) is disseminated as a single
  /// valid `Packet` instead of lingering in the queue, never transmitted.
  pub fn take_one_broadcast(&mut self, num_nodes: u32, limit: usize) -> Option<B::Message> {
    if self.q.is_empty() {
      return None;
    }
    let transmit_limit: u32 = retransmit_limit(self.retransmit_mult, num_nodes);
    let (min_tr, max_tr) = self.transmit_range();
    let mut transmits = min_tr;
    while transmits <= max_tr {
      let geq = Cmp {
        transmits,
        msg_len: limit as u64,
        id: u64::MAX,
      };
      let lt = Cmp {
        transmits: transmits + 1,
        msg_len: u64::MAX,
        id: u64::MAX,
      };
      let candidate = self
        .q
        .iter()
        .filter(|item| geq <= item && lt > item)
        .find(|item| item.msg_len <= limit as u64)
        .cloned();
      match candidate {
        Some(mut keep) => {
          let msg = keep.broadcast.message().clone();
          self.q.remove(&keep);
          if let Some(id) = keep.broadcast.id() {
            self.m.remove(id);
          }
          if (keep.transmits as u32) + 1 >= transmit_limit {
            keep.broadcast.finished();
          } else {
            keep.transmits += 1;
            // Safe to reinsert inline (unlike take_broadcasts, which
            // defers it): we return immediately, so the bumped item
            // cannot be re-found by a continued tier ascent.
            self.insert_into(keep);
          }
          if self.q.is_empty() {
            self.id_gen = 0;
          }
          return Some(msg);
        }
        None => {
          transmits += 1;
        }
      }
    }
    None
  }

  /// Drain all queued broadcasts (calling `finished()` on each).
  pub fn reset(&mut self) {
    let q = core::mem::take(&mut self.q);
    self.m.clear();
    self.id_gen = 0;
    for item in q {
      item.broadcast.finished();
    }
  }

  /// Drop oldest broadcasts (highest retransmit count) until the queue has
  /// at most `max_retain` entries.
  ///
  /// Not wired into the `Endpoint` today: queued broadcasts are id-deduplicated,
  /// so the queue is already bounded by live membership (itself optionally
  /// capped via `EndpointOptions::max_members`). This is an available helper for
  /// a future hard queue-length ceiling; nothing currently calls it, so do not
  /// assume the queue is bounded by anything other than the membership dedup.
  pub fn prune(&mut self, max_retain: usize) {
    while self.q.len() > max_retain {
      let Some(item) = self.q.pop_last() else { break };
      item.broadcast.finished();
      if let Some(id) = item.broadcast.id() {
        self.m.remove(id);
      }
    }
    if self.q.is_empty() {
      self.id_gen = 0;
    }
  }
}

/// The concrete [`Broadcast`] impl used by [`Endpoint`](crate::endpoint::Endpoint):
/// a per-peer wrapper around a wire-format `Message<I, A>`. Newer broadcasts
/// for the same peer id invalidate older ones (the `id()` is the peer id, so
/// `BroadcastQueue::queue_broadcast` deduplicates by id).
#[derive(Debug)]
pub struct MemberlistBroadcast<I, A> {
  node: I,
  msg: Message<I, A>,
}

impl<I, A> MemberlistBroadcast<I, A> {
  /// Construct a new broadcast for the given peer id, carrying `msg`.
  pub const fn new(node: I, msg: Message<I, A>) -> Self {
    Self { node, msg }
  }

  /// The peer id this broadcast is about.
  #[inline(always)]
  pub const fn node_ref(&self) -> &I {
    &self.node
  }

  /// The wire-format message to gossip.
  #[inline(always)]
  pub const fn message_ref(&self) -> &Message<I, A> {
    &self.msg
  }
}

impl<I, A> Broadcast for MemberlistBroadcast<I, A>
where
  I: crate::Id,
  A: crate::CheapClone + crate::Data + 'static,
{
  type Id = I;
  type Message = Message<I, A>;

  fn id(&self) -> Option<&Self::Id> {
    Some(&self.node)
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node == other.node
  }

  fn message(&self) -> &Self::Message {
    &self.msg
  }

  fn encoded_len(msg: &Self::Message) -> usize {
    // `typed::Message` carries no codec (by design); size it via the buffa
    // wire form. The bridge's only failure mode is a missing required
    // `MessageField` (e.g. node/source/target) — `State::Unknown(_)` is
    // silently coerced to alive, never an error. Messages the machine itself
    // constructs always populate those fields, so the encode cannot fail.
    // Mirrors the `.expect("… cannot fail for well-formed data")` convention
    // used for the Ack/PushPull encode paths in stream.rs / endpoint.rs.
    //
    // NOTE: this allocates a Vec per sizing call (and `take_broadcasts`
    // calls it twice per chosen broadcast). Acceptable for correctness now;
    // a future optimisation could add a size-only path to memberlist-wire.
    crate::wire::encode_message::<I, A>(msg)
      .expect("outbound broadcast message must bridge to wire form")
      .len()
  }

  fn is_unique(&self) -> bool {
    false
  }
}

#[cfg(test)]
mod tests;
