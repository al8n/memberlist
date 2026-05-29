//! Broadcast trait and `BroadcastQueue` — the gossip retransmit queue.

use std::{
  collections::{BTreeSet, HashMap},
  sync::Arc,
};

/// Computes the per-message retransmit ceiling using the formula
/// `retransmit_mult * ceil(log10(num_nodes + 1))`.
#[inline]
pub fn retransmit_limit(retransmit_mult: u32, num_nodes: u32) -> u32 {
  let log_n = ((num_nodes as f64) + 1.0).log10().ceil() as u32;
  retransmit_mult * log_n
}

/// Something that can be broadcast via gossip to the cluster.
///
/// Sync version of `memberlist-core::broadcast::Broadcast`: `finished()` is
/// a synchronous notification (default no-op) instead of an async fn.
pub trait Broadcast: core::fmt::Debug + Send + Sync + 'static {
  /// Type used to identify broadcasts so newer ones can invalidate older ones.
  type Id: Clone + Eq + core::hash::Hash + core::fmt::Debug + core::fmt::Display;
  /// Type of the message payload (cloned out of the queue when sent).
  type Message: Clone + core::fmt::Debug + Send + Sync + 'static;

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
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<B> Ord for LimitedBroadcast<B> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
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
  fn partial_cmp(&self, other: &&LimitedBroadcast<B>) -> Option<std::cmp::Ordering> {
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
  m: HashMap<Id, LimitedBroadcast<B>>,
  id_gen: u64,
}

impl<Id, B> BroadcastQueue<Id, B> {
  /// Construct a new queue with the given retransmit multiplier.
  pub fn new(retransmit_mult: u32) -> Self {
    Self {
      retransmit_mult,
      q: BTreeSet::new(),
      m: HashMap::new(),
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
  Id: core::hash::Hash + core::cmp::Eq + core::clone::Clone,
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
    if self.q.is_empty() {
      return Vec::new();
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

      let candidate = self
        .q
        .iter()
        .filter(|item| geq <= item && lt > item)
        .find(|item| B::encoded_len(item.broadcast.message()) <= fit)
        .cloned();

      match candidate {
        Some(mut keep) => {
          let msg = keep.broadcast.message();
          bytes_used += B::encoded_len(msg) + overhead;
          to_send.push(msg.clone());

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
    to_send
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
        .find(|item| B::encoded_len(item.broadcast.message()) <= limit)
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
    let q = std::mem::take(&mut self.q);
    self.m.clear();
    self.id_gen = 0;
    for item in q {
      item.broadcast.finished();
    }
  }

  /// Drop oldest broadcasts (highest retransmit count) until the queue has
  /// at most `max_retain` entries.
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
  msg: memberlist_wire::typed::Message<I, A>,
}

impl<I, A> MemberlistBroadcast<I, A> {
  /// Construct a new broadcast for the given peer id, carrying `msg`.
  pub const fn new(node: I, msg: memberlist_wire::typed::Message<I, A>) -> Self {
    Self { node, msg }
  }

  /// The peer id this broadcast is about.
  #[inline(always)]
  pub const fn node_ref(&self) -> &I {
    &self.node
  }

  /// The wire-format message to gossip.
  #[inline(always)]
  pub const fn message_ref(&self) -> &memberlist_wire::typed::Message<I, A> {
    &self.msg
  }
}

impl<I, A> Broadcast for MemberlistBroadcast<I, A>
where
  I: memberlist_wire::CheapClone
    + memberlist_wire::Data
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::CheapClone
    + memberlist_wire::Data
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  type Id = I;
  type Message = memberlist_wire::typed::Message<I, A>;

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
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicUsize, Ordering};

  /// A simple broadcast type for testing. `id` is `Some(node_name)` so
  /// invalidation by name works. `finished()` increments a counter.
  #[derive(Debug)]
  struct TestBroadcast {
    node: &'static str,
    msg: String,
    finished: Arc<AtomicUsize>,
  }

  impl Broadcast for TestBroadcast {
    type Id = &'static str;
    type Message = String;

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
      msg.len()
    }
    fn finished(&self) {
      self.finished.fetch_add(1, Ordering::SeqCst);
    }
  }

  fn bcast(node: &'static str, msg: &str, finished: Arc<AtomicUsize>) -> TestBroadcast {
    TestBroadcast {
      node,
      msg: msg.to_string(),
      finished,
    }
  }

  #[test]
  fn retransmit_limit_math() {
    // 3 * ceil(log10(N+1))
    assert_eq!(retransmit_limit(3, 0), 0); // log10(1) = 0
    assert_eq!(retransmit_limit(3, 9), 3); // log10(10) = 1
    assert_eq!(retransmit_limit(3, 99), 6); // log10(100) = 2
    assert_eq!(retransmit_limit(3, 999), 9); // log10(1000) = 3
  }

  #[test]
  fn empty_queue_returns_empty() {
    let mut q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
    assert_eq!(q.num_queued(), 0);
    assert!(q.is_empty());
    assert!(q.take_broadcasts(10, 0, 1024).is_empty());
  }

  #[test]
  fn queue_then_get_returns_message() {
    let mut q = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));
    q.queue_broadcast(bcast("alice", "hello", f.clone()));
    assert_eq!(q.num_queued(), 1);

    let got = q.take_broadcasts(10, 0, 1024);
    assert_eq!(got, vec!["hello".to_string()]);
    // Still in queue (transmit_limit for 10 nodes is 6; we've only used 1 transmit).
    assert_eq!(q.num_queued(), 1);
    assert_eq!(f.load(Ordering::SeqCst), 0);
  }

  #[test]
  fn invalidates_replaces_old_broadcast() {
    let mut q = BroadcastQueue::new(3);
    let f1 = Arc::new(AtomicUsize::new(0));
    let f2 = Arc::new(AtomicUsize::new(0));
    q.queue_broadcast(bcast("alice", "hello-1", f1.clone()));
    q.queue_broadcast(bcast("alice", "hello-2", f2.clone()));

    assert_eq!(q.num_queued(), 1);
    assert_eq!(
      f1.load(Ordering::SeqCst),
      1,
      "old broadcast should be finished()"
    );
    assert_eq!(f2.load(Ordering::SeqCst), 0);

    let got = q.take_broadcasts(10, 0, 1024);
    assert_eq!(got, vec!["hello-2".to_string()]);
  }

  #[test]
  fn retransmit_ceiling_finishes_broadcast() {
    // num_nodes=0 ⇒ retransmit_limit = 0, so any broadcast gets finished
    // on first take_broadcasts.
    let mut q = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));
    q.queue_broadcast(bcast("alice", "hi", f.clone()));
    // Drained bytes are not the assertion subject — we only need the side
    // effect of finishing the broadcast under `retransmit_limit = 0`.
    let _ = q.take_broadcasts(0, 0, 1024);
    assert_eq!(q.num_queued(), 0);
    assert_eq!(f.load(Ordering::SeqCst), 1);
  }

  /// `queue_broadcast` must allocate the item's id AFTER the
  /// invalidation/reset block. memberlist-core stamps it first; when a
  /// has-id replacement empties the queue and resets `id_gen = 0`, the
  /// reinserted item then carries a PRE-reset id that is inconsistent with
  /// `id_gen` (id > id_gen). That breaks the monotonic-id FIFO tiebreaker
  /// and can later collide on `(transmits, msg_len, id)`, making `q.insert`
  /// a silent no-op while `m` was updated (m/q desync, dropped gossip).
  /// This test locks the invariant: every queued id stays `<= id_gen`,
  /// `m` and `q` never desync, nothing is silently dropped.
  #[test]
  fn queue_broadcast_id_allocated_after_reset_no_desync() {
    let mut q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));

    // A single replace empties the queue (sole entry) and resets id_gen.
    q.queue_broadcast(bcast("alice", "x", f.clone()));
    q.queue_broadcast(bcast("alice", "x", f.clone())); // replace → empty → reset

    assert_eq!(q.q.len(), 1);
    let surviving_id = q.q.iter().next().unwrap().id;
    // A broken reset would leave surviving id == 2 while id_gen == 0 → invariant violated.
    assert!(
      surviving_id <= q.id_gen,
      "queued id {} must stay consistent with id_gen {} (id allocated after reset)",
      surviving_id,
      q.id_gen
    );
    assert_eq!(q.id_gen, 1, "emptied generator reset then re-allocated → 1");
    assert_eq!(surviving_id, 1);

    // Repeated replaces keep it stable (id never runs away from id_gen).
    for _ in 0..8 {
      q.queue_broadcast(bcast("alice", "x", f.clone()));
    }
    assert_eq!(q.q.len(), 1);
    assert_eq!(q.q.iter().next().unwrap().id, 1);
    assert_eq!(q.id_gen, 1);

    // Burst of distinct nodes, identical message (equal msg_len) and
    // transmits == 0: ids must be monotone and no broadcast is silently
    // dropped by a (0, msg_len, id) collision.
    let nodes: [&'static str; 16] = [
      "b00", "b01", "b02", "b03", "b04", "b05", "b06", "b07", "b08", "b09", "b10", "b11", "b12",
      "b13", "b14", "b15",
    ];
    for n in nodes {
      q.queue_broadcast(bcast(n, "x", f.clone()));
    }

    // No m/q desync (a silent q.insert no-op leaves m longer than q).
    assert_eq!(
      q.m.len(),
      q.q.len(),
      "m/q desync: a broadcast was silently dropped"
    );
    assert_eq!(q.q.len(), 17, "alice + 16 distinct nodes all queued");
    for it in &q.q {
      assert!(
        it.id <= q.id_gen,
        "every queued id ({}) must stay <= id_gen ({})",
        it.id,
        q.id_gen
      );
    }
    // Every queued broadcast is actually transmittable (nothing lost).
    let delivered = q.take_broadcasts(100, 0, 1_000_000);
    assert_eq!(
      delivered.len(),
      17,
      "all 17 broadcasts must be deliverable (no silent drop)"
    );
  }

  #[test]
  fn limit_prevents_oversized_messages() {
    let mut q = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));
    q.queue_broadcast(bcast("alice", "this-is-too-big", f.clone()));
    let got = q.take_broadcasts(10, 0, 4);
    assert!(got.is_empty(), "no message should fit in 4 bytes");
    assert_eq!(q.num_queued(), 1, "broadcast should remain queued");
  }

  #[test]
  fn overhead_is_charged_against_the_budget() {
    let mut q = BroadcastQueue::new(3);
    q.queue_broadcast(bcast("alice", "hello", Arc::new(AtomicUsize::new(0))));
    assert_eq!(q.take_broadcasts(10, 0, 6), vec!["hello".to_string()]);
    let mut q2 = BroadcastQueue::new(3);
    q2.queue_broadcast(bcast("alice", "hello", Arc::new(AtomicUsize::new(0))));
    assert!(
      q2.take_broadcasts(10, 2, 6).is_empty(),
      "msg(5) + overhead(2) > limit(6) must not be selected"
    );
  }

  #[test]
  fn take_one_broadcast_respects_limit_and_accounting() {
    // Nothing fits a tiny limit ⇒ None, queue untouched.
    let mut q = BroadcastQueue::new(3);
    q.queue_broadcast(bcast("a", "hello", Arc::new(AtomicUsize::new(0)))); // 5 B
    assert!(q.take_one_broadcast(10, 4).is_none());
    assert_eq!(q.num_queued(), 1);
    // Fits exactly ⇒ that one message (no per-message overhead charged).
    assert_eq!(q.take_one_broadcast(10, 5), Some("hello".to_string()));

    // An over-limit broadcast is skipped; the fitting one is still picked.
    // num_nodes=0 ⇒ retransmit ceiling 0 ⇒ the selected "tiny" is
    // finished+removed (not reinserted for retransmit), so the skipped
    // "big" is the lone remainder — isolating the skip assertion from
    // take_broadcasts' faithful retransmit-reinsertion accounting.
    let mut q2 = BroadcastQueue::new(3);
    q2.queue_broadcast(bcast(
      "big",
      "this-is-too-big-to-fit",
      Arc::new(AtomicUsize::new(0)),
    )); // 22 B
    q2.queue_broadcast(bcast("ok", "tiny", Arc::new(AtomicUsize::new(0)))); // 4 B
    assert_eq!(q2.take_one_broadcast(0, 8), Some("tiny".to_string()));
    assert_eq!(
      q2.num_queued(),
      1,
      "the over-limit broadcast remains queued (skipped, not stranded-blocking)"
    );

    // num_nodes=0 ⇒ retransmit_limit 0 ⇒ the picked broadcast is
    // finished + removed (same accounting as take_broadcasts).
    let mut q3 = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));
    q3.queue_broadcast(bcast("x", "bye", f.clone()));
    assert_eq!(q3.take_one_broadcast(0, 64), Some("bye".to_string()));
    assert_eq!(q3.num_queued(), 0, "retransmit ceiling 0 ⇒ removed");
    assert_eq!(
      f.load(Ordering::SeqCst),
      1,
      "finished() called exactly once"
    );
  }

  #[test]
  fn reset_drains_and_finishes_all() {
    let mut q = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));
    q.queue_broadcast(bcast("alice", "x", f.clone()));
    q.queue_broadcast(bcast("bob", "y", f.clone()));
    q.reset();
    assert_eq!(q.num_queued(), 0);
    assert_eq!(f.load(Ordering::SeqCst), 2);
  }

  #[test]
  fn prune_keeps_only_max_retain() {
    let mut q = BroadcastQueue::new(3);
    let f = Arc::new(AtomicUsize::new(0));
    for i in 0..10u8 {
      // Use leaked &'static str via Box::leak for simplicity in tests.
      let name: &'static str = Box::leak(format!("n{i}").into_boxed_str());
      q.queue_broadcast(bcast(name, &format!("m{i}"), f.clone()));
    }
    assert_eq!(q.num_queued(), 10);
    q.prune(3);
    assert_eq!(q.num_queued(), 3);
    assert_eq!(
      f.load(Ordering::SeqCst),
      7,
      "7 broadcasts should be finished"
    );
  }

  #[test]
  fn memberlist_broadcast_invalidates_by_node_id() {
    use memberlist_wire::typed::{Ack, Message};
    let m1: MemberlistBroadcast<smol_str::SmolStr, std::net::SocketAddr> =
      MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(7)));
    let m2: MemberlistBroadcast<smol_str::SmolStr, std::net::SocketAddr> =
      MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(8)));
    let m3: MemberlistBroadcast<smol_str::SmolStr, std::net::SocketAddr> =
      MemberlistBroadcast::new(smol_str::SmolStr::new("bob"), Message::Ack(Ack::new(9)));
    assert!(m1.invalidates(&m2));
    assert!(!m1.invalidates(&m3));
    assert!(!m1.is_unique());
    assert_eq!(m1.id(), Some(&smol_str::SmolStr::new("alice")));
  }
}
