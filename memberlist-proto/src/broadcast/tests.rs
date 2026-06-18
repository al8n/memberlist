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

/// Reproduction of al8n/memberlist#131 (the memberlist side). The dropped
/// broadcast there is a serf QUERY — a UNIQUE broadcast (`id() == None`,
/// `is_unique()`), which never invalidates and whose only path to an `id_gen`
/// reset is the drain emptying the queue. After such a reset, several
/// same-length unique broadcasts (serf node-ids are fixed-length UUIDs, so the
/// encoded lengths collide) must coexist and all be delivered. The legacy
/// `TransmitLimitedQueue` could assign two simultaneously-queued items the same
/// `(transmits, msg_len, id)` BTreeSet key, making the second `insert` a silent
/// no-op — the dropped query in the issue. The Sans-I/O queue allocates the id
/// after the reset and `insert_into` refuses an m/q desync, so none is lost.
#[test]
fn issue_131_unique_same_length_broadcasts_survive_reset() {
  let mut q: BroadcastQueue<&'static str, UniqueBroadcast> = BroadcastQueue::new(3);

  // Cycle through full drains that empty the queue and reset id_gen — a node
  // whose earlier queries finished rebroadcasting. retransmit_limit(_, 0) == 0,
  // so each take finishes and removes its single broadcast.
  for i in 0..3 {
    q.queue_broadcast(UniqueBroadcast {
      msg: format!("warm{i}"),
    });
    let _ = q.take_broadcasts(0, 0, 4096);
    assert!(q.is_empty());
  }
  assert_eq!(q.id_gen, 0, "a fully drained queue resets the id generator");

  // Now queue many SAME-LENGTH unique broadcasts that all stay queued (the high
  // node count keeps the retransmit ceiling well above zero). Each must insert.
  let count = 32usize;
  for i in 0..count {
    let before = q.num_queued();
    q.queue_broadcast(UniqueBroadcast {
      msg: format!("q{i:04}"), // every message is length 5 ⇒ identical msg_len
    });
    assert_eq!(
      q.num_queued(),
      before + 1,
      "unique broadcast {i} was silently dropped on insert (key collision)"
    );
  }

  // No two simultaneously-queued items share a queue id (the BTreeSet would
  // silently collapse a duplicate `(transmits, msg_len, id)` key).
  let ids: BTreeSet<u64> = q.q.iter().map(|it| it.id).collect();
  assert_eq!(
    ids.len(),
    count,
    "duplicate queue ids ⇒ a silent drop occurred"
  );

  // Every queued broadcast is actually transmittable.
  let delivered = q.take_broadcasts(10_000, 0, 10_000_000);
  assert_eq!(
    delivered.len(),
    count,
    "all same-length unique queries must be deliverable (none dropped)"
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
  use crate::typed::{Ack, Message};
  let m1: MemberlistBroadcast<smol_str::SmolStr, core::net::SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(7)));
  let m2: MemberlistBroadcast<smol_str::SmolStr, core::net::SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(8)));
  let m3: MemberlistBroadcast<smol_str::SmolStr, core::net::SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("bob"), Message::Ack(Ack::new(9)));
  assert!(m1.invalidates(&m2));
  assert!(!m1.invalidates(&m3));
  assert!(!m1.is_unique());
  assert_eq!(m1.id(), Some(&smol_str::SmolStr::new("alice")));
}

/// A broadcast with no id that is intrinsically unique — `is_unique()` is
/// `true`, so `queue_broadcast` skips the invalidation scan and every
/// instance coexists. Does NOT override `finished()` (default no-op).
#[derive(Debug)]
struct UniqueBroadcast {
  msg: String,
}

impl Broadcast for UniqueBroadcast {
  type Id = &'static str;
  type Message = String;

  fn id(&self) -> Option<&Self::Id> {
    None
  }
  fn invalidates(&self, _other: &Self) -> bool {
    true // would invalidate everything IF consulted — proves is_unique short-circuits it
  }
  fn message(&self) -> &Self::Message {
    &self.msg
  }
  fn encoded_len(msg: &Self::Message) -> usize {
    msg.len()
  }
  fn is_unique(&self) -> bool {
    true
  }
}

#[test]
fn unique_broadcasts_skip_invalidation_and_coexist() {
  let mut q: BroadcastQueue<&'static str, UniqueBroadcast> = BroadcastQueue::new(3);
  for i in 0..4 {
    q.queue_broadcast(UniqueBroadcast {
      msg: format!("u{i}"),
    });
  }
  // Despite `invalidates() == true`, is_unique() short-circuits the scan,
  // so all four remain. (m stays empty because id() is None.)
  assert_eq!(q.num_queued(), 4);
  assert!(
    q.m.is_empty(),
    "id-less broadcasts never populate the id map"
  );
}

#[test]
fn default_finished_is_a_noop() {
  // UniqueBroadcast does not override finished(); pruning it must not panic
  // and must still drop the entries.
  let mut q: BroadcastQueue<&'static str, UniqueBroadcast> = BroadcastQueue::new(3);
  q.queue_broadcast(UniqueBroadcast {
    msg: "u".to_string(),
  });
  q.reset();
  assert!(q.is_empty());
}

/// An id-less, non-unique broadcast: exercises the slow invalidation path in
/// `queue_broadcast` (the `else if !is_unique()` branch). `invalidates` keys
/// on the message string so we can target specific entries.
#[derive(Debug)]
struct IdlessBroadcast {
  msg: String,
  finished: Arc<AtomicUsize>,
}

impl Broadcast for IdlessBroadcast {
  type Id = &'static str;
  type Message = String;

  fn id(&self) -> Option<&Self::Id> {
    None
  }
  fn invalidates(&self, other: &Self) -> bool {
    self.msg == other.msg
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

#[test]
fn idless_non_unique_invalidation_slow_path() {
  let mut q: BroadcastQueue<&'static str, IdlessBroadcast> = BroadcastQueue::new(3);
  let f = Arc::new(AtomicUsize::new(0));
  q.queue_broadcast(IdlessBroadcast {
    msg: "dup".to_string(),
    finished: f.clone(),
  });
  q.queue_broadcast(IdlessBroadcast {
    msg: "other".to_string(),
    finished: f.clone(),
  });
  assert_eq!(q.num_queued(), 2);

  // Re-queueing "dup" invalidates the first "dup" via the slow scan (no id),
  // finishing it; "other" is untouched.
  q.queue_broadcast(IdlessBroadcast {
    msg: "dup".to_string(),
    finished: f.clone(),
  });
  assert_eq!(q.num_queued(), 2, "old dup replaced, other survives");
  assert_eq!(f.load(Ordering::SeqCst), 1, "exactly one finished()");
}

#[test]
fn take_broadcasts_increments_transmits_across_calls() {
  // With a positive retransmit ceiling the broadcast is reinserted with a
  // bumped transmit count, then finished once the ceiling is reached.
  let mut q = BroadcastQueue::new(1); // retransmit_mult=1
  let f = Arc::new(AtomicUsize::new(0));
  // num_nodes=9 ⇒ retransmit_limit = 1 * ceil(log10(10)) = 1.
  q.queue_broadcast(bcast("a", "hi", f.clone()));
  // First take: transmits 0→1 reaches the limit (1) ⇒ finished + removed.
  assert_eq!(q.take_broadcasts(9, 0, 64), vec!["hi".to_string()]);
  assert_eq!(q.num_queued(), 0);
  assert_eq!(f.load(Ordering::SeqCst), 1);
}

#[test]
fn take_broadcasts_reinserts_below_ceiling() {
  // retransmit_limit large enough that the item survives multiple drains,
  // its transmit counter climbing each time (lowest-transmits-first order).
  let mut q = BroadcastQueue::new(3);
  let f = Arc::new(AtomicUsize::new(0));
  q.queue_broadcast(bcast("a", "hi", f.clone())); // ceiling for 99 nodes = 6
  for _ in 0..3 {
    assert_eq!(q.take_broadcasts(99, 0, 64), vec!["hi".to_string()]);
    assert_eq!(q.num_queued(), 1, "still below the retransmit ceiling");
  }
  assert_eq!(f.load(Ordering::SeqCst), 0, "not yet finished");
  assert_eq!(
    q.q.iter().next().unwrap().transmits,
    3,
    "transmit count climbed once per drain"
  );
}

#[test]
fn take_broadcasts_selects_multiple_until_budget() {
  let mut q = BroadcastQueue::new(3);
  q.queue_broadcast(bcast("a", "aaaa", Arc::new(AtomicUsize::new(0)))); // 4 B
  q.queue_broadcast(bcast("b", "bbbb", Arc::new(AtomicUsize::new(0)))); // 4 B
  q.queue_broadcast(bcast("c", "cccc", Arc::new(AtomicUsize::new(0)))); // 4 B
  // Budget fits two 4-byte messages (8) but not the third.
  let got = q.take_broadcasts(99, 0, 8);
  assert_eq!(got.len(), 2, "exactly two messages fit the 8-byte budget");
  assert_eq!(q.num_queued(), 3, "all reinserted (ceiling not reached)");
}

#[test]
fn take_one_broadcast_on_empty_queue_returns_none() {
  let mut q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
  assert!(q.take_one_broadcast(10, 64).is_none());
}

#[test]
fn prune_with_max_retain_above_len_is_noop() {
  let mut q = BroadcastQueue::new(3);
  let f = Arc::new(AtomicUsize::new(0));
  q.queue_broadcast(bcast("a", "x", f.clone()));
  q.queue_broadcast(bcast("b", "y", f.clone()));
  q.prune(10);
  assert_eq!(q.num_queued(), 2, "retain above len leaves everything");
  assert_eq!(f.load(Ordering::SeqCst), 0);
}

#[test]
fn id_gen_wraps_at_u64_max() {
  // Drive the private id_gen to u64::MAX and confirm next_id wraps to 1
  // (rather than overflowing) on the subsequent allocation.
  let mut q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
  q.id_gen = u64::MAX;
  // A fresh (distinct id) broadcast forces a next_id() call.
  q.queue_broadcast(bcast("wrap", "x", Arc::new(AtomicUsize::new(0))));
  assert_eq!(q.q.iter().next().unwrap().id, 1, "id_gen wrapped to 1");
  assert_eq!(q.id_gen, 1);
}

#[test]
fn memberlist_broadcast_accessors_and_wire_encoded_len() {
  use crate::typed::{Ack, Message};
  let b: MemberlistBroadcast<smol_str::SmolStr, core::net::SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(7)));
  assert_eq!(b.node_ref(), &smol_str::SmolStr::new("alice"));
  assert!(matches!(b.message_ref(), Message::Ack(_)));
  // The trait `message()` returns the same wire message.
  assert!(matches!(Broadcast::message(&b), Message::Ack(_)));
  // The real wire encoded_len path (bridges to memberlist-wire) is positive.
  let len =
    <MemberlistBroadcast<smol_str::SmolStr, core::net::SocketAddr> as Broadcast>::encoded_len(
      b.message_ref(),
    );
  assert!(len > 0, "an Ack message must encode to a nonzero length");
  // finished() is the default no-op and must not panic.
  Broadcast::finished(&b);
}

#[test]
fn memberlist_broadcast_queue_dedups_by_node_id() {
  use crate::typed::{Ack, Message};
  let mut q: BroadcastQueue<
    smol_str::SmolStr,
    MemberlistBroadcast<smol_str::SmolStr, core::net::SocketAddr>,
  > = BroadcastQueue::new(3);
  q.queue_broadcast(MemberlistBroadcast::new(
    smol_str::SmolStr::new("alice"),
    Message::Ack(Ack::new(1)),
  ));
  q.queue_broadcast(MemberlistBroadcast::new(
    smol_str::SmolStr::new("alice"),
    Message::Ack(Ack::new(2)),
  ));
  // Same node id ⇒ the second replaces the first via the id map.
  assert_eq!(q.num_queued(), 1);
  let drained = q.take_broadcasts(99, 0, 1_000_000);
  assert_eq!(drained.len(), 1);
}

/// A broadcast whose `id()` is `Some` only when constructed with one, but
/// always reports `is_unique() == false` and `invalidates` by `key`. An
/// instance built with `id == None` takes the slow `invalidates` scan yet can
/// still supersede an id-bearing entry — driving the `m.remove(id)` arm of the
/// slow path (the existing slow-path test only invalidates no-id entries).
#[derive(Debug)]
struct MixedIdBroadcast {
  id: Option<&'static str>,
  key: &'static str,
  msg: String,
  finished: Arc<AtomicUsize>,
}

impl Broadcast for MixedIdBroadcast {
  type Id = &'static str;
  type Message = String;

  fn id(&self) -> Option<&Self::Id> {
    self.id.as_ref()
  }
  fn invalidates(&self, other: &Self) -> bool {
    self.key == other.key
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

#[test]
fn slow_path_invalidation_removes_id_mapping_of_superseded_entry() {
  // Queue an id-bearing entry, then a NO-id entry (slow path) whose `key`
  // matches it. The slow scan supersedes the id-bearing entry and must drop
  // its `m` mapping too (the `if let Some(id) = item.id()` → `m.remove(id)`
  // arm), leaving the map and queue consistent.
  let mut q: BroadcastQueue<&'static str, MixedIdBroadcast> = BroadcastQueue::new(3);
  let f1 = Arc::new(AtomicUsize::new(0));
  let f2 = Arc::new(AtomicUsize::new(0));
  q.queue_broadcast(MixedIdBroadcast {
    id: Some("peer"),
    key: "k",
    msg: "with-id".to_string(),
    finished: f1.clone(),
  });
  assert_eq!(q.num_queued(), 1);
  // A no-id broadcast forces the `else if !is_unique()` slow path.
  q.queue_broadcast(MixedIdBroadcast {
    id: None,
    key: "k",
    msg: "no-id".to_string(),
    finished: f2.clone(),
  });
  assert_eq!(q.num_queued(), 1, "the id-bearing entry was superseded");
  assert_eq!(f1.load(Ordering::SeqCst), 1, "superseded entry finished()");
  // Re-queueing the original id must now insert fresh (its old `m` mapping
  // was removed), not silently no-op against a stale map entry.
  q.queue_broadcast(MixedIdBroadcast {
    id: Some("peer"),
    key: "other",
    msg: "again".to_string(),
    finished: f1.clone(),
  });
  assert_eq!(
    q.num_queued(),
    2,
    "the id mapping was cleared, so this inserts"
  );
}

#[test]
fn limited_broadcast_eq_and_ord_helpers() {
  // `LimitedBroadcast`'s hand-written `PartialEq` / `PartialOrd` are not
  // exercised by the `BTreeSet` (which uses `Ord::cmp`); drive them directly
  // so the equality and ordering contract is covered. Two entries that share
  // `(transmits, msg_len, id)` are equal; a lower transmit count sorts first.
  let f = Arc::new(AtomicUsize::new(0));
  let mk = |transmits: usize, msg_len: u64, id: u64| LimitedBroadcast {
    transmits,
    msg_len,
    id,
    broadcast: Arc::new(bcast("n", "m", f.clone())),
  };
  let a = mk(1, 5, 7);
  let b = mk(1, 5, 7);
  let c = mk(2, 5, 7);
  assert_eq!(a, b);
  assert_ne!(a, c);
  assert!(a < c, "a lower transmit count sorts before a higher one");
  assert_eq!(a.partial_cmp(&b), Some(core::cmp::Ordering::Equal));

  // The `Cmp` probe key's `PartialEq<&LimitedBroadcast>` mirror.
  let probe = Cmp {
    transmits: 1,
    msg_len: 5,
    id: 7,
  };
  assert!(probe == &a);
  assert!(probe != &c);
}
