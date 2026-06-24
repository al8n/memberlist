use super::*;
use bytes::Bytes;
use core::net::SocketAddr;
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
/// `id_gen` (id > id_gen). That breaks the monotonic-id tiebreaker
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
  let m1: MemberlistBroadcast<smol_str::SmolStr, SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(7)));
  let m2: MemberlistBroadcast<smol_str::SmolStr, SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(8)));
  let m3: MemberlistBroadcast<smol_str::SmolStr, SocketAddr> =
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
fn peek_first_sendable_encoded_len_skips_over_limit_head() {
  // Empty queue ⇒ None.
  let mut q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
  assert_eq!(q.peek_first_sendable_encoded_len(1024), None);

  // The queue orders by (transmits asc, msg_len desc, id desc), so among equal
  // (fresh, transmits 0) items the LARGEST msg_len is first — exactly the one
  // take_broadcasts / take_one_broadcast select first under a roomy limit. Peek
  // must report its encoded length.
  q.queue_broadcast(bcast("a", "aa", Arc::new(AtomicUsize::new(0)))); // 2 B
  q.queue_broadcast(bcast("b", "bbbbbb", Arc::new(AtomicUsize::new(0)))); // 6 B
  q.queue_broadcast(bcast("c", "cccc", Arc::new(AtomicUsize::new(0)))); // 4 B
  assert_eq!(
    q.peek_first_sendable_encoded_len(1024),
    Some(6),
    "the largest fitting fresh item (6 B) is the first sendable item"
  );

  // An over-limit head is SKIPPED: under a 5 B limit the 6 B item does not fit,
  // so the next-largest fitting item (4 B) is reported — an untransmittable head
  // cannot mask a sendable item behind it.
  assert_eq!(
    q.peek_first_sendable_encoded_len(5),
    Some(4),
    "the 6 B head exceeds the 5 B limit and is skipped for the 4 B item"
  );

  // It is the SAME item take_one_broadcast drains first (a roomy lone limit
  // selects the top item, whose length the peek just reported).
  assert_eq!(
    q.take_one_broadcast(99, 1024),
    Some("bbbbbb".to_string()),
    "the drained top item is the one peek reported"
  );
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
  let b: MemberlistBroadcast<smol_str::SmolStr, SocketAddr> =
    MemberlistBroadcast::new(smol_str::SmolStr::new("alice"), Message::Ack(Ack::new(7)));
  assert_eq!(b.node_ref(), &smol_str::SmolStr::new("alice"));
  assert!(matches!(b.message_ref(), Message::Ack(_)));
  // The trait `message()` returns the same wire message.
  assert!(matches!(Broadcast::message(&b), Message::Ack(_)));
  // The real wire encoded_len path (bridges to memberlist-wire) is positive.
  let len =
    <MemberlistBroadcast<smol_str::SmolStr, SocketAddr> as Broadcast>::encoded_len(b.message_ref());
  assert!(len > 0, "an Ack message must encode to a nonzero length");
  // finished() is the default no-op and must not panic.
  Broadcast::finished(&b);
}

#[test]
fn memberlist_broadcast_queue_dedups_by_node_id() {
  use crate::typed::{Ack, Message};
  let mut q: BroadcastQueue<smol_str::SmolStr, MemberlistBroadcast<smol_str::SmolStr, SocketAddr>> =
    BroadcastQueue::new(3);
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

#[test]
fn transmit_range_on_empty_queue_is_zero_zero() {
  // The `take_*` paths early-return before calling `transmit_range`, so the
  // empty-queue arm (`q.first()`/`q.last()` are None) is exercised directly.
  let q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
  assert_eq!(q.transmit_range(), (0, 0));

  // A populated queue takes the `(Some, Some)` arm: min and max transmit counts.
  let mut q2 = BroadcastQueue::new(3);
  q2.queue_broadcast(bcast("a", "x", Arc::new(AtomicUsize::new(0))));
  assert_eq!(q2.transmit_range(), (0, 0), "a fresh entry has 0 transmits");
}

#[test]
fn insert_into_mirrors_id_bearing_entry_into_map() {
  // `insert_into`'s `if let Some(id)` arm mirrors an id-bearing broadcast into
  // `m`; the post-insert block leaves `m` and `q` in lockstep.
  let mut q: BroadcastQueue<&'static str, TestBroadcast> = BroadcastQueue::new(3);
  q.queue_broadcast(bcast("peer", "x", Arc::new(AtomicUsize::new(0))));
  assert_eq!(q.q.len(), 1);
  assert_eq!(q.m.len(), 1, "the id-bearing entry is mirrored into m");
  assert!(q.m.contains_key("peer"));

  // A reinsertion (transmits bumped, below ceiling) re-mirrors the same id.
  assert_eq!(q.take_broadcasts(99, 0, 64), vec!["x".to_string()]);
  assert_eq!(q.q.len(), 1, "reinserted below the retransmit ceiling");
  assert_eq!(q.m.len(), 1, "the reinserted id is mirrored again");
  assert!(q.m.contains_key("peer"));
}

#[test]
fn prune_to_empty_resets_id_gen() {
  // Pruning every entry takes `prune`'s post-loop empty-queue arm, resetting
  // the id generator so it does not grow unboundedly across cluster lifetime.
  let mut q = BroadcastQueue::new(3);
  let f = Arc::new(AtomicUsize::new(0));
  q.queue_broadcast(bcast("a", "x", f.clone()));
  q.queue_broadcast(bcast("b", "y", f.clone()));
  assert!(q.id_gen > 0, "ids were allocated");

  q.prune(0);
  assert!(q.is_empty(), "max_retain 0 drops every entry");
  assert_eq!(
    f.load(Ordering::SeqCst),
    2,
    "each pruned entry is finished()"
  );
  assert_eq!(q.id_gen, 0, "an emptied queue resets the id generator");
  assert!(q.m.is_empty(), "the id map is drained alongside the queue");
}

#[test]
fn noid_display_and_bounds() {
  // `NoId` must satisfy the `Broadcast::Id` bounds (Clone/Eq/Hash/Debug/Display).
  let a = NoId;
  let b = a;
  assert_eq!(a, b);
  // Display renders without panicking (placeholder glyph).
  let _ = format!("{a}");
  // Hashable: usable as a map key.
  let mut set = std::collections::HashSet::new();
  set.insert(NoId);
  assert!(set.contains(&NoId));
}

#[test]
fn bytes_broadcast_is_unique_and_no_id() {
  let b = BytesBroadcast::new(Bytes::from_static(b"payload"));
  assert!(b.is_unique(), "user broadcasts are intrinsically unique");
  assert!(b.id().is_none(), "user broadcasts carry no invalidation id");
  assert!(
    !b.invalidates(&b),
    "user broadcasts never invalidate others"
  );
  assert_eq!(b.message(), &Bytes::from_static(b"payload"));
}

#[test]
fn bytes_broadcast_encoded_len_is_byte_length() {
  // `BytesBroadcast` sizes its payload as the plain byte length (no wire
  // encode), the natural sizing for an opaque user payload.
  let data = Bytes::from_static(b"abcdef");
  assert_eq!(
    <BytesBroadcast as Broadcast>::encoded_len(&data),
    6,
    "encoded_len is the byte length"
  );
}

#[test]
fn bytes_broadcast_round_trips_through_queue_with_retransmit() {
  // A user broadcast queued ONCE must be emitted across multiple drains
  // (retransmit), not delivered once. For 99 nodes the ceiling is
  // retransmit_mult(3) * ceil(log10(100)) = 3 * 2 = 6.
  let mut q: BroadcastQueue<NoId, BytesBroadcast> = BroadcastQueue::new(3);
  q.queue_broadcast(BytesBroadcast::new(Bytes::from_static(b"hello")));
  let mut emitted = 0;
  for _ in 0..6 {
    let got = q.take_broadcasts(99, 0, 1024);
    assert_eq!(got, vec![Bytes::from_static(b"hello")]);
    emitted += 1;
  }
  assert_eq!(emitted, 6, "emitted once per drain up to the ceiling");
  // The 6th drain reached the ceiling and evicted it.
  assert_eq!(q.num_queued(), 0, "evicted at the retransmit ceiling");
  // Further drains yield nothing.
  assert!(q.take_broadcasts(99, 0, 1024).is_empty());
}

#[test]
fn bytes_broadcast_finishes_without_panic() {
  // `BytesBroadcast` carries no finished-callback; `finished()` is the default no-op.
  let mut q: BroadcastQueue<NoId, BytesBroadcast> = BroadcastQueue::new(3);
  q.queue_broadcast(BytesBroadcast::new(Bytes::from_static(b"x")));
  q.reset();
  assert!(q.is_empty());
}

#[test]
fn bytes_broadcast_new_drops_without_panic() {
  // `BytesBroadcast` carries no finished-callback; dropping it must not panic.
  let b = BytesBroadcast::new(Bytes::from_static(b"x"));
  drop(b);
}

#[test]
fn take_broadcasts_measured_admits_empty_payload_at_exact_overhead() {
  // `BytesBroadcast::encoded_len` is the raw byte length, so an empty payload has
  // length 0 and is admissible (`0 + overhead <= limit`). The drain guard must
  // keep searching when `free == overhead` — exiting there would skip the empty
  // payload forever, never selecting or retransmit-counting it.
  let mut q: BroadcastQueue<NoId, BytesBroadcast> = BroadcastQueue::new(3);
  q.queue_broadcast(BytesBroadcast::new(Bytes::new())); // encoded_len 0
  let overhead = 11usize;
  // num_nodes=10 ⇒ retransmit ceiling 6, so a selected payload stays queued
  // (transmits 0→1) — proving it was selected + retransmit-counted, not skipped.
  let (msgs, used) = q.take_broadcasts_measured(10, overhead, overhead);
  assert_eq!(
    msgs.len(),
    1,
    "the empty payload fits exactly at limit == overhead and must be selected"
  );
  assert!(msgs[0].is_empty());
  assert_eq!(
    used, overhead,
    "charged exactly the overhead for a 0-length body"
  );
  assert_eq!(
    q.num_queued(),
    1,
    "selected + retransmit-counted (ceiling 6 not reached), still queued"
  );
}

#[test]
fn user_broadcasts_single_tier_matches_one_queue() {
  let mut u = UserBroadcasts::<NoId, BytesBroadcast>::single(3);
  assert!(u.is_empty());
  u.queue_ranked(0, BytesBroadcast::new(Bytes::from_static(b"a")));
  assert_eq!(u.num_queued(), 1);
  let (msgs, used) = u.take_broadcasts_measured(99, 0, 1024);
  assert_eq!(msgs, vec![Bytes::from_static(b"a")]);
  assert_eq!(used, 1, "one byte charged, no overhead");
}

#[test]
fn user_broadcasts_drains_tiers_in_priority_order_under_shrinking_budget() {
  // 3 tiers; rank 0 = highest priority drained first. Each tier holds one
  // 4-byte message. num_nodes=0 ⇒ retransmit ceiling 0 ⇒ each message is
  // evicted after one emission, isolating the priority ordering from
  // retransmit reinsertion. A budget of 4 fits exactly ONE message, so only
  // tier 0 (highest priority) drains; tiers 1 and 2 are starved this round.
  let mut u =
    UserBroadcasts::<NoId, BytesBroadcast>::new(core::num::NonZeroUsize::new(3).unwrap(), 3);
  u.queue_ranked(0, BytesBroadcast::new(Bytes::from_static(b"aaaa")));
  u.queue_ranked(1, BytesBroadcast::new(Bytes::from_static(b"bbbb")));
  u.queue_ranked(2, BytesBroadcast::new(Bytes::from_static(b"cccc")));
  assert_eq!(u.num_queued(), 3);

  let (msgs, used) = u.take_broadcasts_measured(0, 0, 4);
  assert_eq!(
    msgs,
    vec![Bytes::from_static(b"aaaa")],
    "only the highest-priority tier (0) fits the 4-byte budget"
  );
  assert_eq!(used, 4);
  assert_eq!(
    u.num_queued(),
    2,
    "tier 0 emptied (ceiling 0), 1 and 2 remain"
  );

  // A roomy budget then drains the remaining tiers in priority order 1 then 2.
  let (msgs, _used) = u.take_broadcasts_measured(0, 0, 1024);
  assert_eq!(
    msgs,
    vec![Bytes::from_static(b"bbbb"), Bytes::from_static(b"cccc")],
    "tier 1 drains before tier 2"
  );
  assert!(u.is_empty());
}

#[test]
fn user_broadcasts_saturated_high_tier_defers_lower_tier_observably() {
  // Strict priority, NO fairness: a continuously-refilled rank-0 (top) item that
  // fills the byte budget each round preempts the rank-1 item indefinitely — by
  // design (Go-faithful intent>query>event) — but the deferred rank-1 item stays
  // OBSERVABLE in the queue (never silently lost). num_nodes=0 ⇒ ceiling 0 ⇒ each
  // rank-0 emission evicts, so a fresh rank-0 is re-queued each round to keep
  // tier 0 saturated.
  let mut u =
    UserBroadcasts::<NoId, BytesBroadcast>::new(core::num::NonZeroUsize::new(2).unwrap(), 3);
  u.queue_ranked(1, BytesBroadcast::new(Bytes::from_static(b"low")));

  for _ in 0..16 {
    u.queue_ranked(0, BytesBroadcast::new(Bytes::from_static(b"aaaa")));
    // Budget 4 fits exactly one rank-0 part; the residual for tier 1 is 0.
    let (msgs, used) = u.take_broadcasts_measured(0, 0, 4);
    assert_eq!(
      msgs,
      vec![Bytes::from_static(b"aaaa")],
      "the saturated rank-0 tier consumes the whole budget; rank-1 is deferred"
    );
    assert_eq!(used, 4);
  }

  // The deferred rank-1 payload is STILL queued (observable, not lost) after the
  // sustained rank-0 flood — strict priority gives the lower tier no
  // eventual-delivery guarantee, but it remains visible via the queue.
  assert_eq!(
    u.num_queued(),
    1,
    "the deferred rank-1 payload remains observable in the queue"
  );
}

#[test]
fn user_broadcasts_queue_ranked_out_of_range_saturates_to_lowest_tier() {
  // rank >= tier count saturates to the LOWEST tier (min(rank, len-1)).
  // A too-large rank is a too-low priority and belongs in the lowest tier.
  let mut u =
    UserBroadcasts::<NoId, BytesBroadcast>::new(core::num::NonZeroUsize::new(3).unwrap(), 3);
  // rank 99 saturates to tier index 2 (the lowest of 3 tiers).
  u.queue_ranked(99, BytesBroadcast::new(Bytes::from_static(b"low")));
  // A higher-priority message in tier 0 drains first.
  u.queue_ranked(0, BytesBroadcast::new(Bytes::from_static(b"hi")));

  let (msgs, _used) = u.take_broadcasts_measured(99, 0, 1024);
  assert_eq!(
    msgs,
    vec![Bytes::from_static(b"hi"), Bytes::from_static(b"low")],
    "tier 0 drains before the saturated lowest tier"
  );
}

#[test]
fn user_broadcasts_empty_and_num_queued() {
  let mut u = UserBroadcasts::<NoId, BytesBroadcast>::single(3);
  assert!(u.is_empty());
  assert_eq!(u.num_queued(), 0);
  let (msgs, used) = u.take_broadcasts_measured(99, 0, 1024);
  assert!(msgs.is_empty());
  assert_eq!(used, 0);
}
