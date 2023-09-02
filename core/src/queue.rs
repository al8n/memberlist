// use a sync version mutex here is reasonable, because the lock is only held for a short time.
#![allow(clippy::await_holding_lock)]

use crossbeam_utils::CachePadded;
use std::{
  collections::{BTreeSet, HashMap},
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
};

use crate::{broadcast::Broadcast, types::Message, util::retransmit_limit};

pub trait NodeCalculator {
  fn num_nodes(&self) -> usize;
}

struct Inner<B: Broadcast> {
  q: BTreeSet<LimitedBroadcast<B>>,
  m: HashMap<B::Id, LimitedBroadcast<B>>,
  id_gen: u64,
}

impl<B: Broadcast> Inner<B> {
  fn remove(&mut self, item: &LimitedBroadcast<B>) {
    self.q.remove(item);
    let id = item.broadcast.id();
    self.m.remove(id);

    if self.q.is_empty() {
      // At idle there's no reason to let the id generator keep going
      // indefinitely.
      self.id_gen = 0;
    }
  }

  fn insert(&mut self, item: LimitedBroadcast<B>) {
    let id = item.broadcast.id();
    self.m.insert(id.clone(), item.clone());
    self.q.insert(item);
  }

  /// Returns a pair of min/max values for transmit values
  /// represented by the current queue contents. Both values represent actual
  /// transmit values on the interval [0, len).
  fn get_transmit_range(&self) -> (usize, usize) {
    if self.q.is_empty() {
      return (0, 0);
    }
    let (min, max) = (self.q.first(), self.q.last());
    match (min, max) {
      (Some(min), Some(max)) => (min.transmits, max.transmits),
      _ => (0, 0),
    }
  }
}

#[derive(Clone)]
pub(crate) struct DefaultNodeCalculator(Arc<CachePadded<AtomicU32>>);

impl NodeCalculator for DefaultNodeCalculator {
  fn num_nodes(&self) -> usize {
    self.0.load(Ordering::SeqCst) as usize
  }
}

impl DefaultNodeCalculator {
  #[inline]
  pub(crate) const fn new(num: Arc<CachePadded<AtomicU32>>) -> Self {
    Self(num)
  }
}

/// Used to queue messages to broadcast to
/// the cluster (via gossip) but limits the number of transmits per
/// message. It also prioritizes messages with lower transmit counts
/// (hence newer messages).
pub struct TransmitLimitedQueue<B: Broadcast, C: NodeCalculator> {
  num_nodes: C,
  /// The multiplier used to determine the maximum
  /// number of retransmissions attempted.
  retransmit_mult: usize,
  inner: parking_lot::Mutex<Inner<B>>,
}

impl<B: Broadcast, C: NodeCalculator> TransmitLimitedQueue<B, C> {
  pub fn new(calc: C, retransmit_mult: usize) -> Self {
    Self {
      num_nodes: calc,
      retransmit_mult,
      inner: parking_lot::Mutex::new(Inner {
        q: BTreeSet::new(),
        m: HashMap::new(),
        id_gen: 0,
      }),
    }
  }

  pub fn num_queued(&self) -> usize {
    self.inner.lock().q.len()
  }

  pub async fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Message> {
    self
      .get_broadcast_with_prepend(Vec::new(), overhead, limit)
      .await
  }

  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    prepend: Vec<Message>,
    overhead: usize,
    limit: usize,
  ) -> Vec<Message> {
    let mut to_send = prepend;
    let mut inner = self.inner.lock();
    if inner.q.is_empty() {
      return to_send;
    }

    let transmit_limit = retransmit_limit(self.retransmit_mult, self.num_nodes.num_nodes());

    // Visit fresher items first, but only look at stuff that will fit.
    // We'll go tier by tier, grabbing the largest items first.
    let (min_tr, max_tr) = inner.get_transmit_range();
    let mut bytes_used = 0usize;
    let mut transmits = min_tr;
    let mut reinsert = Vec::new();
    while transmits <= max_tr {
      let free = limit.saturating_sub(bytes_used).saturating_sub(overhead);
      if free == 0 {
        break;
      }

      let greater_or_equal = Cmp {
        transmits,
        msg_len: free as u64,
        id: u64::MAX,
      };

      let less_than = Cmp {
        transmits: transmits + 1,
        msg_len: u64::MAX,
        id: u64::MAX,
      };

      let keep = inner
        .q
        .iter()
        .filter(|item| greater_or_equal <= item && less_than > item)
        .find(|item| item.broadcast.message().underlying_bytes().len() <= free)
        .cloned();

      match keep {
        Some(mut keep) => {
          let msg = keep.broadcast.message();
          bytes_used += msg.underlying_bytes().len() + overhead;
          // Add to slice to send
          to_send.push(msg.clone());

          // check if we should stop transmission
          inner.remove(&keep);
          if keep.transmits + 1 >= transmit_limit {
            keep.broadcast.finished().await;
          } else {
            // We need to bump this item down to another transmit tier, but
            // because it would be in the same direction that we're walking the
            // tiers, we will have to delay the reinsertion until we are
            // finished our search. Otherwise we'll possibly re-add the message
            // when we ascend to the next tier.
            keep.transmits += 1;
            reinsert.push(keep);
          }
        }
        None => {
          transmits += 1;
          continue;
        }
      }
    }

    for item in reinsert {
      inner.insert(item);
    }

    to_send
  }

  /// Used to enqueue a broadcast
  pub async fn queue_broadcast(&self, b: B) {
    self.queue_broadcast_in(b, 0).await
  }

  async fn queue_broadcast_in(&self, b: B, initial_transmits: usize) {
    let mut inner = self.inner.lock();
    if inner.id_gen == u64::MAX {
      inner.id_gen = 1;
    } else {
      inner.id_gen += 1;
    }
    let id = inner.id_gen;

    let lb = LimitedBroadcast {
      transmits: initial_transmits,
      msg_len: b.message().0.len() as u64,
      id,
      broadcast: Arc::new(b),
    };

    let unique = lb.broadcast.is_unique();

    // Check if this message invalidates another.
    let id = lb.broadcast.id();
    if let Some(old) = inner.m.remove(id) {
      old.broadcast.finished().await;
      inner.q.remove(&old);
      if inner.q.is_empty() {
        inner.id_gen = 0;
      }
    } else if !unique {
      // Slow path, hopefully nothing hot hits this.
      for item in inner.q.iter() {
        let keep = lb.broadcast.invalidates(&item.broadcast);
        if keep {
          item.broadcast.finished().await;
        }
      }
      inner
        .q
        .retain(|item| !lb.broadcast.invalidates(&item.broadcast));

      if inner.q.is_empty() {
        // At idle there's no reason to let the id generator keep going
        // indefinitely.
        inner.id_gen = 0;
      }
    }

    // Append to the relevant queue.
    inner.insert(lb);
  }

  /// Clears all the queued messages.
  pub async fn reset(&self) {
    let q = {
      let mut inner = self.inner.lock();
      inner.m.clear();
      inner.id_gen = 0;
      std::mem::take(&mut inner.q)
    };

    for l in q {
      l.broadcast.finished().await;
    }
  }

  /// Retain the `max_retain` latest messages, and the rest
  /// will be discarded. This can be used to prevent unbounded queue sizes
  pub async fn prune(&self, max_retain: usize) {
    let mut inner = self.inner.lock();
    // Do nothing if queue size is less than the limit
    while inner.q.len() > max_retain {
      if let Some(item) = inner.q.pop_last() {
        item.broadcast.finished().await;
        inner.remove(&item);
      } else {
        break;
      }
    }
  }
}

struct LimitedBroadcast<B: Broadcast> {
  // btree-key[0]: Number of transmissions attempted.
  transmits: usize,
  // btree-key[1]: copied from len(b.Message())
  msg_len: u64,
  // btree-key[2]: unique incrementing id stamped at submission time
  id: u64,
  broadcast: Arc<B>,
}

impl<B: Broadcast> core::clone::Clone for LimitedBroadcast<B> {
  fn clone(&self) -> Self {
    Self {
      broadcast: self.broadcast.clone(),
      ..*self
    }
  }
}

impl<B: Broadcast> PartialEq for LimitedBroadcast<B> {
  fn eq(&self, other: &Self) -> bool {
    self.transmits == other.transmits && self.msg_len == other.msg_len && self.id == other.id
  }
}

impl<B: Broadcast> Eq for LimitedBroadcast<B> {}

impl<B: Broadcast> PartialOrd for LimitedBroadcast<B> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl<B: Broadcast> Ord for LimitedBroadcast<B> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .transmits
      .cmp(&other.transmits)
      .then_with(|| other.msg_len.cmp(&self.msg_len))
      .then_with(|| other.id.cmp(&self.id))
  }
}

struct Cmp {
  transmits: usize,
  msg_len: u64,
  id: u64,
}

impl<B: Broadcast> PartialEq<&LimitedBroadcast<B>> for Cmp {
  fn eq(&self, other: &&LimitedBroadcast<B>) -> bool {
    self.transmits == other.transmits && self.msg_len == other.msg_len && self.id == other.id
  }
}

impl<B: Broadcast> PartialOrd<&LimitedBroadcast<B>> for Cmp {
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

#[cfg(test)]
mod tests {
  use bytes::{BufMut, BytesMut};
  use futures_util::FutureExt;

  use crate::{broadcast::ShowbizBroadcast, Name, NodeId};

  use super::*;

  struct NC(usize);

  impl NodeCalculator for NC {
    fn num_nodes(&self) -> usize {
      self.0
    }
  }

  impl<B: Broadcast> Inner<B> {
    fn walk_read_only<F>(&self, reverse: bool, f: F)
    where
      F: FnMut(&LimitedBroadcast<B>) -> bool,
    {
      fn iter<'a, B: Broadcast, F>(it: impl Iterator<Item = &'a LimitedBroadcast<B>>, mut f: F)
      where
        F: FnMut(&LimitedBroadcast<B>) -> bool,
      {
        for item in it {
          let prev_transmits = item.transmits;
          let prev_msg_len = item.msg_len;
          let prev_id = item.id;

          let keep_going = f(item);

          if prev_transmits != item.transmits || prev_msg_len != item.msg_len || prev_id != item.id
          {
            panic!("edited queue while walking read only");
          }

          if !keep_going {
            break;
          }
        }
      }
      if reverse {
        iter(self.q.iter().rev(), f)
      } else {
        iter(self.q.iter(), f)
      }
    }
  }

  impl<B: Broadcast, C: NodeCalculator> TransmitLimitedQueue<B, C> {
    async fn ordered_view(&self, reverse: bool) -> Vec<LimitedBroadcast<B>> {
      let inner = self.inner.lock();

      let mut out = vec![];
      inner.walk_read_only(reverse, |b| {
        out.push(b.clone());
        true
      });
      out
    }
  }

  #[test]
  fn test_limited_broadcast_less() {
    struct Case {
      name: &'static str,
      a: Arc<LimitedBroadcast<ShowbizBroadcast>>,
      b: Arc<LimitedBroadcast<ShowbizBroadcast>>,
    }

    let cases = [
      Case {
        name: "diff-transmits",
        a: LimitedBroadcast {
          transmits: 0,
          msg_len: 10,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast {
            node: NodeId::new(
              Name::from_str_unchecked("diff-transmits-a"),
              "127.0.0.1:10".parse().unwrap(),
            ),
            msg: Message(BytesMut::from([0; 10].as_slice())),
            notify: None,
          }),
        }
        .into(),
        b: LimitedBroadcast {
          transmits: 1,
          msg_len: 10,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast {
            node: NodeId::new(
              Name::from_str_unchecked("diff-transmits-b"),
              "127.0.0.1:11".parse().unwrap(),
            ),
            msg: Message(BytesMut::from([0; 10].as_slice())),
            notify: None,
          }),
        }
        .into(),
      },
      Case {
        name: "same-transmits--diff-len",
        a: LimitedBroadcast {
          transmits: 0,
          msg_len: 12,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast {
            node: NodeId::new(
              Name::from_str_unchecked("same-transmits--diff-len-a"),
              "127.0.0.1:10".parse().unwrap(),
            ),
            msg: Message(BytesMut::from([0; 12].as_slice())),
            notify: None,
          }),
        }
        .into(),
        b: LimitedBroadcast {
          transmits: 0,
          msg_len: 10,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast {
            node: NodeId::new(
              Name::from_str_unchecked("same-transmits--diff-len-b"),
              "127.0.0.1:11".parse().unwrap(),
            ),
            msg: Message(BytesMut::from([0; 10].as_slice())),
            notify: None,
          }),
        }
        .into(),
      },
      Case {
        name: "same-transmits--same-len--diff-id",
        a: LimitedBroadcast {
          transmits: 0,
          msg_len: 12,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast {
            node: NodeId::new(
              Name::from_str_unchecked("same-transmits--same-len--diff-id-a"),
              "127.0.0.1:10".parse().unwrap(),
            ),
            msg: Message(BytesMut::from([0; 12].as_slice())),
            notify: None,
          }),
        }
        .into(),
        b: LimitedBroadcast {
          transmits: 0,
          msg_len: 12,
          id: 90,
          broadcast: Arc::new(ShowbizBroadcast {
            node: NodeId::new(
              Name::from_str_unchecked("same-transmits--same-len--diff-id-b"),
              "127.0.0.1:11".parse().unwrap(),
            ),
            msg: Message(BytesMut::from([0; 12].as_slice())),
            notify: None,
          }),
        }
        .into(),
      },
    ];

    for c in cases {
      assert!(c.a < c.b, "case: {}", c.name);

      #[allow(clippy::all)]
      let mut tree = BTreeSet::new();
      tree.insert(c.b.clone());
      tree.insert(c.a.clone());

      let min = tree.iter().min().unwrap();
      assert_eq!(min.transmits, c.a.transmits, "case: {}", c.name);
      assert_eq!(min.msg_len, c.a.msg_len, "case: {}", c.name);
      assert_eq!(min.id, c.a.id, "case: {}", c.name);

      let max = tree.iter().max().unwrap();
      assert_eq!(max.transmits, c.b.transmits, "case: {}", c.name);
      assert_eq!(max.msg_len, c.b.msg_len, "case: {}", c.name);
      assert_eq!(max.id, c.b.id, "case: {}", c.name);
    }
  }

  #[tokio::test]
  async fn test_transmit_limited_queue() {
    let q = TransmitLimitedQueue::new(NC(1), 1);
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("test".try_into().unwrap(), "127.0.0.1:10".parse().unwrap()),
      msg: Message(BytesMut::new()),
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("foo".try_into().unwrap(), "127.0.0.1:11".parse().unwrap()),
      msg: Message(BytesMut::new()),
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("bar".try_into().unwrap(), "127.0.0.1:12".parse().unwrap()),
      msg: Message(BytesMut::new()),
      notify: None,
    })
    .await;

    assert_eq!(q.num_queued(), 3);

    let dump = q.ordered_view(true).await;

    assert_eq!(dump.len(), 3);
    assert_eq!(dump[0].broadcast.node.name(), "test");
    assert_eq!(dump[1].broadcast.node.name(), "foo");
    assert_eq!(dump[2].broadcast.node.name(), "bar");

    // Should invalidate previous message
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("test".try_into().unwrap(), "127.0.0.1:10".parse().unwrap()),
      msg: Message(BytesMut::new()),
      notify: None,
    })
    .await;

    assert_eq!(q.num_queued(), 3);
    let dump = q.ordered_view(true).await;

    assert_eq!(dump.len(), 3);
    assert_eq!(dump[0].broadcast.node.name(), "foo");
    assert_eq!(dump[1].broadcast.node.name(), "bar");
    assert_eq!(dump[2].broadcast.node.name(), "test");
  }

  #[tokio::test]
  async fn test_transmit_limited_get_broadcasts() {
    let q = TransmitLimitedQueue::new(NC(10), 3);

    // 18 bytes per message
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("test".try_into().unwrap(), "127.0.0.1:10".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"1. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("foo".try_into().unwrap(), "127.0.0.1:11".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"2. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("bar".try_into().unwrap(), "127.0.0.1:12".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"3. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("baz".try_into().unwrap(), "127.0.0.1:13".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"4. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;

    // 2 byte overhead per message, should get all 4 messages
    let all = q.get_broadcasts(2, 80).await;
    assert_eq!(all.len(), 4);

    // 3 byte overhead, should only get 3 messages back
    let partial = q.get_broadcasts(3, 80).await;
    assert_eq!(partial.len(), 3);
  }

  #[tokio::test]
  async fn test_transmit_limited_get_broadcasts_limit() {
    let q = TransmitLimitedQueue::new(NC(10), 1);

    assert_eq!(0, q.inner.lock().id_gen);
    assert_eq!(
      2,
      retransmit_limit(q.retransmit_mult, q.num_nodes.num_nodes())
    );

    // 18 bytes per message
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("test".try_into().unwrap(), "127.0.0.1:10".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"1. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("foo".try_into().unwrap(), "127.0.0.1:11".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"2. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("bar".try_into().unwrap(), "127.0.0.1:12".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"3. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("baz".try_into().unwrap(), "127.0.0.1:13".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"4. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;

    assert_eq!(4, q.inner.lock().id_gen);

    // 3 byte overhead, should only get 3 messages back
    let partial = q.get_broadcasts(3, 80).await;
    assert_eq!(partial.len(), 3);

    assert_eq!(
      4,
      q.inner.lock().id_gen,
      "id generator doesn't reset until empty"
    );

    let partial = q.get_broadcasts(3, 80).await;
    assert_eq!(partial.len(), 3);
    assert_eq!(
      4,
      q.inner.lock().id_gen,
      "id generator doesn't reset until empty"
    );

    // Only two not expired
    let partial = q.get_broadcasts(3, 80).await;
    assert_eq!(partial.len(), 2);
    assert_eq!(0, q.inner.lock().id_gen, "id generator resets on empty");

    // Should get nothing
    let partial = q.get_broadcasts(3, 80).await;
    assert_eq!(partial.len(), 0);
    assert_eq!(0, q.inner.lock().id_gen, "id generator resets on empty");
  }

  #[tokio::test]
  async fn test_transmit_limited_prune() {
    let q = TransmitLimitedQueue::new(NC(10), 1);
    let (tx1, rx1) = async_channel::bounded(1);
    let (tx2, rx2) = async_channel::bounded(1);

    // 18 bytes per message
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("test".try_into().unwrap(), "127.0.0.1:10".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"1. this is a test.");
        Message(msg)
      },
      notify: Some(tx1),
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("foo".try_into().unwrap(), "127.0.0.1:11".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"2. this is a test.");
        Message(msg)
      },
      notify: Some(tx2),
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("bar".try_into().unwrap(), "127.0.0.1:12".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"3. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast {
      node: NodeId::new("baz".try_into().unwrap(), "127.0.0.1:13".parse().unwrap()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"4. this is a test.");
        Message(msg)
      },
      notify: None,
    })
    .await;

    // keep only 2
    q.prune(2).await;

    assert_eq!(2, q.num_queued());

    // Should notify the first two
    futures_util::select! {
      _ = rx1.recv().fuse() => {},
      default => panic!("expected invalidation"),
    }

    futures_util::select! {
      _ = rx2.recv().fuse() => {},
      default => panic!("expected invalidation"),
    }

    let dump = q.ordered_view(true).await;
    assert_eq!(dump[0].broadcast.id().name(), "bar");
    assert_eq!(dump[1].broadcast.id().name(), "baz");
  }

  #[tokio::test]
  async fn test_transmit_limited_ordering() {
    let q = TransmitLimitedQueue::new(NC(10), 1);
    let insert = |name: &str, transmits: usize| {
      q.queue_broadcast_in(
        ShowbizBroadcast {
          node: NodeId::new(
            name.try_into().unwrap(),
            format!("127.0.0.1:{transmits}").parse().unwrap(),
          ),
          msg: Message(BytesMut::new()),
          notify: None,
        },
        transmits,
      )
    };

    insert("node0", 0).await;
    insert("node1", 10).await;
    insert("node2", 3).await;
    insert("node3", 4).await;
    insert("node4", 7).await;

    let dump = q.ordered_view(true).await;
    assert_eq!(dump[0].transmits, 10);
    assert_eq!(dump[1].transmits, 7);
    assert_eq!(dump[2].transmits, 4);
    assert_eq!(dump[3].transmits, 3);
    assert_eq!(dump[4].transmits, 0);
  }
}
