use crossbeam_utils::CachePadded;
use std::{
  collections::{BTreeSet, HashMap},
  sync::{
    atomic::{AtomicU32, AtomicUsize, Ordering},
    Arc,
  },
};

use crate::{broadcast::Broadcast, types::Message, util::retransmit_limit};

#[async_trait::async_trait]
pub trait NodeCalculator {
  async fn num_nodes(&self) -> usize;
}

struct Inner<B: Broadcast> {
  q: BTreeSet<Arc<LimitedBroadcast<B>>>,
  m: HashMap<B::Id, Arc<LimitedBroadcast<B>>>,
  id_gen: u64,
}

impl<B: Broadcast> Inner<B> {
  fn remove(&mut self, item: &LimitedBroadcast<B>) {
    let id = item.broadcast.id();
    self.m.remove(id);

    if self.q.is_empty() {
      // At idle there's no reason to let the id generator keep going
      // indefinitely.
      self.id_gen = 0;
    }
  }

  fn insert(&mut self, item: Arc<LimitedBroadcast<B>>) {
    let id = item.broadcast.id();
    self.m.insert(id.clone(), item.clone());
    self.q.insert(item);
  }
}

#[derive(Clone)]
pub(crate) struct DefaultNodeCalculator(Arc<CachePadded<AtomicU32>>);

#[async_trait::async_trait]
impl NodeCalculator for DefaultNodeCalculator {
  async fn num_nodes(&self) -> usize {
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
  #[cfg(feature = "async")]
  inner: async_lock::Mutex<Inner<B>>,
}

impl<B: Broadcast, C: NodeCalculator> TransmitLimitedQueue<B, C> {
  #[cfg(feature = "async")]
  pub fn new(calc: C, retransmit_mult: usize) -> Self {
    Self {
      num_nodes: calc,
      retransmit_mult,
      inner: async_lock::Mutex::new(Inner {
        q: BTreeSet::new(),
        m: HashMap::new(),
        id_gen: 0,
      }),
    }
  }

  #[cfg(feature = "async")]
  pub async fn num_queued(&self) -> usize {
    self.inner.lock().await.q.len()
  }

  #[cfg(feature = "async")]
  pub async fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Message> {
    self
      .get_broadcast_with_prepend(Vec::new(), overhead, limit)
      .await
  }

  #[cfg(feature = "async")]
  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    prepend: Vec<Message>,
    overhead: usize,
    limit: usize,
  ) -> Vec<Message> {
    let mut to_send = prepend;
    let mut inner = self.inner.lock().await;
    if inner.q.is_empty() {
      return Vec::new();
    }

    let transmit_limit = retransmit_limit(self.retransmit_mult, self.num_nodes.num_nodes().await);

    // Visit fresher items first, but only look at stuff that will fit.
    // We'll go tier by tier, grabbing the largest items first.
    let (min_tr, max_tr) = match (inner.q.first(), inner.q.last()) {
      (Some(min), Some(max)) => (
        min.transmits.load(Ordering::Relaxed),
        max.transmits.load(Ordering::Relaxed),
      ),
      _ => (0, 0),
    };
    let mut bytes_used = 0usize;
    let mut transmits = min_tr;
    let mut reinsert = Vec::new();
    while transmits <= max_tr {
      let free = (limit - bytes_used).saturating_sub(overhead);
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
        .find(|item| item.broadcast.message().len() <= free)
        .cloned();

      match keep {
        Some(keep) => {
          let msg = keep.broadcast.message();
          bytes_used += msg.len() + overhead;
          // Add to slice to send
          to_send.push(msg.clone());

          // check if we should stop transmission
          inner.remove(&keep);
          if keep.transmits.load(Ordering::Relaxed) + 1 >= transmit_limit {
            keep.broadcast.finished().await;
          } else {
            // We need to bump this item down to another transmit tier, but
            // because it would be in the same direction that we're walking the
            // tiers, we will have to delay the reinsertion until we are
            // finished our search. Otherwise we'll possibly re-add the message
            // when we ascend to the next tier.
            keep.transmits.fetch_add(1, Ordering::Relaxed);
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
  #[cfg(feature = "async")]
  pub async fn queue_broadcast(&self, b: B) {
    self.queue_broadcast_in(b, 0).await
  }

  #[cfg(feature = "async")]
  async fn queue_broadcast_in(&self, b: B, initial_transmits: usize) {
    let mut inner = self.inner.lock().await;

    if inner.id_gen == u64::MAX {
      inner.id_gen = 1;
    } else {
      inner.id_gen += 1;
    }

    let id = inner.id_gen;

    let lb = LimitedBroadcast {
      transmits: AtomicUsize::new(initial_transmits),
      msg_len: b.message().len() as u64,
      id,
      broadcast: b,
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
    inner.insert(Arc::new(lb));
  }

  /// Clears all the queued messages.
  #[cfg(all(feature = "async", test))]
  pub async fn reset(&self) {
    let mut inner = self.inner.lock().await;

    for b in inner.q.iter() {
      b.broadcast.finished().await;
    }

    inner.q.clear();
    inner.m.clear();
    inner.id_gen = 0;
  }

  /// Retain the maxRetain latest messages, and the rest
  /// will be discarded. This can be used to prevent unbounded queue sizes
  #[cfg(feature = "async")]
  pub async fn prune(&self, max_retain: usize) {
    let mut inner = self.inner.lock().await;
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
  transmits: AtomicUsize,
  // btree-key[1]: copied from len(b.Message())
  msg_len: u64,
  // btree-key[2]: unique incrementing id stamped at submission time
  id: u64,
  broadcast: B,
}

impl<B: Broadcast> PartialEq for LimitedBroadcast<B> {
  fn eq(&self, other: &Self) -> bool {
    self.transmits.load(Ordering::Relaxed) == other.transmits.load(Ordering::Relaxed)
      && self.msg_len == other.msg_len
      && self.id == other.id
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
      .load(Ordering::Relaxed)
      .cmp(&other.transmits.load(Ordering::Relaxed))
      .then_with(|| self.msg_len.cmp(&other.msg_len))
      .then_with(|| self.id.cmp(&other.id))
  }
}

struct Cmp {
  transmits: usize,
  msg_len: u64,
  id: u64,
}

impl<B: Broadcast> PartialEq<&LimitedBroadcast<B>> for Cmp {
  fn eq(&self, other: &&LimitedBroadcast<B>) -> bool {
    self.transmits == other.transmits.load(Ordering::Relaxed)
      && self.msg_len == other.msg_len
      && self.id == other.id
  }
}

impl<B: Broadcast> PartialOrd<&LimitedBroadcast<B>> for Cmp {
  fn partial_cmp(&self, other: &&LimitedBroadcast<B>) -> Option<std::cmp::Ordering> {
    Some(
      self
        .transmits
        .cmp(&other.transmits.load(Ordering::Relaxed))
        .then_with(|| self.msg_len.cmp(&other.msg_len))
        .then_with(|| self.id.cmp(&other.id)),
    )
  }
}
