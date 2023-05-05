use std::{
  collections::{BTreeSet, HashMap},
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
};

use showbiz_traits::Broadcast;
use showbiz_types::SharedString;

use crate::util::retransmit_limit;

pub trait NodeCalculator {
  fn num_nodes(&self) -> usize;
}

struct Inner<B: Broadcast> {
  q: BTreeSet<Arc<LimitedBroadcast<B>>>,
  m: HashMap<SharedString, Arc<LimitedBroadcast<B>>>,
  id_gen: u64,
}

impl<B: Broadcast> Inner<B> {
  fn remove(&mut self, item: &LimitedBroadcast<B>) {
    if let Some(name) = item.broadcast.name() {
      self.m.remove(name);
    }

    if self.q.is_empty() {
      // At idle there's no reason to let the id generator keep going
      // indefinitely.
      self.id_gen = 0;
    }
  }

  fn insert(&mut self, item: LimitedBroadcast<B>) {
    let arc = Arc::new(item);
    if let Some(name) = arc.broadcast.name() {
      self
        .m
        .insert(SharedString::Owned(name.to_owned()), arc.clone());
    }
    self.q.insert(arc);
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
  #[cfg(not(feature = "async"))]
  inner: parking_lot::Mutex<Inner<B>>,
  #[cfg(feature = "async")]
  inner: futures::lock::Mutex<Inner<B>>,
}

impl<B: Broadcast, C: NodeCalculator> TransmitLimitedQueue<B, C> {
  #[cfg(feature = "async")]
  pub fn new(calc: C, retransmit_mult: usize) -> Self {
    Self {
      num_nodes: calc,
      retransmit_mult,
      inner: futures::lock::Mutex::new(Inner {
        q: BTreeSet::new(),
        m: HashMap::new(),
        id_gen: 0,
      }),
    }
  }

  #[cfg(not(feature = "async"))]
  pub const fn new(retransmit_mult: usize) -> Self {
    Self {
      retransmit_mult,
      inner: parking_lot::Mutex::new(Inner {
        q: BTreeSet::new(),
        m: HashMap::new(),
        id_gen: 0,
      }),
    }
  }

  pub async fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<bytes::Bytes> {
    let mut inner = self.inner.lock().await;
    if inner.q.is_empty() {
      return Vec::new();
    }

    let transmit_limit = retransmit_limit(self.retransmit_mult, self.num_nodes.num_nodes());

    todo!()
  }

  /// Used to enqueue a broadcast
  pub async fn queue_broadcast(&self, b: B) {
    self.queue_broadcast_in(b, 0).await
  }

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
    if let Some(name) = lb.broadcast.name() {
      if let Some(old) = inner.m.remove(name) {
        old.broadcast.finished().await;

        inner.q.remove(&old);
        if inner.q.is_empty() {
          inner.id_gen = 0;
        }
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
      #[cfg(not(feature = "async"))]
      {
        inner.q.retain(|item| {
          let keep = lb.broadcast.invalidates(&item.broadcast);
          if keep {
            item.broadcast.finished();
          }
          !keep
        });
      }

      if inner.q.is_empty() {
        // At idle there's no reason to let the id generator keep going
        // indefinitely.
        inner.id_gen = 0;
      }
    }

    // Append to the relevant queue.
    inner.insert(lb);
  }

  /// Used to enqueue a broadcast
  #[cfg(not(feature = "async"))]
  pub fn queue_broadcast(&self, b: B) {
    self.queue_broadcast_in(b, 0)
  }

  #[cfg(not(feature = "async"))]
  fn queue_broadcast_in(&self, b: B, initial_transmits: usize) {
    let mut inner = self.inner.lock();

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
    if let Some(name) = lb.broadcast.name() {
      if let Some(old) = inner.m.remove(name) {
        old.broadcast.finished();

        inner.q.remove(&old);
        if inner.q.is_empty() {
          inner.id_gen = 0;
        }
      }
    } else if !unique {
      // Slow path, hopefully nothing hot hits this.
      inner.q.retain(|item| {
        let keep = lb.broadcast.invalidates(&item.broadcast);
        if keep {
          item.broadcast.finished();
        }
        !keep
      });

      if inner.q.is_empty() {
        // At idle there's no reason to let the id generator keep going
        // indefinitely.
        inner.id_gen = 0;
      }
    }

    // Append to the relevant queue.
    inner.insert(lb);
  }

  #[cfg(all(not(feature = "async"), test))]
  pub fn reset(&self) {
    let mut inner = self.inner.lock();

    for b in inner.q.iter() {
      b.broadcast.finished();
    }

    inner.q.clear();
    inner.m.clear();
    inner.id_gen = 0;
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

  /// Retain the maxRetain latest messages, and the rest
  /// will be discarded. This can be used to prevent unbounded queue sizes
  #[cfg(not(feature = "async"))]
  pub fn prune(&self, max_retain: usize) {
    let mut inner = self.inner.lock().await;
    // Do nothing if queue size is less than the limit
    while inner.q.len() > max_retain {
      if let Some(item) = inner.q.pop_last() {
        item.broadcast.finished();
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
