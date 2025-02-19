// use a sync version mutex here is reasonable, because the lock is only held for a short time.
#![allow(clippy::await_holding_lock)]

use std::{
  collections::{BTreeSet, HashMap},
  future::Future,
  sync::Arc,
};

use futures::lock::Mutex;

use crate::{broadcast::Broadcast, types::TinyVec, util::retransmit_limit};

struct Inner<B: Broadcast> {
  q: BTreeSet<LimitedBroadcast<B>>,
  m: HashMap<B::Id, LimitedBroadcast<B>>,
  id_gen: u64,
}

impl<B: Broadcast> Inner<B> {
  fn remove(&mut self, item: &LimitedBroadcast<B>) {
    self.q.remove(item);
    if let Some(id) = item.broadcast.id() {
      self.m.remove(id);
    }

    if self.q.is_empty() {
      // At idle there's no reason to let the id generator keep going
      // indefinitely.
      self.id_gen = 0;
    }
  }

  fn insert(&mut self, item: LimitedBroadcast<B>) {
    if let Some(id) = item.broadcast.id() {
      self.m.insert(id.clone(), item.clone());
    }
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

/// Used to calculate the number of nodes in the cluster.
pub trait NodeCalculator: Send + Sync + 'static {
  /// Returns the number of nodes in the cluster.
  fn num_nodes(&self) -> impl Future<Output = usize> + Send;
}

macro_rules! impl_node_calculator {
  ($($ty:ty),+$(,)?) => {
    $(
      impl NodeCalculator for ::std::sync::Arc<$ty> {
        async fn num_nodes(&self) -> usize {
          self.load(::std::sync::atomic::Ordering::SeqCst) as usize
        }
      }
    )*
  };
}

impl_node_calculator! {
  ::core::sync::atomic::AtomicUsize,
  ::core::sync::atomic::AtomicIsize,
  ::core::sync::atomic::AtomicU8,
  ::core::sync::atomic::AtomicI8,
  ::core::sync::atomic::AtomicU16,
  ::core::sync::atomic::AtomicI16,
  ::core::sync::atomic::AtomicU32,
  ::core::sync::atomic::AtomicI32,
  ::core::sync::atomic::AtomicU64,
  ::core::sync::atomic::AtomicI64,
}

/// Used to queue messages to broadcast to
/// the cluster (via gossip) but limits the number of transmits per
/// message. It also prioritizes messages with lower transmit counts
/// (hence newer messages).
pub struct TransmitLimitedQueue<B: Broadcast, N> {
  // num_nodes: Box<dyn Fn() -> usize + Send + Sync + 'static>,
  num_nodes: N,
  /// The multiplier used to determine the maximum
  /// number of retransmissions attempted.
  retransmit_mult: usize,
  inner: Mutex<Inner<B>>,
}

impl<B: Broadcast, N: NodeCalculator> TransmitLimitedQueue<B, N> {
  // /// Creates a new [`TransmitLimitedQueue`].
  // pub fn new(retransmit_mult: usize, calc: impl Fn() -> usize + Send + Sync + 'static) -> Self {
  //   Self {
  //     num_nodes: Box::new(calc),
  //     retransmit_mult,
  //     inner: Mutex::new(Inner {
  //       q: BTreeSet::new(),
  //       m: HashMap::new(),
  //       id_gen: 0,
  //     }),
  //   }
  // }

  /// Creates a new [`TransmitLimitedQueue`].
  pub fn new(retransmit_mult: usize, calc: N) -> Self {
    Self {
      num_nodes: calc,
      retransmit_mult,
      inner: Mutex::new(Inner {
        q: BTreeSet::new(),
        m: HashMap::new(),
        id_gen: 0,
      }),
    }
  }

  /// Returns the number of messages queued.
  pub async fn num_queued(&self) -> usize {
    self.inner.lock().await.q.len()
  }

  /// Returns the messages can be broadcast.
  pub async fn get_broadcasts(&self, limit: usize) -> TinyVec<B::Message> {
    self.get_broadcast_with_prepend(TinyVec::new(), limit).await
  }

  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    prepend: TinyVec<B::Message>,
    limit: usize,
  ) -> TinyVec<B::Message> {
    let mut to_send = prepend;
    let mut inner = self.inner.lock().await;
    if inner.q.is_empty() {
      return to_send;
    }

    let transmit_limit = retransmit_limit(self.retransmit_mult, self.num_nodes.num_nodes().await);

    // Visit fresher items first, but only look at stuff that will fit.
    // We'll go tier by tier, grabbing the largest items first.
    let (min_tr, max_tr) = inner.get_transmit_range();
    let mut bytes_used = 0usize;
    let mut transmits = min_tr;
    let mut reinsert = Vec::new();
    while transmits <= max_tr {
      let free = limit.saturating_sub(bytes_used);
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
        .find(|item| B::encoded_len(item.broadcast.message()) <= free)
        .cloned();

      match keep {
        Some(mut keep) => {
          let msg = keep.broadcast.message();
          bytes_used += B::encoded_len(msg);
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
    let mut inner = self.inner.lock().await;
    if inner.id_gen == u64::MAX {
      inner.id_gen = 1;
    } else {
      inner.id_gen += 1;
    }
    let id = inner.id_gen;

    let lb = LimitedBroadcast {
      transmits: initial_transmits,
      msg_len: B::encoded_len(b.message()) as u64,
      id,
      broadcast: Arc::new(b),
    };

    let unique = lb.broadcast.is_unique();

    // Check if this message invalidates another.
    if let Some(bid) = lb.broadcast.id() {
      if let Some(old) = inner.m.remove(bid) {
        old.broadcast.finished().await;
        inner.remove(&old);
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
      let mut inner = self.inner.lock().await;
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

#[derive(Debug)]
pub(crate) struct LimitedBroadcast<B: Broadcast> {
  // btree-key[0]: Number of transmissions attempted.
  transmits: usize,
  // btree-key[1]: copied from len(b.Message())
  msg_len: u64,
  // btree-key[2]: unique incrementing id stamped at submission time
  id: u64,
  pub(crate) broadcast: Arc<B>,
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

#[cfg(any(test, feature = "test"))]
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

        if prev_transmits != item.transmits || prev_msg_len != item.msg_len || prev_id != item.id {
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

#[cfg(any(test, feature = "test"))]
impl<B: Broadcast, N: NodeCalculator> TransmitLimitedQueue<B, N> {
  pub(crate) async fn ordered_view(&self, reverse: bool) -> TinyVec<LimitedBroadcast<B>> {
    let inner = self.inner.lock().await;

    let mut out = TinyVec::new();
    inner.walk_read_only(reverse, |b| {
      out.push(b.clone());
      true
    });
    out
  }
}

#[cfg(test)]
mod tests;
