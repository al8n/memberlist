// use a sync version mutex here is reasonable, because the lock is only held for a short time.
#![allow(clippy::await_holding_lock)]

use std::{
  collections::{BTreeSet, HashMap},
  sync::Arc,
};

use crate::{broadcast::Broadcast, util::retransmit_limit};

pub trait ServerCalculator {
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

/// Used to queue messages to broadcast to
/// the cluster (via gossip) but limits the number of transmits per
/// message. It also prioritizes messages with lower transmit counts
/// (hence newer messages).
pub struct TransmitLimitedQueue<B: Broadcast> {
  num_nodes: Box<dyn Fn() -> usize + Send + Sync + 'static>,
  /// The multiplier used to determine the maximum
  /// number of retransmissions attempted.
  retransmit_mult: usize,
  inner: parking_lot::Mutex<Inner<B>>,
}

impl<B: Broadcast> TransmitLimitedQueue<B> {
  pub fn new(retransmit_mult: usize, calc: impl Fn() -> usize + Send + Sync + 'static) -> Self {
    Self {
      num_nodes: Box::new(calc),
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

  pub async fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<B::Message> {
    self
      .get_broadcast_with_prepend(Vec::new(), overhead, limit)
      .await
  }

  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    prepend: Vec<B::Message>,
    overhead: usize,
    limit: usize,
  ) -> Vec<B::Message> {
    let mut to_send = prepend;
    let mut inner = self.inner.lock();
    if inner.q.is_empty() {
      return to_send;
    }

    let transmit_limit = retransmit_limit(self.retransmit_mult, (self.num_nodes)());

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
        .find(|item| B::encoded_len(item.broadcast.message()) <= free)
        .cloned();

      match keep {
        Some(mut keep) => {
          let msg = keep.broadcast.message();
          bytes_used += B::encoded_len(msg) + overhead;
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
  use std::net::SocketAddr;

  use bytes::{BufMut, Bytes, BytesMut};
  use either::Either;
  use futures::FutureExt;
  use smol_str::SmolStr;

  use crate::{broadcast::ShowbizBroadcast, types::Message};

  use super::*;

  struct DummyWire;

  impl crate::transport::Wire for DummyWire {
    type Error = std::io::Error;

    fn encoded_len<I, A>(msg: &Message<I, A>) -> usize {
      match msg {
        Message::UserData(b) => b.len(),
        _ => unreachable!(),
      }
    }

    fn encode_message<I, A>(
      _msg: Message<I, A>,
      _dst: &mut [u8],
    ) -> Result<(), Self::Error> {
      unreachable!() 
    }

    fn decode_message<I, A>(_src: &[u8]) -> Result<Message<I, A>, Self::Error> {
      unreachable!()
    }
  }

  struct NC(usize);

  impl ServerCalculator for NC {
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

  impl<B: Broadcast> TransmitLimitedQueue<B> {
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
      a: Arc<LimitedBroadcast<ShowbizBroadcast<SmolStr, SocketAddr, DummyWire>>>,
      b: Arc<LimitedBroadcast<ShowbizBroadcast<SmolStr, SocketAddr, DummyWire>>>,
    }

    let cases = [
      Case {
        name: "diff-transmits",
        a: LimitedBroadcast {
          transmits: 0,
          msg_len: 10,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
            node: Either::Left("diff-transmits-a".into()),
            msg: Message::UserData(Bytes::from([0; 10].as_slice())),
            notify: None,
            _marker: std::marker::PhantomData,
          }),
        }
        .into(),
        b: LimitedBroadcast {
          transmits: 1,
          msg_len: 10,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
            node: Either::Left("diff-transmits-b".into()),
            msg: Message::UserData(Bytes::from([0; 10].as_slice())),
            notify: None,
            _marker: std::marker::PhantomData,
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
          broadcast: Arc::new(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
            node: Either::Left("same-transmits--diff-len-a".into()),
            msg: Message::UserData(Bytes::from([0; 12].as_slice())),
            notify: None,
            _marker: std::marker::PhantomData,
          }),
        }
        .into(),
        b: LimitedBroadcast {
          transmits: 0,
          msg_len: 10,
          id: 100,
          broadcast: Arc::new(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
            node: Either::Left("same-transmits--diff-len-b".into()),
            msg: Message::UserData(Bytes::from([0; 10].as_slice())),
            notify: None,
            _marker: std::marker::PhantomData,
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
          broadcast: Arc::new(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
            node: Either::Left("same-transmits--same-len--diff-id-a".into()),
            msg: Message::UserData(Bytes::from([0; 12].as_slice())),
            notify: None,
            _marker: std::marker::PhantomData,
          }),
        }
        .into(),
        b: LimitedBroadcast {
          transmits: 0,
          msg_len: 12,
          id: 90,
          broadcast: Arc::new(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
            node: Either::Left("same-transmits--same-len--diff-id-b".into()),
            msg: Message::UserData(Bytes::from([0; 12].as_slice())),
            notify: None,
            _marker: std::marker::PhantomData,
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
    let q = TransmitLimitedQueue::new(1, || 1);
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("test".into()),
      msg: Message::UserData(Bytes::new()),
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("foo".into()),
      msg: Message::UserData(Bytes::new()),
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("bar".into()),
      msg: Message::UserData(Bytes::new()),
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;

    assert_eq!(q.num_queued(), 3);

    let dump = q.ordered_view(true).await;

    assert_eq!(dump.len(), 3);
    assert_eq!(dump[0].broadcast.node.as_ref().unwrap_left(), "test");
    assert_eq!(dump[1].broadcast.node.as_ref().unwrap_left(), "foo");
    assert_eq!(dump[2].broadcast.node.as_ref().unwrap_left(), "bar");

    // Should invalidate previous message
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("test".into()),
      msg: Message::UserData(Bytes::new()),
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;

    assert_eq!(q.num_queued(), 3);
    let dump = q.ordered_view(true).await;

    assert_eq!(dump.len(), 3);
    assert_eq!(dump[0].broadcast.node.as_ref().unwrap_left(), "foo");
    assert_eq!(dump[1].broadcast.node.as_ref().unwrap_left(), "bar");
    assert_eq!(dump[2].broadcast.node.as_ref().unwrap_left(), "test");
  }

  #[tokio::test]
  async fn test_transmit_limited_get_broadcasts() {
    let q = TransmitLimitedQueue::new(3, || 10);

    // 18 bytes per message
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("test".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"1. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("foo".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"2. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("bar".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"3. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("baz".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"4. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
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
    let q = TransmitLimitedQueue::new(1, || 10);

    assert_eq!(0, q.inner.lock().id_gen);
    assert_eq!(2, retransmit_limit(q.retransmit_mult, (q.num_nodes)()));

    // 18 bytes per message
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("test".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"1. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("foo".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"2. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("bar".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"3. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("baz".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"4. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
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
    let q = TransmitLimitedQueue::new(1, || 10);
    let (tx1, rx1) = async_channel::bounded(1);
    let (tx2, rx2) = async_channel::bounded(1);

    // 18 bytes per message
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("test".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"1. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: Some(tx1),
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("foo".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"2. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: Some(tx2),
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("bar".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"3. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;
    q.queue_broadcast(ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
      node: Either::Left("baz".into()),
      msg: {
        let mut msg = BytesMut::new();
        msg.put_slice(b"4. this is a test.");
        Message::UserData(msg.freeze())
      },
      notify: None,
      _marker: std::marker::PhantomData,
    })
    .await;

    // keep only 2
    q.prune(2).await;

    assert_eq!(2, q.num_queued());

    // Should notify the first two
    futures::select! {
      _ = rx1.recv().fuse() => {},
      default => panic!("expected invalidation"),
    }

    futures::select! {
      _ = rx2.recv().fuse() => {},
      default => panic!("expected invalidation"),
    }

    let dump = q.ordered_view(true).await;
    assert_eq!(
      dump[0].broadcast.id().unwrap().as_ref().unwrap_left(),
      "bar"
    );
    assert_eq!(
      dump[1].broadcast.id().unwrap().as_ref().unwrap_left(),
      "baz"
    );
  }

  #[tokio::test]
  async fn test_transmit_limited_ordering() {
    let q = TransmitLimitedQueue::new(1, || 10);
    let insert = |name: &str, transmits: usize| {
      q.queue_broadcast_in(
        ShowbizBroadcast::<SmolStr, SocketAddr, DummyWire> {
          node: Either::Left(name.into()),
          msg: Message::UserData(Bytes::new()),
          notify: None,
          _marker: std::marker::PhantomData,
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
