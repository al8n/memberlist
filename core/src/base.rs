use std::{
  collections::{HashMap, VecDeque},
  future::Future,
  sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
  },
};

use agnostic::Runtime;
use async_channel::{Receiver, Sender};
use async_lock::{Mutex, RwLock};
use futures::Stream;
use nodecraft::{resolver::AddressResolver, CheapClone, Node};

use super::{
  awareness::Awareness,
  broadcast::MemberlistBroadcast,
  delegate::{Delegate, VoidDelegate},
  error::Error,
  queue::TransmitLimitedQueue,
  state::{AckManager, LocalNodeState},
  suspicion::Suspicion,
  transport::Transport,
  types::{Message, PushNodeState, TinyVec},
  Options,
};

#[cfg(any(test, feature = "test"))]
pub(crate) mod tests;

#[viewit::viewit]
pub(crate) struct HotData {
  sequence_num: AtomicU32,
  incarnation: AtomicU32,
  push_pull_req: AtomicU32,
  leave: AtomicBool,
  num_nodes: Arc<AtomicU32>,
}

impl HotData {
  fn new() -> Self {
    Self {
      sequence_num: AtomicU32::new(0),
      incarnation: AtomicU32::new(0),
      num_nodes: Arc::new(AtomicU32::new(0)),
      push_pull_req: AtomicU32::new(0),
      leave: AtomicBool::new(false),
    }
  }
}

#[viewit::viewit]
pub(crate) struct MessageHandoff<I, A> {
  msg: Message<I, A>,
  from: A,
}

#[viewit::viewit]
pub(crate) struct MessageQueue<I, A> {
  /// high priority messages queue
  high: VecDeque<MessageHandoff<I, A>>,
  /// low priority messages queue
  low: VecDeque<MessageHandoff<I, A>>,
}

impl<I, A> MessageQueue<I, A> {
  const fn new() -> Self {
    Self {
      high: VecDeque::new(),
      low: VecDeque::new(),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Member<I, A, R> {
  pub(crate) state: LocalNodeState<I, A>,
  pub(crate) suspicion: Option<Suspicion<I, R>>,
}

impl<I, A, R> core::ops::Deref for Member<I, A, R> {
  type Target = LocalNodeState<I, A>;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

pub(crate) struct Members<I, A, R> {
  pub(crate) local: Node<I, A>,
  pub(crate) nodes: TinyVec<Member<I, A, R>>,
  pub(crate) node_map: HashMap<I, usize>,
}

impl<I, A, Run: Runtime> rand::seq::SliceRandom for Members<I, A, Run>
where
  I: Eq + core::hash::Hash,
{
  type Item = Member<I, A, Run>;

  fn choose<R>(&self, _rng: &mut R) -> Option<&Self::Item>
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }

  fn choose_mut<R>(&mut self, _rng: &mut R) -> Option<&mut Self::Item>
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }

  fn choose_multiple<R>(
    &self,
    _rng: &mut R,
    _amount: usize,
  ) -> rand::seq::SliceChooseIter<Self, Self::Item>
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }

  fn choose_weighted<R, F, B, X>(
    &self,
    _rng: &mut R,
    _weight: F,
  ) -> Result<&Self::Item, rand::distributions::WeightedError>
  where
    R: rand::Rng + ?Sized,
    F: Fn(&Self::Item) -> B,
    B: rand::distributions::uniform::SampleBorrow<X>,
    X: rand::distributions::uniform::SampleUniform
      + for<'a> core::ops::AddAssign<&'a X>
      + core::cmp::PartialOrd<X>
      + Clone
      + Default,
  {
    unreachable!()
  }

  fn choose_weighted_mut<R, F, B, X>(
    &mut self,
    _rng: &mut R,
    _weight: F,
  ) -> Result<&mut Self::Item, rand::distributions::WeightedError>
  where
    R: rand::Rng + ?Sized,
    F: Fn(&Self::Item) -> B,
    B: rand::distributions::uniform::SampleBorrow<X>,
    X: rand::distributions::uniform::SampleUniform
      + for<'a> core::ops::AddAssign<&'a X>
      + core::cmp::PartialOrd<X>
      + Clone
      + Default,
  {
    unreachable!()
  }

  fn choose_multiple_weighted<R, F, X>(
    &self,
    _rng: &mut R,
    _amount: usize,
    _weight: F,
  ) -> Result<rand::seq::SliceChooseIter<Self, Self::Item>, rand::distributions::WeightedError>
  where
    R: rand::Rng + ?Sized,
    F: Fn(&Self::Item) -> X,
    X: Into<f64>,
  {
    unreachable!()
  }

  fn shuffle<R>(&mut self, rng: &mut R)
  where
    R: rand::Rng + ?Sized,
  {
    // Sample a number uniformly between 0 and `ubound`. Uses 32-bit sampling where
    // possible, primarily in order to produce the same output on 32-bit and 64-bit
    // platforms.
    #[inline]
    fn gen_index<R: rand::Rng + ?Sized>(rng: &mut R, ubound: usize) -> usize {
      if ubound <= (core::u32::MAX as usize) {
        rng.gen_range(0..ubound as u32) as usize
      } else {
        rng.gen_range(0..ubound)
      }
    }

    for i in (1..self.nodes.len()).rev() {
      // invariant: elements with index > i have been locked in place.
      let ridx = gen_index(rng, i + 1);
      let curr = self.node_map.get_mut(self.nodes[i].state.id()).unwrap();
      *curr = ridx;
      let target = self.node_map.get_mut(self.nodes[ridx].state.id()).unwrap();
      *target = i;
      self.nodes.swap(i, ridx);
    }
  }

  fn partial_shuffle<R>(
    &mut self,
    _rng: &mut R,
    _amount: usize,
  ) -> (&mut [Self::Item], &mut [Self::Item])
  where
    R: rand::Rng + ?Sized,
  {
    unreachable!()
  }
}

impl<I, A, R> Members<I, A, R> {
  fn new(local: Node<I, A>) -> Self {
    Self {
      nodes: TinyVec::new(),
      node_map: HashMap::new(),
      local,
    }
  }
}

impl<I: PartialEq, A, R> Members<I, A, R> {
  pub(crate) fn any_alive(&self) -> bool {
    for m in self.nodes.iter() {
      if !m.dead_or_left() && m.id().ne(self.local.id()) {
        return true;
      }
    }

    false
  }
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct MemberlistCore<T: Transport> {
  id: T::Id,
  hot: HotData,
  awareness: Awareness,
  broadcast: TransmitLimitedQueue<
    MemberlistBroadcast<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress, T::Wire>,
  >,
  leave_broadcast_tx: Sender<()>,
  leave_lock: Mutex<()>,
  leave_broadcast_rx: Receiver<()>,
  shutdown_lock: Mutex<Vec<<T::Runtime as Runtime>::JoinHandle<()>>>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  nodes: Arc<RwLock<Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress, T::Runtime>>>,
  ack_manager: AckManager,
  transport: Arc<T>,
  /// We do not call send directly, just directly drop it.
  shutdown_tx: Sender<()>,
  advertise: <T::Resolver as AddressResolver>::ResolvedAddress,
  opts: Arc<Options>,
}

impl<T: Transport> MemberlistCore<T> {
  pub(crate) async fn shutdown(&self) -> Result<(), T::Error> {
    if !self.shutdown_tx.close() {
      return Ok(());
    }

    let mut mu = self.shutdown_lock.lock().await;
    futures::future::join_all(core::mem::take(&mut *mu)).await;

    // Shut down the transport first, which should block until it's
    // completely torn down. If we kill the memberlist-side handlers
    // those I/O handlers might get stuck.
    if let Err(e) = self.transport.shutdown().await {
      tracing::error!(target =  "memberlist", err=%e, "failed to shutdown transport");
      return Err(e);
    }

    Ok(())
  }
}

impl<T: Transport> Drop for MemberlistCore<T> {
  fn drop(&mut self) {
    self.shutdown_tx.close();
  }
}

/// A cluster membership and member failure detection using a gossip based protocol.
///
/// The use cases for such a library are far-reaching: all distributed systems
/// require membership, and memberlist is a re-usable solution to managing
/// cluster membership and node failure detection.
///
/// memberlist is eventually consistent but converges quickly on average.
/// The speed at which it converges can be heavily tuned via various knobs
/// on the protocol. Node failures are detected and network partitions are partially
/// tolerated by attempting to communicate to potentially dead nodes through
/// multiple routes.
pub struct Memberlist<
  T,
  D = VoidDelegate<
    <T as Transport>::Id,
    <<T as Transport>::Resolver as AddressResolver>::ResolvedAddress,
  >,
> where
  T: Transport,
{
  pub(crate) inner: Arc<MemberlistCore<T>>,
  pub(crate) delegate: Option<Arc<D>>,
}

impl<T, D> Clone for Memberlist<T, D>
where
  T: Transport,
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      delegate: self.delegate.clone(),
    }
  }
}

impl<T, D> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) async fn new_in(
    transport: T,
    delegate: Option<D>,
    opts: Options,
  ) -> Result<
    (
      Receiver<()>,
      <T::Resolver as AddressResolver>::ResolvedAddress,
      Self,
    ),
    Error<T, D>,
  > {
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);
    let (leave_broadcast_tx, leave_broadcast_rx) = async_channel::bounded(1);

    // Get the final advertise address from the transport, which may need
    // to see which address we bound to. We'll refresh this each time we
    // send out an alive message.
    let advertise = transport.advertise_address();
    let id = transport.local_id();
    let node = Node::new(id.clone(), advertise.clone());
    let awareness = Awareness::new(
      opts.awareness_max_multiplier as isize,
      #[cfg(feature = "metrics")]
      Arc::new(vec![]),
    );
    let hot = HotData::new();
    let num_nodes = hot.num_nodes.clone();
    let broadcast = TransmitLimitedQueue::new(opts.retransmit_mult, move || {
      num_nodes.load(Ordering::Acquire) as usize
    });

    #[cfg(not(feature = "metrics"))]
    let mut handles = Vec::with_capacity(7);
    #[cfg(feature = "metrics")]
    let mut handles = Vec::with_capacity(8);
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let this = Memberlist {
      inner: Arc::new(MemberlistCore {
        id: id.cheap_clone(),
        hot,
        awareness,
        broadcast,
        leave_broadcast_tx,
        leave_lock: Mutex::new(()),
        leave_broadcast_rx,
        shutdown_lock: Mutex::new(Vec::new()),
        handoff_tx,
        handoff_rx,
        queue: Mutex::new(MessageQueue::new()),
        nodes: Arc::new(RwLock::new(Members::new(node))),
        ack_manager: AckManager::new(),
        shutdown_tx,
        advertise: advertise.cheap_clone(),
        transport: Arc::new(transport),
        opts: Arc::new(opts),
      }),
      delegate: delegate.map(Arc::new),
    };

    handles.push(this.stream_listener(shutdown_rx.clone()));
    handles.push(this.packet_handler(shutdown_rx.clone()));
    handles.push(this.packet_listener(shutdown_rx.clone()));
    #[cfg(feature = "metrics")]
    handles.push(this.check_broadcast_queue_depth(shutdown_rx.clone()));

    *this.inner.shutdown_lock.lock().await = handles;
    Ok((shutdown_rx, this.inner.advertise.cheap_clone(), this))
  }
}

// private impelementation
impl<T, D> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  #[inline]
  pub(crate) fn get_advertise(&self) -> &<T::Resolver as AddressResolver>::ResolvedAddress {
    &self.inner.advertise
  }

  /// Check for any other alive node.
  #[inline]
  pub(crate) async fn any_alive(&self) -> bool {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .any(|n| !n.state.dead_or_left() && n.state.server.id().ne(&self.inner.id))
  }

  #[cfg(feature = "metrics")]
  fn check_broadcast_queue_depth(
    &self,
    shutdown_rx: Receiver<()>,
  ) -> <T::Runtime as Runtime>::JoinHandle<()> {
    use futures::FutureExt;

    let queue_check_interval = self.inner.opts.queue_check_interval;
    let this = self.clone();

    <T::Runtime as Runtime>::spawn(async move {
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          },
          _ = <T::Runtime as Runtime>::sleep(queue_check_interval).fuse() => {
            let numq = this.inner.broadcast.num_queued().await;
            metrics::histogram!("memberlist.queue.broadcasts").record(numq as f64);
          }
        }
      }
    })
  }

  pub(crate) async fn verify_protocol(
    &self,
    _remote: &[PushNodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>],
  ) -> Result<(), Error<T, D>> {
    // TODO: now we do not need to handle this situation, because there is no update
    // on protocol.
    Ok(())
  }
}
