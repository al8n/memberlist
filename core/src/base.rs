use std::{
  collections::{HashMap, VecDeque},
  sync::{
    atomic::{AtomicBool, AtomicU32, AtomicUsize},
    Arc,
  },
};

use agnostic_lite::{AsyncSpawner, RuntimeLite};
use async_channel::{Receiver, Sender};
use async_lock::{Mutex, RwLock};

use atomic_refcell::AtomicRefCell;
use futures::stream::FuturesUnordered;
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

// #[viewit::viewit]
pub(crate) struct Member<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) state: LocalNodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  pub(crate) suspicion: Option<Suspicion<T, D>>,
}

impl<T, D> core::fmt::Debug for Member<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Member")
      .field("state", &self.state)
      .finish()
  }
}

impl<T, D> core::ops::Deref for Member<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  type Target = LocalNodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

pub(crate) struct Members<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) local: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  pub(crate) nodes: TinyVec<Member<T, D>>,
  pub(crate) node_map: HashMap<T::Id, usize>,
}

impl<T, D> rand::seq::SliceRandom for Members<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  type Item = Member<T, D>;

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
      if ubound <= (u32::MAX as usize) {
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

impl<T, D> Members<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn new(local: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>) -> Self {
    Self {
      nodes: TinyVec::new(),
      node_map: HashMap::new(),
      local,
    }
  }
}

impl<T, D> Members<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) fn any_alive(&self) -> bool {
    for m in self.nodes.iter() {
      if !m.dead_or_left() && m.id().ne(self.local.id()) {
        return true;
      }
    }

    false
  }
}

pub(crate) struct MemberlistCore<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) id: T::Id,
  pub(crate) hot: HotData,
  pub(crate) awareness: Awareness,
  pub(crate) broadcast: TransmitLimitedQueue<
    MemberlistBroadcast<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress, T::Wire>,
    Arc<AtomicU32>,
  >,
  pub(crate) leave_broadcast_tx: Sender<()>,
  pub(crate) leave_lock: Mutex<()>,
  pub(crate) leave_broadcast_rx: Receiver<()>,
  pub(crate) handles: AtomicRefCell<
    FuturesUnordered<<<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()>>,
  >,
  pub(crate) probe_index: AtomicUsize,
  pub(crate) handoff_tx: Sender<()>,
  pub(crate) handoff_rx: Receiver<()>,
  pub(crate) queue: Mutex<MessageQueue<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  pub(crate) nodes: Arc<RwLock<Members<T, D>>>,
  pub(crate) ack_manager: AckManager<T::Runtime>,
  pub(crate) transport: Arc<T>,
  /// We do not call send directly, just directly drop it.
  pub(crate) shutdown_tx: Sender<()>,
  pub(crate) advertise: <T::Resolver as AddressResolver>::ResolvedAddress,
  pub(crate) opts: Arc<Options>,
}

impl<T, D> MemberlistCore<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) async fn shutdown(&self) -> Result<(), T::Error> {
    if !self.shutdown_tx.close() {
      return Ok(());
    }

    // Shut down the transport first, which should block until it's
    // completely torn down. If we kill the memberlist-side handlers
    // those I/O handlers might get stuck.
    if let Err(e) = self.transport.shutdown().await {
      tracing::error!(err=%e, "memberlist: failed to shutdown transport");
      return Err(e);
    }

    Ok(())
  }
}

impl<T, D> Drop for MemberlistCore<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
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
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) inner: Arc<MemberlistCore<T, D>>,
  pub(crate) delegate: Option<Arc<D>>,
}

impl<T, D> Clone for Memberlist<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
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
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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
    let broadcast = TransmitLimitedQueue::new(opts.retransmit_mult, num_nodes);

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
        probe_index: AtomicUsize::new(0),
        handles: AtomicRefCell::new(FuturesUnordered::new()),
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

    {
      let handles = this.inner.handles.borrow();
      handles.push(this.stream_listener(shutdown_rx.clone()));
      handles.push(this.packet_handler(shutdown_rx.clone()));
      handles.push(this.packet_listener(shutdown_rx.clone()));
      #[cfg(feature = "metrics")]
      handles.push(this.check_broadcast_queue_depth(shutdown_rx.clone()));
    }

    Ok((shutdown_rx, this.inner.advertise.cheap_clone(), this))
  }
}

// private impelementation
impl<T, D> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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
  ) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()> {
    use futures::{FutureExt, StreamExt};

    let queue_check_interval = self.inner.opts.queue_check_interval;
    let this = self.clone();

    <T::Runtime as RuntimeLite>::spawn(async move {
      let tick = <T::Runtime as RuntimeLite>::interval(queue_check_interval);
      futures::pin_mut!(tick);
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            tracing::debug!("memberlist: broadcast queue checker exits");
            return;
          },
          _ = tick.next().fuse() => {
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
