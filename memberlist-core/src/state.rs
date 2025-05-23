use std::{
  ops::ControlFlow,
  sync::{
    Arc,
    atomic::{AtomicIsize, AtomicU32, Ordering},
  },
  time::Duration,
};

use super::{
  Epoch, Member, Members,
  base::Memberlist,
  delegate::Delegate,
  error::Error,
  proto::{
    Alive, Data, DataRef, Dead, ErrorResponse, IndirectPing, Message, MessageRef, MessageType,
    NodeState, Ping, ProtoWriter, PushNodeState, SmallVec, State, Suspect,
  },
  suspicion::{Suspicion, Suspicioner},
  transport::{Connection, Transport},
};

use agnostic_lite::{AsyncSpawner, RuntimeLite, time::Instant};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use nodecraft::{CheapClone, Node};
use rand::{Rng, seq::SliceRandom};

/// Exports the state unit test cases.
#[cfg(any(test, feature = "test"))]
pub mod tests;

mod ack_manager;
pub(crate) use ack_manager::*;
use smallvec_wrapper::OneOrMore;

#[viewit::viewit]
#[derive(Debug)]
pub(crate) struct LocalNodeState<I, A> {
  server: Arc<NodeState<I, A>>,
  incarnation: Arc<AtomicU32>,
  state_change: Epoch,
  /// The current state of the node
  state: State,
}

impl<I, A> Clone for LocalNodeState<I, A> {
  fn clone(&self) -> Self {
    Self {
      server: self.server.clone(),
      incarnation: self.incarnation.clone(),
      state_change: self.state_change,
      state: self.state,
    }
  }
}

impl<I, A> core::ops::Deref for LocalNodeState<I, A> {
  type Target = NodeState<I, A>;

  fn deref(&self) -> &Self::Target {
    &self.server
  }
}

impl<I, A> LocalNodeState<I, A> {
  #[inline]
  pub(crate) fn dead_or_left(&self) -> bool {
    self.state == State::Dead || self.state == State::Left
  }
}

// private implementation
impl<T, D> Memberlist<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
{
  /// Returns a usable sequence number in a thread safe way
  #[inline]
  pub(crate) fn next_sequence_number(&self) -> u32 {
    self
      .inner
      .hot
      .sequence_num
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
      + 1
  }

  /// Returns the next incarnation number in a thread safe way
  #[inline]
  pub(crate) fn next_incarnation(&self) -> u32 {
    self
      .inner
      .hot
      .incarnation
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
      + 1
  }

  /// Adds the positive offset to the incarnation number.
  #[inline]
  pub(crate) fn skip_incarnation(&self, offset: u32) -> u32 {
    self
      .inner
      .hot
      .incarnation
      .fetch_add(offset, std::sync::atomic::Ordering::SeqCst)
      + offset
  }

  /// Used to get the current estimate of the number of nodes
  #[inline]
  pub(crate) fn estimate_num_nodes(&self) -> u32 {
    self
      .inner
      .hot
      .num_nodes
      .load(std::sync::atomic::Ordering::SeqCst)
  }

  #[inline]
  pub(crate) fn has_shutdown(&self) -> bool {
    self.inner.shutdown_tx.is_closed()
  }

  #[inline]
  pub(crate) fn has_left(&self) -> bool {
    self
      .inner
      .hot
      .leave
      .load(std::sync::atomic::Ordering::SeqCst)
  }
}

// ---------------------------------Crate methods-------------------------------
impl<T, D> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// Does a complete state exchange with a specific node.
  pub(crate) async fn push_pull_node(
    &self,
    id: Node<T::Id, T::ResolvedAddress>,
    join: bool,
  ) -> Result<(), Error<T, D>> {
    #[cfg(feature = "metrics")]
    let now = <T::Runtime as RuntimeLite>::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!("memberlist.push_pull_node", self.inner.opts.metric_labels.iter()).record(now.elapsed().as_millis() as f64);
    );

    // Attempt to connect
    let conn = self
      .inner
      .transport
      .open(
        id.address(),
        <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout,
      )
      .await
      .map_err(Error::transport)?;
    tracing::debug!(local_addr = %self.inner.id, peer_addr = %id, "memberlist: initiating push/pull sync");

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "memberlist.promised.connect",
        self.inner.opts.metric_labels.iter()
      )
      .increment(1);
    }

    let (mut reader, mut writer) = conn.split();

    // Send our state
    if let Err(e) = self.send_local_state(&mut writer, join).await {
      if let Err(e) = writer.close().await {
        tracing::error!(local_addr = %self.inner.id, peer_addr = %id, err = %e, "memberlist: failed to close connection");
      }
      return Err(e);
    }

    let res = <T::Runtime as RuntimeLite>::timeout_at(
      <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout,
      self.read_message(id.address(), &mut reader),
    )
    .await;

    let payload = match res {
      Ok(res) => res?,
      Err(e) => return Err(Error::transport(std::io::Error::from(e).into())),
    };

    let (_, msg) = <MessageRef::<'_, <T::Id as Data>::Ref<'_>, <T::ResolvedAddress as Data>::Ref<'_>> as DataRef<Message<T::Id, T::ResolvedAddress>>>::decode(&payload)?;
    let pp = match msg {
      MessageRef::ErrorResponse(resp) => {
        let resp = <ErrorResponse as Data>::from_ref(resp)?;
        tracing::error!(local_addr = %self.inner.id, peer_addr = %id, err = %resp, "memberlist: push/pull sync failed");
        return Err(Error::remote(resp));
      }
      MessageRef::PushPull(pp) => pp,
      msg => return Err(Error::unexpected_message(MessageType::PushPull, msg.ty())),
    };

    let res = self.merge_remote_state(pp).await;
    if let Err(e) = writer.close().await {
      tracing::error!(local_addr = %self.inner.id, peer_addr = %id, err = %e, "memberlist: failed to close connection");
    }

    res
  }

  pub(crate) async fn dead_node(
    &self,
    memberlist: &mut Members<T, D>,
    d: Dead<T::Id>,
  ) -> Result<(), Error<T, D>> {
    let idx = match memberlist.node_map.get(d.node()) {
      Some(idx) => *idx,
      // If we've never heard about this node before, ignore it
      None => return Ok(()),
    };

    let state = &mut memberlist.nodes[idx];
    // Ignore old incarnation numbers
    if d.incarnation() < state.state.incarnation.load(Ordering::Acquire) {
      return Ok(());
    }

    // Clear out any suspicion timer that may be in effect.
    state.suspicion = None;

    // Ignore if node is already dead
    if state.state.dead_or_left() {
      return Ok(());
    }

    let incarnation = d.incarnation();
    let is_dead_self = d.node() == d.from();
    let is_self = state.id().eq(self.local_id());

    // Check if this is us
    if is_self {
      // If we are not leaving we need to refute
      if !self.has_left() {
        self.refute(state, incarnation).await;
        tracing::warn!(
          "memberlist.state: refuting a dead message (from: {})",
          d.from()
        );
        return Ok(()); // Do not mark ourself dead
      }

      // If we are leaving, we broadcast and wait
      self
        .broadcast_notify(
          d.node().cheap_clone(),
          d.into(),
          Some(self.inner.leave_broadcast_tx.clone()),
        )
        .await;
    } else {
      self.broadcast(d.node().cheap_clone(), d.into()).await;
    }

    #[cfg(feature = "metrics")]
    {
      metrics::counter!("memberlist.msg.dead", self.inner.opts.metric_labels.iter()).increment(1);
    }

    // Update the state
    state
      .state
      .incarnation
      .store(incarnation, Ordering::Release);

    // If the dead message was send by the node itself, mark it is left
    // instead of dead.
    if is_dead_self {
      state.state.state = State::Left;
    } else {
      state.state.state = State::Dead;
    }
    state.state.state_change = Epoch::now();

    // notify of death
    if let Some(ref delegate) = self.delegate {
      delegate.notify_leave(state.state.server.clone()).await;
    }

    Ok(())
  }

  pub(crate) async fn suspect_node(&self, s: Suspect<T::Id>) -> Result<(), Error<T, D>> {
    let mut mu = self.inner.nodes.write().await;

    let Some(&idx) = mu.node_map.get(s.node()) else {
      return Ok(());
    };

    let state = &mut mu.nodes[idx];
    // Ignore old incarnation numbers
    if s.incarnation() < state.state.incarnation.load(Ordering::Relaxed) {
      return Ok(());
    }

    // See if there's a suspicion timer we can confirm. If the info is new
    // to us we will go ahead and re-gossip it. This allows for multiple
    // independent confirmations to flow even when a node probes a node
    // that's already suspect.
    if let Some(timer) = &mut state.suspicion {
      if timer.confirm(s.from()).await {
        self.broadcast(s.node().cheap_clone(), s.into()).await;
      }
      return Ok(());
    }

    // Ignore non-alive nodes
    if state.state.state != State::Alive {
      return Ok(());
    }

    // If this is us we need to refute, otherwise re-broadcast
    let snode = s.node().cheap_clone();
    let sfrom = s.from().cheap_clone();
    let sincarnation = s.incarnation();
    if state.id().eq(self.local_id()) {
      self.refute(&state.state, s.incarnation()).await;
      tracing::warn!(
        "memberlist.state: refuting a suspect message (from: {})",
        s.from()
      );
      // Do not mark ourself suspect
      return Ok(());
    } else {
      self.broadcast(s.node().clone(), s.into()).await;
    }

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "memberlist.msg.suspect",
        self.inner.opts.metric_labels.iter()
      )
      .increment(1);
    }

    // Update the state
    state
      .state
      .incarnation
      .store(sincarnation, Ordering::Relaxed);
    state.state.state = State::Suspect;
    let change_time = Epoch::now();
    state.state.state_change = change_time;

    // Setup a suspicion timer. Given that we don't have any known phase
    // relationship with our peers, we set up k such that we hit the nominal
    // timeout two probe intervals short of what we expect given the suspicion
    // multiplier.
    let mut k = self.inner.opts.suspicion_mult as isize - 2;

    // If there aren't enough nodes to give the expected confirmations, just
    // set k to 0 to say that we don't expect any. Note we subtract 2 from n
    // here to take out ourselves and the node being probed.
    let n = self.estimate_num_nodes() as isize;
    if n - 2 < k {
      k = 0;
    }

    // Compute the timeouts based on the size of the cluster.
    let min = suspicion_timeout(
      self.inner.opts.suspicion_mult,
      n as usize,
      self.inner.opts.probe_interval,
    );
    let max = min * (self.inner.opts.suspicion_max_timeout_mult as u32);

    let this = self.clone();
    state.suspicion = Some(Suspicion::new(
      sfrom,
      k,
      min,
      max,
      Suspicioner::new(
        this,
        snode,
        change_time,
        sincarnation,
        #[cfg(feature = "metrics")]
        k,
      ),
    ));
    Ok(())
  }

  /// Invoked by the network layer when we get a message about a
  /// live node.
  pub(crate) async fn alive_node(
    &self,
    alive: Alive<T::Id, T::ResolvedAddress>,
    notify_tx: Option<async_channel::Sender<()>>,
    bootstrap: bool,
  ) {
    let mut memberlist = self.inner.nodes.write().await;

    // It is possible that during a Leave(), there is already an aliveMsg
    // in-queue to be processed but blocked by the locks above. If we let
    // that aliveMsg process, it'll cause us to re-join the cluster. This
    // ensures that we don't.
    if self.has_left() && alive.node().id().eq(&self.inner.id) {
      return;
    }

    let anode = alive.node();
    let server = Arc::new(
      NodeState::new(
        anode.id().cheap_clone(),
        anode.address().cheap_clone(),
        State::Alive,
      )
      .with_meta(alive.meta().cheap_clone())
      .with_protocol_version(alive.protocol_version())
      .with_delegate_version(alive.delegate_version()),
    );
    // Invoke the Alive delegate if any. This can be used to filter out
    // alive messages based on custom logic. For example, using a cluster name.
    // Using a merge delegate is not enough, as it is possible for passive
    // cluster merging to still occur.
    if let Some(delegate) = &self.delegate {
      if let Err(e) = delegate.notify_alive(server.clone()).await {
        tracing::warn!(local = %self.inner.id, peer = %anode, err=%e, "memberlist.state: ignoring alive message");
        return;
      }
    }

    let mut updates_node = false;
    if let Some(idx) = memberlist.node_map.get(anode.id()) {
      // Check if this address is different than the existing node unless the old node is dead.
      let state = &memberlist.nodes[*idx].state;
      if state.address() != alive.node().address() {
        if let Err(err) = self.inner.transport.blocked_address(anode.address()) {
          tracing::warn!(local = %self.inner.id, remote = %anode, err=%err, "memberlist.state: rejected IP update from {} to {} for node {}", alive.node().id(), state.address(), anode.address());
          return;
        };

        // If DeadNodeStateReclaimTime is configured, check if enough time has elapsed since the node died.
        let can_reclaim = self.inner.opts.dead_node_reclaim_time > Duration::ZERO
          && state.state_change.elapsed() > self.inner.opts.dead_node_reclaim_time;

        // Allow the address to be updated if a dead node is being replaced.
        if state.state == State::Left || (state.state == State::Dead && can_reclaim) {
          tracing::info!(local = %self.inner.id, "memberlist.state: updating address for left or failed node {} from {} to {}", state.id(), state.address(), alive.node().address());
          updates_node = true;
        } else {
          tracing::error!(local = %self.inner.id, "memberlist.state: conflicting address for {}(mine: {}, theirs: {}, old state: {})", state.id(), state.address(), alive.node(), state.state);

          // Inform the conflict delegate if provided
          if let Some(delegate) = self.delegate.as_ref() {
            delegate
              .notify_conflict(state.server.cheap_clone(), Arc::new(NodeState::from(alive)))
              .await;
          }
          return;
        }
      }
    } else {
      if let Err(err) = self.inner.transport.blocked_address(anode.address()) {
        tracing::warn!(local = %self.inner.id, remote = %anode, err=%err, "memberlist.state: rejected node");
        return;
      };

      let state = LocalNodeState {
        server,
        incarnation: Arc::new(AtomicU32::new(0)),
        state_change: Epoch::now(),
        state: State::Dead,
      };

      // Get a random offset. This is important to ensure
      // the failure detection bound is low on average. If all
      // nodes did an append, failure detection bound would be
      // very high.
      let n = memberlist.nodes.len();
      let offset = random_offset(n);

      // Add at the end and swap with the node at the offset
      memberlist.nodes.push(Member {
        state: state.clone(),
        suspicion: None,
      });
      memberlist.nodes.swap(n, offset);

      // Add to map
      memberlist.node_map.insert(anode.id().cheap_clone(), offset);
      let id = memberlist.nodes[n].state.id().clone();
      *memberlist.node_map.get_mut(&id).unwrap() = n;

      // Update numNodeStates after we've added a new node
      self.inner.hot.num_nodes.fetch_add(1, Ordering::Relaxed);
    }

    let idx = memberlist.node_map.get(anode.id()).copied().unwrap();
    let member = &mut memberlist.nodes[idx];
    let local_incarnation = member.state.incarnation.load(Ordering::Relaxed);
    // Bail if the incarnation number is older, and this is not about us
    let is_local_node = anode.id().eq(&self.inner.id);
    if !updates_node && !is_local_node && alive.incarnation() <= local_incarnation {
      return;
    }
    // Bail if strictly less and this is about us
    if is_local_node && alive.incarnation() < local_incarnation {
      return;
    }

    // Clear out any suspicion timer that may be in effect.
    member.suspicion = None;

    // Store the old state and meta data
    let old_state = member.state.state;
    let old_meta = member.meta().cheap_clone();

    // If this is us we need to refute, otherwise re-broadcast
    if !bootstrap && is_local_node {
      let (pv, dv) = (member.protocol_version(), member.delegate_version());

      // If the Incarnation is the same, we need special handling, since it
      // possible for the following situation to happen:
      // 1) Start with configuration C, join cluster
      // 2) Hard fail / Kill / Shutdown
      // 3) Restart with configuration C', join cluster
      //
      // In this case, other nodes and the local node see the same incarnation,
      // but the values may not be the same. For this reason, we always
      // need to do an equality check for this Incarnation. In most cases,
      // we just ignore, but we may need to refute.
      //
      if alive.incarnation() == local_incarnation
        && alive.meta() == member.meta()
        && alive.protocol_version() == pv
        && alive.delegate_version() == dv
      {
        return;
      }
      self.refute(&member.state, alive.incarnation()).await;
      tracing::warn!(local = %self.inner.id, peer = %alive.node(), local_meta = ?member.meta(), remote_meta = ?alive.meta(), "memberlist.state: refuting an alive message");
    } else {
      self
        .broadcast_notify(
          alive.node().id().cheap_clone(),
          alive.cheap_clone().into(),
          notify_tx,
        )
        .await;

      // Update the state and incarnation number
      member
        .state
        .incarnation
        .store(alive.incarnation(), Ordering::Release);
      member.state.server = Arc::new(NodeState::from(alive));
      if member.state.state != State::Alive {
        member.state.state = State::Alive;
        member.state.state_change = Epoch::now();
      }
    }

    // Update metrics
    #[cfg(feature = "metrics")]
    {
      metrics::counter!("memberlist.msg.alive", self.inner.opts.metric_labels.iter()).increment(1);
    }

    // Notify the delegate of any relevant updates
    if let Some(delegate) = &self.delegate {
      if old_state == State::Dead || old_state == State::Left {
        // if Dead/Left -> Alive, notify of join
        delegate
          .notify_join(member.state.server.cheap_clone())
          .await
      } else if old_meta.ne(member.state.meta()) {
        // if Meta changed, trigger an update notification
        delegate
          .notify_update(member.state.server.cheap_clone())
          .await
      }
    }
  }

  pub(crate) async fn merge_state<'a>(
    &'a self,
    remote: &'a [PushNodeState<T::Id, T::ResolvedAddress>],
  ) {
    for r in remote {
      let state = match r.state() {
        State::Alive => StateMessage::Alive(
          Alive::new(r.incarnation(), r.node())
            .with_meta(r.meta().cheap_clone())
            .with_protocol_version(r.protocol_version())
            .with_delegate_version(r.delegate_version()),
        ),
        State::Left => StateMessage::Left(Dead::new(
          r.incarnation(),
          r.id().cheap_clone(),
          self.local_id().cheap_clone(),
        )),
        // If the remote node believes a node is dead, we prefer to
        // suspect that node instead of declaring it dead instantly
        State::Dead | State::Suspect => StateMessage::Suspect(Suspect::new(
          r.incarnation(),
          r.id().cheap_clone(),
          self.local_id().cheap_clone(),
        )),
        State::Unknown(val) => {
          tracing::warn!(local = %self.inner.id, remote = %r.id(), state=%val, "memberlist.state: unknown state");
          continue;
        }
        state => {
          tracing::warn!(local = %self.inner.id, remote = %r.id(), state=%state, "memberlist.state: unknown state");
          continue;
        }
      };

      state.run(self).await;
    }
  }
}

enum StateMessage<T: Transport> {
  Alive(Alive<T::Id, T::ResolvedAddress>),
  Left(Dead<T::Id>),
  Suspect(Suspect<T::Id>),
}

impl<T> StateMessage<T>
where
  T: Transport,
{
  async fn run<D>(self, s: &Memberlist<T, D>)
  where
    D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  {
    match self {
      StateMessage::Alive(alive) => s.alive_node(alive, None, false).await,
      StateMessage::Left(dead) => {
        let id = dead.node().cheap_clone();
        let mut memberlist = s.inner.nodes.write().await;
        if let Err(e) = s.dead_node(&mut memberlist, dead).await {
          tracing::error!(id=%id, err=%e, "memberlist.state: fail to dead node");
        }
      }
      StateMessage::Suspect(suspect) => {
        let id = suspect.node().cheap_clone();
        if let Err(e) = s.suspect_node(suspect).await {
          tracing::error!(id=%id, err=%e, "memberlist.state: fail to suspect node");
        }
      }
    }
  }
}

// -------------------------------Private Methods--------------------------------

#[inline]
fn move_dead_nodes<T, D>(nodes: &mut [Member<T, D>], gossip_to_the_dead_time: Duration) -> usize
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  let mut num_dead = 0;

  let n = nodes.len();
  let mut i = 0;

  while i < n - num_dead {
    let node = &nodes[i];
    if !node.state.dead_or_left() {
      i += 1;
      continue;
    }

    // Respect the gossip to the dead interval
    if node.state.state_change.elapsed() <= gossip_to_the_dead_time {
      i += 1;
      continue;
    }

    // Move this node to the end
    nodes.swap(i, n - num_dead - 1);
    num_dead += 1;
  }

  n - num_dead
}

macro_rules! bail_trigger {
  ($fn:ident) => {
    paste::paste! {
      async fn [<trigger _ $fn>](&self, stagger: Duration, interval: Duration, stop_rx: async_channel::Receiver<()>) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()>
      {
        let this = self.clone();
        // Use a random stagger to avoid syncronizing
        let rand_stagger = random_stagger(stagger);
        <T::Runtime as RuntimeLite>::spawn(async move {
          let delay = <T::Runtime as RuntimeLite>::sleep(rand_stagger);

          futures::select! {
            _ = delay.fuse() => {},
            _ = stop_rx.recv().fuse() => {
              tracing::debug!(concat!("memberlist.state: ", stringify!($fn), " trigger exits"));
              return;
            },
          }

          let mut timer = <T::Runtime as RuntimeLite>::interval(interval);
          'outer: loop {
            futures::select_biased! {
              _ = stop_rx.recv().fuse() => {
                break 'outer;
              }
              _ = futures::StreamExt::next(&mut timer).fuse() => {
                if this.inner.shutdown_tx.is_closed() {
                  break 'outer;
                }
                this.$fn(&stop_rx).await;

                match stop_rx.try_recv() {
                  Ok(_) => return,
                  Err(async_channel::TryRecvError::Empty) => {}
                  Err(async_channel::TryRecvError::Closed) => return,
                }
              }
            }
          }

          tracing::debug!(concat!("memberlist.state: ", stringify!($fn), " trigger exits"));
        })
      }
    }
  };
}

impl<T, D> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// Used to ensure the Tick is performed periodically.
  pub(crate) async fn schedule(&self, shutdown_rx: async_channel::Receiver<()>) {
    let handles = self.inner.handles.borrow();
    // Create a new probeTicker
    if self.inner.opts.probe_interval > Duration::ZERO {
      handles.push(
        self
          .trigger_probe(
            self.inner.opts.probe_interval,
            self.inner.opts.probe_interval,
            shutdown_rx.clone(),
          )
          .await,
      );
    }

    // Create a push pull ticker if needed
    if self.inner.opts.push_pull_interval > Duration::ZERO {
      handles.push(self.trigger_push_pull(shutdown_rx.clone()).await);
    }

    // Create a gossip ticker if needed
    if self.inner.opts.gossip_interval > Duration::ZERO && self.inner.opts.gossip_nodes > 0 {
      handles.push(
        self
          .trigger_gossip(
            self.inner.opts.gossip_interval,
            self.inner.opts.gossip_interval,
            shutdown_rx.clone(),
          )
          .await,
      );
    }
  }

  bail_trigger!(probe);

  bail_trigger!(gossip);

  async fn trigger_push_pull(
    &self,
    stop_rx: async_channel::Receiver<()>,
  ) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()> {
    let interval = self.inner.opts.push_pull_interval;
    let this = self.clone();
    // Use a random stagger to avoid syncronizing
    let mut rng = rand::rng();

    let rand_stagger = Duration::from_millis(rng.random_range(0..interval.as_millis() as u64));

    <T::Runtime as RuntimeLite>::spawn(async move {
      futures::select! {
        _ = <T::Runtime as RuntimeLite>::sleep(rand_stagger).fuse() => {},
        _ = stop_rx.recv().fuse() => {
          tracing::debug!("memberlist.state: push pull trigger exits");
          return;
        },
      }

      // Tick using a dynamic timer
      loop {
        let tick_time = push_pull_scale(interval, this.estimate_num_nodes() as usize);
        let mut timer = <T::Runtime as RuntimeLite>::interval(tick_time);
        futures::select! {
          _ = futures::StreamExt::next(&mut timer).fuse() => {
            this.push_pull().await;
            match stop_rx.try_recv() {
              Ok(_) => return,
              Err(async_channel::TryRecvError::Empty) => {}
              Err(async_channel::TryRecvError::Closed) => return,
            }
          }
          _ = stop_rx.recv().fuse() => {
            tracing::debug!("memberlist.state: push pull trigger exits");
            return;
          },
        }
      }
    })
  }

  // Used to perform a single round of failure detection and gossip
  async fn probe(&self, shutdown_rx: &async_channel::Receiver<()>) -> bool {
    // Track the number of indexes we've considered probing
    let mut num_check = 0;
    loop {
      match shutdown_rx.try_recv() {
        Ok(_) => return true,
        Err(async_channel::TryRecvError::Empty) => {}
        Err(async_channel::TryRecvError::Closed) => return true,
      }

      let memberlist = self.inner.nodes.read().await;
      let num_nodes = memberlist.nodes.len();
      // Make sure we don't wrap around infinitely
      if num_check >= num_nodes {
        return false;
      }

      // Handle the wrap around case
      let probe_index = self.inner.probe_index.load(Ordering::Acquire);
      if probe_index >= num_nodes {
        drop(memberlist);
        self.reset_nodes().await;
        self.inner.probe_index.store(0, Ordering::Release);
        num_check += 1;
        continue;
      }

      // Determine if we should probe this node
      let mut skip = false;
      let node = memberlist.nodes[probe_index].state.clone();
      if node.dead_or_left() || node.id() == self.local_id() {
        skip = true;
      }

      // Potentially skip
      drop(memberlist);
      self.inner.probe_index.fetch_add(1, Ordering::AcqRel);
      if skip {
        num_check += 1;
        continue;
      }

      // Probe the specific node
      self.probe_node(&node).await;
      return false;
    }
  }

  async fn probe_node(&self, target: &LocalNodeState<T::Id, T::ResolvedAddress>) {
    #[cfg(feature = "metrics")]
    let now = <T::Runtime as RuntimeLite>::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer! {
      metrics::histogram!("memberlist.probe_node", self.inner.opts.metric_labels.iter()).record(now.elapsed().as_millis() as f64);
    }

    // We use our health awareness to scale the overall probe interval, so we
    // slow down if we detect problems. The ticker that calls us can handle
    // us running over the base interval, and will skip missed ticks.
    let probe_interval = self
      .inner
      .awareness
      .scale_timeout(self.inner.opts.probe_interval);

    #[cfg(feature = "metrics")]
    {
      if probe_interval > self.inner.opts.probe_interval {
        metrics::counter!(
          "memberlist.degraded.probe",
          self.inner.opts.metric_labels.iter()
        )
        .increment(1);
      }
    }

    // Prepare a ping message and setup an ack handler.
    let ping = Ping::new(
      self.next_sequence_number(),
      self.advertise_node(),
      target.node(),
    );

    let (ack_tx, ack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);
    let (nack_tx, nack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);

    // Mark the sent time here, which should be after any pre-processing but
    // before system calls to do the actual send. This probably over-reports
    // a bit, but it's the best we can do. We had originally put this right
    // after the I/O, but that would sometimes give negative RTT measurements
    // which was not desirable.
    let sent = <T::Runtime as RuntimeLite>::now();
    // Send a ping to the node. If this node looks like it's suspect or dead,
    // also tack on a suspect message so that it has a chance to refute as
    // soon as possible.
    let deadline = sent + probe_interval;
    self.inner.ack_manager.set_probe_channels(
      ping.sequence_number(),
      ack_tx.clone(),
      Some(nack_tx),
      sent,
      probe_interval,
    );

    let awareness_delta = AtomicIsize::new(0);
    scopeguard::defer!(
      self
        .inner
        .awareness
        .apply_delta(awareness_delta.load(Ordering::Acquire))
    );

    if target.state == State::Alive {
      let stream = self
        .send_packets(target.address(), ping.cheap_clone().into())
        .await;
      let errs = stream.collect::<OneOrMore<_>>().await;
      if !errs.is_empty() {
        let e = match errs.into_either() {
          either::Either::Left([e]) => e,
          either::Either::Right(e) => Error::Multiple(e.into_vec().into()),
        };

        tracing::error!(local = %self.inner.id, remote = %target.id(), err=%e, "memberlist.state: failed to send ping by unreliable connection");
        if e.is_remote_failure() {
          return self
            .handle_remote_failure(
              target,
              ping.sequence_number(),
              &ack_rx,
              &nack_rx,
              deadline,
              &awareness_delta,
            )
            .await;
        }

        return;
      }
    } else {
      let suspect = Suspect::new(
        target.incarnation.load(Ordering::SeqCst),
        target.id().cheap_clone(),
        self.local_id().cheap_clone(),
      );
      let stream = self
        .transport_send_packets(
          target.address(),
          [ping.cheap_clone().into(), suspect.into()],
        )
        .await;
      futures::pin_mut!(stream);

      while let Some(res) = stream.next().await {
        if let Err(e) = res {
          tracing::error!(local = %self.inner.id, remote = %target.id(), err=%e, "memberlist.state: failed to send compound ping and suspect message by unreliable connection");
          if e.is_remote_failure() {
            return self
              .handle_remote_failure(
                target,
                ping.sequence_number(),
                &ack_rx,
                &nack_rx,
                deadline,
                &awareness_delta,
              )
              .await;
          }

          return;
        }
      }
    }

    // Arrange for our self-awareness to get updated. At this point we've
    // sent the ping, so any return statement means the probe succeeded
    // which will improve our health until we get to the failure scenarios
    // at the end of this function, which will alter this delta variable
    // accordingly.
    awareness_delta.store(-1, Ordering::Release);

    let delegate = self.delegate.as_ref();

    // Wait for response or round-trip-time.
    let fut1 = async {
      match ack_rx.recv().await {
        Ok(v) => {
          if v.complete {
            if let Some(delegate) = delegate {
              let rtt = v.timestamp.elapsed();
              tracing::trace!(local = %self.inner.id, remote = %target.id(), "memberlist.state: notify ping complete ack");
              delegate
                .notify_ping_complete(target.server.cheap_clone(), rtt, v.payload)
                .await;
            }

            return ControlFlow::Break(());
          }

          // As an edge case, if we get a timeout, we need to re-enqueue it
          // here to break out of the select below.
          if !v.complete {
            if let Err(e) = ack_tx.send(v).await {
              tracing::error!(local = %self.inner.id, remote = %target.id(), err=%e, "memberlist.state: failed to re-enqueue UDP ping ack");
            }
          }
          ControlFlow::Continue(())
        }
        Err(e) => {
          // This branch should never be reached, if there's an error in your log, please report an issue.
          tracing::debug!(local = %self.inner.id, remote = %target.id(), err = %e, "memberlist.state: failed unreliable connection ping (ack channel closed)");
          ControlFlow::Continue(())
        }
      }
    };

    let fut2 = async {
      <T::Runtime as RuntimeLite>::sleep(self.inner.opts.probe_timeout).await;
      // Note that we don't scale this timeout based on awareness and
      // the health score. That's because we don't really expect waiting
      // longer to help get UDP through. Since health does extend the
      // probe interval it will give the TCP fallback more time, which
      // is more active in dealing with lost packets, and it gives more
      // time to wait for indirect acks/nacks.
      tracing::debug!(local = %self.inner.id, remote = %target.id(), "memberlist.state: failed unreliable connection ping (timeout reached)");
    };

    futures::pin_mut!(fut1, fut2);

    if let futures::future::Either::Left((ControlFlow::Break(_), _)) =
      futures::future::select(fut1, fut2).await
    {
      return;
    }

    self
      .handle_remote_failure(
        target,
        ping.sequence_number(),
        &ack_rx,
        &nack_rx,
        deadline,
        &awareness_delta,
      )
      .await
  }

  async fn handle_remote_failure(
    &self,
    target: &LocalNodeState<T::Id, T::ResolvedAddress>,
    ping_sequence_number: u32,
    ack_rx: &async_channel::Receiver<AckMessage<<T::Runtime as RuntimeLite>::Instant>>,
    nack_rx: &async_channel::Receiver<()>,
    deadline: <T::Runtime as RuntimeLite>::Instant,
    awareness_delta: &AtomicIsize,
  ) {
    // Get some random live nodes.
    let nodes = {
      let nodes = self
        .inner
        .nodes
        .read()
        .await
        .nodes
        .iter()
        .filter_map(|m| {
          if m.id() == &self.inner.id || m.id() == target.id() || m.state.state != State::Alive {
            None
          } else {
            Some(m.state.server.cheap_clone())
          }
        })
        .collect::<SmallVec<_>>();
      random_nodes(self.inner.opts.indirect_checks, nodes)
    };

    // Attempt an indirect ping.
    let expected_nacks = nodes.len() as isize;
    let ind = IndirectPing::new(ping_sequence_number, self.advertise_node(), target.node());

    let mut futs = nodes.into_iter().map(|peer| {
      let ind = ind.cheap_clone();
      async move {
        let stream = self
          .send_packets(peer.address(), ind.into())
          .await;
        futures::pin_mut!(stream);
        while let Some(e) = stream.next().await {
          tracing::error!(local = %self.inner.id, remote = %peer, err=%e, "memberlist.state: failed to send indirect unreliable ping");
        }
      }
    }).collect::<futures::stream::FuturesUnordered<_>>();

    while futs.next().await.is_some() {}

    // Also make an attempt to contact the node directly over TCP. This
    // helps prevent confused clients who get isolated from UDP traffic
    // but can still speak TCP (which also means they can possibly report
    // misinformation to other nodes via anti-entropy), avoiding flapping in
    // the cluster.
    //
    // This is a little unusual because we will attempt a TCP ping to any
    // member who understands version 3 of the protocol, regardless of
    // which protocol version we are speaking. That's why we've included a
    // config option to turn this off if desired.
    let (fallback_tx, fallback_rx) = async_channel::bounded(1);

    let mut disable_reliable_pings = self.inner.opts.disable_reliable_pings;
    if let Some(delegate) = self.delegate.as_ref() {
      disable_reliable_pings |= delegate.disable_reliable_pings(target.id());
    }
    if !disable_reliable_pings {
      let target_addr = target.address().cheap_clone();
      let this = self.clone();
      <T::Runtime as RuntimeLite>::spawn_detach(async move {
        scopeguard::defer!(fallback_tx.close(););
        match this
          .send_ping_and_wait_for_ack(&target_addr, ind.into(), deadline)
          .await
        {
          Ok(did_contact) => {
            // The error should never happen, because we do not drop the rx,
            // handle error here for good manner, and if you see this log, please
            // report an issue.
            if let Err(e) = fallback_tx.send(did_contact).await {
              tracing::error!(local = %this.inner.id, remote_addr = %target_addr, err=%e, "memberlist.state: failed to send fallback");
            }
          }
          Err(e) => {
            tracing::error!(local = %this.inner.id, remote_addr = %target_addr, err=%e, "memberlist.state: failed to send ping by reliable connection");
            // The error should never happen, because we do not drop the rx,
            // handle error here for good manner, and if you see this log, please
            // report an issue.
            if let Err(e) = fallback_tx.send(false).await {
              tracing::error!(local = %this.inner.id, remote_addr = %target_addr, err=%e, "memberlist.state: failed to send fallback");
            }
          }
        }
      });
    }

    // Wait for the acks or timeout. Note that we don't check the fallback
    // channel here because we want to issue a warning below if that's the
    // *only* way we hear back from the peer, so we have to let this time
    // out first to allow the normal unreliable-connection-based acks to come in.
    futures::select! {
      v = ack_rx.recv().fuse() => {
        if let Ok(v) = v {
          if v.complete {
            return;
          }
        }
      }
    }

    // Finally, poll the fallback channel. The timeouts are set such that
    // the channel will have something or be closed without having to wait
    // any additional time here.
    if !disable_reliable_pings {
      futures::pin_mut!(fallback_rx);
      while let Some(did_contact) = fallback_rx.next().await {
        if did_contact {
          tracing::warn!(local = %self.inner.id, remote = %target.id(), "memberlist.state: was able to connect to target over reliable connection but unreliable probes failed, network may be misconfigured");
          return;
        }
      }
    }

    // Update our self-awareness based on the results of this failed probe.
    // If we don't have peers who will send nacks then we penalize for any
    // failed probe as a simple health metric. If we do have peers to nack
    // verify, then we can use that as a more sophisticated measure of self-
    // health because we assume them to be working, and they can help us
    // decide if the probed node was really dead or if it was something wrong
    // with ourselves.
    awareness_delta.store(0, Ordering::Release);
    if expected_nacks > 0 {
      let nack_count = nack_rx.len() as isize;
      if nack_count < expected_nacks {
        awareness_delta.fetch_add(expected_nacks - nack_count, Ordering::AcqRel);
      }
    } else {
      awareness_delta.fetch_add(1, Ordering::AcqRel);
    }

    // No acks received from target, suspect it as failed.
    tracing::info!(local = %self.inner.id, remote = %target.id(), "memberlist.state: suspecting has failed, no acks received");
    let s = Suspect::new(
      target.incarnation.load(Ordering::SeqCst),
      target.id().cheap_clone(),
      self.local_id().cheap_clone(),
    );
    if let Err(e) = self.suspect_node(s).await {
      tracing::error!(local = %self.inner.id, remote = %target.id(), err=%e, "memberlist.state: failed to suspect node");
    }
  }

  /// Used when the tick wraps around. It will reap the
  /// dead nodes and shuffle the node list.
  async fn reset_nodes(&self) {
    let mut memberlist = self.inner.nodes.write().await;

    // Move dead nodes, but respect gossip to the dead interval
    let dead_idx = move_dead_nodes(
      &mut memberlist.nodes,
      self.inner.opts.gossip_to_the_dead_time,
    );

    // Trim the nodes to exclude the dead nodes and deregister the dead nodes
    let mut i = 0;
    let num_remove = memberlist.node_map.len() - dead_idx;
    while i < num_remove {
      let node = memberlist.nodes.pop().unwrap();
      memberlist.node_map.remove(node.state.id());
      i += 1;
    }

    // Update num_nodes after we've trimmed the dead nodes
    self
      .inner
      .hot
      .num_nodes
      .store(dead_idx as u32, Ordering::Release);

    // Shuffle live nodes
    memberlist.shuffle(&mut rand::rng());
  }

  /// Invoked every GossipInterval period to broadcast our gossip
  /// messages to a few random nodes.
  async fn gossip(&self, shutdown_rx: &async_channel::Receiver<()>) -> bool {
    let fut1 = shutdown_rx.recv();
    let fut2 = async {
      #[cfg(feature = "metrics")]
      let now = <T::Runtime as RuntimeLite>::now();
      #[cfg(feature = "metrics")]
      scopeguard::defer!(
        metrics::histogram!("memberlist.gossip", self.inner.opts.metric_labels.iter()).record(now.elapsed().as_millis() as f64);
      );

      // Get some random live, suspect, or recently dead nodes
      let nodes = {
        let nodes = self
          .inner
          .nodes
          .read()
          .await
          .nodes
          .iter()
          .filter_map(|m| {
            if m.state.id() == &self.inner.id {
              return None;
            }

            match m.state.state {
              State::Alive | State::Suspect => Some(m.state.server.clone()),
              State::Dead => {
                if m.state.state_change.elapsed() > self.inner.opts.gossip_to_the_dead_time {
                  None
                } else {
                  Some(m.state.server.clone())
                }
              }
              _ => None,
            }
          })
          .collect::<SmallVec<_>>();
        random_nodes(self.inner.opts.gossip_nodes, nodes)
      };

      // Compute the bytes available
      let bytes_avail =
        self.inner.transport.max_packet_size() - self.inner.transport.header_overhead();
      let mut futs = nodes.into_iter().map(|server| async move {
          // Get any pending broadcasts
          let msgs = match self
            .get_broadcast_with_prepend(
              Default::default(),
              bytes_avail,
            )
            .await
          {
            Ok(msgs) => msgs,
            Err(e) => {
              tracing::error!(err = %e, "memberlist.state: failed to get broadcast messages from {}", server);
              return;
            }
          };
          if msgs.is_empty() {
            return;
          }

          let addr = server.address();
          let stream = self.transport_send_packets(addr, msgs).await;
          futures::pin_mut!(stream);

          while let Some(res) = stream.next().await {
            if let Err(e) = res {
              tracing::error!(err = %e, "memberlist.state: failed to send gossip to {}", addr);
            }
          }
        }).collect::<FuturesUnordered<_>>();

      while futs.next().await.is_some() {}
      false
    };

    futures::pin_mut!(fut1, fut2);

    match futures::future::select(fut1, fut2).await {
      futures::future::Either::Left((_, _)) => true,
      futures::future::Either::Right((v, _)) => v,
    }
  }

  /// invoked periodically to randomly perform a complete state
  /// exchange. Used to ensure a high level of convergence, but is also
  /// reasonably expensive as the entire state of this node is exchanged
  /// with the other node.
  async fn push_pull(&self) {
    // get a random live node
    let nodes = {
      let nodes = self
        .inner
        .nodes
        .read()
        .await
        .nodes
        .iter()
        .filter_map(|n| {
          if n.state.id() == &self.inner.id || n.state.state != State::Alive {
            None
          } else {
            Some(n.state.server.clone())
          }
        })
        .collect::<SmallVec<_>>();
      random_nodes(1, nodes)
    };

    if nodes.is_empty() {
      return;
    }

    let server = &nodes[0];
    // Attempt a push pull
    if let Err(e) = self.push_pull_node(server.node(), false).await {
      tracing::error!(err = %e, "memberlist.state: push/pull with {} failed", server.id());
    }
  }

  /// Gossips an alive message in response to incoming information that we
  /// are suspect or dead. It will make sure the incarnation number beats the given
  /// accusedInc value, or you can supply 0 to just get the next incarnation number.
  /// This alters the node state that's passed in so this MUST be called while the
  /// nodeLock is held.
  async fn refute(&self, state: &LocalNodeState<T::Id, T::ResolvedAddress>, accused_inc: u32) {
    // Make sure the incarnation number beats the accusation.
    let mut inc = self.next_incarnation();
    if accused_inc >= inc {
      inc = self.skip_incarnation(accused_inc - inc + 1);
    }
    state.incarnation.store(inc, Ordering::Relaxed);

    // Decrease our health because we are being asked to refute a problem.
    self.inner.awareness.apply_delta(1);

    // Format and broadcast an alive message.
    let anode = Node::new(state.id().cheap_clone(), state.address().cheap_clone());
    let a = Alive::new(inc, anode)
      .with_meta(state.meta().cheap_clone())
      .with_protocol_version(state.protocol_version())
      .with_delegate_version(state.delegate_version());
    self.broadcast(a.node().id().cheap_clone(), a.into()).await;
  }
}

#[inline]
fn suspicion_timeout(suspicion_mult: usize, n: usize, interval: Duration) -> Duration {
  let node_scale = ((n as f64).max(1.0)).log10().max(1.0);
  let interval = interval * (suspicion_mult as u32);
  let interval = (interval.as_millis() as f64 * node_scale * 1000.0) as u64;
  Duration::from_millis(interval / 1000)
}

/// push_pull_scale is used to scale the time interval at which push/pull
/// syncs take place. It is used to prevent network saturation as the
/// cluster size grows
#[inline]
fn push_pull_scale(interval: Duration, n: usize) -> Duration {
  /// the minimum number of nodes
  /// before we start scaling the push/pull timing. The scale
  /// effect is the log2(NodeStates) - log2(pushPullScale). This means
  /// that the 33rd node will cause us to double the interval,
  /// while the 65th will triple it.
  const PUSH_PULL_SCALE_THRESHOLD: usize = 32;

  // Don't scale until we cross the threshold
  if n <= PUSH_PULL_SCALE_THRESHOLD {
    return interval;
  }

  let multiplier = (f64::log2(n as f64) - f64::log2(PUSH_PULL_SCALE_THRESHOLD as f64)).ceil() + 1.0;
  interval * multiplier as u32
}

#[inline]
fn random_offset(n: usize) -> usize {
  if n == 0 {
    return 0;
  }
  (rand::random::<u32>() % (n as u32)) as usize
}

#[inline]
fn random_nodes<I, A>(
  k: usize,
  mut nodes: SmallVec<Arc<NodeState<I, A>>>,
) -> SmallVec<Arc<NodeState<I, A>>> {
  let n = nodes.len();
  if n == 0 {
    return SmallVec::new();
  }

  // The modified Fisher-Yates algorithm, but up to 3*n times to ensure exhaustive search for small n.
  let rounds = 3 * n;
  let mut i = 0;

  while i < rounds && i < n {
    let j = (rand::random::<u64>() as usize) % (n - i) + i;
    nodes.swap(i, j);
    i += 1;
    if i >= k && i >= rounds {
      break;
    }
  }

  nodes.truncate(k);
  nodes
}

#[inline]
fn random_stagger(duration: Duration) -> Duration {
  let mut rng = rand::rng();
  Duration::from_nanos(rng.random_range(0..u64::MAX) % (duration.as_nanos() as u64))
}

#[test]
fn test_random_stagger() {
  let d = Duration::from_millis(1);
  let stagger = random_stagger(d);
  assert!(stagger <= d, "bad stagger");
}

#[test]
fn test_push_pull_scale() {
  let sec = Duration::from_secs(1);
  for i in 0..=32 {
    let s = push_pull_scale(sec, i);
    assert_eq!(s, sec, "Bad time scale {s:?}");
  }

  for i in 33..=64 {
    let s = push_pull_scale(sec, i);
    assert_eq!(s, sec * 2, "Bad time scale {s:?}");
  }

  for i in 65..=128 {
    let s = push_pull_scale(sec, i);
    assert_eq!(s, sec * 3, "Bad time scale {s:?}");
  }
}

#[test]
fn test_suspicion_timeout() {
  let timeouts: &[(usize, Duration)] = &[
    (5, Duration::from_millis(1000)),
    (10, Duration::from_millis(1000)),
    (50, Duration::from_secs_f64(1.698666666)),
    (100, Duration::from_millis(2000)),
    (500, Duration::from_secs_f64(2.698666666)),
    (1000, Duration::from_millis(3000)),
  ];

  for (n, expected) in timeouts {
    let actual = suspicion_timeout(3, *n, Duration::from_secs(1)) / 3;
    assert_eq!(actual, *expected);
  }
}

#[test]
fn test_random_offset() {
  let mut vals = std::collections::HashSet::new();
  for _ in 0..100 {
    let offset = random_offset(2 << 30);
    assert!(!vals.contains(&offset), "got collision");
    vals.insert(offset);
  }
}

#[test]
fn test_random_offset_zero() {
  let offset = random_offset(0);
  assert_eq!(offset, 0, "bad offset");
}
