use std::{collections::hash_map::Entry, net::SocketAddr, sync::atomic::Ordering, time::Duration};

use crate::{
  network::{COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD},
  security::encrypt_overhead,
  showbiz::{AckHandler, Member, Memberlist},
  suspicion::Suspicion,
  timer::Timer,
  types::{Alive, Dead, IndirectPing, Message, MessageType, Name, Ping, Suspect},
};

use super::*;
use agnostic::Runtime;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{future::BoxFuture, Future, FutureExt, Stream};
use rand::{seq::SliceRandom, Rng};

#[cfg(any(test, feature = "test"))]
pub(crate) mod tests;

#[inline]
fn random_offset(n: usize) -> usize {
  if n == 0 {
    return 0;
  }
  (rand::random::<u32>() % (n as u32)) as usize
}

#[inline]
fn random_nodes(k: usize, mut nodes: Vec<Arc<Node>>) -> Vec<Arc<Node>> {
  let n = nodes.len();
  if n == 0 {
    return Vec::new();
  }

  // The modified Fisher-Yates algorithm, but up to 3*n times to ensure exhaustive search for small n.
  let rounds = 3 * n;
  let mut i = 0;

  while i < rounds && i < n {
    let j = rand::random::<usize>() % (n - i) + i;
    nodes.swap(i, j);
    i += 1;
    if i >= k && i >= rounds {
      break;
    }
  }

  nodes.truncate(k);
  nodes
}

struct AckMessage {
  complete: bool,
  payload: Bytes,
  timestamp: Instant,
}

// -------------------------------Public methods---------------------------------

impl<D, T, R> Showbiz<D, T, R>
where
  T: Transport,
  D: Delegate,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  /// Initiates a ping to the node with the specified node.
  pub async fn ping(&self, node: NodeId) -> Result<Duration, Error<D, T>> {
    // Prepare a ping message and setup an ack handler.
    let self_addr = self.get_advertise();
    let ping = Ping {
      seq_no: self.next_seq_no(),
      source: NodeId {
        name: self.inner.opts.name.clone(),
        addr: self_addr,
      },
      target: node.clone(),
    };

    let (ack_tx, ack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);
    self
      .set_probe_channels(
        ping.seq_no,
        ack_tx,
        None,
        Instant::now(),
        self.inner.opts.probe_interval,
      )
      .await;

    // Send a ping to the node.
    let mut msg = BytesMut::with_capacity(ping.encoded_len() + MessageType::SIZE);
    msg.put_u8(MessageType::Ping as u8);
    ping.encode_to(&mut msg);
    self.send_msg(&node, Message(msg)).await?;
    // Mark the sent time here, which should be after any pre-processing and
    // system calls to do the actual send. This probably under-reports a bit,
    // but it's the best we can do.
    let sent = Instant::now();

    // Wait for response or timeout.
    futures_util::select! {
      v = ack_rx.recv().fuse() => {
        // If we got a response, update the RTT.
        if let Ok(AckMessage { complete, .. }) = v {
          if complete {
            return Ok(sent.elapsed());
          }
        }
      }
      _ = R::sleep(self.inner.opts.probe_timeout).fuse() => {}
    }

    // If we timed out, return Error.
    tracing::debug!(target: "showbiz", "failed UDP ping {} (timeout reached)", node);
    Err(Error::NoPingResponse(node))
  }
}

// ---------------------------------Crate methods-------------------------------
impl<D, T, R> Showbiz<D, T, R>
where
  T: Transport,
  D: Delegate,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  /// Does a complete state exchange with a specific node.
  pub(crate) async fn push_pull_node(
    &self,
    name: &Name,
    addr: SocketAddr,
    join: bool,
  ) -> Result<(), Error<D, T>> {
    #[cfg(feature = "metrics")]
    let now = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      observe_push_pull_node(now.elapsed().as_millis() as f64, self.inner.metrics_labels.iter());
    );

    self
      .merge_remote_state(self.send_and_receive_state(name, addr, join).await?)
      .await
  }

  pub(crate) async fn dead_node(
    &self,
    memberlist: &mut Memberlist<R>,
    d: Dead,
  ) -> Result<(), Error<D, T>> {
    let state = match memberlist.node_map.get_mut(&d.node.name) {
      Some(state) => state,
      // If we've never heard about this node before, ignore it
      None => return Ok(()),
    };

    // Ignore old incarnation numbers
    if d.incarnation < state.state.incarnation.load(Ordering::Relaxed) {
      return Ok(());
    }

    // Clear out any suspicion timer that may be in effect.
    state.suspicion = None;

    // Ignore if node is already dead
    if state.state.dead_or_left() {
      return Ok(());
    }

    // Check if this is us
    if d.dead_self() {
      // If we are not leaving we need to refute
      if !self.is_left() {
        // self.refute().await?;
        tracing::warn!(
          target = "showbiz",
          "refuting a dead message (from: {})",
          d.from
        );
        return Ok(()); // Do not mark ourself dead
      }

      // If we are leaving, we broadcast and wait
      let msg = d.encode_to_msg();
      self
        .broadcast_notify(
          d.node.clone(),
          msg,
          Some(self.inner.leave_broadcast_tx.clone()),
        )
        .await;
    } else {
      let msg = d.encode_to_msg();
      self.broadcast(d.node.clone(), msg).await;
    }

    #[cfg(feature = "metrics")]
    {
      incr_msg_dead(self.inner.metrics_labels.iter());
    }

    // Update the state
    state
      .state
      .incarnation
      .store(d.incarnation, Ordering::Relaxed);

    // If the dead message was send by the node itself, mark it is left
    // instead of dead.
    if d.dead_self() {
      state.state.state = NodeState::Left;
    } else {
      state.state.state = NodeState::Dead;
    }
    state.state.state_change = Instant::now();

    // notify of death
    if let Some(ref delegate) = self.inner.delegate {
      delegate
        .notify_leave(state.state.node.clone())
        .await
        .map_err(Error::delegate)?;
    }

    Ok(())
  }

  pub(crate) async fn suspect_node(&self, s: Suspect) -> Result<(), Error<D, T>> {
    let mut mu = self.inner.nodes.write().await;

    let Some(state) = mu.node_map.get_mut(&s.node.name) else {
      return Ok(());
    };

    // Ignore old incarnation numbers
    if s.incarnation < state.state.incarnation.load(Ordering::Relaxed) {
      return Ok(());
    }

    // See if there's a suspicion timer we can confirm. If the info is new
    // to us we will go ahead and re-gossip it. This allows for multiple
    // independent confirmations to flow even when a node probes a node
    // that's already suspect.
    if let Some(timer) = &mut state.suspicion {
      if timer.confirm(s.from.name()).await {
        let mut buf = BytesMut::with_capacity(s.encoded_len());
        s.encode_to(&mut buf);
        self.broadcast(s.node.clone(), Message(buf)).await;
      }
      return Ok(());
    }

    // Ignore non-alive nodes
    if state.state.state != NodeState::Alive {
      return Ok(());
    }

    // If this is us we need to refute, otherwise re-broadcast
    if state.state.node.name() == &self.inner.opts.name {
      self.refute(&state.state, s.incarnation).await;
      tracing::warn!(
        target = "showbiz",
        "refuting a suspect message (from: {})",
        s.from
      );
      // Do not mark ourself suspect
      return Ok(());
    } else {
      let mut buf = BytesMut::with_capacity(s.encoded_len());
      s.encode_to(&mut buf);
      self.broadcast(s.node.clone(), Message(buf)).await;
    }

    #[cfg(feature = "metrics")]
    {
      incr_msg_suspect(self.inner.metrics_labels.iter());
    }

    // Update the state
    state
      .state
      .incarnation
      .store(s.incarnation, Ordering::Relaxed);
    state.state.state = NodeState::Suspect;
    let change_time = Instant::now();
    state.state.state_change = change_time;

    // Setup a suspicion timer. Given that we don't have any known phase
    // relationship with our peers, we set up k such that we hit the nominal
    // timeout two probe intervals short of what we expect given the suspicion
    // multiplier.
    let mut k = self.inner.opts.suspicion_mult.saturating_sub(2);

    // If there aren't enough nodes to give the expected confirmations, just
    // set k to 0 to say that we don't expect any. Note we subtract 2 from n
    // here to take out ourselves and the node being probed.
    let n = self.estimate_num_nodes() as usize;
    if n - 2 < k {
      k = 0;
    }

    // Compute the timeouts based on the size of the cluster.
    let min = suspicion_timeout(
      self.inner.opts.suspicion_mult,
      n,
      self.inner.opts.probe_interval,
    );
    let max = (self.inner.opts.suspicion_max_timeout_mult as u64) * min;

    let this = self.clone();
    state.suspicion = Some(Suspicion::new(
      s.from.name().clone(),
      k as u32,
      Duration::from_millis(min),
      Duration::from_millis(max),
      move |num_confirmations| {
        let t = this.clone();
        let n = s.node.clone();
        async move {
          let timeout = {
            t.inner
              .nodes
              .read()
              .await
              .node_map
              .get(&n.name)
              .and_then(|state| {
                let timeout = state.state.state == NodeState::Suspect
                  && state.state.state_change == change_time;
                if timeout {
                  Some(Dead {
                    node: state.state.node.id.clone(),
                    from: t.inner.id.clone(),
                    incarnation: s.incarnation,
                  })
                } else {
                  None
                }
              })
          };

          if let Some(dead) = timeout {
            #[cfg(feature = "metrics")]
            {
              if k > 0 && k > num_confirmations as usize {
                incr_degraded_timeout(t.inner.metrics_labels.iter())
              }
            }

            tracing::info!(
              target = "showbiz",
              "marking {} as failed, suspect timeout reached ({} peer confirmations)",
              dead.node,
              num_confirmations
            );
            let mut memberlist = t.inner.nodes.write().await;
            let err_info = format!("failed to mark {} as failed", dead.node);
            if let Err(e) = t.dead_node(&mut memberlist, dead).await {
              tracing::error!(target = "showbiz", err=%e, err_info);
            }
          }
        }
        .boxed()
      },
    ));
    Ok(())
  }

  /// Invoked by the network layer when we get a message about a
  /// live node.
  pub(crate) async fn alive_node(
    &self,
    alive: Alive,
    notify_tx: Option<async_channel::Sender<()>>,
    bootstrap: bool,
  ) {
    let mut memberlist = self.inner.nodes.write().await;
    let member = memberlist.node_map.entry(alive.node.name.clone());

    // It is possible that during a Leave(), there is already an aliveMsg
    // in-queue to be processed but blocked by the locks above. If we let
    // that aliveMsg process, it'll cause us to re-join the cluster. This
    // ensures that we don't.
    if self.is_left() && alive.node == self.inner.id {
      return;
    }

    let vsn = alive.vsn;
    let node = Arc::new(Node {
      id: alive.node.clone(),
      meta: alive.meta.clone(),
      state: NodeState::Alive,
      vsn,
    });
    // Invoke the Alive delegate if any. This can be used to filter out
    // alive messages based on custom logic. For example, using a cluster name.
    // Using a merge delegate is not enough, as it is possible for passive
    // cluster merging to still occur.
    if let Some(delegate) = &self.inner.delegate {
      if let Err(e) = delegate.notify_alive(node.clone()).await {
        tracing::warn!(target = "showbiz", local = %self.inner.id, remote = %alive.node, err=%e, "ignoring alive message");
        return;
      }
    }

    let mut updates_node = false;
    match member {
      Entry::Occupied(member) => {
        // Check if this address is different than the existing node unless the old node is dead.
        let state = &member.get().state;
        if state.address() != alive.node.addr() {
          if let Err(err) = self.inner.opts.ip_allowed(alive.node.addr().ip()) {
            tracing::warn!(target = "showbiz", local = %self.inner.id, remote = %alive.node, err=%err, "rejected IP update from {} to {} for node {}", alive.node.name(), state.node.id().addr(), alive.node.addr());
            return;
          };

          // If DeadNodeReclaimTime is configured, check if enough time has elapsed since the node died.
          let can_reclaim = self.inner.opts.dead_node_reclaim_time > Duration::ZERO
            && state.state_change.elapsed() > self.inner.opts.dead_node_reclaim_time;

          // Allow the address to be updated if a dead node is being replaced.
          if state.state == NodeState::Left || (state.state == NodeState::Dead && can_reclaim) {
            tracing::info!(target = "showbiz", local = %self.inner.id, "updating address for left or failed node {} from {} to {}", state.node.name(), state.node.id().addr(), alive.node.addr());
            updates_node = true;
          } else {
            tracing::error!(target = "showbiz", local = %self.inner.id, "conflicting address for {}(mine: {}, theirs: {}, old state: {})", state.node.id().name(), state.node.id().addr(), alive.node, state.state);

            // Inform the conflict delegate if provided
            if let Some(delegate) = self.inner.delegate.as_ref() {
              if let Err(e) = delegate
                .notify_conflict(
                  state.node.clone(),
                  Arc::new(Node {
                    id: alive.node.clone(),
                    meta: alive.meta.clone(),
                    state: NodeState::Alive,
                    vsn: alive.vsn,
                  }),
                )
                .await
              {
                tracing::error!(target = "showbiz", local = %self.inner.id, err=%e, "failed to notify conflict delegate");
              }
            }
            return;
          }
        }
      }
      Entry::Vacant(ent) => {
        if let Err(err) = self.inner.opts.ip_allowed(alive.node.addr().ip()) {
          tracing::warn!(target = "showbiz", local = %self.inner.id, remote = %alive.node, err=%err, "rejected node");
          return;
        };

        let state = LocalNodeState {
          node,
          incarnation: Arc::new(AtomicU32::new(alive.incarnation)),
          state_change: Instant::now(),
          state: NodeState::Dead,
        };

        // Add to map
        ent.insert(Member {
          state: state.clone(),
          suspicion: None,
        });

        // Get a random offset. This is important to ensure
        // the failure detection bound is low on average. If all
        // nodes did an append, failure detection bound would be
        // very high.
        let n = memberlist.nodes.len();
        let offset = random_offset(n);

        // Add at the end and swap with the node at the offset
        memberlist.nodes.push(state);
        memberlist.nodes.swap(n, offset);

        // Update numNodes after we've added a new node
        self.inner.hot.num_nodes.fetch_add(1, Ordering::Relaxed);
      }
    }

    let member = memberlist.node_map.get_mut(&alive.node.name).unwrap();
    let local_incarnation = member.state.incarnation.load(Ordering::Relaxed);
    // Bail if the incarnation number is older, and this is not about us
    let is_local_node = alive.node.name == self.inner.id.name;
    if !updates_node && !is_local_node && alive.incarnation <= local_incarnation {
      return;
    }
    // Bail if strictly less and this is about us
    if is_local_node && alive.incarnation < local_incarnation {
      return;
    }

    // Clear out any suspicion timer that may be in effect.
    member.suspicion = None;

    // Store the old state and meta data
    let old_state = member.state.state;
    let old_meta = member.state.node.meta.clone();

    // If this is us we need to refute, otherwise re-broadcast
    if !bootstrap && is_local_node {
      let versions = member.state.node.vsn();

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
      if alive.incarnation == local_incarnation
        && alive.meta == member.state.node.meta
        && alive.vsn == versions
      {
        return;
      }
      self.refute(&member.state, alive.incarnation).await;
      tracing::warn!(target = "showbiz", local = %self.inner.id, remote = %alive.node, local_meta = ?member.state.node.meta.as_ref(), remote_meta = ?alive.meta.as_ref(), "refuting an alive message");
    } else {
      let mut buf = BytesMut::with_capacity(alive.encoded_len() + MessageType::SIZE);
      buf.put_u8(MessageType::Alive as u8);
      alive.encode_to(&mut buf);
      self
        .broadcast_notify(alive.node.clone(), Message(buf), notify_tx)
        .await;

      // Update the state and incarnation number
      if updates_node {
        member
          .state
          .incarnation
          .store(alive.incarnation, Ordering::Relaxed);
        member.state.node = Arc::new(Node {
          id: alive.node,
          meta: alive.meta,
          state: NodeState::Alive,
          vsn: alive.vsn,
        });
        if member.state.state != NodeState::Alive {
          member.state.state = NodeState::Alive;
          member.state.state_change = Instant::now();
        }
      }
    }

    // Update metrics
    #[cfg(feature = "metrics")]
    incr_msg_alive(self.inner.metrics_labels.iter());

    // Notify the delegate of any relevant updates
    if let Some(delegate) = &self.inner.delegate {
      if old_state == NodeState::Dead || old_state == NodeState::Left {
        // if Dead/Left -> Alive, notify of join
        if let Err(e) = delegate.notify_join(member.state.node.clone()).await {
          tracing::error!(target = "showbiz", local = %self.inner.id, err=%e, "failed to notify join");
        }
      } else if old_meta != member.state.node.meta {
        // if Meta changed, trigger an update notification
        if let Err(e) = delegate.notify_update(member.state.node.clone()).await {
          tracing::error!(target = "showbiz", local = %self.inner.id, err=%e, "failed to notify update");
        }
      }
    }
  }

  pub(crate) async fn merge_state(&self, remote: Vec<PushNodeState>) -> Result<(), Error<D, T>> {
    for r in remote {
      match r.state {
        NodeState::Alive => {
          let alive = Alive {
            incarnation: r.incarnation,
            node: r.node,
            vsn: r.vsn,
            meta: r.meta,
          };
          self.alive_node(alive, None, false).await;
        }
        NodeState::Left => {
          let dead = Dead {
            incarnation: r.incarnation,
            node: r.node,
            from: self.inner.id.clone(),
          };
          let mut memberlist = self.inner.nodes.write().await;
          self.dead_node(&mut memberlist, dead).await?;
        }
        // If the remote node believes a node is dead, we prefer to
        // suspect that node instead of declaring it dead instantly
        NodeState::Dead | NodeState::Suspect => {
          let s = Suspect {
            incarnation: r.incarnation,
            node: r.node,
            from: self.inner.id.clone(),
          };
          self.suspect_node(s).await?;
        }
      }
    }
    Ok(())
  }

  pub(crate) async fn set_ack_handler<F>(&self, seq_no: u32, timeout: Duration, f: F)
  where
    F: FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static,
  {
    // Add the handler
    let tlock = self.inner.ack_handlers.clone();
    let mut mu = self.inner.ack_handlers.lock().await;
    mu.insert(
      seq_no,
      AckHandler {
        ack_fn: Box::new(f),
        nack_fn: None,
        timer: Timer::after::<_, R>(timeout, async move {
          tlock.lock().await.remove(&seq_no);
        }),
      },
    );
  }
}

// -------------------------------Private Methods--------------------------------

#[inline]
fn move_dead_nodes(nodes: &mut Vec<LocalNodeState>, gossip_to_the_dead_time: Duration) -> usize {
  let mut num_dead = 0;

  let n = nodes.len();
  let mut i = 0;

  while i < n - num_dead {
    let node = &nodes[i];
    if !node.dead_or_left() {
      i += 1;
      continue;
    }

    // Respect the gossip to the dead interval
    if node.state_change.elapsed() <= gossip_to_the_dead_time {
      i += 1;
      continue;
    }

    // Move this node to the end
    nodes.swap(i, n - num_dead - 1);
    num_dead += 1;
  }

  n - num_dead
}

macro_rules! apply_delta {
  ($this:ident <= $delta:expr) => {
    $this.inner.awareness.apply_delta($delta).await;
  };
}

macro_rules! bail_trigger {
  ($fn:ident) => {
    paste::paste! {
      async fn [<trigger _ $fn>](&self, stagger: Duration, interval: Duration, stop_rx: async_channel::Receiver<()>)
      {
        let this = self.clone();
        // Use a random stagger to avoid syncronizing
        let mut rng = rand::thread_rng();
        let rand_stagger = Duration::from_millis(rng.gen_range(0..stagger.as_millis() as u64));

        R::spawn_detach(async move {
          let delay = R::sleep(rand_stagger);

          futures_util::select! {
            _ = delay.fuse() => {},
            _ = stop_rx.recv().fuse() => return,
          }

          let mut timer = R::interval(interval);
          loop {
            futures_util::select! {
              _ = futures_util::StreamExt::next(&mut timer).fuse() => {
                this.$fn().await;
              }
              _ = stop_rx.recv().fuse() => {
                return;
              }
            }
          }
        });
      }
    }
  };
}

impl<D, T, R> Showbiz<D, T, R>
where
  T: Transport,
  D: Delegate,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  /// Used to ensure the Tick is performed periodically.
  pub(crate) async fn schedule(&self, shutdown_rx: async_channel::Receiver<()>) {
    // Create a new probeTicker
    if self.inner.opts.probe_interval > Duration::ZERO {
      self
        .trigger_probe(
          self.inner.opts.probe_interval,
          self.inner.opts.probe_interval,
          shutdown_rx.clone(),
        )
        .await;
    }

    // Create a push pull ticker if needed
    if self.inner.opts.push_pull_interval > Duration::ZERO {
      self.trigger_push_pull(shutdown_rx.clone()).await;
    }

    // Create a gossip ticker if needed
    if self.inner.opts.gossip_interval > Duration::ZERO && self.inner.opts.gossip_nodes > 0 {
      self
        .trigger_gossip(
          self.inner.opts.gossip_interval,
          self.inner.opts.gossip_interval,
          shutdown_rx.clone(),
        )
        .await;
    }
  }

  bail_trigger!(probe);

  bail_trigger!(gossip);

  async fn trigger_push_pull(&self, stop_rx: async_channel::Receiver<()>) {
    let interval = self.inner.opts.push_pull_interval;
    let this = self.clone();
    // Use a random stagger to avoid syncronizing
    let mut rng = rand::thread_rng();
    let rand_stagger = Duration::from_millis(rng.gen_range(0..interval.as_millis() as u64));

    R::spawn_detach(async move {
      futures_util::select! {
        _ = R::sleep(rand_stagger).fuse() => {},
        _ = stop_rx.recv().fuse() => return,
      }

      // Tick using a dynamic timer
      loop {
        let tick_time = push_pull_scale(interval, this.estimate_num_nodes() as usize);
        let mut timer = R::interval(tick_time);
        futures_util::select! {
          _ = futures_util::StreamExt::next(&mut timer).fuse() => {
            this.push_pull().await;
          }
          _ = stop_rx.recv().fuse() => return,
        }
      }
    });
  }

  // Used to perform a single round of failure detection and gossip
  async fn probe(&self) {
    // Track the number of indexes we've considered probing
    let mut num_check = 0;
    let mut probe_index = 0;
    loop {
      let memberlist = self.inner.nodes.read().await;

      // Make sure we don't wrap around infinitely
      if num_check >= memberlist.nodes.len() {
        return;
      }

      // Handle the wrap around case
      if probe_index >= memberlist.nodes.len() {
        drop(memberlist);
        self.reset_nodes().await;
        probe_index = 0;
        num_check += 1;
        continue;
      }

      // Determine if we should probe this node
      let mut skip = false;
      let node = memberlist.nodes[probe_index].clone();
      if node.dead_or_left() || node.id().name == self.inner.opts.name {
        skip = true;
      }

      // Potentially skip
      drop(memberlist);
      probe_index += 1;
      if skip {
        num_check += 1;
        continue;
      }

      // Probe the specific node
      self.probe_node(&node).await;
      return;
    }
  }

  async fn probe_node(&self, target: &LocalNodeState) {
    #[cfg(feature = "metrics")]
    let now = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer! {
      observe_probe_node(now.elapsed().as_millis() as f64, self.inner.metrics_labels.iter());
    }

    // We use our health awareness to scale the overall probe interval, so we
    // slow down if we detect problems. The ticker that calls us can handle
    // us running over the base interval, and will skip missed ticks.
    let probe_interval = self
      .inner
      .awareness
      .scale_timeout(self.inner.opts.probe_interval)
      .await;

    #[cfg(feature = "metrics")]
    {
      if probe_interval > self.inner.opts.probe_interval {
        incr_degraded_probe(self.inner.metrics_labels.iter())
      }
    }

    // Prepare a ping message and setup an ack handler.
    let self_addr = self.get_advertise();
    let ping = Ping {
      seq_no: self.next_seq_no(),
      source: NodeId {
        name: self.inner.opts.name.clone(),
        addr: self_addr,
      },
      target: target.id().clone(),
    };

    let (ack_tx, ack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);
    let (nack_tx, nack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);

    // Mark the sent time here, which should be after any pre-processing but
    // before system calls to do the actual send. This probably over-reports
    // a bit, but it's the best we can do. We had originally put this right
    // after the I/O, but that would sometimes give negative RTT measurements
    // which was not desirable.
    let sent = Instant::now();
    // Send a ping to the node. If this node looks like it's suspect or dead,
    // also tack on a suspect message so that it has a chance to refute as
    // soon as possible.
    let deadline = sent + probe_interval;
    self
      .set_probe_channels(
        ping.seq_no,
        ack_tx.clone(),
        Some(nack_tx),
        sent,
        probe_interval,
      )
      .await;

    if target.state == NodeState::Alive {
      let mut mbuf = BytesMut::with_capacity(MessageType::SIZE + ping.encoded_len());
      mbuf.put_u8(MessageType::Ping as u8);
      ping.encode_to(&mut mbuf);
      if let Err(e) = self.send_msg(target.id(), Message(mbuf)).await {
        tracing::error!(target = "showbiz", local = %self.inner.id, remote = %target.id(), err=%e, "failed to send ping by unreliable connection");
        if e.failed_remote() {
          self
            .handle_remote_failure(target, ping, ack_rx, nack_rx, deadline)
            .await;
        }
        return;
      }
    } else {
      let mut ping_buf = BytesMut::with_capacity(MessageType::SIZE + ping.encoded_len());
      ping_buf.put_u8(MessageType::Ping as u8);
      ping.encode_to(&mut ping_buf);

      let suspect = Suspect {
        incarnation: target.incarnation.load(Ordering::SeqCst),
        node: target.id().clone(),
        from: self.inner.id.clone(),
      };
      let mut suspect_buf = BytesMut::with_capacity(MessageType::SIZE + suspect.encoded_len());
      suspect_buf.put_u8(MessageType::Suspect as u8);
      suspect.encode_to(&mut suspect_buf);
      let compound = Message::compound(vec![Message(ping_buf), Message(suspect_buf)]);
      if let Err(e) = self.raw_send_msg_packet(target.id(), compound).await {
        tracing::error!(target = "showbiz", local = %self.inner.id, remote = %target.id(), err=%e, "failed to send compound ping and suspect message by unreliable connection");
        if e.failed_remote() {
          self
            .handle_remote_failure(target, ping, ack_rx, nack_rx, deadline)
            .await;
        }
        return;
      }
    }

    let delegate = self.inner.delegate.as_ref();

    // Wait for response or round-trip-time.
    futures_util::select! {
      v = ack_rx.recv().fuse() => {
        match v {
          Ok(v) => {
            if v.complete {
              if let Some(delegate) = delegate {
                let rtt = v.timestamp.elapsed();

                if let Err(e) = delegate.notify_ping_complete(target.node.clone(), rtt, v.payload).await {
                  tracing::error!(target = "showbiz", local = %self.inner.id, remote = %target.id(), err=%e, "failed to notify ping complete ack");
                }
              }

              apply_delta!(self <= -1);
              return;
            }

            // As an edge case, if we get a timeout, we need to re-enqueue it
            // here to break out of the select below.
            if !v.complete {
              if let Err(e) = ack_tx.send(v).await {
                tracing::error!(target = "showbiz", local = %self.inner.id, remote = %target.id(), err=%e, "failed to re-enqueue UDP ping ack");
              }
            }
          }
          Err(e) => {
            // This branch should never be reached, if there's an error in your log, please report an issue.
            tracing::debug!(target = "showbiz", local = %self.inner.id, remote = %target.id(), err = %e, "failed unreliable connection ping (ack channel closed)");
          }
        }
      },
      _ = R::sleep(self.inner.opts.probe_timeout).fuse() => {
        // Note that we don't scale this timeout based on awareness and
        // the health score. That's because we don't really expect waiting
        // longer to help get UDP through. Since health does extend the
        // probe interval it will give the TCP fallback more time, which
        // is more active in dealing with lost packets, and it gives more
        // time to wait for indirect acks/nacks.
        tracing::debug!(target = "showbiz", local = %self.inner.id, remote = %target.id(), "failed unreliable connection ping (timeout reached)");
      }
    }

    self
      .handle_remote_failure(target, ping, ack_rx, nack_rx, deadline)
      .await
  }

  async fn handle_remote_failure(
    &self,
    target: &LocalNodeState,
    ping: Ping,
    ack_rx: async_channel::Receiver<AckMessage>,
    nack_rx: async_channel::Receiver<()>,
    deadline: Instant,
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
        .filter_map(|n| {
          if n.id().name == self.inner.opts.name
            || n.id().name == target.id().name
            || n.state != NodeState::Alive
          {
            None
          } else {
            Some(n.node.clone())
          }
        })
        .collect::<Vec<_>>();
      random_nodes(self.inner.opts.indirect_checks, nodes)
    };

    // Attempt an indirect ping.
    let expected_nacks = nodes.len() as isize;
    let ind = IndirectPing::from(ping);

    for peer in nodes {
      // We only expect nack to be sent from peers who understand
      // version 4 of the protocol.
      let mut buf = BytesMut::with_capacity(MessageType::SIZE + ind.encoded_len());
      buf.put_u8(MessageType::IndirectPing as u8);
      ind.encode_to(&mut buf);
      if let Err(e) = self.send_msg(&ind.target, Message(buf)).await {
        tracing::error!(target = "showbiz", local = %self.inner.id, remote = %peer, err=%e, "failed to send indirect unreliable ping");
      }
    }

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
    let (fallback_tx, fallback_rx) = futures_channel::oneshot::channel();

    let mut disable_reliable_pings = self.inner.opts.disable_tcp_pings;
    if let Some(delegate) = self.inner.delegate.as_ref() {
      disable_reliable_pings |= delegate.disable_reliable_pings(target.id());
    }
    if !disable_reliable_pings {
      let target_id = target.id().clone();
      let this = self.clone();
      R::spawn_detach(async move {
        match this
          .send_ping_and_wait_for_ack(&target_id, ind.into(), deadline - Instant::now())
          .await
        {
          Ok(ack) => {
            // The error should never happen, because we do not drop the rx,
            // handle error here for good manner, and if you see this log, please
            // report an issue.
            if let Err(e) = fallback_tx.send(ack) {
              tracing::error!(target = "showbiz", local = %this.inner.id, remote = %target_id, err=%e, "failed to send fallback");
            }
          }
          Err(e) => {
            tracing::error!(target = "showbiz", local = %this.inner.id, remote = %target_id, err=%e, "failed to send ping by reliable connection");
            // The error should never happen, because we do not drop the rx,
            // handle error here for good manner, and if you see this log, please
            // report an issue.
            if let Err(e) = fallback_tx.send(false) {
              tracing::error!(target = "showbiz", local = %this.inner.id, remote = %target_id, err=%e, "failed to send fallback");
            }
          }
        }
      });
    }

    // Wait for the acks or timeout. Note that we don't check the fallback
    // channel here because we want to issue a warning below if that's the
    // *only* way we hear back from the peer, so we have to let this time
    // out first to allow the normal unreliable-connection-based acks to come in.
    futures_util::select! {
      v = ack_rx.recv().fuse() => {
        if let Ok(v) = v {
          if v.complete {
            apply_delta!(self <= -1);
            return;
          }
        }
      }
    }

    // Finally, poll the fallback channel. The timeouts are set such that
    // the channel will have something or be closed without having to wait
    // any additional time here.
    if !disable_reliable_pings {
      if let Ok(did_contact) = fallback_rx.await {
        if did_contact {
          tracing::warn!(target = "showbiz", local = %self.inner.id, remote = %target.id(), "was able to connect to target over reliable connection but unreliable probes failed, network may be misconfigured");
          apply_delta!(self <= -1);
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
    let awareness_delta = if expected_nacks > 0 {
      let nack_count = nack_rx.len() as isize;
      if nack_count < expected_nacks {
        expected_nacks - nack_count - 1
      } else {
        0
      }
    } else {
      0
    };

    // No acks received from target, suspect it as failed.
    tracing::info!(target = "showbiz", local = %self.inner.id, remote = %target.id(), "suspecting has failed, no acks received");
    let s = Suspect {
      incarnation: target.incarnation.load(Ordering::SeqCst),
      node: target.id().clone(),
      from: self.inner.id.clone(),
    };
    if let Err(e) = self.suspect_node(s).await {
      tracing::error!(target = "showbiz", local = %self.inner.id, remote = %target.id(), err=%e, "failed to suspect node");
    }
    if awareness_delta != 0 {
      apply_delta!(self <= awareness_delta);
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
    let num_remove = memberlist.nodes.len() - dead_idx;
    while i < num_remove {
      let node = memberlist.nodes.pop().unwrap();
      memberlist.node_map.remove(node.id().name());
      i += 1;
    }

    // Update num_nodes after we've trimmed the dead nodes
    self
      .inner
      .hot
      .num_nodes
      .store(dead_idx as u32, Ordering::Relaxed);

    // Shuffle live nodes
    memberlist.nodes.shuffle(&mut rand::thread_rng());
  }

  /// Used to attach the ackCh to receive a message when an ack
  /// with a given sequence number is received. The `complete` field of the message
  /// will be false on timeout. Any nack messages will cause an empty struct to be
  /// passed to the nackCh, which can be nil if not needed.
  async fn set_probe_channels(
    &self,
    seq_no: u32,
    ack_tx: async_channel::Sender<AckMessage>,
    nack_tx: Option<async_channel::Sender<()>>,
    sent: Instant,
    timeout: Duration,
  ) {
    let tx = ack_tx.clone();
    let ack_fn = |payload, timestamp| {
      async move {
        futures_util::select! {
          _ = tx.send(AckMessage {
            payload,
            timestamp,
            complete: true,
          }).fuse() => {},
          default => {}
        }
      }
      .boxed()
    };

    let nack_fn = move || {
      let tx = nack_tx.clone();
      async move {
        if let Some(nack_tx) = tx {
          futures_util::select! {
            _ = nack_tx.send(()).fuse() => {},
            default => {}
          }
        }
      }
      .boxed()
    };

    let ack_handlers = self.inner.ack_handlers.clone();
    self.inner.ack_handlers.lock().await.insert(
      seq_no,
      AckHandler {
        ack_fn: Box::new(ack_fn),
        nack_fn: Some(Arc::new(nack_fn)),
        timer: Timer::after::<_, R>(timeout, async move {
          ack_handlers.lock().await.remove(&seq_no);
          futures_util::select! {
            _ = ack_tx.send(AckMessage {
              payload: Bytes::new(),
              timestamp: sent,
              complete: false,
            }).fuse() => {},
            default => {}
          }
        }),
      },
    );
  }

  /// Invoked every GossipInterval period to broadcast our gossip
  /// messages to a few random nodes.
  async fn gossip(&self) {
    #[cfg(feature = "metrics")]
    let now = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      observe_gossip(now.elapsed().as_millis() as f64, self.inner.metrics_labels.iter());
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
        .filter_map(|n| {
          if n.id().name == self.inner.opts.name {
            return None;
          }

          match n.state {
            NodeState::Alive | NodeState::Suspect => Some(n.node.clone()),
            NodeState::Dead => {
              if n.state_change.elapsed() > self.inner.opts.gossip_to_the_dead_time {
                None
              } else {
                Some(n.node.clone())
              }
            }
            _ => None,
          }
        })
        .collect::<Vec<_>>();
      random_nodes(self.inner.opts.gossip_nodes, nodes)
    };

    // Compute the bytes available
    let mut bytes_avail = self.inner.opts.packet_buffer_size
      - COMPOUND_HEADER_OVERHEAD
      - self.inner.opts.label.label_overhead();

    if self.encryption_enabled() {
      bytes_avail = bytes_avail.saturating_sub(encrypt_overhead(self.inner.opts.encryption_algo));
    }

    for node in nodes {
      // Get any pending broadcasts
      let mut msgs = match self
        .get_broadcast_with_prepend(vec![], COMPOUND_OVERHEAD, bytes_avail)
        .await
      {
        Ok(msgs) => msgs,
        Err(e) => {
          tracing::error!(target = "showbiz", err = %e, "failed to get broadcast messages from {}", node);
          return;
        }
      };
      if msgs.is_empty() {
        return;
      }

      let addr = node.id();
      if msgs.len() == 1 {
        // Send single message as is
        if let Err(e) = self.raw_send_msg_packet(addr, msgs.pop().unwrap().0).await {
          tracing::error!(target = "showbiz", err = %e, "failed to send gossip to {}", addr);
        }
      } else {
        // Otherwise create and send one or more compound messages
        for compound in Message::compounds(msgs) {
          if let Err(e) = self.raw_send_msg_packet(addr, compound).await {
            tracing::error!(target = "showbiz", err = %e, "failed to send gossip to {}", addr);
          }
        }
      }
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
          if n.id().name == self.inner.opts.name || n.state != NodeState::Alive {
            None
          } else {
            Some(n.node.clone())
          }
        })
        .collect::<Vec<_>>();
      random_nodes(1, nodes)
    };

    if nodes.is_empty() {
      return;
    }

    let node = &nodes[0];
    // Attempt a push pull
    if let Err(e) = self
      .push_pull_node(node.name(), node.id().addr(), false)
      .await
    {
      tracing::error!(target = "showbiz", err = %e, "push/pull with {} failed", node.id());
    }
  }

  /// Gossips an alive message in response to incoming information that we
  /// are suspect or dead. It will make sure the incarnation number beats the given
  /// accusedInc value, or you can supply 0 to just get the next incarnation number.
  /// This alters the node state that's passed in so this MUST be called while the
  /// nodeLock is held.
  async fn refute(&self, state: &LocalNodeState, accused_inc: u32) {
    // Make sure the incarnation number beats the accusation.
    let mut inc = self.next_incarnation();
    if accused_inc >= inc {
      inc = self.skip_incarnation(accused_inc - inc + 1);
    }
    state.incarnation.store(inc, Ordering::Relaxed);

    // Decrease our health because we are being asked to refute a problem.
    self.inner.awareness.apply_delta(1).await;

    // Format and broadcast an alive message.
    let a = Alive {
      incarnation: inc,
      vsn: state.node.vsn(),
      meta: state.node.meta.clone(),
      node: state.node.id.clone(),
    };

    let mut buf = BytesMut::with_capacity(a.encoded_len() + 1);
    buf.put_u8(MessageType::Alive as u8);
    a.encode_to(&mut buf);
    self.broadcast(state.node.id.clone(), Message(buf)).await;
  }
}

#[inline]
fn suspicion_timeout(suspicion_mult: usize, n: usize, interval: Duration) -> u64 {
  let node_scale = (n as f64).log10().max(1.0);
  // multiply by 1000 to keep some precision because time.Duration is an int64 type
  (suspicion_mult as u64) * ((node_scale * 1000.0) as u64) * (interval.as_millis() as u64 / 1000)
}

/// push_pull_scale is used to scale the time interval at which push/pull
/// syncs take place. It is used to prevent network saturation as the
/// cluster size grows
#[inline]
fn push_pull_scale(interval: Duration, n: usize) -> Duration {
  /// the minimum number of nodes
  /// before we start scaling the push/pull timing. The scale
  /// effect is the log2(Nodes) - log2(pushPullScale). This means
  /// that the 33rd node will cause us to double the interval,
  /// while the 65th will triple it.
  const PUSH_PULL_SCALE_THRESHOLD: usize = 32;

  // Don't scale until we cross the threshold
  if n <= PUSH_PULL_SCALE_THRESHOLD {
    return interval;
  }

  let multiplier = (f64::log2(n as f64) - f64::log2(PUSH_PULL_SCALE_THRESHOLD as f64) + 1.0).ceil();
  interval * multiplier as u32
}
