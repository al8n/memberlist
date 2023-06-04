use std::{net::SocketAddr, sync::atomic::Ordering, time::Duration};

use crate::{
  network::{RemoteNodeState, COMPOUND_HEADER_OVERHEAD, COMPOUND_OVERHEAD},
  security::encrypt_overhead,
  showbiz::{AckHandler, Member, Memberlist, Spawner},
  suspicion::Suspicion,
  timer::Timer,
  types::{Alive, Dead, Message, MessageType, Name, Ping, Suspect},
};

use super::*;
use bytes::{BufMut, Bytes, BytesMut};
use futures_channel::oneshot::Sender;
use futures_timer::Delay;
use futures_util::{future::BoxFuture, FutureExt};
use rand::seq::SliceRandom;

fn random_offset(n: usize) -> usize {
  if n == 0 {
    return 0;
  }
  (rand::random::<u32>() % (n as u32)) as usize
}

fn random_nodes<F>(k: usize, nodes: &Memberlist, exclude: Option<F>) -> Vec<Arc<Node>>
where
  F: Fn(&LocalNodeState) -> bool,
{
  let n = nodes.nodes.len();
  let mut knodes: Vec<Arc<Node>> = Vec::with_capacity(k);

  'outer: loop {
    // Probe up to 3*n times, with large n this is not necessary
    // since k << n, but with small n we want search to be
    // exhaustive
    for i in 0..k {
      // Get random node state
      let idx = random_offset(n);
      let node = &nodes.nodes[idx];

      // Give the filter a shot at it
      if let Some(exclude) = &exclude {
        if exclude(node) {
          continue 'outer;
        }
      }

      // Check if we have this node already
      #[allow(clippy::needless_range_loop)]
      for j in 0..knodes.len() {
        if knodes[j].id == node.node.id {
          continue 'outer;
        }
      }

      // Append the node
      knodes.push(node.node.clone());

      if i >= 3 * n && knodes.len() >= k {
        break;
      }
    }

    return knodes;
  }
}

struct AckMessage {
  complete: bool,
  payload: Bytes,
  timestamp: Instant,
}

// -------------------------------Public methods---------------------------------

impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  D: Delegate,
  S: Spawner,
{
  /// Initiates a ping to the node with the specified node.
  pub async fn ping(&self, node: NodeId) -> Result<Duration, Error<D, T>> {
    // Prepare a ping message and setup an ack handler.
    let self_addr = self.get_advertise().await;
    let ping = Ping {
      seq_no: self.next_seq_no(),
      source: NodeId {
        name: self.inner.opts.name.clone(),
        port: Some(self_addr.port()),
        addr: self_addr.ip().into(),
      },
      target: Some(node.clone()),
    };

    let (ack_tx, ack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);
    self
      .set_probe_channels(ping.seq_no, ack_tx, None, self.inner.opts.probe_interval)
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
      _ = Delay::new(self.inner.opts.probe_timeout).fuse() => {}
    }

    // If we timed out, return Error.
    tracing::debug!(target: "showbiz", "failed UDP ping {} (timeout reached)", node);
    Err(Error::NoPingResponse(node))
  }
}

// ---------------------------------Crate methods-------------------------------
impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  D: Delegate,
  S: Spawner,
{
  /// Does a complete state exchange with a specific node.
  pub(crate) async fn push_pull_node(&self, addr: &NodeId, join: bool) -> Result<(), Error<D, T>> {
    #[cfg(feature = "metrics")]
    let now = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      observe_push_pull_node(now.elapsed().as_millis() as f64, self.inner.metrics_labels.iter());
    );

    self
      .merge_remote_state(self.send_and_receive_state(addr, join).await?)
      .await
  }

  pub(crate) async fn dead_node(
    &self,
    memberlist: &mut Memberlist,
    d: Dead,
  ) -> Result<(), Error<D, T>> {
    let state = if d.dead_self() {
      &mut memberlist.local
    } else {
      match memberlist.node_map.get_mut(&d.node) {
        Some(state) => state,
        // If we've never heard about this node before, ignore it
        None => return Ok(()),
      }
    };

    // Ignore old incarnation numbers
    if d.incarnation < state.state.incarnation {
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
      if !self.has_left() {
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
        .broadcast_notify(d.node.clone(), msg, self.inner.leave_broadcast_tx.clone())
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
    state.state.incarnation = d.incarnation;

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

    let Some(state) = mu.node_map.get_mut(&s.node) else {
      return Ok(());
    };

    // Ignore old incarnation numbers
    if s.incarnation < state.state.incarnation {
      return Ok(());
    }

    // See if there's a suspicion timer we can confirm. If the info is new
    // to us we will go ahead and re-gossip it. This allows for multiple
    // independent confirmations to flow even when a node probes a node
    // that's already suspect.
    if let Some(timer) = &mut state.suspicion {
      if timer.confirm(s.from.clone()).await {
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
      self.refute(state, s.incarnation).await;
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
    state.state.incarnation = s.incarnation;
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
    let spawner = self.inner.spawner;
    state.suspicion = Some(Suspicion::new(
      s.from.clone(),
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
              .get(&n)
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
      move |fut| spawner.spawn(fut),
    ));
    Ok(())
  }

  pub(crate) async fn alive_node(
    &self,
    alive: Alive,
    notify_tx: Option<Sender<()>>,
    bootstrap: bool,
  ) -> Result<(), Error<D, T>> {
    Ok(())
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
          self.alive_node(alive, None, false).await?;
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
        timer: Timer::after(
          timeout,
          async move {
            tlock.lock().await.remove(&seq_no);
          },
          |fut| self.inner.spawner.spawn(fut),
        ),
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

impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  D: Delegate,
  S: Spawner,
{
  async fn probe_node(&self, node: &LocalNodeState) {
    #[cfg(feature = "metrics")]
    let now = Instant::now();
    #[cfg(feature = "metrics")]
    scopeguard::defer! {
      observe_probe_node(now.elapsed().as_millis() as f64, self.inner.metrics_labels.iter());
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
      memberlist.node_map.remove(node.id());
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
        timer: Timer::after(
          timeout,
          async move {
            ack_handlers.lock().await.remove(&seq_no);
            futures_util::select! {
              _ = ack_tx.send(AckMessage {
                payload: Bytes::new(),
                timestamp: Instant::now(),
                complete: false,
              }).fuse() => {},
              default => {}
            }
          },
          |fut| self.inner.spawner.spawn(fut),
        ),
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
      let memberlist = self.inner.nodes.read().await;
      random_nodes(
        self.inner.opts.gossip_nodes,
        &memberlist,
        Some(|n: &LocalNodeState| match n.state {
          NodeState::Alive | NodeState::Suspect => false,
          NodeState::Dead => n.state_change.elapsed() > self.inner.opts.gossip_to_the_dead_time,
          _ => true,
        }),
      )
    };

    // Compute the bytes available
    let mut bytes_avail = self.inner.opts.packet_buffer_size
      - COMPOUND_HEADER_OVERHEAD
      - Self::label_overhead(&self.inner.opts.label);

    if self.encryption_enabled().await {
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
      let memberlist = self.inner.nodes.read().await;
      random_nodes(
        1,
        &memberlist,
        Some(|n: &LocalNodeState| n.state != NodeState::Alive),
      )
    };

    if nodes.is_empty() {
      return;
    }

    let node = &nodes[0];
    // Attempt a push pull
    if let Err(e) = self.push_pull_node(node.id(), false).await {
      tracing::error!(target = "showbiz", err = %e, "push/pull with {} failed", node.id());
    }
  }

  /// Gossips an alive message in response to incoming information that we
  /// are suspect or dead. It will make sure the incarnation number beats the given
  /// accusedInc value, or you can supply 0 to just get the next incarnation number.
  /// This alters the node state that's passed in so this MUST be called while the
  /// nodeLock is held.
  async fn refute(&self, me: &mut Member, accused_inc: u32) {
    // Make sure the incarnation number beats the accusation.
    let mut inc = self.next_incarnation();
    if accused_inc >= inc {
      inc = self.skip_incarnation(accused_inc - inc + 1);
    }
    me.state.incarnation = inc;

    // Decrease our health because we are being asked to refute a problem.
    self.inner.awareness.apply_delta(1).await;

    // Format and broadcast an alive message.
    let a = Alive {
      incarnation: inc,
      vsn: me.state.node.vsn(),
      meta: me.state.node.meta.clone(),
      node: me.state.node.id.clone(),
    };

    let mut buf = BytesMut::with_capacity(a.encoded_len() + 1);
    buf.put_u8(MessageType::Alive as u8);
    a.encode_to(&mut buf);
    self.broadcast(me.state.node.id.clone(), Message(buf)).await;
  }
}

#[inline]
fn suspicion_timeout(suspicion_mult: usize, n: usize, interval: Duration) -> u64 {
  let node_scale = (n as f64).log10().max(1.0);
  // multiply by 1000 to keep some precision because time.Duration is an int64 type
  (suspicion_mult as u64) * ((node_scale * 1000.0) as u64) * (interval.as_millis() as u64 / 1000)
}
