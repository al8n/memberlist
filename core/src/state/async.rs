use std::{net::SocketAddr, time::Duration};

use crate::{
  showbiz::{Memberlist, AckHandler, Member},
  types::{Alive, Dead, Name, Suspect, Message, MessageType}, timer::Timer, suspicion::Suspicion,
};

use super::*;
use bytes::{Bytes, BytesMut, BufMut};
use futures_channel::oneshot::Sender;
use futures_util::{future::BoxFuture, FutureExt};

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  /// Does a complete state exchange with a specific node.
  pub(crate) async fn push_pull_node(
    &self,
    name: &Name,
    _addr: SocketAddr,
    _join: bool,
  ) -> Result<(), Error<T, D>> {
    // TODO: metrics

    // self.send_and_receive_state(a, join).await
    todo!()
  }

  pub(crate) async fn dead_node(
    &self,
    memberlist: &mut Memberlist,
    d: Dead,
  ) -> Result<(), Error<T, D>> {
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
        .broadcast_notify(
          d.node.clone(),
          msg,
          self.inner.leave_broadcast_tx.clone(),
        )
        .await;
    } else {
      let msg = d.encode_to_msg();
      self.broadcast(d.node.clone(), msg).await;
    }

    // TODO: update metrics

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

  pub(crate) async fn suspect_node<R, S>(&self, s: Suspect, spawner: S) -> Result<(), Error<T, D>>
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
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

    // TODO: Update metrics

    // Update the state
    state.state.incarnation = s.incarnation;
    state.state.state = NodeState::Suspect;
    let change_time = Instant::now();
    state.state.state_change = change_time;

    // Setup a suspicion timer. Given that we don't have any known phase
    // relationship with our peers, we set up k such that we hit the nominal
    // timeout two probe intervals short of what we expect given the suspicion
    // multiplier.
    let mut k = self.inner.opts.suspicion_mult.checked_sub(2).unwrap_or(0);

    // If there aren't enough nodes to give the expected confirmations, just
	  // set k to 0 to say that we don't expect any. Note we subtract 2 from n
	  // here to take out ourselves and the node being probed.
    let n = self.estimate_num_nodes() as usize;
    if n-2 < k {
      k = 0;
    }

    // Compute the timeouts based on the size of the cluster.
    let min = suspicion_timeout(self.inner.opts.suspicion_mult, n, self.inner.opts.probe_interval);
    let max = (self.inner.opts.suspicion_max_timeout_mult as u64) * min;

    let this = self.clone();
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
            t.inner.nodes.read().await.node_map.get(&n).and_then(|state| {
              let timeout = state.state.state == NodeState::Suspect && state.state.state_change == change_time;
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
            if k > 0 && k > num_confirmations as usize {
              // TODO: metrics
            }

            tracing::info!(target = "showbiz", "marking {} as failed, suspect timeout reached ({} peer confirmations)", dead.node, num_confirmations);
            let mut memberlist = t.inner.nodes.write().await;
            let err_info = format!("failed to mark {} as failed", dead.node);
            if let Err(e) = t.dead_node(&mut memberlist, dead).await {
              tracing::error!(target = "showbiz", err=%e, err_info);
            }
          }
        }.boxed()
      },
      move |fut| {
        spawner(fut);
      }
    ));
    Ok(())
  }

  pub(crate) async fn alive_node(
    &self,
    alive: Alive,
    notify_tx: Option<Sender<()>>,
    bootstrap: bool,
  ) -> Result<(), Error<T, D>> {


    Ok(())
  }

  pub(crate) async fn merge_state<R, S>(&self, remote: Vec<PushNodeState>, spawner: S) -> Result<(), Error<T, D>>
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
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
        },
        NodeState::Left => {
          let dead = Dead {
            incarnation: r.incarnation,
            node: r.node,
            from: self.inner.id.clone(),
          };
          let mut memberlist = self.inner.nodes.write().await;
          self.dead_node(&mut memberlist, dead).await?;
        },
        // If the remote node believes a node is dead, we prefer to
			  // suspect that node instead of declaring it dead instantly
        NodeState::Dead | NodeState::Suspect => {
          let s = Suspect {
            incarnation: r.incarnation,
            node: r.node,
            from: self.inner.id.clone(),
          };
          self.suspect_node(s, spawner).await?;
        },
      }
    }
    Ok(())
  }

  pub(crate) async fn set_ack_handler<R, S, F>(&self, seq_no: u32, timeout: Duration, f: F, s: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
    F: FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static,
  {
    // Add the handler
    let tlock = self.inner.ack_handlers.clone();
    let mut mu = self.inner.ack_handlers.lock().await;
    mu.insert(seq_no, AckHandler {
      ack_fn: Box::new(f),
      nack_fn: None,
      timer: Timer::after(timeout, async move {
        tlock.lock().await.remove(&seq_no);
      }, s),
    });
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