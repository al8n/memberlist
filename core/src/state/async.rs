use std::{net::SocketAddr, time::Duration};

use crate::{
  showbiz::{Memberlist, AckHandler},
  types::{Alive, Dead, Name}, timer::Timer,
};

use super::*;
use bytes::Bytes;
use futures_channel::oneshot::Sender;
use futures_util::future::BoxFuture;

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
    if d.incarnation < state.incarnation {
      return Ok(());
    }

    // Clear out any suspicion timer that may be in effect.
    memberlist.node_timers.remove(&d.node);

    // Ignore if node is already dead
    if state.dead_or_left() {
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
          d.node.name.clone(),
          msg,
          self.inner.leave_broadcast_tx.clone(),
        )
        .await;
    } else {
      let msg = d.encode_to_msg();
      self.broadcast(d.node.name.clone(), msg).await;
    }

    // TODO: update metrics

    // Update the state
    state.incarnation = d.incarnation;

    // If the dead message was send by the node itself, mark it is left
    // instead of dead.
    if d.dead_self() {
      state.state = NodeState::Left;
    } else {
      state.state = NodeState::Dead;
    }
    state.state_change = Instant::now();

    // notify of death
    if let Some(ref delegate) = self.inner.delegate {
      delegate
        .notify_leave(state.node.clone())
        .await
        .map_err(Error::delegate)?;
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

  pub(crate) async fn alive_node(
    &self,
    _alive: Alive,
    _notify_tx: Sender<()>,
    _bootstrap: bool,
  ) -> Result<(), Error<T, D>> {
    todo!("implement alive node")
  }

  pub(crate) async fn merge_state(&self, _remote: Vec<PushNodeState>) -> Result<(), Error<T, D>> {
    todo!("implement merge state")
  }
}
