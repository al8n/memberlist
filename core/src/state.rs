use std::{sync::Arc, time::Instant};

use crate::{
  showbiz::Spawner,
  types::{AckResponse, NackResponse, NodeAddress},
};

use super::{
  delegate::Delegate,
  error::Error,
  showbiz::Showbiz,
  transport::Transport,
  types::{Node, NodeId, NodeState, PushNodeState},
};
mod r#async;
#[viewit::viewit]
#[derive(Debug, Clone)]
pub(crate) struct LocalNodeState {
  node: Arc<Node>,
  incarnation: u32,
  state_change: Instant,
  state: NodeState,
}

impl LocalNodeState {
  pub(crate) fn id(&self) -> &NodeId {
    self.node.id()
  }

  pub(crate) fn address(&self) -> &NodeAddress {
    self.node.id().addr()
  }

  #[inline]
  pub(crate) fn dead_or_left(&self) -> bool {
    self.state == NodeState::Dead || self.state == NodeState::Left
  }
}

// private implementation
impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  S: Spawner,
  D: Delegate,
{
  /// Returns a usable sequence number in a thread safe way
  #[inline]
  pub(crate) fn next_seq_no(&self) -> u32 {
    self
      .inner
      .hot
      .sequence_num
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
  }

  /// Returns the next incarnation number in a thread safe way
  #[inline]
  pub(crate) fn next_incarnation(&self) -> u32 {
    self
      .inner
      .hot
      .incarnation
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
  }

  /// Adds the positive offset to the incarnation number.
  #[inline]
  pub(crate) fn skip_incarnation(&self, offset: u32) -> u32 {
    self
      .inner
      .hot
      .incarnation
      .fetch_add(offset, std::sync::atomic::Ordering::SeqCst)
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
    self
      .inner
      .hot
      .shutdown
      .load(std::sync::atomic::Ordering::SeqCst)
      == 1
  }

  #[inline]
  pub(crate) fn has_left(&self) -> bool {
    self
      .inner
      .hot
      .leave
      .load(std::sync::atomic::Ordering::SeqCst)
      == 1
  }

  #[inline]
  pub(crate) async fn invoke_ack_handler(&self, ack: AckResponse, timestamp: Instant) {
    let ah = self.inner.ack_handlers.lock().await.remove(&ack.seq_no);
    if let Some(handler) = ah {
      handler.timer.stop().await;
      (handler.ack_fn)(ack.payload, timestamp).await;
    }
  }

  #[inline]
  pub(crate) async fn invoke_nack_handler(&self, nack: NackResponse) {
    let ah = self
      .inner
      .ack_handlers
      .lock()
      .await
      .get(&nack.seq_no)
      .and_then(|ah| ah.nack_fn.clone());
    if let Some(nack_fn) = ah {
      (nack_fn)().await;
    }
  }
}
