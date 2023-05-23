use std::{net::SocketAddr, sync::Arc, time::Instant};

use super::{error::Error, showbiz::Showbiz, types::PushNodeState};
use showbiz_traits::{Broadcast, Delegate, Transport};
use showbiz_types::{Address, Node, NodeState};

mod r#async;

#[viewit::viewit]
#[derive(Debug, Clone)]
pub(crate) struct LocalNodeState {
  node: Arc<Node>,
  incarnation: u32,
  state_change: Instant,
}

impl LocalNodeState {
  pub(crate) fn address(&self) -> SocketAddr {
    self.node.full_address().addr()
  }

  pub(crate) fn full_address(&self) -> &Address {
    self.node.full_address()
  }

  #[inline]
  pub(crate) fn dead_or_left(&self) -> bool {
    self.node.state == NodeState::Dead || self.node.state == NodeState::Left
  }
}

// private implementation
impl<T, D> Showbiz<T, D>
where
  T: Transport,
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
}
