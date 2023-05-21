use std::{net::SocketAddr, sync::Arc, time::Instant};

use super::{error::Error, showbiz::Showbiz, types::PushNodeState};
use showbiz_traits::{Delegate, Transport};
use showbiz_types::{Address, Node, NodeState};

#[viewit::viewit]
#[derive(Debug, Clone)]
pub(crate) struct LocalNodeState {
  node: Arc<Node>,
  incarnation: u32,
  state: NodeState,
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
    self.state == NodeState::Dead || self.state == NodeState::Left
  }
}

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  pub(crate) async fn merge_state(&self, remote: Vec<PushNodeState>) -> Result<(), Error<T, D>> {
    // TODO: implement

    Ok(())
  }
}
