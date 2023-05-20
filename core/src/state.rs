use std::{net::SocketAddr, time::Instant};

use bytes::Bytes;
use showbiz_traits::{
  AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, PingDelegate, Transport,
};
use showbiz_types::{Address, Node, NodeState};

use super::{error::Error, showbiz::Showbiz, types::PushNodeState};

#[viewit::viewit]
pub(crate) struct LocalNodeState {
  node: Node,
  incarnation: u32,
  state: NodeState,
  state_change: Instant,
}

impl LocalNodeState {
  pub(crate) const fn address(&self) -> SocketAddr {
    self.node.full_address().addr()
  }

  pub(crate) const fn full_address(&self) -> &Address {
    self.node.full_address()
  }

  #[inline]
  pub(crate) fn dead_or_left(&self) -> bool {
    self.state == NodeState::Dead || self.state == NodeState::Left
  }
}

impl<T, D, ED, CD, MD, PD, AD> Showbiz<T, D, ED, CD, MD, PD, AD>
where
  T: Transport,
  D: Delegate,
  ED: EventDelegate,
  CD: ConflictDelegate,
  MD: MergeDelegate,
  PD: PingDelegate,
  AD: AliveDelegate,
{
  pub(crate) async fn merge_state(&self, remote: Vec<PushNodeState>) -> Result<(), Error> {
    // TODO: implement

    Ok(())
  }
}
