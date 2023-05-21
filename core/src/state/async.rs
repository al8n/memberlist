use crate::types::Alive;

use super::*;
use futures_channel::oneshot::Sender;

impl<B, T, D> Showbiz<B, T, D>
where
  B: Broadcast,
  T: Transport,
  D: Delegate,
{
  /// Does a complete state exchange with a specific node.
  pub(crate) async fn push_pull_node(&self, a: Address, join: bool) -> Result<(), Error<B, T, D>> {
    // TODO: metrics

    // self.send_and_receive_state(a, join).await
    todo!()
  }

  pub(crate) async fn alive_node(
    &self,
    alive: Alive,
    notify_tx: Sender<()>,
    bootstrap: bool,
  ) -> Result<(), Error<B, T, D>> {
    todo!("implement alive node")
  }

  pub(crate) async fn merge_state(&self, remote: Vec<PushNodeState>) -> Result<(), Error<B, T, D>> {
    todo!("implement merge state")
  }
}
