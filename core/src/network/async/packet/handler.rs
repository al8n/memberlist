use futures::{Future, Stream};

use crate::{
  showbiz::MessageHandoff,
  types::{Alive, Dead, Suspect},
};

use super::*;

impl<D, T> Showbiz<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  /// a long running thread that processes messages received
  /// over the packet interface, but is decoupled from the listener to avoid
  /// blocking the listener which may cause ping/ack messages to be delayed.
  pub(crate) fn packet_handler(&self, shutdown_rx: async_channel::Receiver<()>) {
    let this = self.clone();
    let handoff_rx = this.inner.handoff_rx.clone();
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          _ = handoff_rx.recv().fuse() => while let Some(msg) = this.get_next_message().await {
            match msg.msg {
              Message::Suspect(m) => this.handle_suspect(msg.from, m).await,
              Message::Alive(m) => this.handle_alive(msg.from, m).await,
              Message::Dead(m) => this.handle_dead(msg.from, m).await,
              Message::UserData(m) => this.handle_user(msg.from, m).await,
              m => {
                tracing::error!(target:  "showbiz.packet", "message type ({}) not supported {} (packet handler)", m.kind(), msg.from);
              }
            }
          }
        }
      }
    });
  }

  /// Returns the next message to process in priority order, using LIFO
  async fn get_next_message(
    &self,
  ) -> Option<MessageHandoff<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    let mut queue = self.inner.queue.lock().await;
    queue.high.pop_back().or_else(|| queue.low.pop_back())
  }

  async fn handle_suspect(
    &self,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
    suspect: Suspect<T::Id>,
  ) {
    if let Err(e) = self.suspect_node(suspect).await {
      tracing::error!(target:  "showbiz.packet", err=%e, remote_addr = %from, "failed to suspect node");
    }
  }

  async fn handle_alive(
    &self,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
    alive: Alive<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
    if let Err(e) = self.inner.transport.blocked_address(&from) {
      tracing::error!(target:  "showbiz.packet", err=%e, remote_addr = %from, "blocked alive message");
      return;
    }

    if let Err(e) = self.inner.transport.blocked_address(alive.node.address()) {
      tracing::error!(target:  "showbiz.packet", err=%e, remote_addr = %from, "blocked alive address {} message from {}", alive.node.address(), from);
      return;
    }

    self.alive_node(alive, None, false).await
  }

  async fn handle_dead(
    &self,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
    dead: Dead<T::Id>,
  ) {
    let mut memberlist = self.inner.nodes.write().await;
    if let Err(e) = self.dead_node(&mut memberlist, dead).await {
      tracing::error!(target:  "showbiz.packet", err=%e, remote_addr = %from, "failed to mark node as dead");
    }
  }

  async fn handle_user(
    &self,
    from: <T::Resolver as AddressResolver>::ResolvedAddress,
    data: Bytes,
  ) {
    if let Some(d) = self.delegate.as_ref() {
      if let Err(e) = d.notify_message(data).await {
        tracing::error!(target:  "showbiz.packet", err=%e, remote_addr = %from, "failed to handle user message");
      }
    }
  }
}
