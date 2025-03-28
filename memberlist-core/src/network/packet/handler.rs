use crate::base::MessageHandoff;

use agnostic_lite::AsyncSpawner;

use super::*;

impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// a long running thread that processes messages received
  /// over the packet interface, but is decoupled from the listener to avoid
  /// blocking the listener which may cause ping/ack messages to be delayed.
  pub(crate) fn packet_handler(
    &self,
    shutdown_rx: async_channel::Receiver<()>,
  ) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()> {
    let this = self.clone();
    let handoff_rx = this.inner.handoff_rx.clone();
    <T::Runtime as RuntimeLite>::spawn(async move {
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            tracing::debug!("memberlist: packet handler exits");
            return;
          }
          _ = handoff_rx.recv().fuse() => while let Some(msg) = this.get_next_message().await {
            match msg.msg {
              Message::Suspect(m) => this.handle_suspect(msg.from, m).await,
              Message::Alive(m) => this.handle_alive(msg.from, m).await,
              Message::Dead(m) => this.handle_dead(msg.from, m).await,
              Message::UserData(m) => this.handle_user(msg.from, m.as_ref()).await,
              m => {
                tracing::error!("memberlist: message type ({}) not supported {} (packet handler)", m.ty(), msg.from);
              }
            }
          }
        }
      }
    })
  }

  /// Returns the next message to process in priority order, using LIFO
  async fn get_next_message(&self) -> Option<MessageHandoff<T::Id, T::ResolvedAddress>> {
    let mut queue = self.inner.queue.lock().await;
    queue.high.pop_back().or_else(|| queue.low.pop_back())
  }

  async fn handle_suspect(&self, from: T::ResolvedAddress, suspect: Suspect<T::Id>) {
    if let Err(e) = self.suspect_node(suspect).await {
      tracing::error!(err=%e, remote_addr = %from, "memberlist.packet: failed to suspect node");
    }
  }

  async fn handle_alive(&self, from: T::ResolvedAddress, alive: Alive<T::Id, T::ResolvedAddress>) {
    if let Err(e) = self.inner.transport.blocked_address(&from) {
      tracing::error!(err=%e, remote_addr = %from, "memberlist.packet: blocked alive message");
      return;
    }

    if let Err(e) = self.inner.transport.blocked_address(alive.node().address()) {
      tracing::error!(err=%e, remote_addr = %from, "memberlist.packet: blocked alive address {} message from {}", alive.node().address(), from);
      return;
    }

    self.alive_node(alive, None, false).await
  }

  async fn handle_dead(&self, from: T::ResolvedAddress, dead: Dead<T::Id>) {
    let mut memberlist = self.inner.nodes.write().await;
    if let Err(e) = self.dead_node(&mut memberlist, dead).await {
      tracing::error!(err=%e, remote_addr = %from, "memberlist.packet: failed to mark node as dead");
    }
  }

  async fn handle_user(&self, from: T::ResolvedAddress, data: &[u8]) {
    if let Some(d) = self.delegate.as_ref() {
      tracing::trace!(remote_addr = %from, data=?data, "memberlist.packet: handle user data");
      d.notify_message(data.into()).await
    }
  }
}
