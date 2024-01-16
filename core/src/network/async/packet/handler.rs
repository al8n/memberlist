use std::net::IpAddr;

use futures::{Future, Stream};

use crate::{
  showbiz::MessageHandoff,
  types::{Alive, Dead, Suspect},
};

use super::*;

impl<D, T> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
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
            match msg.msg_ty {
              MessageType::Suspect => this.handle_suspect(msg).await,
              MessageType::Alive => this.handle_alive(msg).await,
              MessageType::Dead => this.handle_dead(msg).await,
              MessageType::User => this.handle_user(msg).await,
              _ => {
                tracing::error!(target = "showbiz.packet", "message type ({}) not supported {} (packet handler)", msg.msg_ty, msg.from);
              }
            }
          }
        }
      }
    });
  }

  /// Returns the next message to process in priority order, using LIFO
  async fn get_next_message(&self) -> Option<MessageHandoff> {
    let mut queue = self.inner.queue.lock().await;
    queue.high.pop_back().or_else(|| queue.low.pop_back())
  }

  async fn handle_suspect(&self, msg: MessageHandoff) {
    let (_, suspect) = match Suspect::decode(&msg.buf) {
      Ok(rst) => rst,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "failed to decode suspect message");
        return;
      }
    };

    if let Err(e) = self.suspect_node(suspect).await {
      tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "failed to suspect node");
    }
  }

  async fn handle_alive(&self, msg: MessageHandoff) {
    if let Err(e) = self.ensure_can_connect(msg.from.ip()) {
      tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "blocked alive message");
      return;
    }

    let (_, alive) = match Alive::decode_archived(&msg.buf) {
      Ok(alive) => alive,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "failed to decode alive message");
        return;
      }
    };

    if self.inner.opts.ip_must_be_checked() {
      if let Err(e) = self.inner.opts.ip_allowed(alive.node.addr().ip()) {
        tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "blocked alive message");
        return;
      }
    }

    self
      .alive_node(either::Either::Right(alive), None, false)
      .await
  }

  async fn handle_dead(&self, msg: MessageHandoff) {
    let (_, dead) = match Dead::decode(&msg.buf) {
      Ok(dead) => dead,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "failed to decode dead message");
        return;
      }
    };

    let mut memberlist = self.inner.nodes.write().await;
    if let Err(e) = self.dead_node(&mut memberlist, dead).await {
      tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "failed to mark node as dead");
    }
  }

  async fn handle_user(&self, msg: MessageHandoff) {
    if let Some(d) = self.delegate.as_ref() {
      if let Err(e) = d.notify_message(msg.buf).await {
        tracing::error!(target = "showbiz.packet", err=%e, remote_addr = %msg.from, "failed to handle user message");
      }
    }
  }

  fn ensure_can_connect(&self, from: IpAddr) -> Result<(), Error<T, D>> {
    if !self.inner.opts.ip_must_be_checked() {
      return Ok(());
    }

    self.inner.opts.ip_allowed(from).map_err(From::from)
  }
}
