use std::net::IpAddr;

use futures_util::{Future, Stream};

use crate::showbiz::MessageHandoff;

use super::*;

impl<D, T, R> Showbiz<D, T, R>
where
  T: Transport,
  D: Delegate,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  /// a long running thread that processes messages received
  /// over the packet interface, but is decoupled from the listener to avoid
  /// blocking the listener which may cause ping/ack messages to be delayed.
  pub(crate) fn packet_handler(&self, shutdown_rx: async_channel::Receiver<()>) {
    let this = self.clone();
    let handoff_rx = this.inner.handoff_rx.clone();
    self.inner.runtime.spawn_detach(async move {
      loop {
        futures_util::select! {
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
                tracing::error!(target = "showbiz", "message type ({}) not supported {} (packet handler)", msg.msg_ty, msg.from);
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

  async fn handle_suspect(&self, mut msg: MessageHandoff) {
    let len = match Suspect::decode_len(&mut msg.buf) {
      Ok(len) => len,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode suspect message");
        return;
      }
    };

    let suspect = match Suspect::decode_from(msg.buf.split_to(len)) {
      Ok(suspect) => suspect,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode suspect message");
        return;
      }
    };

    if let Err(e) = self.suspect_node(suspect).await {
      tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to suspect node");
    }
  }

  async fn handle_alive(&self, mut msg: MessageHandoff) {
    if let Err(e) = self.ensure_can_connect(msg.from.ip()) {
      tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "blocked alive message");
      return;
    }

    let len = match Alive::decode_len(&mut msg.buf) {
      Ok(len) => len,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode alive message");
        return;
      }
    };

    let alive = match Alive::decode_from(msg.buf.split_to(len)) {
      Ok(alive) => alive,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode alive message");
        return;
      }
    };

    if self.inner.opts.ip_must_be_checked() {
      if let Err(e) = self.inner.opts.ip_allowed(alive.node.addr().ip()) {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "blocked alive message");
        return;
      }
    }

    self.alive_node(alive, None, false).await
  }

  async fn handle_dead(&self, mut msg: MessageHandoff) {
    let len = match Dead::decode_len(&mut msg.buf) {
      Ok(len) => len,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode dead message");
        return;
      }
    };

    let dead = match Dead::decode_from(msg.buf.split_to(len)) {
      Ok(dead) => dead,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode dead message");
        return;
      }
    };

    let mut memberlist = self.inner.nodes.write().await;
    if let Err(e) = self.dead_node(&mut memberlist, dead).await {
      tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to mark node as dead");
    }
  }

  async fn handle_user(&self, msg: MessageHandoff) {
    if let Some(d) = self.inner.delegate.as_ref() {
      if let Err(e) = d.notify_user_msg(msg.buf).await {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to handle user message");
      }
    }
  }

  fn ensure_can_connect(&self, from: IpAddr) -> Result<(), Error<D, T>> {
    if !self.inner.opts.ip_must_be_checked() {
      return Ok(());
    }

    self.inner.opts.ip_allowed(from).map_err(From::from)
  }
}
