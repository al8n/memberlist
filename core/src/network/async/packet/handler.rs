use std::net::IpAddr;

use crate::showbiz::MessageHandoff;

use super::*;

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  /// a long running thread that processes messages received
  /// over the packet interface, but is decoupled from the listener to avoid
  /// blocking the listener which may cause ping/ack messages to be delayed.
  pub(crate) fn packet_handler<R, S>(&self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let this = self.clone();
    let shutdown_rx = this.inner.shutdown_rx.clone();
    let handoff_rx = this.inner.handoff_rx.clone();
    (spawner)(Box::pin(async move {
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          _ = handoff_rx.recv().fuse() => while let Some(msg) = this.get_next_message().await {
            match msg.msg_ty {
              MessageType::Suspect => this.handle_suspect(msg, spawner).await,
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
    }));
  }

  /// Returns the next message to process in priority order, using LIFO
  async fn get_next_message(&self) -> Option<MessageHandoff> {
    let mut queue = self.inner.queue.lock().await;
    queue.high.pop_back().or_else(|| queue.low.pop_back())
  }

  async fn handle_suspect<R, S>(&self, mut msg: MessageHandoff, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let len = match Suspect::decode_len(&mut msg.buf) {
      Ok(len) => len,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode suspect message");
        return;
      },
    };

    let suspect = match Suspect::decode_from(msg.buf.split_to(len)) {
      Ok(suspect) => suspect,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode suspect message");
        return;
      },
    };

    if let Err(e) = self.suspect_node(suspect, spawner).await {
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
      },
    };

    let mut alive = match Alive::decode_from(msg.buf.split_to(len)) {
      Ok(alive) => alive,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode alive message");
        return;
      },
    };

    if self.inner.opts.ip_must_be_checked() {
      let inner_ip = alive.node.addr();
      match inner_ip {
        NodeAddress::Ip(ip) => {
          if let Err(e) = self.inner.opts.ip_allowed(*ip) {
            tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "blocked alive message");
            return;
          }
        },
        NodeAddress::Domain(d) => {
          match d.as_str().parse() {
            Ok(ip) => {
              if let Err(e) = self.inner.opts.ip_allowed(ip) {
                tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "blocked alive message");
                return;
              }
            },
            Err(_) => {
              tracing::error!(target = "showbiz", err=%Error::<T, D>::ParseIpFailed(d.clone()), remote_addr = %msg.from, "blocked alive message");
              return;
            },
          }
        },
      }
    }

    alive.node.port.get_or_insert(self.inner.opts.bind_addr.port());

    if let Err(e) = self.alive_node(alive, None, false).await {
      tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to alive node");
    }
  }

  async fn handle_dead(&self, mut msg: MessageHandoff) {
    let len = match Dead::decode_len(&mut msg.buf) {
      Ok(len) => len,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode dead message");
        return;
      },
    };

    let dead = match Dead::decode_from(msg.buf.split_to(len)) {
      Ok(dead) => dead,
      Err(e) => {
        tracing::error!(target = "showbiz", err=%e, remote_addr = %msg.from, "failed to decode dead message");
        return;
      },
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

  fn ensure_can_connect(&self, from: IpAddr) -> Result<(), Error<T, D>> {
    if !self.inner.opts.ip_must_be_checked() {
      return Ok(());
    }

    self.inner.opts.ip_allowed(from).map_err(From::from)
  }
}
