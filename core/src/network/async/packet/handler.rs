use crate::showbiz::MessageHandoff;

use super::*;

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
  /// a long running thread that processes messages received
  /// over the packet interface, but is decoupled from the listener to avoid
  /// blocking the listener which may cause ping/ack messages to be delayed.
  pub(crate) fn packet_handler<R, S>(&self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let this = self.clone();
    (spawner)(Box::pin(async move {
      loop {
        futures_util::select! {
          _ = this.inner.shutdown_rx.recv().fuse() => {
            return;
          }
          _ = this.inner.handoff_rx.recv().fuse() => {
            loop {
              if let Some(msg) = self.get_next_message().await {
                match msg.msg_ty {
                  MessageType::Suspect => {
                    self.handle_suspect(msg).await;
                  }
                  MessageType::Alive => {
                    self.handle_alive(msg).await;
                  }
                  MessageType::Dead => {
                    self.handle_dead(msg).await;
                  }
                  MessageType::User => {
                    self.handle_user(msg).await;
                  }
                  _ => {
                    tracing::error!(target = "showbiz", "message type ({}) not supported {} (packet handler)", msg.msg_ty, msg.from);
                  }
                }
              } else {
                break;
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

  async fn handle_suspect(&self, msg: MessageHandoff) {}

  async fn handle_alive(&self, msg: MessageHandoff) {}

  async fn handle_dead(&self, msg: MessageHandoff) {}

  async fn handle_user(&self, msg: MessageHandoff) {}
}
