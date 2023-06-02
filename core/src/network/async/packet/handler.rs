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
              MessageType::Suspect => {
                this.handle_suspect(msg).await;
              }
              MessageType::Alive => {
                this.handle_alive(msg).await;
              }
              MessageType::Dead => {
                this.handle_dead(msg).await;
              }
              MessageType::User => {
                this.handle_user(msg).await;
              }
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

  async fn handle_suspect(&self, _msg: MessageHandoff) {}

  async fn handle_alive(&self, _msg: MessageHandoff) {}

  async fn handle_dead(&self, _msg: MessageHandoff) {}

  async fn handle_user(&self, _msg: MessageHandoff) {}
}
