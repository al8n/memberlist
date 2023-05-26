use async_channel::Sender;
use bytes::Bytes;
use showbiz_traits::{Broadcast, Delegate, Transport};
use showbiz_types::Name;

use crate::{showbiz::Showbiz, types::Message};

pub(crate) struct ShowbizBroadcast {
  node: Name,
  msg: Bytes,
  #[cfg(feature = "async")]
  notify: Option<async_channel::Sender<()>>,
  #[cfg(not(feature = "async"))]
  notify: Option<crossbeam_channel::Sender<()>>,
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl Broadcast for ShowbizBroadcast {
  #[cfg(feature = "async")]
  type Error = async_channel::SendError<()>;

  #[cfg(not(feature = "async"))]
  type Error = crossbeam_channel::SendError<()>;

  fn name(&self) -> &Name {
    &self.node
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node == other.node
  }

  fn message(&self) -> &Bytes {
    &self.msg
  }

  #[cfg(feature = "async")]
  async fn finished(&self) -> Result<(), Self::Error> {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()).await {
        tracing::error!(target = "showbiz", "failed to notify: {}", e);
        return Err(e);
      }
    }
    Ok(())
  }

  #[cfg(not(feature = "async"))]
  fn finished(&self) -> Result<(), Self::Error> {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()) {
        tracing::error!(target = "showbiz", "failed to notify: {}", e);
        return Err(e);
      }
    }
    Ok(())
  }

  fn is_unique(&self) -> bool {
    false
  }
}

#[cfg(feature = "async")]
impl<T: Transport, D: Delegate> Showbiz<T, D> {
  #[inline]
  pub(crate) async fn broadcast_notify(&self, node: Name, msg: Message, notify_tx: Sender<()>) {
    let _ = self.queue_broadcast(node, msg, Some(notify_tx)).await;
  }

  #[inline]
  pub(crate) async fn broadcast(&self, node: Name, msg: Message) {
    let _ = self.queue_broadcast(node, msg, None).await;
  }

  #[inline]
  pub(crate) async fn queue_broadcast(
    &self,
    node: Name,
    msg: Message,
    notify_tx: Option<Sender<()>>,
  ) -> Result<(), async_channel::SendError<()>> {
    self
      .inner
      .broadcast
      .queue_broadcast(ShowbizBroadcast {
        node,
        msg: msg.freeze(),
        notify: notify_tx,
      })
      .await
  }
}
