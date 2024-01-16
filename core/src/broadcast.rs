use crate::{
  delegate::Delegate, error::Error, network::USER_MSG_OVERHEAD, showbiz::Showbiz,
  transport::Transport, types::Message,
};
use async_channel::Sender;
use nodecraft::{resolver::AddressResolver, Node};

/// Something that can be broadcasted via gossip to
/// the memberlist cluster.
pub trait Broadcast: Send + Sync + 'static {
  type Id: Clone + Eq + core::hash::Hash + core::fmt::Debug + core::fmt::Display;

  /// An optional extension of the Broadcast trait that
  /// gives each message a unique id and that is used to optimize
  fn id(&self) -> Option<&Self::Id>;

  /// Checks if enqueuing the current broadcast
  /// invalidates a previous broadcast
  fn invalidates(&self, other: &Self) -> bool;

  /// Returns bytes form of the message
  fn message(&self) -> &Message;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached

  async fn finished(&self);

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  #[cfg(feature = "nightly")]
  fn finished<'a>(&'a self) -> impl std::future::Future<Output = ()> + Send + 'a;

  /// Indicates that each message is
  /// intrinsically unique and there is no need to scan the broadcast queue for
  /// duplicates.
  ///
  /// You should ensure that `invalidates` always returns false if implementing
  /// this.
  fn is_unique(&self) -> bool {
    false
  }
}

#[viewit::viewit]
pub(crate) struct ShowbizBroadcast<I, A> {
  node: Node<I, A>,
  msg: Message,
  notify: Option<async_channel::Sender<()>>,
}

impl<I, A> Broadcast for ShowbizBroadcast<I, A> {
  type Id = Node<I, A>;

  fn id(&self) -> Option<&Self::Id> {
    Some(&self.node)
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node == other.node
  }

  fn message(&self) -> &Message {
    &self.msg
  }

  async fn finished(&self) {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()).await {
        tracing::error!(target = "showbiz", "broadcast failed to notify: {}", e);
        return;
      }
    }
  }

  #[cfg(feature = "nightly")]
  fn finished<'a>(&'a self) -> impl std::future::Future<Output = ()> + Send + 'a {
    async move {
      if let Some(tx) = &self.notify {
        if let Err(e) = tx.send(()).await {
          tracing::error!(target = "showbiz", "failed to notify: {}", e);
          return;
        }
      }
    }
  }

  fn is_unique(&self) -> bool {
    false
  }
}

impl<D: Delegate, T: Transport> Showbiz<T, D> {
  #[inline]
  pub(crate) async fn broadcast_notify(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    msg: Message,
    notify_tx: Option<Sender<()>>,
  ) {
    let _ = self.queue_broadcast(node, msg, notify_tx).await;
  }

  #[inline]
  pub(crate) async fn broadcast(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    msg: Message,
  ) {
    let _ = self.queue_broadcast(node, msg, None).await;
  }

  #[inline]
  pub(crate) async fn queue_broadcast(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
    msg: Message,
    notify_tx: Option<Sender<()>>,
  ) {
    self
      .inner
      .broadcast
      .queue_broadcast(ShowbizBroadcast {
        node,
        msg,
        notify: notify_tx,
      })
      .await
  }

  /// Used to return a slice of broadcasts to send up to
  /// a maximum byte size, while imposing a per-broadcast overhead. This is used
  /// to fill a UDP packet with piggybacked data
  #[inline]
  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    to_send: Vec<Message>,
    overhead: usize,
    limit: usize,
  ) -> Result<Vec<Message>, Error<T, D>> {
    // Get memberlist messages first
    let mut to_send = self
      .inner
      .broadcast
      .get_broadcast_with_prepend(to_send, overhead, limit)
      .await;

    // Check if the user has anything to broadcast
    if let Some(delegate) = &self.delegate {
      // Determine the bytes used already
      let mut bytes_used = 0;
      for msg in &to_send {
        bytes_used += msg.0.len() + overhead;
      }

      // Check space remaining for user messages
      let avail = limit.saturating_sub(bytes_used);
      if avail > overhead + USER_MSG_OVERHEAD {
        to_send.extend(
          delegate
            .get_broadcasts(overhead + USER_MSG_OVERHEAD, avail)
            .await
            .map_err(Error::delegate)?,
        );
      }
    }

    Ok(to_send)
  }
}
