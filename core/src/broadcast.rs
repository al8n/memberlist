use crate::{
  base::Memberlist,
  delegate::Delegate,
  error::Error,
  transport::{Transport, Wire},
  types::{Message, TinyVec},
};
use async_channel::Sender;

use nodecraft::{resolver::AddressResolver, CheapClone};

/// Something that can be broadcasted via gossip to
/// the memberlist cluster.
pub trait Broadcast: core::fmt::Debug + Send + Sync + 'static {
  type Id: Clone + Eq + core::hash::Hash + core::fmt::Debug + core::fmt::Display;
  type Message: Clone + core::fmt::Debug + Send + Sync + 'static;

  /// An optional extension of the Broadcast trait that
  /// gives each message a unique id and that is used to optimize
  fn id(&self) -> Option<&Self::Id>;

  /// Checks if enqueuing the current broadcast
  /// invalidates a previous broadcast
  fn invalidates(&self, other: &Self) -> bool;

  /// Returns the message
  fn message(&self) -> &Self::Message;

  /// Returns the encoded length of the message
  fn encoded_len(msg: &Self::Message) -> usize;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  fn finished(&self) -> impl std::future::Future<Output = ()> + Send;

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
pub(crate) struct MemberlistBroadcast<I, A, W> {
  node: I,
  msg: Message<I, A>,
  notify: Option<async_channel::Sender<()>>,
  _marker: std::marker::PhantomData<W>,
}

impl<I: core::fmt::Debug, A: core::fmt::Debug, W> core::fmt::Debug
  for MemberlistBroadcast<I, A, W>
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct(std::any::type_name::<Self>())
      .field("node", &self.node)
      .field("msg", &self.msg)
      .finish()
  }
}

impl<I, A, W> Broadcast for MemberlistBroadcast<I, A, W>
where
  I: CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Display
    + core::fmt::Debug
    + Send
    + Sync
    + 'static,
  W: Wire<Id = I, Address = A>,
{
  type Id = I;
  type Message = Message<I, A>;

  fn id(&self) -> Option<&Self::Id> {
    Some(&self.node)
  }

  fn invalidates(&self, other: &Self) -> bool {
    self.node.eq(&other.node)
  }

  fn message(&self) -> &Self::Message {
    &self.msg
  }

  fn encoded_len(msg: &Self::Message) -> usize {
    W::encoded_len(msg)
  }

  async fn finished(&self) {
    if let Some(tx) = &self.notify {
      if let Err(e) = tx.send(()).await {
        tracing::error!(
          target = "memberlist.broadcast",
          "broadcast failed to notify: {}",
          e
        );
      }
    }
  }

  fn is_unique(&self) -> bool {
    false
  }
}

impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[inline]
  pub(crate) async fn broadcast_notify(
    &self,
    node: T::Id,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    notify_tx: Option<Sender<()>>,
  ) {
    let _ = self.queue_broadcast(node, msg, notify_tx).await;
  }

  #[inline]
  pub(crate) async fn broadcast(
    &self,
    node: T::Id,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
    let _ = self.queue_broadcast(node, msg, None).await;
  }

  #[inline]
  async fn queue_broadcast(
    &self,
    node: T::Id,
    msg: Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    notify_tx: Option<Sender<()>>,
  ) {
    self
      .inner
      .broadcast
      .queue_broadcast(MemberlistBroadcast {
        node,
        msg,
        notify: notify_tx,
        _marker: std::marker::PhantomData,
      })
      .await
  }

  /// Used to return a slice of broadcasts to send up to
  /// a maximum byte size, while imposing a per-broadcast overhead. This is used
  /// to fill a UDP packet with piggybacked data
  #[inline]
  pub(crate) async fn get_broadcast_with_prepend(
    &self,
    to_send: TinyVec<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    overhead: usize,
    limit: usize,
  ) -> Result<TinyVec<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>, Error<T, D>>
  {
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
      for msg in to_send.iter() {
        bytes_used += <T::Wire as Wire>::encoded_len(msg) + overhead;
      }

      // Check space remaining for user messages
      let avail = limit.saturating_sub(bytes_used);
      if avail > overhead {
        to_send.extend(
          delegate
            .broadcast_messages(overhead, avail, |b| {
              let msg =
                Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::UserData(b);
              let len = <T::Wire as Wire>::encoded_len(&msg);
              (len, msg.unwrap_user_data())
            })
            .await
            .into_iter()
            .map(Message::UserData),
        );
      }
    }

    Ok(to_send)
  }
}
