use std::{
  sync::{atomic::AtomicU32, Arc},
  time::Instant,
};

use super::{
  delegate::Delegate,
  error::Error,
  memberlist::Memberlist,
  transport::Transport,
  types::{Ack, Nack, Server, ServerState},
};

mod r#async;

// #[cfg(feature = "test")]
// pub use r#async::tests::*;

#[viewit::viewit]
#[derive(Debug, Clone)]
pub(crate) struct LocalServerState<I, A> {
  server: Arc<Server<I, A>>,
  incarnation: Arc<AtomicU32>,
  state_change: Instant,
  /// The current state of the node
  state: ServerState,
}

impl<I, A> core::ops::Deref for LocalServerState<I, A> {
  type Target = Server<I, A>;

  fn deref(&self) -> &Self::Target {
    &self.server
  }
}

impl<I, A> LocalServerState<I, A> {
  #[inline]
  pub(crate) fn dead_or_left(&self) -> bool {
    self.state == ServerState::Dead || self.state == ServerState::Left
  }
}

// private implementation
impl<D, T> Memberlist<T, D>
where
  T: Transport,
  D: Delegate,
{
  /// Returns a usable sequence number in a thread safe way
  #[inline]
  pub(crate) fn next_seq_no(&self) -> u32 {
    self
      .inner
      .hot
      .sequence_num
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
      + 1
  }

  /// Returns the next incarnation number in a thread safe way
  #[inline]
  pub(crate) fn next_incarnation(&self) -> u32 {
    self
      .inner
      .hot
      .incarnation
      .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
      + 1
  }

  /// Adds the positive offset to the incarnation number.
  #[inline]
  pub(crate) fn skip_incarnation(&self, offset: u32) -> u32 {
    self
      .inner
      .hot
      .incarnation
      .fetch_add(offset, std::sync::atomic::Ordering::SeqCst)
      + offset
  }

  /// Used to get the current estimate of the number of nodes
  #[inline]
  pub(crate) fn estimate_num_nodes(&self) -> u32 {
    self
      .inner
      .hot
      .num_nodes
      .load(std::sync::atomic::Ordering::SeqCst)
  }

  #[inline]
  pub(crate) fn has_shutdown(&self) -> bool {
    self
      .inner
      .hot
      .shutdown
      .load(std::sync::atomic::Ordering::SeqCst)
  }

  #[inline]
  pub(crate) fn has_left(&self) -> bool {
    self
      .inner
      .hot
      .leave
      .load(std::sync::atomic::Ordering::SeqCst)
  }

  #[inline]
  pub(crate) async fn invoke_ack_handler(&self, ack: Ack, timestamp: Instant) {
    let ah = self.inner.ack_handlers.lock().await.remove(&ack.seq_no);
    if let Some(handler) = ah {
      handler.timer.stop().await;
      (handler.ack_fn)(ack.payload, timestamp).await;
    }
  }

  #[inline]
  pub(crate) async fn invoke_nack_handler(&self, nack: Nack) {
    let ah = self
      .inner
      .ack_handlers
      .lock()
      .await
      .get(&nack.seq_no)
      .and_then(|ah| ah.nack_fn.clone());
    if let Some(nack_fn) = ah {
      (nack_fn)().await;
    }
  }
}
