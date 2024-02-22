use std::sync::Arc;

use bytes::Bytes;
use nodecraft::{CheapClone, Id};

use crate::types::{NodeState, SmallVec};

#[cfg(any(test, feature = "test"))]
#[doc(hidden)]
pub mod mock;

mod alive;
pub use alive::*;

mod conflict;
pub use conflict::*;

mod composite;
pub use composite::*;

mod event;
pub use event::*;

mod node;
pub use node::*;

mod merge;
pub use merge::*;

mod ping;
pub use ping::*;

/// Error trait for [`Delegate`]
pub trait DelegateError: std::error::Error + Send + Sync + 'static {
  type AliveDelegateError: std::error::Error + Send + Sync + 'static;
  type MergeDelegateError: std::error::Error + Send + Sync + 'static;

  fn alive(err: Self::AliveDelegateError) -> Self;

  fn merge(err: Self::MergeDelegateError) -> Self;
}

pub trait Delegate<I, A>:
  NodeDelegate<Id = I, Address = A>
  + PingDelegate<Id = I, Address = A>
  + EventDelegate<Id = I, Address = A>
  + ConflictDelegate<Id = I, Address = A>
  + AliveDelegate<Id = I, Address = A>
  + MergeDelegate<Id = I, Address = A>
{
  /// The error type of the delegate
  type Error: DelegateError<
    AliveDelegateError = <Self as AliveDelegate>::Error,
    MergeDelegateError = <Self as MergeDelegate>::Error,
  >;
}

#[derive(Debug, Copy, Clone)]
pub struct VoidDelegateError;

impl std::fmt::Display for VoidDelegateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "void delegate error")
  }
}

impl std::error::Error for VoidDelegateError {}

impl DelegateError for VoidDelegateError {
  type AliveDelegateError = Self;

  type MergeDelegateError = Self;

  fn alive(err: Self::AliveDelegateError) -> Self {
    err
  }

  fn merge(err: Self::MergeDelegateError) -> Self {
    err
  }
}

#[derive(Debug, Copy, Clone)]
pub struct VoidDelegate<I, A>(core::marker::PhantomData<(I, A)>);

impl<I, A> Default for VoidDelegate<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> VoidDelegate<I, A> {
  /// Creates a new [`VoidDelegate`].
  #[inline]
  pub const fn new() -> Self {
    Self(core::marker::PhantomData)
  }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> AliveDelegate for VoidDelegate<I, A> {
  type Error = VoidDelegateError;
  type Id = I;
  type Address = A;

  async fn notify_alive(
    &self,
    _peer: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> MergeDelegate for VoidDelegate<I, A> {
  type Error = VoidDelegateError;
  type Id = I;
  type Address = A;

  async fn notify_merge(
    &self,
    _peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> ConflictDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;

  async fn notify_conflict(
    &self,
    _existing: Arc<NodeState<Self::Id, Self::Address>>,
    _other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
  }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> PingDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;

  async fn ack_payload(&self) -> Bytes {
    Bytes::new()
  }

  async fn notify_ping_complete(
    &self,
    _node: Arc<NodeState<Self::Id, Self::Address>>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) {
  }

  fn disable_promised_pings(&self, _target: &Self::Id) -> bool {
    false
  }
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> EventDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;

  async fn notify_join(&self, _node: Arc<NodeState<Self::Id, Self::Address>>) {}

  async fn notify_leave(&self, _node: Arc<NodeState<Self::Id, Self::Address>>) {}

  async fn notify_update(&self, _node: Arc<NodeState<Self::Id, Self::Address>>) {}
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> NodeDelegate for VoidDelegate<I, A> {
  type Id = I;
  type Address = A;

  async fn node_meta(&self, _limit: usize) -> Bytes {
    Bytes::new()
  }

  async fn notify_message(&self, _msg: Bytes) {}

  async fn broadcast_messages<F>(
    &self,
    _overhead: usize,
    _limit: usize,
    _encoded_len: F,
  ) -> SmallVec<Bytes>
  where
    F: Fn(Bytes) -> (usize, Bytes) + Send,
  {
    SmallVec::new()
  }

  async fn local_state(&self, _join: bool) -> Bytes {
    Bytes::new()
  }

  async fn merge_remote_state(&self, _buf: Bytes, _join: bool) {}
}

impl<I: Id, A: CheapClone + Send + Sync + 'static> Delegate<I, A> for VoidDelegate<I, A> {
  type Error = VoidDelegateError;
}
