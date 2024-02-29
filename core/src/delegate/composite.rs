use super::*;

/// `CompositeDelegate` is a helpful struct to split the [`Delegate`] into multiple small delegates,
/// so that users do not need to implement full [`Delegate`] when they only want to custom some methods
/// in the [`Delegate`].
pub struct CompositeDelegate<
  I,
  Address,
  A = VoidDelegate<I, Address>,
  C = VoidDelegate<I, Address>,
  E = VoidDelegate<I, Address>,
  M = VoidDelegate<I, Address>,
  N = VoidDelegate<I, Address>,
  P = VoidDelegate<I, Address>,
> {
  alive_delegate: A,
  conflict_delegate: C,
  event_delegate: E,
  merge_delegate: M,
  node_delegate: N,
  ping_delegate: P,
  _m: std::marker::PhantomData<(I, Address)>,
}

impl<I, Address> Default for CompositeDelegate<I, Address> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, Address> CompositeDelegate<I, Address> {
  /// Create a new `CompositeDelegate`
  #[inline]
  pub const fn new() -> Self {
    Self {
      alive_delegate: VoidDelegate::new(),
      conflict_delegate: VoidDelegate::new(),
      event_delegate: VoidDelegate::new(),
      merge_delegate: VoidDelegate::new(),
      node_delegate: VoidDelegate::new(),
      ping_delegate: VoidDelegate::new(),
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  /// Set the alive delegate
  pub fn with_alive_delegate<NA>(
    self,
    alive_delegate: NA,
  ) -> CompositeDelegate<I, Address, NA, C, E, M, N, P> {
    CompositeDelegate {
      alive_delegate,
      conflict_delegate: self.conflict_delegate,
      event_delegate: self.event_delegate,
      merge_delegate: self.merge_delegate,
      node_delegate: self.node_delegate,
      ping_delegate: self.ping_delegate,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  /// Set the conflict delegate
  pub fn with_conflict_delegate<NC>(
    self,
    conflict_delegate: NC,
  ) -> CompositeDelegate<I, Address, A, NC, E, M, N, P> {
    CompositeDelegate {
      alive_delegate: self.alive_delegate,
      conflict_delegate,
      event_delegate: self.event_delegate,
      merge_delegate: self.merge_delegate,
      node_delegate: self.node_delegate,
      ping_delegate: self.ping_delegate,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  /// Set the event delegate
  pub fn with_event_delegate<NE>(
    self,
    event_delegate: NE,
  ) -> CompositeDelegate<I, Address, A, C, NE, M, N, P> {
    CompositeDelegate {
      alive_delegate: self.alive_delegate,
      conflict_delegate: self.conflict_delegate,
      event_delegate,
      merge_delegate: self.merge_delegate,
      node_delegate: self.node_delegate,
      ping_delegate: self.ping_delegate,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  /// Set the merge delegate
  pub fn with_merge_delegate<NM>(
    self,
    merge_delegate: NM,
  ) -> CompositeDelegate<I, Address, A, C, E, NM, N, P> {
    CompositeDelegate {
      alive_delegate: self.alive_delegate,
      conflict_delegate: self.conflict_delegate,
      event_delegate: self.event_delegate,
      merge_delegate,
      node_delegate: self.node_delegate,
      ping_delegate: self.ping_delegate,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  /// Set the node delegate
  pub fn with_node_delegate<NN>(
    self,
    node_delegate: NN,
  ) -> CompositeDelegate<I, Address, A, C, E, M, NN, P> {
    CompositeDelegate {
      alive_delegate: self.alive_delegate,
      conflict_delegate: self.conflict_delegate,
      event_delegate: self.event_delegate,
      merge_delegate: self.merge_delegate,
      node_delegate,
      ping_delegate: self.ping_delegate,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  /// Set the ping delegate
  pub fn with_ping_delegate<NP>(
    self,
    ping_delegate: NP,
  ) -> CompositeDelegate<I, Address, A, C, E, M, N, NP> {
    CompositeDelegate {
      alive_delegate: self.alive_delegate,
      conflict_delegate: self.conflict_delegate,
      event_delegate: self.event_delegate,
      merge_delegate: self.merge_delegate,
      node_delegate: self.node_delegate,
      ping_delegate,
      _m: std::marker::PhantomData,
    }
  }
}

#[cfg(any(feature = "test", test))]
impl<I, Address, A, C, E, M, N, P> CompositeDelegate<I, Address, A, C, E, M, N, P> {
  pub(crate) fn node_delegate(&self) -> &N {
    &self.node_delegate
  }

  pub(crate) fn event_delegate(&self) -> &E {
    &self.event_delegate
  }

  pub(crate) fn merge_delegate(&self) -> &M {
    &self.merge_delegate
  }

  pub(crate) fn alive_delegate(&self) -> &A {
    &self.alive_delegate
  }

  pub(crate) fn conflict_delegate(&self) -> &C {
    &self.conflict_delegate
  }

  pub(crate) fn ping_delegate(&self) -> &P {
    &self.ping_delegate
  }
}

impl<I, Address, A, C, E, M, N, P> AliveDelegate for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  type Error = A::Error;
  type Id = I;
  type Address = Address;

  async fn notify_alive(
    &self,
    peer: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    self.alive_delegate.notify_alive(peer).await
  }
}

impl<I, Address, A, C, E, M, N, P> MergeDelegate for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  type Error = M::Error;
  type Id = I;
  type Address = Address;

  async fn notify_merge(
    &self,
    peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
  ) -> Result<(), Self::Error> {
    self.merge_delegate.notify_merge(peers).await
  }
}

impl<I, Address, A, C, E, M, N, P> ConflictDelegate
  for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  type Id = I;
  type Address = Address;

  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
    self
      .conflict_delegate
      .notify_conflict(existing, other)
      .await
  }
}

impl<I, Address, A, C, E, M, N, P> PingDelegate for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  type Id = I;
  type Address = Address;

  async fn ack_payload(&self) -> Bytes {
    self.ping_delegate.ack_payload().await
  }

  async fn notify_ping_complete(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) {
    self
      .ping_delegate
      .notify_ping_complete(node, rtt, payload)
      .await
  }

  fn disable_promised_pings(&self, target: &Self::Id) -> bool {
    self.ping_delegate.disable_promised_pings(target)
  }
}

impl<I, Address, A, C, E, M, N, P> EventDelegate for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  type Id = I;

  type Address = Address;

  async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    self.event_delegate.notify_join(node).await
  }

  async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    self.event_delegate.notify_leave(node).await
  }

  async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    self.event_delegate.notify_update(node).await
  }
}

impl<I, Address, A, C, E, M, N, P> NodeDelegate for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  async fn node_meta(&self, limit: usize) -> Bytes {
    self.node_delegate.node_meta(limit).await
  }

  async fn notify_message(&self, msg: Bytes) {
    self.node_delegate.notify_message(msg).await
  }

  async fn broadcast_messages<F>(
    &self,
    overhead: usize,
    limit: usize,
    encoded_len: F,
  ) -> SmallVec<Bytes>
  where
    F: Fn(Bytes) -> (usize, Bytes) + Send,
  {
    self
      .node_delegate
      .broadcast_messages(overhead, limit, encoded_len)
      .await
  }

  async fn local_state(&self, join: bool) -> Bytes {
    self.node_delegate.local_state(join).await
  }

  async fn merge_remote_state(&self, buf: Bytes, join: bool) {
    self.node_delegate.merge_remote_state(buf, join).await
  }
}

impl<I, Address, A, C, E, M, N, P> Delegate for CompositeDelegate<I, Address, A, C, E, M, N, P>
where
  I: Id,
  Address: CheapClone + Send + Sync + 'static,
  A: AliveDelegate<Id = I, Address = Address>,
  C: ConflictDelegate<Id = I, Address = Address>,
  E: EventDelegate<Id = I, Address = Address>,
  M: MergeDelegate<Id = I, Address = Address>,
  N: NodeDelegate,
  P: PingDelegate<Id = I, Address = Address>,
{
  type Address = Address;
  type Id = I;
}
