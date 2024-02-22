use std::{
  marker::PhantomData,
  sync::atomic::AtomicUsize,
  time::{Duration, Instant},
};

use memberlist_utils::SmallVec;
use nodecraft::Id;

use crate::{
  delegate::{mock::MockDelegate, AliveDelegate, CompositeDelegate, MergeDelegate},
  transport::MaybeResolvedAddress,
  types::{NodeState, State},
};

use super::*;

impl<I: Eq + core::hash::Hash, A, R> Members<I, A, R> {
  pub(crate) fn get_state<Q>(&self, id: &Q) -> Option<LocalNodeState<I, A>>
  where
    I: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq,
  {
    self
      .node_map
      .get(id)
      .map(|idx| self.nodes[*idx].state.clone())
  }

  pub(crate) fn set_state<Q>(&mut self, id: &Q, new_state: crate::types::State)
  where
    I: core::borrow::Borrow<Q>,
    Q: core::hash::Hash + Eq,
  {
    if let Some(idx) = self.node_map.get(id) {
      let state = &mut self.nodes[*idx].state;
      state.state = new_state;
    }
  }
}

impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  #[cfg(any(test, feature = "test"))]
  pub(crate) async fn change_node<F>(&self, id: &T::Id, f: F)
  where
    F: Fn(&mut LocalNodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  {
    let mut nodes = self.inner.nodes.write().await;
    if let Some(n) = nodes.node_map.get(id).copied() {
      f(&mut nodes.nodes[n].state);
    }
  }

  #[cfg(any(test, feature = "test"))]
  pub(crate) async fn get_node_state(&self, id: &T::Id) -> Option<crate::types::State> {
    let nodes = self.inner.nodes.read().await;
    nodes.node_map.get(id).map(|n| nodes.nodes[*n].state.state)
  }

  #[cfg(any(test, feature = "test"))]
  pub(crate) async fn get_node_state_change(&self, id: &T::Id) -> Option<std::time::Instant> {
    let nodes = self.inner.nodes.read().await;
    nodes.node_map.get(id).map(|n| nodes.nodes[*n].state_change)
  }
}

/// Unit tests for create a `Memberlist`.
pub async fn memberlist_create<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = Memberlist::new(t1, t1_opts).await.unwrap();

  R::sleep(Duration::from_millis(250)).await;

  let num = m.members().await;
  assert_eq!(num.len(), 1);

  let num = m.members_by(|state| state.state == State::Alive).await;
  assert_eq!(num.len(), 1);

  let num = m.num_members().await;
  assert_eq!(num, 1);

  let num = m.num_members_by(|state| state.state == State::Alive).await;
  assert_eq!(num, 1);
}

/// Unit tests for create a `Memberlist` and shutdown.
pub async fn memberlist_create_shutdown<T, R>(t1: T, t1_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m = Memberlist::new(t1, t1_opts).await.unwrap();

  R::sleep(Duration::from_millis(250)).await;

  let num = m.members().await;
  assert_eq!(num.len(), 1);

  let num = m
    .members_map_by(|state| {
      if state.state == State::Alive {
        Some(state.state)
      } else {
        None
      }
    })
    .await;
  assert_eq!(num.len(), 1);

  let num = m.num_members().await;
  assert_eq!(num, 1);

  m.shutdown().await.unwrap();
}

/// Unit tests for join a `Memberlist`.
pub async fn memberlist_join<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::new(t1, t1_opts).await.unwrap();
  let m2 = Memberlist::new(t2, t2_opts).await.unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  m2.join(target).await.unwrap();

  R::sleep(Duration::from_millis(250)).await;

  let num = m2.num_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);
  let num = m2.estimate_num_nodes();
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);
}

struct Canceled(&'static str);

impl std::fmt::Debug for Canceled {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "Custom {} canceled", self.0)
  }
}

impl std::fmt::Display for Canceled {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "Custom {} canceled", self.0)
  }
}

impl std::error::Error for Canceled {}

struct CustomMergeDelegate<I, A> {
  invoked: AtomicBool,
  _marker: PhantomData<(I, A)>,
}

impl<I, A> CustomMergeDelegate<I, A> {
  fn new() -> Self {
    Self {
      invoked: AtomicBool::new(false),
      _marker: PhantomData,
    }
  }
}

impl<I, A> MergeDelegate for CustomMergeDelegate<I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  type Id = I;
  type Address = A;
  type Error = Canceled;

  async fn notify_merge(
    &self,
    _peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
  ) -> Result<(), Self::Error> {
    tracing::info!("cancel merge");
    self.invoked.store(true, Ordering::SeqCst);
    Err(Canceled("merge"))
  }
}

/// Unit tests for join a `Memberlist` and cancel.
pub async fn memberlist_join_cancel<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::with_delegate(
    t1,
    CompositeDelegate::new().with_merge_delegate(CustomMergeDelegate::new()),
    t1_opts,
  )
  .await
  .unwrap();
  let m2 = Memberlist::with_delegate(
    t2,
    CompositeDelegate::new().with_merge_delegate(CustomMergeDelegate::new()),
    t2_opts,
  )
  .await
  .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  let err = m2.join_many([target].into_iter()).await.unwrap_err();
  let err = err.errors().values().next().unwrap();
  assert!(
    err.to_string().contains("Custom merge canceled"),
    "unexpected error: {}",
    err
  );

  // Check the hosts
  let num = m2.num_members().await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);

  let num = m1.num_members().await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);

  // Check delegate invocation
  let delegate = m2.delegate().unwrap().merge_delegate();

  assert!(delegate.invoked.load(Ordering::SeqCst));

  let delegate = m1.delegate().unwrap().merge_delegate();
  assert!(delegate.invoked.load(Ordering::SeqCst));
}

struct CustomAliveDelegate<I, A> {
  ignore: I,
  count: AtomicUsize,
  _marker: PhantomData<(I, A)>,
}

impl<I, A> CustomAliveDelegate<I, A> {
  fn new(ignore: I) -> Self {
    Self {
      ignore,
      count: AtomicUsize::new(0),
      _marker: PhantomData,
    }
  }
}

impl<I, A> AliveDelegate for CustomAliveDelegate<I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  type Id = I;
  type Address = A;
  type Error = Canceled;

  async fn notify_alive(
    &self,
    peer: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    self.count.fetch_add(1, Ordering::SeqCst);
    if self.ignore.eq(peer.id()) {
      return Ok(());
    }
    tracing::info!("Cancel alive");
    Err(Canceled("alive"))
  }
}

/// Unit tests for join a `Memberlist` and cancel passive.
pub async fn memberlist_join_cancel_passive<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let id1 = t1.local_id().clone();
  let m1 = Memberlist::with_delegate(
    t1,
    CompositeDelegate::new().with_alive_delegate(CustomAliveDelegate::new(id1)),
    t1_opts,
  )
  .await
  .unwrap();
  let id2 = t2.local_id().clone();
  let m2 = Memberlist::with_delegate(
    t2,
    CompositeDelegate::new().with_alive_delegate(CustomAliveDelegate::new(id2)),
    t2_opts,
  )
  .await
  .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  let res = m2.join_many([target].into_iter()).await.unwrap();
  assert_eq!(res.len(), 1, "unexpected num {}", res.len());

  let num = m2.num_members().await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);

  let num = m1.num_members().await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);

  // Check delegate invocation
  let delegate = m2.delegate().unwrap().alive_delegate();
  assert_eq!(delegate.count.load(Ordering::SeqCst), 1);

  let delegate = m1.delegate().unwrap().alive_delegate();
  assert_eq!(delegate.count.load(Ordering::SeqCst), 1);
}

/// Unit tests for join and shutdown a `Memberlist`.
pub async fn memberlist_join_shutdown<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::new(t1, t1_opts).await.unwrap();
  let m2 = Memberlist::new(t2, t2_opts).await.unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  m2.join(target).await.unwrap();

  let num = m2.num_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);

  m1.shutdown().await.unwrap();

  wait_for_condition(|| async {
    let num = m2.num_members().await;
    (num == 1, format!("expected 1 node, got {num}"))
  })
  .await;

  m2.shutdown().await.unwrap();
}

/// Unit test for join a dead node
pub async fn test_memberlist_join_dead_node() {}

/// Unit test for node delegate meta
pub async fn memberlist_node_delegate_meta<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::with_delegate(
    t1,
    CompositeDelegate::new().with_node_delegate(MockDelegate::<
      T::Id,
      <T::Resolver as AddressResolver>::ResolvedAddress,
    >::with_meta("web".into())),
    t1_opts,
  )
  .await
  .unwrap();

  let m2 = Memberlist::with_delegate(
    t2,
    CompositeDelegate::new().with_node_delegate(MockDelegate::with_meta("lb".into())),
    t2_opts,
  )
  .await
  .unwrap();

  let target = Node::new(
    m2.local_id().clone(),
    MaybeResolvedAddress::resolved(m2.advertise_address().clone()),
  );

  m1.join(target).await.unwrap();

  R::sleep(Duration::from_millis(250)).await;

  // Check the roles of members of m1
  let m1m = m1.members().await;
  assert_eq!(m1m.len(), 2, "expected 2 members, got {}", m1m.len());

  let roles = m1m
    .into_iter()
    .map(|state| (state.id().clone(), state.meta().clone()))
    .collect::<HashMap<_, _>>();

  assert_eq!(
    roles.get(m1.local_id()).unwrap(),
    "web",
    "bad role for {}",
    m1.local_id()
  );
  assert_eq!(
    roles.get(m2.local_id()).unwrap(),
    "lb",
    "bad role for {}",
    m2.local_id()
  );

  let m2m = m2.members().await;
  assert_eq!(m2m.len(), 2, "expected 2 members, got {}", m2m.len());

  let roles = m2m
    .into_iter()
    .map(|state| (state.id().clone(), state.meta().clone()))
    .collect::<HashMap<_, _>>();

  assert_eq!(
    roles.get(m1.local_id()).unwrap(),
    "web",
    "bad role for {}",
    m1.local_id()
  );
  assert_eq!(
    roles.get(m2.local_id()).unwrap(),
    "lb",
    "bad role for {}",
    m2.local_id()
  );
}

/// Unit test for node delegate meta update
pub async fn memberlist_node_delegate_meta_update() {}

/// Unit test for user data
pub async fn test_memberlist_user_data() {}

/// Unit test for send
pub async fn test_memberlist_send() {}

/// Unit tests for leave
pub async fn memberlist_leave<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::new(t1, t1_opts).await.unwrap();
  let m2 = Memberlist::new(t2, t2_opts).await.unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  m2.join(target).await.unwrap();

  let num = m2.num_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);

  m1.leave(Duration::from_secs(1)).await.unwrap();
  // wait for leave
  R::sleep(Duration::from_millis(10)).await;

  // m1 should think dead
  let num = m1
    .num_members_by(|state| state.state != State::Dead && state.state != State::Left)
    .await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);

  let num = m2
    .num_members_by(|state| state.state != State::Dead && state.state != State::Left)
    .await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);
  let state = m2.get_node_state(m1.local_id()).await.unwrap();
  assert_eq!(state, State::Left, "bad state");
}

/// Unit test for advertise addr
pub async fn test_memberlist_advertise_addr() {}

/// Unit test for conflict delegate
pub async fn test_memberlist_conflict_delegate() {}

/// Unit test for ping delegate
pub async fn test_memberlist_ping_delegate() {}

async fn wait_for_condition<'a, Fut, F>(mut f: F)
where
  F: FnMut() -> Fut,
  Fut: Future<Output = (bool, String)> + 'a,
{
  let start = Instant::now();
  let mut msg = String::new();
  while start.elapsed() < Duration::from_secs(20) {
    let (done, msg1) = f().await;
    if done {
      return;
    }
    msg = msg1;
    std::thread::sleep(Duration::from_secs(5));
  }
  panic!("timeout waiting for condition {}", msg);
}
