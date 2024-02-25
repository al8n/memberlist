use std::{
  marker::PhantomData,
  sync::atomic::AtomicUsize,
  time::{Duration, Instant},
};

use bytes::Bytes;
use memberlist_utils::{Label, SmallVec};
use nodecraft::Id;

use crate::{
  delegate::{
    mock::MockDelegate, AliveDelegate, CompositeDelegate, ConflictDelegate, MergeDelegate,
    PingDelegate,
  },
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

/// Unit tests for join a `Memberlist` with labels.
pub async fn memberlist_join_with_labels<F, T, R>(mut get_transport: impl FnMut(usize, Label) -> F)
where
  F: Future<Output = T>,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let label1 = Label::try_from("blah").unwrap();
  let m1 = Memberlist::new(get_transport(1, label1.clone()).await, Options::lan())
    .await
    .unwrap();
  let m2 = Memberlist::new(get_transport(2, label1.clone()).await, Options::lan())
    .await
    .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  m2.join(target.clone()).await.unwrap();

  let m1m = m1.num_online_members().await;
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);
  let m1m = m1.estimate_num_nodes();
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);

  let m2m = m2.num_online_members().await;
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);
  let m2m = m2.estimate_num_nodes();
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);

  // Create a third node that uses no label
  let m3 = Memberlist::new(get_transport(3, Label::empty()).await, Options::lan())
    .await
    .unwrap();
  m3.join(target.clone()).await.unwrap_err();

  let m1m = m1.num_online_members().await;
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);
  let m1m = m1.estimate_num_nodes();
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);

  let m2m = m2.num_online_members().await;
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);
  let m2m = m2.estimate_num_nodes();
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);

  let m3m = m3.num_online_members().await;
  assert_eq!(m3m, 1, "expected 1 member, got {}", m3m);
  let m3m = m3.estimate_num_nodes();
  assert_eq!(m3m, 1, "expected 1 member, got {}", m3m);

  // Create a fourth node that uses a mismatched label
  let label = Label::try_from("not-blah").unwrap();
  let m4 = Memberlist::new(get_transport(4, label).await, Options::lan())
    .await
    .unwrap();
  m4.join(target).await.unwrap_err();

  let m1m = m1.num_online_members().await;
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);
  let m1m = m1.estimate_num_nodes();
  assert_eq!(m1m, 2, "expected 2 members, got {}", m1m);

  let m2m = m2.num_online_members().await;
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);
  let m2m = m2.estimate_num_nodes();
  assert_eq!(m2m, 2, "expected 2 members, got {}", m2m);

  let m3m = m3.num_online_members().await;
  assert_eq!(m3m, 1, "expected 1 member, got {}", m3m);
  let m3m = m3.estimate_num_nodes();
  assert_eq!(m3m, 1, "expected 1 member, got {}", m3m);

  let m4m = m4.num_online_members().await;
  assert_eq!(m4m, 1, "expected 1 member, got {}", m4m);
  let m4m = m4.estimate_num_nodes();
  assert_eq!(m4m, 1, "expected 1 member, got {}", m4m);

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
  m4.shutdown().await.unwrap();
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
  assert_ne!(
    delegate.count.load(Ordering::SeqCst),
    0,
    "should invoke delegate"
  );

  let delegate = m1.delegate().unwrap().alive_delegate();
  assert_ne!(
    delegate.count.load(Ordering::SeqCst),
    0,
    "should invoke delegate"
  );
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
    let num = m2.num_online_members().await;
    (num == 1, format!("expected 1 node, got {num}"))
  })
  .await;

  m2.shutdown().await.unwrap();
}

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
pub async fn memberlist_node_delegate_meta_update<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t2_opts: Options,
) where
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

  // Update the meta data roles
  m1.delegate()
    .unwrap()
    .node_delegate()
    .set_meta("api".into())
    .await;

  m2.delegate()
    .unwrap()
    .node_delegate()
    .set_meta("db".into())
    .await;

  m1.update_node(Duration::ZERO).await.unwrap();

  m2.update_node(Duration::ZERO).await.unwrap();

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
    "api",
    "bad role for {}",
    m1.local_id()
  );
  assert_eq!(
    roles.get(m2.local_id()).unwrap(),
    "db",
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
    "api",
    "bad role for {}",
    m1.local_id()
  );
  assert_eq!(
    roles.get(m2.local_id()).unwrap(),
    "db",
    "bad role for {}",
    m2.local_id()
  );
}

/// Unit test for user data
pub async fn memberlist_user_data<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::with_delegate(
    t1,
    CompositeDelegate::new()
      .with_node_delegate(MockDelegate::with_state(Bytes::from_static(b"something"))),
    t1_opts
      .with_gossip_interval(Duration::from_millis(100))
      .with_push_pull_interval(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let bcasts = (0..256)
    .map(|i| i.to_string().as_bytes().to_vec().into())
    .collect::<SmallVec<_>>();

  let m2 = Memberlist::with_delegate(
    t2,
    CompositeDelegate::new().with_node_delegate(MockDelegate::with_state_and_broadcasts(
      Bytes::from_static(b"my state"),
      bcasts.clone(),
    )),
    t2_opts
      .with_gossip_interval(Duration::from_millis(100))
      .with_push_pull_interval(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );

  m2.join(target).await.unwrap();

  // Check the hosts
  let num = m2.num_online_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);

  // Wait for a little while
  R::sleep(Duration::from_millis(1500)).await;

  let mut msg1 = m1.delegate().unwrap().node_delegate().get_messages().await;
  msg1.sort();

  assert_eq!(msg1.as_slice(), bcasts.as_slice());
  let rs1 = m1
    .delegate()
    .unwrap()
    .node_delegate()
    .get_remote_state()
    .await;
  let rs2 = m2
    .delegate()
    .unwrap()
    .node_delegate()
    .get_remote_state()
    .await;

  assert_eq!(rs1.as_ref(), b"my state");
  assert_eq!(rs2.as_ref(), b"something");
  assert_eq!(msg1.len(), 256, "expected 256 messages, got {}", msg1.len());
}

/// Unit test for send
pub async fn memberlist_send<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::with_delegate(
    t1,
    CompositeDelegate::new().with_node_delegate(MockDelegate::new()),
    t1_opts
      .with_gossip_interval(Duration::from_millis(1))
      .with_push_pull_interval(Duration::from_millis(1)),
  )
  .await
  .unwrap();

  let m2 = Memberlist::with_delegate(
    t2,
    CompositeDelegate::new().with_node_delegate(MockDelegate::new()),
    t2_opts
      .with_gossip_interval(Duration::from_millis(1))
      .with_push_pull_interval(Duration::from_millis(1)),
  )
  .await
  .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );

  m2.join(target).await.unwrap();

  // Check the hots
  let num = m2.num_online_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);

  // Try to do a direct send
  m1.send(m2.advertise_address(), "ping".into())
    .await
    .unwrap();

  m2.send(m1.advertise_address(), "pong".into())
    .await
    .unwrap();

  wait_for_condition(|| async {
    let msgs = m1.delegate().unwrap().node_delegate().get_messages().await;

    (
      msgs.len() == 1 && msgs[0].as_ref() == b"pong",
      format!("expected 1 messages, got {}", msgs.len()),
    )
  })
  .await;

  wait_for_condition(|| async {
    let msgs = m2.delegate().unwrap().node_delegate().get_messages().await;

    (
      msgs.len() == 1 && msgs[0].as_ref() == b"ping",
      format!("expected 1 messages, got {}", msgs.len()),
    )
  })
  .await;
}

/// Unit tests for leave
pub async fn memberlist_leave<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::new(t1, t1_opts.with_gossip_interval(Duration::from_millis(1)))
    .await
    .unwrap();
  let m2 = Memberlist::new(t2, t2_opts.with_gossip_interval(Duration::from_millis(1)))
    .await
    .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );
  m2.join(target).await.unwrap();

  let num = m2.num_online_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);

  let num = m1.num_online_members().await;
  assert_eq!(num, 2, "should have 2 nodes! got {}", num);

  m1.leave(Duration::from_secs(1)).await.unwrap();
  // wait for leave
  R::sleep(Duration::from_millis(10)).await;

  // m1 should think dead
  let num = m1.num_online_members().await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);

  let num = m2.num_online_members().await;
  assert_eq!(num, 1, "should have 1 node! got {}", num);
  let state = m2.get_node_state(m1.local_id()).await.unwrap();
  assert_eq!(state, State::Left, "bad state");
}

struct CustomConflictDelegateInner<I, A> {
  existing: Option<Node<I, A>>,
  other: Option<Node<I, A>>,
  _marker: PhantomData<(I, A)>,
}

struct CustomConflictDelegate<I, A>(Mutex<CustomConflictDelegateInner<I, A>>);

impl<I, A> CustomConflictDelegate<I, A> {
  fn new() -> Self {
    Self(Mutex::new(CustomConflictDelegateInner {
      existing: None,
      other: None,
      _marker: PhantomData,
    }))
  }
}

impl<I, A> ConflictDelegate for CustomConflictDelegate<I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  type Id = I;
  type Address = A;

  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
    let mut inner = self.0.lock().await;
    inner.existing = Some(Node::new(existing.id().clone(), existing.address().clone()));
    inner.other = Some(Node::new(other.id().clone(), other.address().clone()));
  }
}

/// Unit test for conflict delegate
pub async fn memberlist_conflict_delegate<F, T, R>(
  mut get_transport: impl FnMut(T::Id) -> F,
  id: T::Id,
) where
  F: Future<Output = T>,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1 = Memberlist::with_delegate(
    get_transport(id.clone()).await,
    CompositeDelegate::new().with_conflict_delegate(CustomConflictDelegate::new()),
    Options::lan(),
  )
  .await
  .unwrap();

  let m2 = Memberlist::new(get_transport(id).await, Options::lan())
    .await
    .unwrap();

  let target = Node::new(
    m2.local_id().clone(),
    MaybeResolvedAddress::resolved(m2.advertise_address().clone()),
  );

  m1.join(target).await.unwrap();

  R::sleep(Duration::from_millis(250)).await;

  // Ensure we were notified
  let inner = m1.delegate().unwrap().conflict_delegate().0.lock().await;

  assert!(inner.existing.is_some());
  assert!(inner.other.is_some());

  assert_eq!(
    inner.existing.as_ref().map(|n| n.id()),
    inner.other.as_ref().map(|n| n.id())
  );
}

struct CustomPingDelegateInner<I, A> {
  payload: Bytes,
  rtt: Duration,
  other: Option<Node<I, A>>,
  _marker: PhantomData<(I, A)>,
}

struct CustomPingDelegate<I, A>(Mutex<CustomPingDelegateInner<I, A>>);

impl<I, A> CustomPingDelegate<I, A> {
  fn new() -> Self {
    Self(Mutex::new(CustomPingDelegateInner {
      payload: Bytes::new(),
      rtt: Duration::from_secs(0),
      other: None,
      _marker: PhantomData,
    }))
  }
}

impl<I, A> PingDelegate for CustomPingDelegate<I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  type Id = I;
  type Address = A;

  async fn ack_payload(&self) -> Bytes {
    Bytes::from_static(b"whatever")
  }

  async fn notify_ping_complete(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
    rtt: Duration,
    payload: Bytes,
  ) {
    let mut inner = self.0.lock().await;
    inner.rtt = rtt;
    inner.payload = payload;
    inner.other = Some(Node::new(node.id().clone(), node.address().clone()));
  }
}

/// Unit test for ping delegate
pub async fn memberlist_ping_delegate<T, R>(t1: T, t1_opts: Options, t2: T, t2_opts: Options)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let probe_interval = t1_opts.probe_interval();
  let m1 = Memberlist::with_delegate(
    t1,
    CompositeDelegate::new().with_ping_delegate(CustomPingDelegate::new()),
    t1_opts.with_probe_interval(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let m2 = Memberlist::with_delegate(
    t2,
    CompositeDelegate::new().with_ping_delegate(CustomPingDelegate::new()),
    t2_opts.with_probe_interval(Duration::from_millis(100)),
  )
  .await
  .unwrap();

  let target = Node::new(
    m1.local_id().clone(),
    MaybeResolvedAddress::resolved(m1.advertise_address().clone()),
  );

  m2.join(target).await.unwrap();

  wait_until_size::<_, _, R>(&m1, 2).await;
  wait_until_size::<_, _, R>(&m2, 2).await;

  R::sleep(probe_interval * 2).await;

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();

  let delegate = m2.delegate().unwrap().ping_delegate();

  let inner = delegate.0.lock().await;
  assert!(inner.other.is_some(), "should get notified");
  assert_eq!(
    inner.other.as_ref().unwrap(),
    &m1.advertise_node(),
    "not notified about the correct node; expected: {} actual: {}",
    m1.advertise_node(),
    inner.other.as_ref().unwrap()
  );
  assert!(inner.rtt > Duration::ZERO, "rtt should be greater than 0");
  assert_eq!(
    inner.payload.as_ref(),
    b"whatever",
    "incorrect payload. expected: {:?}, actual: {:?}",
    b"whatever",
    inner.payload.as_ref()
  );
}

pub async fn wait_until_size<T, D, R>(m: &Memberlist<T, D>, expected: usize)
where
  T: Transport<Runtime = R>,
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  retry::<R, _, _>(15, Duration::from_millis(500), || async {
    if m.online_members().await.len() != expected {
      return (
        true,
        format!(
          "expected {} nodes, got {}",
          expected,
          m.num_online_members().await
        ),
      );
    }
    (false, "".to_string())
  })
  .await
}

pub async fn wait_for_condition<'a, Fut, F>(mut f: F)
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

async fn retry<'a, R, F, Fut>(n: usize, w: Duration, mut f: F)
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  F: FnMut() -> Fut + Clone,
  Fut: Future<Output = (bool, String)> + Send + Sync + 'a,
{
  for idx in 1..=n {
    let (failed, failed_msg) = f().await;
    if !failed {
      return;
    }
    if idx == n {
      panic!("failed after {} attempts: {}", n, failed_msg);
    }

    R::sleep(w).await;
  }
}
