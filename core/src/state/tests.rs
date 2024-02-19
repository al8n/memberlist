use std::{
  sync::atomic::Ordering,
  time::{Duration, Instant},
};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{Future, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone, Node};

use crate::{
  broadcast::Broadcast,
  delegate::VoidDelegate,
  error::Error,
  transport::Transport,
  types::{Alive, Message, ServerState},
  Memberlist, Options,
};

async fn host_memberlist<T, R: Runtime>(
  t: T,
  opts: Options,
) -> Result<
  Memberlist<T>,
  Error<T, VoidDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
>
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  Memberlist::new_in(t, None, opts).await.map(|(_, _, t)| t)
}

/// Unit test to test the probe functionality
pub async fn probe<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // should ping addr2
  m1.probe().await;

  // Should not be marked suspect
  let nodes = m1.inner.nodes.read().await;
  let idx = *nodes.node_map.get(&m2.inner.id).unwrap();
  let n = &nodes.nodes[idx];
  assert_eq!(n.state.state, ServerState::Alive);

  // Should increment seqno
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(seq_no, 1, "bad seq no: {seq_no}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node suspect functionality
pub async fn probe_node_suspect<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t3: T,
  suspect_node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let m3: Memberlist<T> = host_memberlist(t3, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m3.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a3, None, false).await;

  let a4 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: suspect_node.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m1.alive_node(a4, None, false).await;

  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(suspect_node.id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  let state = m1
    .inner
    .nodes
    .read()
    .await
    .get_state(suspect_node.id())
    .unwrap()
    .state;
  // Should be marked suspect.
  assert_eq!(state, ServerState::Suspect, "bad state: {state}");
  R::sleep(Duration::from_millis(1000)).await;

  // One of the peers should have attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(
    s2 == 1 && s3 == 1,
    "bad seqnos, expected both to be 1: {s2}, {s3}"
  );

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

pub async fn test_probe_node_dogpile() {
  todo!()
}

/// Unit test to test the probe node awareness degraded functionality
pub async fn probe_node_awareness_degraded<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t3: T,
  node4: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let probe_time_min = Duration::from_millis(200) * 2 - Duration::from_millis(50);

  let m2: Memberlist<T> = host_memberlist(
    t2,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m3: Memberlist<T> = host_memberlist(
    t3,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m3.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a3, None, false).await;

  // Node 4 never gets started.
  let a4 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: node4.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  m1.alive_node(a4, None, false).await;

  // Start the health in a degraded state.
  m1.inner.awareness.apply_delta(1).await;
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 1, "bad: {score}");

  // Have node m1 probe m4.
  let start_probe = {
    let n = m1.inner.nodes.read().await.get_state(node4.id()).unwrap();
    let start = Instant::now();
    m1.probe_node(&n).await;
    start
  };

  let probe_time = start_probe.elapsed();

  // Node should be reported suspect.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(node4.id())
      .unwrap()
      .state;
    assert_eq!(state, ServerState::Suspect, "expect node to be suspect");
  }

  // Make sure we timed out approximately on time (note that we accounted
  // for the slowed-down failure detector in the probeTimeMin calculation.
  assert!(
    probe_time >= probe_time_min,
    "probed too quickly: {}s",
    probe_time.as_secs_f64()
  );

  // Confirm at least one of the peers attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(s2 == 1 && s3 == 1, "bad seqnos: {s2}, {s3}");

  // We should have gotten all the nacks, so our score should remain the
  // same, since we didn't get a successful probe.
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 1, "bad: {score}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

/// Unit test to test the probe node awareness improved functionality
pub async fn probe_node_awareness_improved<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(
    t2,
    Options::lan()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // Start the health in a degraded state.
  m1.inner.awareness.apply_delta(1).await;
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 1, "bad: {score}");

  // Have node m1 probe m2.
  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  // Node should be reported alive.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap()
      .state;
    assert_eq!(state, ServerState::Alive, "expect node to be alive");
  }

  // Our score should have improved since we did a good probe.
  let score = m1.inner.awareness.get_health_score().await;
  assert_eq!(score, 0, "bad: {score}");

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe node awareness missed nack functionality
pub async fn probe_node_awareness_missed_nack<T, R>(
  t1: T,
  t1_opts: Options,
  t2: T,
  t2_opts: Options,
  node3: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  node4: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let probe_time_max = Duration::from_millis(200) + Duration::from_millis(50);

  let m2: Memberlist<T> = host_memberlist(
    t2,
    t2_opts
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  // Node 3 and node 4 never get started.
  let a3 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: node3,
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  let a4 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: node4.cheap_clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };
  // Node 3 and node 4 never get started.
  m1.alive_node(a3, None, false).await;
  m1.alive_node(a4, None, false).await;

  // Make sure health looks good
  let health = m1.inner.awareness.get_health_score().await;
  assert_eq!(health, 0, "bad: {health}");

  // Have node m1 probe m4.
  let start_probe = {
    let n = m1.inner.nodes.read().await.get_state(node4.id()).unwrap();
    let start = Instant::now();
    m1.probe_node(&n).await;
    start
  };
  let probe_time = start_probe.elapsed();

  // Node should be reported suspect.
  {
    let state = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(node4.id())
      .unwrap()
      .state;
    assert_eq!(state, ServerState::Suspect, "expect node to be suspect");
  }

  // Make sure we timed out approximately on time.
  assert!(
    probe_time <= probe_time_max,
    "took to long to probe: {}",
    probe_time.as_secs_f64()
  );

  for i in 0..10 {
    let score = m1.inner.awareness.get_health_score().await;
    if score == 1 {
      break;
    }
    if i == 9 {
      panic!("expected health score to decrement on missed nack. want 1  got {score}");
    }
    R::sleep(Duration::from_millis(25)).await;
  }

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

pub async fn probe_node_buddy<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2.cheap_clone(), None, false).await;
  m2.alive_node(a2, None, true).await;

  // Force the state to suspect so we piggyback a suspect message with the ping.
  // We should see this get refuted later, and the ping will succeed.
  {
    let mut members = m1.inner.nodes.write().await;
    let id = m2.local_id();
    members.set_state(id, ServerState::Suspect);
    let n = members.get_state(id).unwrap();
    drop(members);
    m1.probe_node(&n).await;
  };

  // Make sure a ping was sent.
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(seq_no, 1, "bad seq no: {seq_no}");

  // Check a broadcast is queued.
  let num = m2.inner.broadcast.num_queued().await;
  assert_eq!(num, 1, "expected only one queued message: {num}");

  // Should be alive msg
  let broadcasts = m2.inner.broadcast.ordered_view(true).await;
  let msg = broadcasts[0].broadcast.message();
  assert!(matches!(msg, Message::Alive(_)), "bad message: {msg:?}");
  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
}

/// Unit test to test the probe functionality
pub async fn probe_node<T, R>(t1: T, t1_opts: Options, t2: T)
where
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let m1: Memberlist<T> = host_memberlist(
    t1,
    t1_opts
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  )
  .await
  .unwrap();

  let m2: Memberlist<T> = host_memberlist(t2, Options::lan()).await.unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a1, None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.advertise_node(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(a2, None, false).await;

  {
    let n = m1
      .inner
      .nodes
      .read()
      .await
      .get_state(m2.local_id())
      .unwrap();
    m1.probe_node(&n).await;
  };

  // Should not be marked alive
  let state = m1
    .inner
    .nodes
    .read()
    .await
    .get_state(m2.local_id())
    .unwrap()
    .state;
  assert_eq!(
    state,
    ServerState::Alive,
    "expect node to be alive: {state}"
  );

  // Should increment seqno
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_eq!(seq_no, 1, "bad seq no: {seq_no}");
}

pub async fn test_ping() {
  todo!()
}

pub async fn test_reset_nodes() {
  todo!()
}

pub async fn test_next_seq() {
  todo!()
}

pub async fn test_set_probe_channels() {
  todo!()
}

pub async fn test_set_ack_handler() {
  todo!()
}

pub async fn test_invoke_handler() {
  todo!()
}

pub async fn test_invoke_ack_handler_channel_ack() {
  todo!()
}

pub async fn test_invoke_ack_handler_channel_nack() {
  todo!()
}

pub async fn test_alive_node_new_node() {
  todo!()
}

pub async fn test_alive_node_suspect_node() {
  todo!()
}

pub async fn test_alive_node_idempotent() {
  todo!()
}

pub async fn test_alive_node_change_meta() {
  todo!()
}

pub async fn test_alive_node_refute() {
  todo!()
}

pub async fn test_alive_node_conflict() {
  todo!()
}

pub async fn test_suspect_node_no_node() {
  todo!()
}

pub async fn test_suspect_node() {
  todo!()
}

pub async fn test_suspect_node_double_suspect() {
  todo!()
}

pub async fn test_suspect_node_old_suspect() {
  todo!()
}

pub async fn test_suspect_node_refute() {
  todo!()
}

pub async fn test_dead_node_no_node() {
  todo!()
}

pub async fn test_dead_node_left() {
  todo!()
}

pub async fn test_dead_node() {
  todo!()
}

pub async fn test_dead_node_double() {
  todo!()
}

pub async fn test_dead_node_old_dead() {
  todo!()
}

pub async fn test_dead_node_alive_replay() {
  todo!()
}

pub async fn test_dead_node_refute() {
  todo!()
}

pub async fn test_merge_state() {
  todo!()
}

pub async fn test_gossip() {
  todo!()
}

pub async fn test_gossip_to_dead() {
  todo!()
}

pub async fn test_failed_remote() {
  todo!()
}

pub async fn test_push_pull() {
  todo!()
}
