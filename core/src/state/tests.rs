use std::{sync::atomic::Ordering, time::Duration};

use agnostic::Runtime;
use bytes::Bytes;
use futures::{Future, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone, Node};

use crate::{
  broadcast::Broadcast, delegate::VoidDelegate, error::Error, transport::Transport, types::{Alive, Message, ServerState}, Memberlist, Options
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
    let n = m1.inner.nodes.read().await.get_state(suspect_node.id()).unwrap();
    m1.probe_node(&n).await;
  };

  let state = m1.inner.nodes.read().await.get_state(suspect_node.id()).unwrap().state;
  // Should be marked suspect.
  assert_eq!(state, ServerState::Suspect, "bad state: {state}");
  R::sleep(Duration::from_millis(1000)).await;

  // One of the peers should have attempted an indirect probe.
  let s2 = m2.inner.hot.sequence_num.load(Ordering::SeqCst);
  let s3 = m3.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert!(s2 == 1 && s3 == 1, "bad seqnos, expected both to be 1: {s2}, {s3}");

  m1.shutdown().await.unwrap();
  m2.shutdown().await.unwrap();
  m3.shutdown().await.unwrap();
}

pub async fn test_probe_node_dogpile() {
  todo!()
}

pub async fn test_probe_node_awareness_degraded() {
  todo!()
}

pub async fn test_probe_node_awareness_improved() {
  todo!()
}

pub async fn test_probe_node_awareness_missed_nack() {
  todo!()
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
    let n = m1.inner.nodes.read().await.get_state(m2.local_id()).unwrap();
    m1.probe_node(&n).await;
  };

  // Should not be marked alive
  let state = m1.inner.nodes.read().await.get_state(m2.local_id()).unwrap().state;
  assert_eq!(state, ServerState::Alive, "expect node to be alive: {state}");

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
