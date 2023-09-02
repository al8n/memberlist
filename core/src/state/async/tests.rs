use std::{net::SocketAddr, time::Duration};

use agnostic::Runtime;
use atomic::Ordering;
use bytes::Bytes;
use either::Either;
use futures_util::{Future, Stream};

use crate::{
  delegate::VoidDelegate, error::Error, showbiz::tests::get_bind_addr,
  transport::net::NetTransport, types::Alive, Name, Options, Showbiz,
};

async fn host_showbiz<F, R: Runtime>(
  addr: SocketAddr,
  f: Option<F>,
) -> Result<Showbiz<NetTransport<R>>, Error<NetTransport<R>, VoidDelegate>>
where
  F: FnOnce(Options<NetTransport<R>>) -> Options<NetTransport<R>>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let c = Options::lan()
    .with_name(Name::from_str_unchecked(&addr.to_string()))
    .with_bind_addr(addr.ip())
    .with_bind_port(Some(0));
  let c = if let Some(f) = f { f(c) } else { c };

  Showbiz::new_in(None, c).await.map(|(_, _, t)| t)
}

pub async fn test_probe<R>()
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  let addr1 = get_bind_addr();
  let addr2 = get_bind_addr();

  let m1: Showbiz<NetTransport<R>> = host_showbiz::<_, R>(
    addr1,
    Some(|c: Options<NetTransport<R>>| {
      c.with_probe_timeout(Duration::from_millis(1))
        .with_probe_interval(Duration::from_millis(10))
    }),
  )
  .await
  .unwrap();

  let m2: Showbiz<NetTransport<R>> = host_showbiz::<_, R>(
    addr2,
    Some(|c: Options<NetTransport<R>>| c.with_bind_port(m1.inner.opts.bind_port())),
  )
  .await
  .unwrap();

  let a1 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m1.inner.id.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(Either::Left(a1), None, true).await;

  let a2 = Alive {
    incarnation: 1,
    meta: Bytes::new(),
    node: m2.inner.id.clone(),
    protocol_version: crate::ProtocolVersion::V0,
    delegate_version: crate::DelegateVersion::V0,
  };

  m1.alive_node(Either::Left(a2), None, false).await;

  // should ping addr2
  m1.probe().await;

  // Should not be marked suspect
  let nodes = m1.inner.nodes.read().await;
  let idx = *nodes.node_map.get(&m2.inner.id).unwrap();
  let n = &nodes.nodes[idx];
  assert_eq!(n.state.state, crate::NodeState::Alive);

  // Should increment seqno
  let seq_no = m1.inner.hot.sequence_num.load(Ordering::SeqCst);
  assert_ne!(seq_no, 1, "bad seq no: {seq_no}");
}

pub async fn test_probe_node_suspect() {
  todo!()
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

pub async fn test_probe_node_buddy() {
  todo!()
}

pub async fn test_probe_node() {
  todo!()
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
