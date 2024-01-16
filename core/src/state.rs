use std::{
  sync::{atomic::AtomicU32, Arc},
  time::Instant,
};

use super::{
  delegate::Delegate,
  error::Error,
  showbiz::Showbiz,
  transport::Transport,
  types::{AckResponse, NackResponse, Server, ServerState},
};

mod r#async;

#[cfg(feature = "test")]
pub use r#async::tests::*;

#[cfg(feature = "metrics")]
use sealed_metrics::*;

#[cfg(feature = "metrics")]
mod sealed_metrics {
  use std::sync::Once;

  static DEGRADED_PROBE: Once = Once::new();

  #[inline]
  pub(super) fn incr_degraded_probe<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // DEGRADED_PROBE.call_once(|| {
    //   metrics::register_counter!("showbiz.degraded.probe");
    // });
    metrics::counter!("showbiz.degraded.probe", labels).increment(1);
  }

  static DEGRADED_TIMEOUT: Once = Once::new();

  #[inline]
  pub(super) fn incr_degraded_timeout<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // DEGRADED_TIMEOUT.call_once(|| {
    //   metrics::register_counter!("showbiz.degraded.timeout");
    // });
    metrics::counter!("showbiz.degraded.timeout", labels).increment(1);
  }

  static MSG_ALIVE: Once = Once::new();

  #[inline]
  pub(super) fn incr_msg_alive<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // MSG_ALIVE.call_once(|| {
    //   metrics::register_counter!("showbiz.msg.alive");
    // });
    metrics::counter!("showbiz.msg.alive", labels).increment(1);
  }

  static MSG_SUSPECT: Once = Once::new();

  #[inline]
  pub(super) fn incr_msg_suspect<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // MSG_SUSPECT.call_once(|| {
    //   metrics::register_counter!("showbiz.msg.suspect");
    // });
    metrics::counter!("showbiz.msg.suspect", labels).increment(1);
  }

  static MSG_DEAD: Once = Once::new();

  #[inline]
  pub(super) fn incr_msg_dead<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // MSG_DEAD.call_once(|| {
    //   metrics::register_counter!("showbiz.msg.dead");
    // });
    metrics::counter!("showbiz.msg.dead", labels).increment(1);
  }

  static PUSH_PULL_NODE_HISTOGRAM: Once = Once::new();

  #[inline]
  pub(super) fn observe_push_pull_node<'a>(
    value: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // PUSH_PULL_NODE_HISTOGRAM.call_once(|| {
    //   metrics::register_histogram!("showbiz.push_pull_node");
    // });
    metrics::histogram!("showbiz.push_pull_node", labels).record(value);
  }

  static GOSSIP_HISTOGRAM: Once = Once::new();

  #[inline]
  pub(super) fn observe_gossip<'a>(
    value: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // GOSSIP_HISTOGRAM.call_once(|| {
    //   metrics::register_histogram!("showbiz.gossip");
    // });
    metrics::histogram!("showbiz.gossip", labels).record(value);
  }

  static PROBE_HISTOGRAM: Once = Once::new();

  #[inline]
  pub(super) fn observe_probe_node<'a>(
    value: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    // PROBE_HISTOGRAM.call_once(|| {
    //   metrics::register_histogram!("showbiz.probe_node");
    // });
    metrics::histogram!("showbiz.probe_node", labels).record(value);
  }
}

#[viewit::viewit]
#[derive(Debug, Clone)]
pub(crate) struct LocalServerState<I, A> {
  node: Arc<Server<I, A>>,
  incarnation: Arc<AtomicU32>,
  state_change: Instant,
  /// The current state of the node
  state: ServerState,
}

impl<I, A> LocalServerState<I, A> {
  pub(crate) fn id(&self) -> &I {
    self.node.id()
  }

  pub(crate) fn address(&self) -> &A {
    self.node.addr()
  }

  #[inline]
  pub(crate) fn dead_or_left(&self) -> bool {
    self.state == ServerState::Dead || self.state == ServerState::Left
  }
}

// private implementation
impl<D, T> Showbiz<T, D>
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
  pub(crate) async fn invoke_ack_handler(&self, ack: AckResponse, timestamp: Instant) {
    let ah = self.inner.ack_handlers.lock().await.remove(&ack.seq_no);
    if let Some(handler) = ah {
      handler.timer.stop().await;
      (handler.ack_fn)(ack.payload, timestamp).await;
    }
  }

  #[inline]
  pub(crate) async fn invoke_nack_handler(&self, nack: NackResponse) {
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
