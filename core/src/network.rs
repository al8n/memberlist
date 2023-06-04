use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;

use crate::{showbiz::Showbiz, transport::Transport, types::*};

#[cfg(feature = "async")]
mod r#async;
#[cfg(feature = "async")]
pub use r#async::*;

#[cfg(feature = "sync")]
mod sync;

/// Maximum size for node meta data
pub const META_MAX_SIZE: usize = 512;

/// Assumed header overhead
pub(crate) const COMPOUND_HEADER_OVERHEAD: usize = 2;

/// Assumed overhead per entry in compound header
pub(crate) const COMPOUND_OVERHEAD: usize = 2;

pub(crate) const USER_MSG_OVERHEAD: usize = 1;

/// Warn if a UDP packet takes this long to process
const BLOCKING_WARNING: Duration = Duration::from_millis(10);

const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;
/// Maximum number of concurrent push/pull requests
const MAX_PUSH_PULL_REQUESTS: u32 = 128;

#[cfg(feature = "metrics")]
use sealed_metrics::*;

#[cfg(feature = "metrics")]
mod sealed_metrics {
  use std::sync::Once;

  const TCP_ACCEPT_COUNTER: Once = Once::new();

  #[inline]
  pub(super) fn incr_tcp_accept_counter<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    TCP_ACCEPT_COUNTER.call_once(|| {
      metrics::register_counter!("showbiz.tcp.accept");
    });
    metrics::increment_counter!("showbiz.tcp.accept", labels);
  }

  const TCP_SENT_COUNTER: Once = Once::new();

  #[inline]
  pub(super) fn incr_tcp_sent_counter<'a>(
    val: u64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    TCP_SENT_COUNTER.call_once(|| {
      metrics::register_counter!("showbiz.tcp.sent");
    });
    metrics::counter!("showbiz.tcp.sent", val, labels);
  }

  const TCP_CONNECT_COUNTER: Once = Once::new();

  #[inline]
  pub(super) fn incr_tcp_connect_counter<'a>(
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    TCP_CONNECT_COUNTER.call_once(|| {
      metrics::register_counter!("showbiz.tcp.connect");
    });
    metrics::increment_counter!("showbiz.tcp.connect", labels);
  }

  const UDP_SENT_COUNTER: Once = Once::new();
  #[inline]
  pub(super) fn incr_udp_sent_counter<'a>(
    val: u64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    UDP_SENT_COUNTER.call_once(|| {
      metrics::register_counter!("showbiz.udp.sent");
    });
    metrics::counter!("showbiz.udp.sent", val, labels);
  }

  const LOCAL_SIZE_GAUGE: Once = Once::new();
  #[inline]
  pub(super) fn set_local_size_gauge<'a>(
    val: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    LOCAL_SIZE_GAUGE.call_once(|| {
      metrics::register_gauge!("showbiz.local.size");
    });
    metrics::gauge!("showbiz.local.size", val, labels);
  }

  const REMOTE_SIZE_HISTOGRAM: Once = Once::new();
  #[inline]
  pub(super) fn add_sample_to_remote_size_histogram<'a>(
    val: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    REMOTE_SIZE_HISTOGRAM.call_once(|| {
      metrics::register_histogram!("showbiz.remote.size");
    });
    metrics::histogram!("showbiz.remote.size", val, labels);
  }

  const NODE_INSTANCES_GAUGE: Once = Once::new();
  #[inline]
  pub(super) fn set_node_instances_gauge<'a>(
    val: f64,
    labels: impl Iterator<Item = &'a metrics::Label> + metrics::IntoLabels,
  ) {
    NODE_INSTANCES_GAUGE.call_once(|| {
      metrics::register_gauge!("showbiz.node.instances");
    });
    metrics::gauge!("showbiz.node.instances", val, labels);
  }
}

#[viewit::viewit]
pub(crate) struct RemoteNodeState {
  join: bool,
  push_states: Vec<PushNodeState>,
  user_state: Bytes,
}

#[test]
fn test_() {}

// impl Showbiz {
//   fn stream_listen(&self) {}
// }
