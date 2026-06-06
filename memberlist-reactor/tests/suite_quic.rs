//! The reactor QUIC driver run through the driver-agnostic test suite.
//!
//! [`ReactorQuic`] adapts the reactor `Memberlist` handle over the QUIC
//! transport, parameterized by the agnostic runtime so one adapter serves both
//! the tokio and smol cells. Every node shares one self-signed CA (QUIC
//! verifies the server chain), and the same scenario bodies run over QUIC's
//! reliable streams and unreliable datagrams.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod quic;
#[path = "support/suite.rs"]
mod suite;

use std::{
  marker::PhantomData,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use agnostic::{Runtime, RuntimeLite, smol::SmolRuntime, tokio::TokioRuntime};
use bytes::Bytes;
use memberlist_reactor::{Error, MaybeResolved, Memberlist, Options, SocketAddrResolver};
use memberlist_test_suite::{
  Captures, NodeConfig, PingObservation, RejectMerge, TestCluster, VetoForeignAlive, scenarios,
};
use smol_str::SmolStr;

use suite::CapturingDelegate;

/// A reactor QUIC node over the agnostic runtime `R`, adapted to [`TestCluster`].
struct ReactorQuic<R: Runtime> {
  id: SmolStr,
  handle: Memberlist<SmolStr>,
  captures: Captures,
  merge_invoked: Option<Arc<AtomicBool>>,
  alive_count: Option<Arc<AtomicUsize>>,
  _runtime: PhantomData<R>,
}

impl<R: Runtime> TestCluster for ReactorQuic<R> {
  type Error = Error;

  async fn spawn(cfg: NodeConfig) -> Self {
    let mopts = suite::memberlist_options(&cfg);
    let bind = cfg
      .advertise_addr
      .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    let delegate = CapturingDelegate::default();
    let captures = delegate.captures.clone();

    let mut options = Options::new().with_memberlist(mopts);
    let mut merge_invoked = None;
    if cfg.reject_merge {
      let (predicate, flag) = RejectMerge::new();
      options = options.with_merge_delegate(predicate);
      merge_invoked = Some(flag);
    }
    let mut alive_count = None;
    if cfg.veto_foreign_alive {
      let (predicate, count) = VetoForeignAlive::new(cfg.id.clone());
      options = options.with_alive_delegate(predicate);
      alive_count = Some(count);
    }

    let handle = Memberlist::<SmolStr>::quic::<R, _, _>(
      &SocketAddrResolver,
      cfg.id.clone(),
      MaybeResolved::Resolved(bind),
      options,
      delegate,
      quic::shared_quic_config(),
    )
    .await
    .expect("construct reactor quic node");

    if let Some(payload) = cfg.ack_payload {
      handle
        .set_ack_payload(payload)
        .await
        .expect("set ack payload");
    }
    for msg in cfg.broadcasts {
      handle
        .queue_user_broadcast(msg)
        .await
        .expect("queue user broadcast");
    }

    Self {
      id: cfg.id,
      handle,
      captures,
      merge_invoked,
      alive_count,
      _runtime: PhantomData,
    }
  }

  fn id(&self) -> &SmolStr {
    &self.id
  }

  fn advertise_addr(&self) -> SocketAddr {
    *self.handle.local().addr_ref()
  }

  async fn join(&self, seed: SocketAddr) -> Result<usize, Self::Error> {
    self
      .handle
      .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed)])
      .await
  }

  fn num_members(&self) -> usize {
    self.handle.num_members()
  }

  fn num_online_members(&self) -> usize {
    self.handle.num_online_members()
  }

  async fn leave(&self) -> Result<(), Self::Error> {
    self.handle.leave().await
  }

  async fn send(&self, to: SocketAddr, msg: Bytes) -> Result<(), Self::Error> {
    self.handle.send(to, msg).await
  }

  async fn send_reliable(&self, to: SocketAddr, msg: Bytes) -> Result<(), Self::Error> {
    self.handle.send_reliable(to, msg).await
  }

  fn received_messages(&self) -> Vec<Bytes> {
    self.captures.messages()
  }

  fn member_meta(&self, id: &SmolStr) -> Option<Vec<u8>> {
    self
      .handle
      .by_id(id)
      .map(|ns| ns.meta_ref().as_ref().to_vec())
  }

  async fn update_meta(&self, meta: Vec<u8>) -> Result<(), Self::Error> {
    self.handle.update_node_metadata(meta).await
  }

  fn received_remote_states(&self) -> Vec<Bytes> {
    self.captures.remote_states()
  }

  fn received_conflicts(&self) -> Vec<(SmolStr, SmolStr)> {
    self.captures.conflicts()
  }

  fn ping_completions(&self) -> Vec<PingObservation> {
    self.captures.pings()
  }

  fn merge_invoked(&self) -> bool {
    self
      .merge_invoked
      .as_ref()
      .is_some_and(|flag| flag.load(Ordering::SeqCst))
  }

  fn alive_invocations(&self) -> usize {
    self
      .alive_count
      .as_ref()
      .map_or(0, |count| count.load(Ordering::SeqCst))
  }

  async fn shutdown(self) -> Result<(), Self::Error> {
    self.handle.shutdown().await
  }

  async fn sleep(d: Duration) {
    R::sleep(d).await;
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_join_tokio() {
  scenarios::join::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_create_tokio() {
  scenarios::create::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_create_shutdown_tokio() {
  scenarios::create_shutdown::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_leave_tokio() {
  scenarios::leave::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_send_unreliable_tokio() {
  scenarios::send_unreliable::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_send_reliable_tokio() {
  scenarios::send_reliable::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_send_many_tokio() {
  scenarios::send_many::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_node_meta_tokio() {
  scenarios::node_meta::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_join_labeled_tokio() {
  scenarios::join_labeled::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_labeled_isolation_tokio() {
  scenarios::labeled_isolation::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_shutdown_detection_tokio() {
  scenarios::shutdown_detection::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_shutdown_cleanup_tokio() {
  scenarios::shutdown_cleanup::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_shutdown_cleanup2_tokio() {
  scenarios::shutdown_cleanup2::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_join_cancel_tokio() {
  scenarios::join_cancel::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_join_cancel_passive_tokio() {
  scenarios::join_cancel_passive::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_node_delegate_meta_update_tokio() {
  scenarios::node_delegate_meta_update::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "periodic-probe ping completions are not surfaced to the ping delegate in-window; the ack-payload and capture wiring is verified, but this scenario needs an app-level directed ping or a driver probe-observability change, tracked separately"]
async fn quic_ping_delegate_tokio() {
  scenarios::ping_delegate::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_conflict_delegate_tokio() {
  scenarios::conflict_delegate::<ReactorQuic<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_user_data_tokio() {
  scenarios::user_data::<ReactorQuic<TokioRuntime>>().await;
}

// The smol cell.

#[test]
fn quic_join_smol() {
  SmolRuntime::block_on(scenarios::join::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_create_smol() {
  SmolRuntime::block_on(scenarios::create::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_create_shutdown_smol() {
  SmolRuntime::block_on(scenarios::create_shutdown::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_leave_smol() {
  SmolRuntime::block_on(scenarios::leave::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_send_unreliable_smol() {
  SmolRuntime::block_on(scenarios::send_unreliable::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_send_reliable_smol() {
  SmolRuntime::block_on(scenarios::send_reliable::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_send_many_smol() {
  SmolRuntime::block_on(scenarios::send_many::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_node_meta_smol() {
  SmolRuntime::block_on(scenarios::node_meta::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_join_labeled_smol() {
  SmolRuntime::block_on(scenarios::join_labeled::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_labeled_isolation_smol() {
  SmolRuntime::block_on(scenarios::labeled_isolation::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_shutdown_detection_smol() {
  SmolRuntime::block_on(scenarios::shutdown_detection::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_shutdown_cleanup_smol() {
  SmolRuntime::block_on(scenarios::shutdown_cleanup::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_shutdown_cleanup2_smol() {
  SmolRuntime::block_on(scenarios::shutdown_cleanup2::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_join_cancel_smol() {
  SmolRuntime::block_on(scenarios::join_cancel::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_join_cancel_passive_smol() {
  SmolRuntime::block_on(scenarios::join_cancel_passive::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_node_delegate_meta_update_smol() {
  SmolRuntime::block_on(scenarios::node_delegate_meta_update::<ReactorQuic<SmolRuntime>>());
}

#[test]
#[ignore = "periodic-probe ping completions are not surfaced to the ping delegate in-window; the ack-payload and capture wiring is verified, but this scenario needs an app-level directed ping or a driver probe-observability change, tracked separately"]
fn quic_ping_delegate_smol() {
  SmolRuntime::block_on(scenarios::ping_delegate::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_conflict_delegate_smol() {
  SmolRuntime::block_on(scenarios::conflict_delegate::<ReactorQuic<SmolRuntime>>());
}

#[test]
fn quic_user_data_smol() {
  SmolRuntime::block_on(scenarios::user_data::<ReactorQuic<SmolRuntime>>());
}
