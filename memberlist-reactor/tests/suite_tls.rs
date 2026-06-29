//! The reactor TLS driver run through the driver-agnostic test suite.
//!
//! [`ReactorTls`] adapts the reactor `Memberlist` handle over the TLS-backed
//! reliable plane, parameterized by the agnostic runtime so one adapter serves
//! both the tokio and smol cells. Every node shares one self-signed CA (TLS
//! verifies the server chain), and the SNI closure always returns the cert's
//! `localhost` SAN.

#![cfg(feature = "tls-rustls-ring")]

#[path = "support/suite.rs"]
mod suite;
#[path = "support/tls.rs"]
mod tls;

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

/// A reactor TLS node over the agnostic runtime `R`, adapted to [`TestCluster`].
struct ReactorTls<R: Runtime> {
  id: SmolStr,
  handle: Memberlist<SmolStr, SocketAddr, R>,
  captures: Captures,
  merge_invoked: Option<Arc<AtomicBool>>,
  alive_count: Option<Arc<AtomicUsize>>,
  _runtime: PhantomData<R>,
}

impl<R: Runtime> TestCluster for ReactorTls<R> {
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

    let handle = Memberlist::<SmolStr, _, R>::tls(
      &SocketAddrResolver,
      cfg.id.clone(),
      MaybeResolved::Resolved(bind),
      options,
      delegate,
      tls::shared_tls_options(),
      |_: &SocketAddr| Some("localhost".to_string()),
    )
    .await
    .expect("construct reactor tls node");

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
      .map(|reached| reached.len())
      .map_err(|(_, e)| e)
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
async fn tls_join_tokio() {
  scenarios::join::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_create_tokio() {
  scenarios::create::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_create_shutdown_tokio() {
  scenarios::create_shutdown::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_leave_tokio() {
  scenarios::leave::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_send_unreliable_tokio() {
  scenarios::send_unreliable::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_send_reliable_tokio() {
  scenarios::send_reliable::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_send_many_tokio() {
  scenarios::send_many::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_node_meta_tokio() {
  scenarios::node_meta::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_join_labeled_tokio() {
  scenarios::join_labeled::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_labeled_isolation_tokio() {
  scenarios::labeled_isolation::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_shutdown_detection_tokio() {
  scenarios::shutdown_detection::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_shutdown_cleanup_tokio() {
  scenarios::shutdown_cleanup::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_shutdown_cleanup2_tokio() {
  scenarios::shutdown_cleanup2::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_join_cancel_tokio() {
  scenarios::join_cancel::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_join_cancel_passive_tokio() {
  scenarios::join_cancel_passive::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_node_delegate_meta_update_tokio() {
  scenarios::node_delegate_meta_update::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_ping_delegate_tokio() {
  scenarios::ping_delegate::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_conflict_delegate_tokio() {
  scenarios::conflict_delegate::<ReactorTls<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_user_data_tokio() {
  scenarios::user_data::<ReactorTls<TokioRuntime>>().await;
}

// The smol cell.

#[test]
fn tls_join_smol() {
  SmolRuntime::block_on(scenarios::join::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_create_smol() {
  SmolRuntime::block_on(scenarios::create::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_create_shutdown_smol() {
  SmolRuntime::block_on(scenarios::create_shutdown::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_leave_smol() {
  SmolRuntime::block_on(scenarios::leave::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_send_unreliable_smol() {
  SmolRuntime::block_on(scenarios::send_unreliable::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_send_reliable_smol() {
  SmolRuntime::block_on(scenarios::send_reliable::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_send_many_smol() {
  SmolRuntime::block_on(scenarios::send_many::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_node_meta_smol() {
  SmolRuntime::block_on(scenarios::node_meta::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_join_labeled_smol() {
  SmolRuntime::block_on(scenarios::join_labeled::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_labeled_isolation_smol() {
  SmolRuntime::block_on(scenarios::labeled_isolation::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_shutdown_detection_smol() {
  SmolRuntime::block_on(scenarios::shutdown_detection::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_shutdown_cleanup_smol() {
  SmolRuntime::block_on(scenarios::shutdown_cleanup::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_shutdown_cleanup2_smol() {
  SmolRuntime::block_on(scenarios::shutdown_cleanup2::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_join_cancel_smol() {
  SmolRuntime::block_on(scenarios::join_cancel::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_join_cancel_passive_smol() {
  SmolRuntime::block_on(scenarios::join_cancel_passive::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_node_delegate_meta_update_smol() {
  SmolRuntime::block_on(scenarios::node_delegate_meta_update::<
    ReactorTls<SmolRuntime>,
  >());
}

#[test]
fn tls_ping_delegate_smol() {
  SmolRuntime::block_on(scenarios::ping_delegate::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_conflict_delegate_smol() {
  SmolRuntime::block_on(scenarios::conflict_delegate::<ReactorTls<SmolRuntime>>());
}

#[test]
fn tls_user_data_smol() {
  SmolRuntime::block_on(scenarios::user_data::<ReactorTls<SmolRuntime>>());
}
