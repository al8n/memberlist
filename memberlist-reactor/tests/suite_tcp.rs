//! The reactor TCP driver run through the driver-agnostic test suite.
//!
//! [`ReactorTcp`] adapts the reactor `Memberlist` handle to
//! [`memberlist_test_suite::TestCluster`], parameterized by the agnostic
//! runtime so one adapter serves both the tokio and smol cells. The same
//! scenario bodies that exercise compio's TCP cluster run here under tokio.

#![cfg(feature = "tcp")]

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

/// A reactor TCP node over the agnostic runtime `R`, adapted to [`TestCluster`].
struct ReactorTcp<R: Runtime> {
  id: SmolStr,
  handle: Memberlist<SmolStr, SocketAddr>,
  captures: Captures,
  merge_invoked: Option<Arc<AtomicBool>>,
  alive_count: Option<Arc<AtomicUsize>>,
  _runtime: PhantomData<R>,
}

impl<R: Runtime> TestCluster for ReactorTcp<R> {
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

    let handle = Memberlist::<SmolStr, _>::tcp::<R, _, _>(
      &SocketAddrResolver,
      cfg.id.clone(),
      MaybeResolved::Resolved(bind),
      options,
      delegate,
    )
    .await
    .expect("construct reactor tcp node");

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
async fn tcp_join_tokio() {
  scenarios::join::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_create_tokio() {
  scenarios::create::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_create_shutdown_tokio() {
  scenarios::create_shutdown::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_leave_tokio() {
  scenarios::leave::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_send_unreliable_tokio() {
  scenarios::send_unreliable::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_send_reliable_tokio() {
  scenarios::send_reliable::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_send_many_tokio() {
  scenarios::send_many::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_node_meta_tokio() {
  scenarios::node_meta::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_join_labeled_tokio() {
  scenarios::join_labeled::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_labeled_isolation_tokio() {
  scenarios::labeled_isolation::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_shutdown_detection_tokio() {
  scenarios::shutdown_detection::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_shutdown_cleanup_tokio() {
  scenarios::shutdown_cleanup::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_shutdown_cleanup2_tokio() {
  scenarios::shutdown_cleanup2::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_join_cancel_tokio() {
  scenarios::join_cancel::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_join_cancel_passive_tokio() {
  scenarios::join_cancel_passive::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_node_delegate_meta_update_tokio() {
  scenarios::node_delegate_meta_update::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_ping_delegate_tokio() {
  scenarios::ping_delegate::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_conflict_delegate_tokio() {
  scenarios::conflict_delegate::<ReactorTcp<TokioRuntime>>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_user_data_tokio() {
  scenarios::user_data::<ReactorTcp<TokioRuntime>>().await;
}

// The smol cell: the identical adapter, instantiated over `SmolRuntime` and
// driven by smol's `block_on`. The reactor poll task runs on smol's global
// executor, so the same scenario bodies verify the driver under a second
// runtime with no per-runtime scenario code.

#[test]
fn tcp_join_smol() {
  SmolRuntime::block_on(scenarios::join::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_create_smol() {
  SmolRuntime::block_on(scenarios::create::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_create_shutdown_smol() {
  SmolRuntime::block_on(scenarios::create_shutdown::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_leave_smol() {
  SmolRuntime::block_on(scenarios::leave::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_send_unreliable_smol() {
  SmolRuntime::block_on(scenarios::send_unreliable::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_send_reliable_smol() {
  SmolRuntime::block_on(scenarios::send_reliable::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_send_many_smol() {
  SmolRuntime::block_on(scenarios::send_many::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_node_meta_smol() {
  SmolRuntime::block_on(scenarios::node_meta::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_join_labeled_smol() {
  SmolRuntime::block_on(scenarios::join_labeled::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_labeled_isolation_smol() {
  SmolRuntime::block_on(scenarios::labeled_isolation::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_shutdown_detection_smol() {
  SmolRuntime::block_on(scenarios::shutdown_detection::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_shutdown_cleanup_smol() {
  SmolRuntime::block_on(scenarios::shutdown_cleanup::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_shutdown_cleanup2_smol() {
  SmolRuntime::block_on(scenarios::shutdown_cleanup2::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_join_cancel_smol() {
  SmolRuntime::block_on(scenarios::join_cancel::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_join_cancel_passive_smol() {
  SmolRuntime::block_on(scenarios::join_cancel_passive::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_node_delegate_meta_update_smol() {
  SmolRuntime::block_on(scenarios::node_delegate_meta_update::<
    ReactorTcp<SmolRuntime>,
  >());
}

#[test]
fn tcp_ping_delegate_smol() {
  SmolRuntime::block_on(scenarios::ping_delegate::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_conflict_delegate_smol() {
  SmolRuntime::block_on(scenarios::conflict_delegate::<ReactorTcp<SmolRuntime>>());
}

#[test]
fn tcp_user_data_smol() {
  SmolRuntime::block_on(scenarios::user_data::<ReactorTcp<SmolRuntime>>());
}

/// The transform matrix: the directed-I/O round-trip run under each transform
/// and all three combined, on tokio. Gated on the codec features the algorithms
/// need, so the `suite_tcp` target's required-features stay `["tcp"]` and these
/// activate only when CI also enables the transform backends. TCP under tokio is
/// representative — the transform stack sits above the transport and runtime.
#[cfg(all(
  feature = "compression-lz4",
  feature = "checksum-crc32",
  feature = "encryption-aes-gcm"
))]
mod transform_matrix {
  use memberlist_proto::{ChecksumAlgorithm, CompressAlgorithm, SecretKey};

  use super::{ReactorTcp, TokioRuntime, scenarios};

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn tcp_compressed_roundtrip() {
    scenarios::transforms_roundtrip::<ReactorTcp<TokioRuntime>>("tx-lz4", |c| {
      c.with_compression(CompressAlgorithm::Lz4)
    })
    .await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn tcp_checksummed_roundtrip() {
    scenarios::transforms_roundtrip::<ReactorTcp<TokioRuntime>>("tx-crc32", |c| {
      c.with_checksum(ChecksumAlgorithm::Crc32)
    })
    .await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn tcp_encrypted_roundtrip() {
    scenarios::transforms_roundtrip::<ReactorTcp<TokioRuntime>>("tx-aes", |c| {
      c.with_encryption(SecretKey::Aes128([0x42; 16]))
    })
    .await;
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn tcp_all_transforms_roundtrip() {
    scenarios::transforms_roundtrip::<ReactorTcp<TokioRuntime>>("tx-all", |c| {
      c.with_compression(CompressAlgorithm::Lz4)
        .with_checksum(ChecksumAlgorithm::Crc32)
        .with_encryption(SecretKey::Aes128([0x42; 16]))
    })
    .await;
  }
}
