//! The compio QUIC driver run through the driver-agnostic test suite.
//!
//! [`CompioQuic`] adapts the compio `Memberlist` handle to
//! [`memberlist_test_suite::TestCluster`]. Every node shares one self-signed CA
//! (QUIC verifies the server chain, unlike the accept-any TLS smoke verifier),
//! and the same scenario bodies run here over QUIC's reliable streams and
//! unreliable datagrams.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod quic;
#[path = "support/suite.rs"]
mod suite;

use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use bytes::Bytes;
use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Memberlist, MemberlistError, Options, QuicTransport,
  QuicTransportOptions, SocketAddrResolver,
};
use memberlist_test_suite::{
  Captures, NodeConfig, PingObservation, RejectMerge, TestCluster, VetoForeignAlive, scenarios,
};
use smol_str::SmolStr;

use suite::CapturingDelegate;

/// A compio QUIC node adapted to [`TestCluster`].
struct CompioQuic {
  id: SmolStr,
  handle: Memberlist<SmolStr, SocketAddr>,
  captures: Captures,
  merge_invoked: Option<Arc<AtomicBool>>,
  alive_count: Option<Arc<AtomicUsize>>,
}

impl TestCluster for CompioQuic {
  type Error = MemberlistError;

  async fn spawn(cfg: NodeConfig) -> Self {
    let mopts = suite::memberlist_options(&cfg);
    let bind = cfg
      .advertise_addr
      .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    let mut opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
      QuicTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(cfg.id.clone())
        .with_advertise_addr(MaybeResolved::Resolved(bind))
        .with_quic_config(quic::shared_quic_config()),
    )
    .with_memberlist(mopts);

    let mut merge_invoked = None;
    if cfg.reject_merge {
      let (delegate, flag) = RejectMerge::new();
      opts = opts.with_merge_delegate(delegate);
      merge_invoked = Some(flag);
    }
    let mut alive_count = None;
    if cfg.veto_foreign_alive {
      let (delegate, count) = VetoForeignAlive::new(cfg.id.clone());
      opts = opts.with_alive_delegate(delegate);
      alive_count = Some(count);
    }

    let delegate = CapturingDelegate::default();
    let captures = delegate.captures.clone();
    let handle = Memberlist::new(opts, delegate, &SocketAddrResolver, &FirstAddrResolver)
      .await
      .expect("construct compio quic node");

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
    }
  }

  fn id(&self) -> &SmolStr {
    &self.id
  }

  fn advertise_addr(&self) -> SocketAddr {
    self.handle.advertise_address()
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
    compio::time::sleep(d).await;
  }
}

#[compio::test]
async fn quic_join() {
  scenarios::join::<CompioQuic>().await;
}

#[compio::test]
async fn quic_create() {
  scenarios::create::<CompioQuic>().await;
}

#[compio::test]
async fn quic_create_shutdown() {
  scenarios::create_shutdown::<CompioQuic>().await;
}

#[compio::test]
async fn quic_leave() {
  scenarios::leave::<CompioQuic>().await;
}

#[compio::test]
async fn quic_send_unreliable() {
  scenarios::send_unreliable::<CompioQuic>().await;
}

#[compio::test]
async fn quic_send_reliable() {
  scenarios::send_reliable::<CompioQuic>().await;
}

#[compio::test]
async fn quic_send_many() {
  scenarios::send_many::<CompioQuic>().await;
}

#[compio::test]
async fn quic_node_meta() {
  scenarios::node_meta::<CompioQuic>().await;
}

#[compio::test]
async fn quic_join_labeled() {
  scenarios::join_labeled::<CompioQuic>().await;
}

#[compio::test]
async fn quic_labeled_isolation() {
  scenarios::labeled_isolation::<CompioQuic>().await;
}

#[compio::test]
async fn quic_shutdown_detection() {
  scenarios::shutdown_detection::<CompioQuic>().await;
}

#[compio::test]
async fn quic_shutdown_cleanup() {
  scenarios::shutdown_cleanup::<CompioQuic>().await;
}

#[compio::test]
async fn quic_shutdown_cleanup2() {
  scenarios::shutdown_cleanup2::<CompioQuic>().await;
}

#[compio::test]
async fn quic_join_cancel() {
  scenarios::join_cancel::<CompioQuic>().await;
}

#[compio::test]
async fn quic_join_cancel_passive() {
  scenarios::join_cancel_passive::<CompioQuic>().await;
}

#[compio::test]
async fn quic_node_delegate_meta_update() {
  scenarios::node_delegate_meta_update::<CompioQuic>().await;
}

#[compio::test]
async fn quic_ping_delegate() {
  scenarios::ping_delegate::<CompioQuic>().await;
}

#[compio::test]
async fn quic_conflict_delegate() {
  scenarios::conflict_delegate::<CompioQuic>().await;
}

#[compio::test]
async fn quic_user_data() {
  scenarios::user_data::<CompioQuic>().await;
}
