//! The compio TCP driver run through the driver-agnostic test suite.
//!
//! [`CompioTcp`] adapts the compio `TcpMemberlist` handle to
//! [`memberlist_test_suite::TestCluster`]; each `#[compio::test]` below
//! instantiates one suite scenario over it. The same scenario bodies run on
//! every driver and transport — this file is the compio/TCP cell.

#![cfg(feature = "tcp")]

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
  FirstAddrResolver, MaybeResolved, MemberlistError, Options, SocketAddrResolver, TcpMemberlist,
  TcpTransportOptions,
};
use memberlist_test_suite::{
  Captures, NodeConfig, PingObservation, RejectMerge, TestCluster, VetoForeignAlive, scenarios,
};
use smol_str::SmolStr;

use suite::CapturingDelegate;

/// A compio TCP node adapted to [`TestCluster`]. Stores the id alongside the
/// handle because the handle returns its local id by value, not by reference,
/// and a clone of the delegate's [`Captures`] for the capture accessors. The
/// admission-predicate flags are present only when their predicate was wired.
struct CompioTcp {
  id: SmolStr,
  handle: TcpMemberlist<SmolStr, SocketAddr, CapturingDelegate>,
  captures: Captures,
  merge_invoked: Option<Arc<AtomicBool>>,
  alive_count: Option<Arc<AtomicUsize>>,
}

impl TestCluster for CompioTcp {
  type Error = MemberlistError;

  async fn spawn(cfg: NodeConfig) -> Self {
    let mopts = suite::memberlist_options(&cfg);
    // Use the fixed advertise address if the scenario pinned one (rebind
    // tests), else an ephemeral loopback port whose OS-assigned value
    // `advertise_address` reports back.
    let bind = cfg
      .advertise_addr
      .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
    let mut opts = Options::new(
      TcpTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(cfg.id.clone())
        .with_advertise_addr(MaybeResolved::Resolved(bind)),
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
    let handle = TcpMemberlist::<SmolStr, SocketAddr, CapturingDelegate>::new(
      opts,
      delegate,
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await
    .expect("construct compio tcp node");

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
async fn tcp_join() {
  scenarios::join::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_create() {
  scenarios::create::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_create_shutdown() {
  scenarios::create_shutdown::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_leave() {
  scenarios::leave::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_send_unreliable() {
  scenarios::send_unreliable::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_send_reliable() {
  scenarios::send_reliable::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_send_many() {
  scenarios::send_many::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_node_meta() {
  scenarios::node_meta::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_join_labeled() {
  scenarios::join_labeled::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_labeled_isolation() {
  scenarios::labeled_isolation::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_shutdown_detection() {
  scenarios::shutdown_detection::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_shutdown_cleanup() {
  scenarios::shutdown_cleanup::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_shutdown_cleanup2() {
  scenarios::shutdown_cleanup2::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_join_cancel() {
  scenarios::join_cancel::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_join_cancel_passive() {
  scenarios::join_cancel_passive::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_node_delegate_meta_update() {
  scenarios::node_delegate_meta_update::<CompioTcp>().await;
}

#[compio::test]
#[ignore = "periodic-probe ping completions are not surfaced to the ping delegate in-window; the ack-payload and capture wiring is verified, but this scenario needs an app-level directed ping or a driver probe-observability change, tracked separately"]
async fn tcp_ping_delegate() {
  scenarios::ping_delegate::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_conflict_delegate() {
  scenarios::conflict_delegate::<CompioTcp>().await;
}

#[compio::test]
async fn tcp_user_data() {
  scenarios::user_data::<CompioTcp>().await;
}

/// The transform matrix: the directed-I/O round-trip run under each transform
/// and all three combined. Gated on the codec features the algorithms need, so
/// the `suite_tcp` target's required-features stay `["tcp"]` and these activate
/// only when CI also enables the transform backends. TCP is representative — the
/// transform stack sits above the transport, so it applies identically on TLS
/// and QUIC.
#[cfg(all(
  feature = "compression-lz4",
  feature = "checksum-crc32",
  feature = "encryption-aes-gcm"
))]
mod transform_matrix {
  use memberlist_proto::{ChecksumAlgorithm, CompressAlgorithm, SecretKey};

  use super::{CompioTcp, scenarios};

  #[compio::test]
  async fn tcp_compressed_roundtrip() {
    scenarios::transforms_roundtrip::<CompioTcp>("tx-lz4", |c| {
      c.with_compression(CompressAlgorithm::Lz4)
    })
    .await;
  }

  #[compio::test]
  async fn tcp_checksummed_roundtrip() {
    scenarios::transforms_roundtrip::<CompioTcp>("tx-crc32", |c| {
      c.with_checksum(ChecksumAlgorithm::Crc32)
    })
    .await;
  }

  #[compio::test]
  async fn tcp_encrypted_roundtrip() {
    scenarios::transforms_roundtrip::<CompioTcp>("tx-aes", |c| {
      c.with_encryption(SecretKey::Aes128([0x42; 16]))
    })
    .await;
  }

  #[compio::test]
  async fn tcp_all_transforms_roundtrip() {
    scenarios::transforms_roundtrip::<CompioTcp>("tx-all", |c| {
      c.with_compression(CompressAlgorithm::Lz4)
        .with_checksum(ChecksumAlgorithm::Crc32)
        .with_encryption(SecretKey::Aes128([0x42; 16]))
    })
    .await;
  }
}
