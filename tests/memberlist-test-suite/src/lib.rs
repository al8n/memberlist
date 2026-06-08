//! A driver-agnostic real-node test suite for the memberlist drivers.
//!
//! Each driver implements [`TestCluster`] over its public handle; the scenarios
//! in [`scenarios`] — ported from the legacy `memberlist::*` real-node test
//! helpers — are then written ONCE and run across every driver, transport, and
//! transform configuration. The driver's own test crate supplies the adapter
//! and the per-cell instantiations (under its native test runtime).
//!
//! The suite holds no driver dependency: a scenario speaks only [`TestCluster`]
//! and the basic wire types, so the same scenario body verifies compio's TCP
//! cluster and reactor's QUIC cluster alike.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use memberlist_proto::{ChecksumAlgorithm, CompressAlgorithm, SecretKey};
use smol_str::SmolStr;

pub mod delegate;
pub mod scenarios;

pub use delegate::{Captures, PingObservation, RejectMerge, VetoForeignAlive};

/// Per-node construction config — the knobs a scenario (and the transform
/// matrix) varies. A driver's [`TestCluster::spawn`] maps these onto its own
/// options builders, binding an ephemeral loopback node.
///
/// The transform knobs ([`compression`](Self::compression),
/// [`checksum`](Self::checksum), [`encryption_key`](Self::encryption_key))
/// carry driver-agnostic `memberlist-proto` types; naming an algorithm needs no
/// codec-backend feature, but a driver only *applies* one it was built with.
#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
  /// The node's SWIM id.
  pub id: SmolStr,
  /// The cluster label, or `None` for an unlabeled node.
  pub label: Option<Vec<u8>>,
  /// The node's initial advertised metadata, or `None` for empty meta.
  pub meta: Option<Vec<u8>>,
  /// The gossip/stream compression algorithm, or `None` to disable.
  pub compression: Option<CompressAlgorithm>,
  /// The gossip-plane checksum algorithm, or `None` to disable.
  pub checksum: Option<ChecksumAlgorithm>,
  /// The encryption primary key, or `None` to disable encryption.
  pub encryption_key: Option<SecretKey>,
  /// A fixed advertise address to bind, or `None` for an ephemeral one.
  pub advertise_addr: Option<SocketAddr>,
  /// Whether to install a merge-cancelling admission predicate, vetoing every
  /// join merge.
  pub reject_merge: bool,
  /// Whether to install an admission predicate that admits only this node,
  /// vetoing every foreign alive.
  pub veto_foreign_alive: bool,
  /// The ack payload this node attaches to its ping replies, or `None` for an
  /// empty payload.
  pub ack_payload: Option<Bytes>,
  /// The local push/pull state this node advertises, or `None` for none.
  pub local_state: Option<Bytes>,
  /// User messages this node broadcasts over gossip after it joins.
  pub broadcasts: Vec<Bytes>,
}

impl NodeConfig {
  /// A config for a node with the given id, no label, empty metadata, and no
  /// transforms.
  pub fn new(id: impl Into<SmolStr>) -> Self {
    Self {
      id: id.into(),
      label: None,
      meta: None,
      compression: None,
      checksum: None,
      encryption_key: None,
      advertise_addr: None,
      reject_merge: false,
      veto_foreign_alive: false,
      ack_payload: None,
      local_state: None,
      broadcasts: Vec::new(),
    }
  }

  /// Isolate this node's traffic under `label`.
  #[must_use]
  pub fn with_label(mut self, label: impl Into<Vec<u8>>) -> Self {
    self.label = Some(label.into());
    self
  }

  /// Advertise `meta` as this node's initial metadata.
  #[must_use]
  pub fn with_meta(mut self, meta: impl Into<Vec<u8>>) -> Self {
    self.meta = Some(meta.into());
    self
  }

  /// Compress payloads with `algorithm`.
  #[must_use]
  pub fn with_compression(mut self, algorithm: CompressAlgorithm) -> Self {
    self.compression = Some(algorithm);
    self
  }

  /// Checksum gossip-plane payloads with `algorithm`.
  #[must_use]
  pub fn with_checksum(mut self, algorithm: ChecksumAlgorithm) -> Self {
    self.checksum = Some(algorithm);
    self
  }

  /// Encrypt payloads under `key` (used as the keyring primary).
  #[must_use]
  pub fn with_encryption(mut self, key: SecretKey) -> Self {
    self.encryption_key = Some(key);
    self
  }

  /// Bind this node to the fixed advertise address `addr` instead of an
  /// ephemeral one.
  #[must_use]
  pub fn with_advertise_addr(mut self, addr: SocketAddr) -> Self {
    self.advertise_addr = Some(addr);
    self
  }

  /// Install a merge admission predicate that cancels every join merge.
  #[must_use]
  pub fn with_reject_merge(mut self) -> Self {
    self.reject_merge = true;
    self
  }

  /// Install an alive admission predicate that admits only this node and vetoes
  /// every foreign alive.
  #[must_use]
  pub fn with_veto_foreign_alive(mut self) -> Self {
    self.veto_foreign_alive = true;
    self
  }

  /// Attach `payload` to this node's ping replies.
  #[must_use]
  pub fn with_ack_payload(mut self, payload: impl Into<Bytes>) -> Self {
    self.ack_payload = Some(payload.into());
    self
  }

  /// Advertise `state` as this node's local push/pull state.
  #[must_use]
  pub fn with_local_state(mut self, state: impl Into<Bytes>) -> Self {
    self.local_state = Some(state.into());
    self
  }

  /// Broadcast `messages` over gossip after this node joins.
  #[must_use]
  pub fn with_broadcasts(mut self, messages: Vec<Bytes>) -> Self {
    self.broadcasts = messages;
    self
  }
}

/// A real memberlist node, abstracted across drivers and transports.
///
/// The async methods mirror the public driver handle. [`spawn`](Self::spawn)
/// binds an ephemeral loopback node; [`sleep`](Self::sleep) is the driver's own
/// runtime timer, so the convergence polling in [`scenarios`] is
/// runtime-agnostic (compio, tokio, smol, …).
#[allow(async_fn_in_trait)]
pub trait TestCluster: Sized {
  /// The driver's error type, surfaced by the fallible operations.
  type Error: core::fmt::Debug;

  /// Construct and start a node bound to an ephemeral loopback address.
  async fn spawn(cfg: NodeConfig) -> Self;

  /// This node's SWIM id.
  fn id(&self) -> &SmolStr;

  /// The concrete bound address peers dial to reach this node.
  fn advertise_addr(&self) -> SocketAddr;

  /// Join the cluster via `seed`, returning the number of seeds contacted.
  async fn join(&self, seed: SocketAddr) -> Result<usize, Self::Error>;

  /// The number of members this node tracks (any state).
  fn num_members(&self) -> usize;

  /// The number of members this node currently considers alive.
  fn num_online_members(&self) -> usize;

  /// Gracefully leave the cluster.
  async fn leave(&self) -> Result<(), Self::Error>;

  /// Send one unreliable (gossip-plane) user message to `to`.
  async fn send(&self, to: SocketAddr, msg: bytes::Bytes) -> Result<(), Self::Error>;

  /// Send one reliable (stream-plane) user message to `to`.
  async fn send_reliable(&self, to: SocketAddr, msg: bytes::Bytes) -> Result<(), Self::Error>;

  /// A snapshot of the inbound user messages this node's delegate has captured,
  /// in arrival order. Both [`send`](Self::send) and
  /// [`send_reliable`](Self::send_reliable) payloads land here.
  fn received_messages(&self) -> Vec<bytes::Bytes>;

  /// This node's view of `id`'s advertised metadata, or `None` if it does not
  /// track that member. Empty metadata reads back as `Some(vec![])`.
  fn member_meta(&self, id: &SmolStr) -> Option<Vec<u8>>;

  /// Update this node's advertised metadata and re-broadcast it to the cluster.
  async fn update_meta(&self, meta: Vec<u8>) -> Result<(), Self::Error>;

  /// A snapshot of the remote push/pull states this node received from peers,
  /// in arrival order.
  fn received_remote_states(&self) -> Vec<Bytes>;

  /// A snapshot of the node conflicts this node observed, as
  /// `(existing_id, conflicting_id)` pairs.
  fn received_conflicts(&self) -> Vec<(SmolStr, SmolStr)>;

  /// A snapshot of the ping completions this node observed.
  fn ping_completions(&self) -> Vec<crate::delegate::PingObservation>;

  /// Whether this node's merge admission predicate has fired.
  fn merge_invoked(&self) -> bool;

  /// How many times this node's alive admission predicate has fired.
  fn alive_invocations(&self) -> usize;

  /// Shut the node down, releasing its sockets and driver task.
  async fn shutdown(self) -> Result<(), Self::Error>;

  /// Sleep `d` on the driver's runtime. Scenarios poll convergence through
  /// this so the suite runs under whatever runtime the driver's tests use.
  async fn sleep(d: Duration);
}

/// Poll `predicate` until it holds or `timeout` elapses, sleeping on the
/// driver's runtime between checks. Returns the final value of `predicate`.
///
/// The drivers publish a lock-free membership snapshot rather than offering an
/// async "wait for convergence" primitive, so a scenario observes convergence
/// by polling — the same shape the drivers' own integration tests use.
pub async fn wait_until<C: TestCluster>(
  mut predicate: impl FnMut() -> bool,
  timeout: Duration,
) -> bool {
  const STEP: Duration = Duration::from_millis(50);
  let steps = (timeout.as_millis() / STEP.as_millis()).max(1);
  for _ in 0..steps {
    if predicate() {
      return true;
    }
    C::sleep(STEP).await;
  }
  predicate()
}
