//! Shared adapter pieces for the compio driver-agnostic suite cells
//! (`suite_tcp` / `suite_tls` / `suite_quic`).
//!
//! The capturing delegate records every inbound observation — user messages,
//! received remote push/pull state, node conflicts, and ping completions — into
//! a shared [`Captures`] the adapter reads back through the `TestCluster`
//! capture accessors. compio splits delegation across four sub-traits composed
//! into `Delegate`, so all four are implemented here.

#![allow(dead_code)] // Each suite binary uses this through its own adapter.

use std::{borrow::Cow, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use memberlist_compio::{
  ConflictDelegate, Delegate, EventDelegate, MemberlistOptions, NodeDelegate, PingDelegate,
};
use memberlist_proto::{
  ChecksumOptions, CompressionOptions, EncryptionOptions, Keyring,
  typed::{Meta, NodeState},
};
use memberlist_test_suite::{Captures, NodeConfig, PingObservation};
use smol_str::SmolStr;

/// Records every inbound observation into a shared [`Captures`] the adapter
/// reads back through the `TestCluster` capture accessors.
#[derive(Clone, Default)]
pub struct CapturingDelegate {
  pub captures: Captures,
}

impl EventDelegate for CapturingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl NodeDelegate for CapturingDelegate {
  async fn notify_user_msg(&self, msg: Cow<'_, [u8]>) {
    self
      .captures
      .msgs
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(msg.as_ref()));
  }

  async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
    self
      .captures
      .remote_states
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(buf));
  }
}

impl PingDelegate for CapturingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_ping_complete(
    &self,
    peer_id: &Self::Id,
    _peer_addr: &Self::Address,
    rtt: Duration,
    payload: Bytes,
  ) {
    self.captures.pings.lock().unwrap().push(PingObservation {
      peer: peer_id.clone(),
      rtt,
      payload,
    });
  }
}

impl ConflictDelegate for CapturingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
    self
      .captures
      .conflicts
      .lock()
      .unwrap()
      .push((existing.id_ref().clone(), other.id_ref().clone()));
  }
}

impl Delegate for CapturingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

/// Build the compio `MemberlistOptions` for `cfg`: label, initial metadata, the
/// initial local push/pull state, and the compression / checksum / encryption
/// transform knobs. Shared by every compio suite adapter so the option mapping
/// lives in one place.
pub fn memberlist_options(cfg: &NodeConfig) -> MemberlistOptions {
  let mut mopts = MemberlistOptions::new();
  if let Some(label) = &cfg.label {
    mopts = mopts
      .with_label(Some(label.clone()))
      .expect("valid cluster label");
  }
  if let Some(meta) = &cfg.meta {
    mopts =
      mopts.with_initial_meta(Meta::try_from(Bytes::from(meta.clone())).expect("meta within cap"));
  }
  if let Some(state) = &cfg.local_state {
    mopts = mopts.with_initial_local_state(state.clone());
  }
  if let Some(algorithm) = cfg.compression {
    mopts = mopts.with_compression(CompressionOptions::new().with_algorithm(algorithm));
  }
  if let Some(algorithm) = cfg.checksum {
    mopts = mopts.with_checksum(ChecksumOptions::new().with_algorithm(algorithm));
  }
  if let Some(key) = cfg.encryption_key {
    mopts = mopts.with_encryption(EncryptionOptions::new().with_keyring(Keyring::new(key)));
  }
  mopts
}
