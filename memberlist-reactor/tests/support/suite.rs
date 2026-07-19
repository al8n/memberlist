//! Shared adapter pieces for the reactor driver-agnostic suite cells
//! (`suite_tcp` / `suite_tls` / `suite_quic`).
//!
//! The capturing delegate records every inbound observation (user messages,
//! remote push/pull states, node conflicts, ping completions) into the shared
//! [`Captures`] so the adapter can satisfy the `TestCluster` capture accessors;
//! every other hook keeps its no-op default. The reactor exposes a single
//! unified `Delegate` trait, so all four hooks live in one impl.

#![allow(dead_code)] // Each suite binary uses this through its own adapter.

use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
#[cfg(checksum)]
use memberlist_proto::ChecksumOptions;
#[cfg(compression)]
use memberlist_proto::CompressionOptions;
use memberlist_proto::typed::{Meta, NodeState};
#[cfg(encryption)]
use memberlist_proto::{EncryptionOptions, Keyring};
use memberlist_reactor::{Delegate, MemberlistOptions};
use memberlist_test_suite::{Captures, NodeConfig, PingObservation};
use smol_str::SmolStr;

/// Records every inbound observation into a shared [`Captures`] the adapter
/// reads back through the `TestCluster` capture accessors.
#[derive(Clone, Default)]
pub struct CapturingDelegate {
  pub captures: Captures,
}

impl Delegate for CapturingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  fn notify_user_msg(&self, msg: Bytes) -> impl Future<Output = ()> + Send + '_ {
    let msgs = self.captures.msgs.clone();
    async move {
      msgs.lock().unwrap().push(msg);
    }
  }

  fn merge_remote_state(&self, state: Bytes, _join: bool) -> impl Future<Output = ()> + Send + '_ {
    let remote_states = self.captures.remote_states.clone();
    async move {
      remote_states.lock().unwrap().push(state);
    }
  }

  fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> impl Future<Output = ()> + Send + '_ {
    let conflicts = self.captures.conflicts.clone();
    async move {
      conflicts
        .lock()
        .unwrap()
        .push((existing.id_ref().clone(), other.id_ref().clone()));
    }
  }

  fn notify_ping_complete(
    &self,
    peer_id: Self::Id,
    _peer_addr: Self::Address,
    rtt: Duration,
    payload: Bytes,
  ) -> impl Future<Output = ()> + Send + '_ {
    let pings = self.captures.pings.clone();
    async move {
      pings.lock().unwrap().push(PingObservation {
        peer: peer_id,
        rtt,
        payload,
      });
    }
  }
}

/// Build the reactor `MemberlistOptions` for `cfg`: label, initial metadata,
/// initial local push/pull state, and the compression / checksum / encryption
/// transform knobs. Shared by every reactor suite adapter so the option mapping
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
  #[cfg(compression)]
  if let Some(algorithm) = cfg.compression {
    mopts = mopts.with_compression(CompressionOptions::new().with_algorithm(algorithm));
  }
  #[cfg(checksum)]
  if let Some(algorithm) = cfg.checksum {
    mopts = mopts.with_checksum(ChecksumOptions::new().with_algorithm(algorithm));
  }
  #[cfg(encryption)]
  if let Some(key) = cfg.encryption_key {
    mopts = mopts.with_encryption(EncryptionOptions::new().with_keyring(Keyring::new(key)));
  }
  mopts
}
