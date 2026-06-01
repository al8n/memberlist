//! `StreamPhase` — the per-bridge phase carried by `StreamBridge`.
//!
//! Consumed by `StreamBridge`.

use crate::bridge_phase::BridgePhase;

/// Phase of a per-exchange `StreamBridge`.
pub(crate) enum StreamPhase {
  /// Pre-promote: the record layer is handshaking or queuing its
  /// outbound prefix. No `Stream` minted yet.
  Handshaking,
  /// Post-promote: the `Stream` exists; the bridge tracks the
  /// `BridgePhase` half-close lifecycle.
  Established(BridgePhase),
}
