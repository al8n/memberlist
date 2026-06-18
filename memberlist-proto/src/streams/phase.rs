//! `BridgePhase` — the per-bridge phase carried by `StreamBridge`.
//!
//! Consumed by `StreamBridge`.

use crate::bridge_phase::LinkState;

/// Phase of a per-exchange `StreamBridge`.
pub(crate) enum BridgePhase {
  /// Pre-promote: the record layer is handshaking or queuing its
  /// outbound prefix. No `Stream` minted yet.
  Handshaking,
  /// Post-promote: the `Stream` exists; the bridge tracks the
  /// `LinkState` half-close lifecycle.
  Established(LinkState),
}
