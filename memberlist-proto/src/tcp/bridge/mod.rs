//! Tests for the generic `memberlist::Stream` <-> record-layer byte-pump
//! ([`crate::streams::bridge::StreamBridge`]) wrapping the raw-passthrough
//! plain-TCP record layer ([`RawRecords`](super::records::RawRecords)).

// Symbols the `#[cfg(test)] mod tests` glob-imports via `use super::*`. Gated
// so the tests-only module has no non-test-build footprint.
#[cfg(test)]
use crate::{
  bridge_phase::{BridgeFailure, BridgePhase},
  endpoint::Endpoint,
};

#[cfg(test)]
mod tests;
