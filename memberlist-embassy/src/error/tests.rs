use core::time::Duration;
use std::error::Error;

use super::{InitError, OpError, SocketTimeoutOutOfRange};

#[test]
fn init_error_variants_display_and_source() {
  let too_small = InitError::TcpPoolTooSmall(1);
  assert!(!too_small.to_string().is_empty());
  assert!(too_small.source().is_none());

  let zero_ring = InitError::ZeroBridgeRing;
  assert!(!zero_ring.to_string().is_empty());
  assert!(zero_ring.source().is_none());

  let out_of_range = InitError::SocketTimeoutOutOfRange(SocketTimeoutOutOfRange {
    socket_timeout: Duration::from_secs(1),
    close_timeout: Duration::from_secs(10),
    stream_timeout: Duration::from_secs(15),
    max: Duration::from_secs(86_400),
    tick_hz: 1_000,
  });
  assert!(!out_of_range.to_string().is_empty());
  assert!(out_of_range.source().is_none());

  // The `Engine` variant wraps an embedded init error and forwards its source.
  let engine: InitError = memberlist_embedded::InitError::ZeroPort.into();
  assert!(matches!(engine, InitError::Engine(_)));
  assert!(!engine.to_string().is_empty());
  assert!(engine.source().is_some());
}

#[test]
fn op_error_variants_display_and_eq() {
  for err in [
    OpError::PingTimeout,
    OpError::SendFailed,
    OpError::NotRunning,
  ] {
    assert!(!err.to_string().is_empty(), "Display non-empty for {err:?}");
    assert_eq!(err.clone(), err);
  }
  assert_ne!(OpError::PingTimeout, OpError::SendFailed);
}
