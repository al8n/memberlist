use core::time::Duration;
use std::error::Error;

use super::{InitError, OpError, SocketTimeoutOutOfRange};

/// A sample resolver error for the boxed `Resolve` variants.
#[derive(Debug)]
struct SampleResolveError;

impl core::fmt::Display for SampleResolveError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("sample resolve error")
  }
}

impl std::error::Error for SampleResolveError {}

#[test]
fn init_error_variants_display_and_source() {
  let too_small = InitError::TcpPoolTooSmall(1);
  assert!(!too_small.to_string().is_empty());
  assert!(too_small.source().is_none());
  assert!(too_small.is_tcp_pool_too_small());

  let zero_ring = InitError::ZeroBridgeRing;
  assert!(!zero_ring.to_string().is_empty());
  assert!(zero_ring.source().is_none());
  assert!(zero_ring.is_zero_bridge_ring());

  let out_of_range = InitError::SocketTimeoutOutOfRange(SocketTimeoutOutOfRange {
    socket_timeout: Duration::from_secs(1),
    close_timeout: Duration::from_secs(10),
    stream_timeout: Duration::from_secs(15),
    max: Duration::from_secs(86_400),
    tick_hz: 1_000,
  });
  assert!(!out_of_range.to_string().is_empty());
  assert!(out_of_range.source().is_none());
  assert!(out_of_range.is_socket_timeout_out_of_range());

  // The `Engine` variant wraps an embedded init error and forwards its source.
  let engine: InitError = memberlist_embedded::InitError::ZeroPort.into();
  assert!(engine.is_engine());
  assert!(!engine.to_string().is_empty());
  assert!(engine.source().is_some());

  // The `Resolve` variant boxes a generic error and forwards its source.
  let resolve = InitError::Resolve(alloc::boxed::Box::new(SampleResolveError));
  assert!(resolve.is_resolve());
  assert!(!resolve.to_string().is_empty());
  assert!(resolve.source().is_some());

  let no_addrs = InitError::NoAddresses;
  assert!(no_addrs.is_no_addresses());
  assert!(!no_addrs.to_string().is_empty());
  assert!(no_addrs.source().is_none());

  let entropy = InitError::Entropy;
  assert!(entropy.is_entropy());
  assert!(!entropy.to_string().is_empty());
  assert!(entropy.source().is_none());
}

#[test]
fn op_error_variants_display_and_predicates() {
  let ping = OpError::PingTimeout;
  assert!(!ping.to_string().is_empty());
  assert!(ping.is_ping_timeout());
  assert!(ping.source().is_none());

  let send = OpError::SendFailed;
  assert!(!send.to_string().is_empty());
  assert!(send.is_send_failed());

  let not_running = OpError::NotRunning;
  assert!(!not_running.to_string().is_empty());
  assert!(not_running.is_not_running());

  // The `Resolve` variant boxes a generic error and forwards its source.
  let resolve = OpError::Resolve(alloc::boxed::Box::new(SampleResolveError));
  assert!(resolve.is_resolve());
  assert!(!resolve.to_string().is_empty());
  assert!(resolve.source().is_some());

  let no_addrs = OpError::NoAddresses;
  assert!(no_addrs.is_no_addresses());
  assert!(!no_addrs.to_string().is_empty());
  assert!(no_addrs.source().is_none());
}
